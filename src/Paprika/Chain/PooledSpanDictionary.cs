using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Chain;

/// <summary>
/// The pooled span dictionary uses memory from <see cref="BufferPool"/> to store large chunks of memory with minimal or no allocations.
/// </summary>
public class PooledSpanDictionary : IDisposable
{
    private const int BufferSize = BufferPool.BufferSize;
    
    private readonly BufferPool _pool;
    private readonly bool _preserveOldValues;
    private readonly List<Page> _pages = new();

    private Page _current;
    private int _position;

    private readonly Root _root;

    /// <summary>
    /// Initializes a new pooled dictionary instance.
    /// </summary>
    /// <param name="pool">The pool to take data from and return to.</param>
    /// <param name="preserveOldValues">Whether dictionary should preserve previous values or aggressively reuse memory when possible.</param>
    /// <remarks>
    /// Set <paramref name="preserveOldValues"/> to true, if the data written once should not be overwritten.
    /// This allows to hold values returned by the dictionary through multiple operations.
    /// 
    /// This dictionary uses <see cref="ThreadLocal{T}"/> to store keys buffers to allow concurrent readers
    /// </remarks>
    public PooledSpanDictionary(BufferPool pool, bool preserveOldValues = false)
    {
        _pool = pool;
        _preserveOldValues = preserveOldValues;
        _root = new Root(RentNewPage(true));

        AllocateNewPage();
    }

    public bool TryGet(scoped ReadOnlySpan<byte> key, ulong hash, out ReadOnlySpan<byte> result)
    {
        var search = TryGetImpl(key, hash);
        if (search.IsFound)
        {
            result = search.Data;
            return true;
        }

        result = default;
        return false;
    }

    // Preamble
    private const byte PreambleBits = 0b1100_0000;
    private const byte DestroyedBit = 0b1000_0000;
    private const byte MetadataBit = 0b0100_0000;
    
    private const byte Byte0Mask = 0b0011_1111;

    // How many bytes are used for preamble + hash leftover
    private const int PreambleLength = 3;

    private const int AddressLength = 4;
    
    private const int KeyLengthLength = 1;
    private const int ValueLengthLength = 2;
    
    
    private SearchResult TryGetImpl(scoped ReadOnlySpan<byte> key, ulong hash)
    {
        var mixed = Mix(hash);
        var (leftover, bucket) = Math.DivRem(mixed, Root.BucketCount);

        Debug.Assert(BitOperations.LeadingZeroCount(leftover) >= 10, "First 10 bits should be left unused");
        
        var address = _root.Buckets[(int)bucket];
        while (address != 0)
        {
            var (pageNo, atPage) = Math.DivRem(address, Page.PageSize);

            // TODO: optimize, unsafe ref?
            var sliced = _pages[(int)pageNo].Span.Slice((int)atPage);

            ref var at = ref sliced[0];
            
            var header = sliced[0] & PreambleBits;
            if ((header & DestroyedBit) != DestroyedBit)
            {
                // not destroyed, ready to be searched, decode leftover, big endian
                var leftoverStored = ((sliced[0] & Byte0Mask) << 16) +
                                     (sliced[1] << 8) +
                                     sliced[2];
                
                if (leftoverStored == leftover)
                {
                    ref var payload = ref Unsafe.Add(ref at, PreambleLength + AddressLength);
                    
                    var storedKeyLength = payload;
                    if (storedKeyLength == key.Length)
                    {
                        var storedKey = MemoryMarshal.CreateSpan(ref Unsafe.Add(ref payload, KeyLengthLength), storedKeyLength);
                        if (storedKey.SequenceEqual(key))
                        {
                            ref var data = ref Unsafe.Add(ref payload, KeyLengthLength + storedKeyLength);
                            return new SearchResult(ref at, ref data);
                        }
                    }
                }
            }
            
            // Decode next entry address
            address = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref at, PreambleLength));
        }

        return default;
    }

    private readonly ref struct SearchResult
    {
        public bool IsFound => !Unsafe.IsNullRef(ref _header);

        private readonly ref byte _header;
        private readonly ref byte _data;

        public SearchResult(ref byte header, ref byte data)
        {
            _data = ref data;
            _header = ref header;
        }

        /// <summary>
        /// The length is encoded as the first byte.
        /// </summary>
        public ReadOnlySpan<byte> Data =>
            MemoryMarshal.CreateSpan(ref Unsafe.Add(ref _data, ValueLengthLength),
                Unsafe.ReadUnaligned<ushort>(ref _data));

        public bool TryUpdateInSitu(ReadOnlySpan<byte> data0, ReadOnlySpan<byte> data1)
        {
            if (IsFound == false)
                return false;

            var length = data0.Length + data1.Length;
            if (Unsafe.ReadUnaligned<ushort>(ref _data) >= length)
            {
                // There's the place to have it update in place
                Unsafe.WriteUnaligned(ref _data, (ushort)length);
                
                var destination = MemoryMarshal.CreateSpan(ref Unsafe.Add(ref _data, ValueLengthLength), length);
                data0.CopyTo(destination);
                if (data1.IsEmpty == false)
                {
                    data1.CopyTo(destination[data0.Length..]);
                }

                return true;
            }

            return false;
        }

        public void Destroy()
        {
            Debug.Assert(IsFound, "Only found can be destroyed");
            _header |= DestroyedBit;
        }
    } 

    public void Set(scoped ReadOnlySpan<byte> key, ulong hash, ReadOnlySpan<byte> data, byte metadata) =>
        Set(key, hash, data, ReadOnlySpan<byte>.Empty, metadata);


    public void Set(scoped ReadOnlySpan<byte> key, ulong hash, ReadOnlySpan<byte> data0, ReadOnlySpan<byte> data1, byte metadata)
    {
        var search = _preserveOldValues == false ? TryGetImpl(key, hash) : default;

        if (search.TryUpdateInSitu(data0, data1))
        {
            // TODO: metadata!
            return;
        }
        
        var mixed = Mix(hash);
        var (leftover, bucket) = Math.DivRem(mixed, Root.BucketCount);

        Debug.Assert(BitOperations.LeadingZeroCount(leftover) >= 10, "First 10 bits should be left unused");
        
        var root = _root.Buckets[(int)bucket];

        var dataLength = data1.Length + data0.Length;
        
        var size = PreambleLength + AddressLength + KeyLengthLength + key.Length + ValueLengthLength + dataLength;
        Span<byte> destination = Write(size, out var address);

        // Write preamble, big endian
        destination[0] = (byte)(leftover >> 16);
        destination[1] = (byte)(leftover >> 8);
        destination[2] = (byte)(leftover & 0xFF);

        // Write next
        Unsafe.WriteUnaligned(ref destination[PreambleLength], root);
        
        // Key length
        const int keyStart = PreambleLength + AddressLength; 
        destination[keyStart] = (byte)key.Length;
        
        // Key
        key.CopyTo(destination.Slice(keyStart + KeyLengthLength));
        
        // Value length
        var valueStart = keyStart + KeyLengthLength + key.Length;

        Unsafe.WriteUnaligned(ref destination[valueStart], (ushort)dataLength);
        
        data0.CopyTo(destination[(valueStart + ValueLengthLength)..]);
        data1.CopyTo(destination[(valueStart + ValueLengthLength + data0.Length)..]);

        _root.Buckets[(int)bucket] = address;
    }

    public void Destroy(scoped ReadOnlySpan<byte> key, ulong hash)
    {
        var found = TryGetImpl(key, hash);
        
        if (found.IsFound)
            found.Destroy();
    }

    public Enumerator GetEnumerator() => new(this);

    /// <summary>
    /// Enumerator walks through all the values beside the ones that were destroyed in this dictionary
    /// with <see cref="PooledSpanDictionary.Destroy"/>.
    /// </summary>
    public ref struct Enumerator
    {
        private readonly PooledSpanDictionary _dictionary;

        public Enumerator(PooledSpanDictionary dictionary)
        {
            _dictionary = dictionary;
        }

        public bool MoveNext()
        {
            return false;
            // bool moved;
            // do
            // {
            //     moved = _enumerator.MoveNext();
            // } while (moved && _enumerator.Current.Value.IsDestroyed);
            //
            // return moved;
        }

        public KeyValue Current
        {
            get
            {
                //var (key, value) = _enumerator.Current;
                return default;
            }
        }

        public void Dispose()
        {
            // throw new ex
        }

        public readonly ref struct KeyValue
        {
            public ReadOnlySpan<byte> Key { get; }
            public ReadOnlySpan<byte> Value { get; }

            public ushort ShortHash { get; }

            public byte Metadata { get; }

            public KeyValue(ReadOnlySpan<byte> key, ushort shortHash, ReadOnlySpan<byte> value, byte metadata)
            {
                Key = key;
                ShortHash = shortHash;
                Value = value;
                Metadata = metadata;
            }
        }
    }

    private void AllocateNewPage()
    {
        var page = RentNewPage(false);
        _current = page;
        _position = 0;
    }

    private Page RentNewPage(bool clear)
    {
        var page = _pool.Rent(clear);
        _pages.Add(page);
        return page;
    }


    private static uint Mix(ulong hash) => unchecked((uint)((hash >> 32) ^ hash));


    private Span<byte> Write(int size, out uint addr)
    {
        if (BufferSize - _position < size)
        {
            // not enough memory
            AllocateNewPage();
        }

        // allocated before the position is changed
        var span = _current.Span.Slice(_position, size);
        
        addr = (uint)(_position + (_pages.Count - 1) * BufferSize);
        _position += size;

        return span;
    }

    public void Dispose()
    {
        foreach (var page in _pages)
        {
            _pool.Return(page);
        }

        _pages.Clear();
    }

    public override string ToString() => $"Memory: {_pages.Count * BufferSize / 1024}kb";

    public void Describe(TextWriter text, Key.Predicate? predicate = null)
    {
        predicate ??= (in Key _) => true;

        foreach (var kvp in this)
        {
            Key.ReadFrom(kvp.Key, out var key);

            if (predicate(key) == false)
                continue;

            switch (key.Type)
            {
                case DataType.Account:
                    Account.ReadFrom(kvp.Value, out Account account);
                    text.WriteLine($"Account [{S(key.Path)}] -> {account.ToString()}");
                    break;
                case DataType.StorageCell:
                    text.WriteLine(
                        $"Storage [{S(key.Path)}, {S(key.StoragePath)}] -> {kvp.Value.ToHexString(true)}");
                    break;
                case DataType.Merkle:
                    if (key.StoragePath.Length <= 0)
                    {
                        text.WriteLine($"Merkle, State [{(key.Path.ToString())}] (updated)");
                    }
                    else
                        text.WriteLine($"Merkle, Storage [{S(key.Path)}, {key.StoragePath.ToString()}] (updated)");

                    break;
                default:
                    throw new ArgumentOutOfRangeException($"The type {key.Type} is not handled");
            }
        }

        return;

        static string S(in NibblePath full) => full.UnsafeAsKeccak.ToString();
    }

    private readonly struct Root(Page page)
    {
        public const int BucketCount = Page.PageSize / sizeof(uint);
        public const int BucketMask = BucketCount - 1;
        
        public Span<uint> Buckets => MemoryMarshal.Cast<byte, uint>(page.Span);
    }
}