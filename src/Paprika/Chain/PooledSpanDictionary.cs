using System.Data;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Chain;

/// <summary>
/// The pooled span dictionary uses memory from <see cref="BufferPool"/> to store large chunks of memory.
/// </summary>
/// <remarks>
/// This component hacks into the dictionary to provide the underlying keys by implementing the custom equality comparer.
/// It allows to store in the underlying dict just simple entries and not pass the reference in there. To get the reference,
/// the <see cref="Equals"/> gets the underlying <see cref="Span{Byte}"/> from pages;
/// </remarks>
public class PooledSpanDictionary : IEqualityComparer<PooledSpanDictionary.KeySpan>, IDisposable
{
    private const int BufferSize = BufferPool.BufferSize;
    private readonly BufferPool _pool;
    private readonly bool _preserveOldValues;
    private readonly bool _allowConcurrentReaders;
    private readonly Dictionary<KeySpan, ValueSpan> _dict;
    private readonly List<Page> _pages = new();

    private Page _current;
    private int _position;

    private static readonly int KeyBytesCount =
        Key.StorageCell(NibblePath.FromKey(Keccak.EmptyTreeHash), Keccak.EmptyTreeHash).MaxByteLength;

    private readonly object _key;

    private static readonly ThreadLocal<byte[]> ConcurrentBuffers = new(() => new byte[KeyBytesCount]);
    private readonly Root _root;

    private byte[] KeyBuffer =>
        _allowConcurrentReaders ? Unsafe.As<ThreadLocal<byte[]>>(_key).Value! : Unsafe.As<byte[]>(_key);

    private const int InlineKeyPointer = -1;
    private const short ValueDestroyedLength = short.MinValue;

    /// <summary>
    /// Initializes a new pooled dictionary instance.
    /// </summary>
    /// <param name="pool">The pool to take data from and return to.</param>
    /// <param name="preserveOldValues">Whether dictionary should preserve previous values or aggressively reuse memory when possible.</param>
    /// <param name="allowConcurrentReaders">Whether concurrent readers should be allowed.</param>
    /// <remarks>
    /// Set <paramref name="preserveOldValues"/> to true, if the data written once should not be overwritten.
    /// This allows to hold values returned by the dictionary through multiple operations.
    ///
    /// This dictionary uses <see cref="ThreadLocal{T}"/> to store keys buffers to allow concurrent readers
    /// </remarks>
    public PooledSpanDictionary(BufferPool pool, bool preserveOldValues = false, bool allowConcurrentReaders = false)
    {
        _pool = pool;
        _preserveOldValues = preserveOldValues;
        _allowConcurrentReaders = allowConcurrentReaders;
        _dict = new Dictionary<KeySpan, ValueSpan>(this);

        _key = allowConcurrentReaders ? ConcurrentBuffers : new byte[KeyBytesCount];

        _root = new Root(RentNewPage(true));

        AllocateNewPage();
    }

    public bool TryGet(scoped ReadOnlySpan<byte> key, ulong hash, out ReadOnlySpan<byte> result)
    {
        const byte preambleBits = 0b1100_0000;
        const byte byte0 = 0b0011_1111;
        const byte destroyed = 0b1000_0000;
        const byte hasNext = 0b0100_0000;
        
        var mixed = Mix(hash);

        var (leftover, bucket) = Math.DivRem(mixed, Root.BucketCount);

        Debug.Assert(BitOperations.LeadingZeroCount(leftover) >= 10, "First 10 bits should be left unused");
        
        var address = _root.Buckets[(int)bucket];
        while (address != 0)
        {
            var (pageNo, atPage) = Math.DivRem(address, Page.PageSize);

            // TODO: optimize access, more raw
            ref var at = ref MemoryMarshal.GetReference(_pages[(int)pageNo].Span.Slice((int)atPage));

            var header = at & preambleBits;
            if ((header & destroyed) != destroyed)
            {
                // not destroyed, ready to be searched
                var leftoverStored = (at & byte0) + (Unsafe.Add(ref at, 1) << 8) + (Unsafe.Add(ref at, 2) << 16);
                if (leftoverStored == leftover)
                {
                    throw new Exception("Match! Try to search");
                }
            }

            // Check if has next entry linked, if not return
            if ((header & hasNext) != hasNext)
            {
                break;
            }
            
            // Has next entry, decode it
        }
        
        result = default;
        return false;
    }

    public void Set(scoped ReadOnlySpan<byte> key, ulong hash, ReadOnlySpan<byte> data, byte metadata) =>
        Set(key, hash, data, ReadOnlySpan<byte>.Empty, metadata);


    public void Set(scoped ReadOnlySpan<byte> key, ulong hash, ReadOnlySpan<byte> data0, ReadOnlySpan<byte> data1, byte metadata)
    {
        var mixed = Mix(hash);

        var tempKey = BuildKeyTemp(key, mixed);
        ref var refValue = ref CollectionsMarshal.GetValueRefOrNullRef(_dict, tempKey);

        if (Unsafe.IsNullRef(ref refValue))
        {
            // key, does not exist
            _dict.Add(BuildKey(key, mixed), BuildValue(data0, data1, metadata));
            return;
        }

        if (!_preserveOldValues)
        {
            var size = data0.Length + data1.Length;

            // if old values does not need to be preserved, try to reuse memory
            if (refValue.Length == size)
            {
                // data lengths are equal, write in-situ
                var span = GetAt(refValue);

                data0.CopyTo(span);
                data1.CopyTo(span.Slice(data1.Length));

                if (metadata != refValue.Metadata)
                {
                    // requires update
                    refValue = new ValueSpan(refValue.Pointer, refValue.Length, metadata);
                }

                return;
            }
        }

        refValue = BuildValue(data0, data1, metadata);
    }

    public void Remove(ReadOnlySpan<byte> key, ulong hash)
    {
        if (_dict.Count == 0)
            return;

        var mixed = Mix(hash);
        var tempKey = BuildKeyTemp(key, mixed);

        _dict.Remove(tempKey);
    }

    public void Destroy(scoped ReadOnlySpan<byte> key, ulong hash)
    {
        var mixed = Mix(hash);
        var tempKey = BuildKeyTemp(key, mixed);

        ref var entry = ref CollectionsMarshal.GetValueRefOrNullRef(_dict, tempKey);
        Debug.Assert(Unsafe.IsNullRef(ref entry) == false, "Can be used only to clean the existing entries");

        // empty value span produces an empty span
        entry = new ValueSpan(0, ValueDestroyedLength, ValueSpan.DefaultMetadata);
    }

    public Enumerator GetEnumerator() => new(this);

    /// <summary>
    /// Enumerator walks through all the values beside the ones that were destroyed in this dictionary
    /// with <see cref="PooledSpanDictionary.Destroy"/>.
    /// </summary>
    public ref struct Enumerator
    {
        private Dictionary<KeySpan, ValueSpan>.Enumerator _enumerator;
        private readonly PooledSpanDictionary _dictionary;

        public Enumerator(PooledSpanDictionary dictionary)
        {
            _enumerator = dictionary._dict.GetEnumerator();
            _dictionary = dictionary;
        }

        public bool MoveNext()
        {
            bool moved;
            do
            {
                moved = _enumerator.MoveNext();
            } while (moved && _enumerator.Current.Value.IsDestroyed);

            return moved;
        }

        public KeyValue Current
        {
            get
            {
                var (key, value) = _enumerator.Current;
                return new KeyValue(_dictionary.GetAt(key), key.ShortHash, _dictionary.GetAt(value), value.Metadata);
            }
        }

        public void Dispose() => _enumerator.Dispose();

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

    private KeySpan BuildKeyTemp(ReadOnlySpan<byte> key, ushort hash)
    {
        key.CopyTo(KeyBuffer);
        return new KeySpan(hash, InlineKeyPointer, (ushort)key.Length);
    }

    private KeySpan BuildKey(ReadOnlySpan<byte> key, ushort hash) => new(hash, Write(key), (ushort)key.Length);

    private ValueSpan BuildValue(ReadOnlySpan<byte> value0, ReadOnlySpan<byte> value1, byte metadata) =>
        new(Write(value0, value1), (short)(value0.Length + value1.Length), metadata);

    private static uint Mix(ulong hash) => unchecked((uint)((hash >> 32) ^ hash));

    private ReadOnlySpan<byte> GetAt(KeySpan key)
    {
        return key.Pointer == InlineKeyPointer
            ? KeyBuffer.AsSpan(0, key.Length)
            : GetAt(new ValueSpan(key.Pointer, (short)key.Length, default));
    }

    private Span<byte> GetAt(ValueSpan value)
    {
        if (value.IsDestroyed || value.Length == 0)
        {
            return Span<byte>.Empty;
        }

        var pageNo = Math.DivRem(value.Pointer, BufferSize, out var offset);
        return _pages[pageNo].Span.Slice(offset, value.Length);
    }

    private int Write(ReadOnlySpan<byte> data)
    {
        var size = data.Length;

        if (BufferSize - _position < size)
        {
            // not enough memory
            AllocateNewPage();
        }

        data.CopyTo(_current.Span.Slice(_position));
        var pointer = _position + (_pages.Count - 1) * BufferSize;
        _position += size;
        return pointer;
    }

    private int Write(ReadOnlySpan<byte> data, ReadOnlySpan<byte> data1)
    {
        var size = data.Length + data1.Length;

        if (BufferSize - _position < size)
        {
            // not enough memory
            AllocateNewPage();
        }

        data.CopyTo(_current.Span.Slice(_position));
        data1.CopyTo(_current.Span.Slice(_position + data.Length));
        var pointer = _position + (_pages.Count - 1) * BufferSize;
        _position += size;
        return pointer;
    }

    /// <summary>
    /// Key, packed to 8 bytes.
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public readonly struct KeySpan
    {
        public KeySpan(ushort shortHash, int pointer, ushort length)
        {
            ShortHash = shortHash;
            Length = length;
            Pointer = pointer;
        }

        [FieldOffset(0)] public readonly ushort ShortHash;

        [FieldOffset(2)] public readonly ushort Length;

        /// <summary>
        /// Starts at 0, combining the short hash and the length to make hash more unique.
        /// </summary>
        [FieldOffset(0)] public readonly int Hash;

        [FieldOffset(4)] public readonly int Pointer;

        public override string ToString() =>
            $"{nameof(Length)}: {Length}, {nameof(Hash)}: {Hash}, {nameof(Pointer)}: {Pointer}";
    }

    private readonly struct ValueSpan
    {
        public const byte DefaultMetadata = 0;

        private readonly int _raw;
        public readonly short Length;

        private const int MetadataBit = 1;
        private const int MetadataShift = 31;

        public ValueSpan(int pointer, short length, byte metadata)
        {
            Debug.Assert(metadata <= MetadataBit, $"Metadata should be less or equal to {MetadataBit}");

            _raw = pointer | (metadata << MetadataShift);
            Length = length;
        }

        public int Pointer => _raw & ~(MetadataBit << MetadataShift);
        public byte Metadata => (byte)((_raw >> MetadataShift) & MetadataBit);

        public bool IsDestroyed => Length == ValueDestroyedLength;

        public override string ToString() => $"{nameof(Pointer)}: {Pointer}, {nameof(Length)}: {Length}";
    }

    bool IEqualityComparer<KeySpan>.Equals(KeySpan x, KeySpan y)
    {
        if (x.Hash != y.Hash)
            return false;

        return GetAt(x).SequenceEqual(GetAt(y));
    }

    int IEqualityComparer<KeySpan>.GetHashCode(KeySpan obj) => obj.Hash;

    public void Dispose()
    {
        foreach (var page in _pages)
        {
            _pool.Return(page);
        }

        _pages.Clear();
        _dict.Clear();
    }

    public override string ToString() => $"Count: {_dict.Count}, Memory: {_pages.Count * BufferSize / 1024}kb";

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