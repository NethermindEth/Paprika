using System.CodeDom.Compiler;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
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

    private byte[] KeyBuffer =>
        _allowConcurrentReaders ? Unsafe.As<ThreadLocal<byte[]>>(_key).Value! : Unsafe.As<byte[]>(_key);

    private const int InlineKeyPointer = -1;
    private const int ValueDestroyedPointer = -2;

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

        AllocateNewPage();
    }

    public bool TryGet(scoped ReadOnlySpan<byte> key, ulong hash, out ReadOnlySpan<byte> result)
    {
        var mixed = Mix(hash);
        if (_dict.TryGetValue(BuildKeyTemp(key, mixed), out var value))
        {
            result = GetAt(value);
            return true;
        }

        result = default;
        return false;
    }

    public bool TryGet(scoped ReadOnlySpan<byte> key, ushort shortHash, out ReadOnlySpan<byte> result)
    {
        if (_dict.TryGetValue(BuildKeyTemp(key, shortHash), out var value))
        {
            result = GetAt(value);
            return true;
        }

        result = default;
        return false;
    }

    public bool Contains(scoped ReadOnlySpan<byte> key, ushort shortHash)
    {
        return _dict.ContainsKey(BuildKeyTemp(key, shortHash));
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
        entry = new ValueSpan(ValueDestroyedPointer, 0, ValueSpan.DefaultMetadata);
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
        var page = _pool.Rent(false);
        _pages.Add(page);
        _current = page;
        _position = 0;
    }

    private KeySpan BuildKeyTemp(ReadOnlySpan<byte> key, ushort hash)
    {
        key.CopyTo(KeyBuffer);
        return new KeySpan(hash, InlineKeyPointer, (ushort)key.Length);
    }

    private KeySpan BuildKey(ReadOnlySpan<byte> key, ushort hash) => new(hash, Write(key), (ushort)key.Length);

    private ValueSpan BuildValue(ReadOnlySpan<byte> value0, ReadOnlySpan<byte> value1, byte metadata) =>
        new(Write(value0, value1), (ushort)(value0.Length + value1.Length), metadata);

    private static ushort Mix(ulong hash)
    {
        unchecked
        {
            var mixed = (uint)((hash >> 32) ^ hash);
            return (ushort)((mixed >> 16) ^ mixed);
        }
    }

    private ReadOnlySpan<byte> GetAt(KeySpan key)
    {
        return key.Pointer == InlineKeyPointer
            ? KeyBuffer.AsSpan(0, key.Length)
            : GetAt(new ValueSpan(key.Pointer, key.Length, default));
    }

    private Span<byte> GetAt(ValueSpan value)
    {
        if (value.Pointer == ValueDestroyedPointer)
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

        public readonly int Pointer;
        public readonly ushort Length;
        public readonly byte Metadata;

        public ValueSpan(int pointer, ushort length, byte metadata)
        {
            Pointer = pointer;
            Length = length;
            Metadata = metadata;
        }

        public bool IsDestroyed => Pointer == ValueDestroyedPointer;

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
                case DataType.CompressedAccount:
                    throw new Exception("Should not use compressed accounts");
                default:
                    throw new ArgumentOutOfRangeException($"The type {key.Type} is not handled");
            }
        }

        return;

        static string S(in NibblePath full) => full.UnsafeAsKeccak.ToString();
    }
}