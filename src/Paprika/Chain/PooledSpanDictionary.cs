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
    private readonly Dictionary<KeySpan, ValueSpan> _dict;
    private readonly List<Page> _pages = new();

    private Page _current;
    private int _position;

    private static readonly int KeyBytesCount =
        Key.StorageCell(NibblePath.FromKey(Keccak.EmptyTreeHash), Keccak.EmptyTreeHash).MaxByteLength;

    private readonly byte[] _key = new byte[KeyBytesCount];

    private const int InlineKeyPointer = -1;

    public PooledSpanDictionary(BufferPool pool)
    {
        _pool = pool;
        _dict = new Dictionary<KeySpan, ValueSpan>(this);

        AllocateNewPage();
    }

    public bool TryGet(scoped ReadOnlySpan<byte> key, int hash, out ReadOnlySpan<byte> result)
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

    public void Set(scoped ReadOnlySpan<byte> key, int hash, ReadOnlySpan<byte> data)
    {
        var mixed = Mix(hash);

        var tempKey = BuildKeyTemp(key, mixed);
        ref var refValue = ref CollectionsMarshal.GetValueRefOrNullRef(_dict, tempKey);

        if (Unsafe.IsNullRef(ref refValue))
        {
            // key, does not exist
            _dict.Add(BuildKey(key, mixed), BuildValue(data));
            return;
        }

        // key does exist, write value
        if (refValue.Length == data.Length)
        {
            // data lengths are equal, write in-situ
            data.CopyTo(GetAt(refValue));
            return;
        }

        refValue = BuildValue(data);
    }

    public Enumerator GetEnumerator() => new(this);

    public ref struct Enumerator
    {
        private Dictionary<KeySpan, ValueSpan>.Enumerator _enumerator;
        private readonly PooledSpanDictionary _dictionary;

        public Enumerator(PooledSpanDictionary dictionary)
        {
            _enumerator = dictionary._dict.GetEnumerator();
            _dictionary = dictionary;
        }

        public bool MoveNext() => _enumerator.MoveNext();

        public KeyValue Current
        {
            get
            {
                var (key, value) = _enumerator.Current;
                return new KeyValue(_dictionary.GetAt(key), _dictionary.GetAt(value));
            }
        }

        public void Dispose() => _enumerator.Dispose();

        public readonly ref struct KeyValue
        {
            public ReadOnlySpan<byte> Key { get; }
            public ReadOnlySpan<byte> Value { get; }

            public KeyValue(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
            {
                Key = key;
                Value = value;
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
        key.CopyTo(_key);
        return new KeySpan(hash, InlineKeyPointer, (ushort)key.Length);
    }

    private KeySpan BuildKey(ReadOnlySpan<byte> key, ushort hash) => new(hash, Write(key), (ushort)key.Length);

    private ValueSpan BuildValue(ReadOnlySpan<byte> value) => new(Write(value), (ushort)value.Length);

    private static ushort Mix(int hash) => unchecked((ushort)((hash >> 16) ^ hash));

    private ReadOnlySpan<byte> GetAt(KeySpan key)
    {
        return key.Pointer == InlineKeyPointer
            ? _key.AsSpan(0, key.Length)
            : GetAt(new ValueSpan(key.Pointer, key.Length));
    }

    private Span<byte> GetAt(ValueSpan value)
    {
        var pageNo = Math.DivRem(value.Pointer, BufferSize, out var offset);
        return _pages[pageNo].Span.Slice(offset, value.Length);
    }

    private int Write(ReadOnlySpan<byte> toWrite)
    {
        if (BufferSize - _position < toWrite.Length)
        {
            // not enough memory
            AllocateNewPage();
        }

        toWrite.CopyTo(_current.Span.Slice(_position));
        var pointer = _position + (_pages.Count - 1) * BufferSize;
        _position += toWrite.Length;
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
        public readonly int Pointer;
        public readonly ushort Length;

        public ValueSpan(int pointer, ushort length)
        {
            Pointer = pointer;
            Length = length;
        }

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
    }

    public override string ToString() => $"Count: {_dict.Count}, Memory: {_pages.Count * BufferSize / 1024}kb";
}