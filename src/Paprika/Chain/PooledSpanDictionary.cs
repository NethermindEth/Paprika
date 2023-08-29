using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Store;

namespace Paprika.Chain;

public class PooledSpanDictionary : IEqualityComparer<PooledSpanDictionary.KeySpan>, IDisposable
{
    private const int BufferSize = BufferPool.BufferSize;
    private readonly BufferPool _pool;
    private readonly Dictionary<KeySpan, ValueSpan> _dict;
    private readonly List<Page> _pages = new();

    private Page _current;
    private int _position;

    private readonly byte[] _key = new byte[1024];

    private const int InlineKeyPointer = -1;

    public PooledSpanDictionary(BufferPool pool)
    {
        _pool = pool;
        _dict = new Dictionary<KeySpan, ValueSpan>(this);

        AllocateNewPage();
    }

    public bool TryGet(ReadOnlySpan<byte> key, int hash, out ReadOnlySpan<byte> result)
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

    public void Set(ReadOnlySpan<byte> key, int hash, ReadOnlySpan<byte> data)
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
        // TODO: provide a replacement policy instead of appending in all the cases
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

    private ReadOnlySpan<byte> GetAt(KeySpan key) => GetAt(new ValueSpan(key.Pointer, key.Length));

    private ReadOnlySpan<byte> GetAt(ValueSpan value)
    {
        if (value.Pointer == InlineKeyPointer)
        {
            return _key.AsSpan(0, value.Length);
        }

        var offset = Math.DivRem(value.Pointer, BufferSize, out var pageNo);
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
        var position = _position + _pages.Count * BufferSize;
        _position += toWrite.Length;
        return position;
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

        [FieldOffset(0)]
        public readonly ushort ShortHash;

        [FieldOffset(2)]
        public readonly ushort Length;

        [FieldOffset(0)]
        public readonly int Hash;

        [FieldOffset(4)]
        public readonly int Pointer;
    }

    public readonly struct ValueSpan
    {
        public readonly int Pointer;
        public readonly ushort Length;

        public ValueSpan(int pointer, ushort length)
        {
            Pointer = pointer;
            Length = length;
        }
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
    }
}