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
    private int _currentNumber;
    private int _position;

    private Page _entries;
    private int _entryPage;
    private int _entryPosition = Page.PageSize;

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

        Span<Page> pages = stackalloc Page[Root.PageCount];
        for (var i = 0; i < Root.PageCount; i++)
        {
            pages[i] = RentNewPage(true);
        }

        _root = new Root(pages);

        AllocateNewPage();
    }

    public bool TryGet(scoped ReadOnlySpan<byte> key, ulong hash, out ReadOnlySpan<byte> result)
    {
        ref var entry = ref TryGetImpl(hash, key);

        if (Entry.Exists(in entry))
        {
            ref var pages = ref MemoryMarshal.GetReference(CollectionsMarshal.AsSpan(_pages));
            result = GetData(entry, in pages);
            return true;
        }

        result = default;
        return false;
    }

    /// <summary>
    /// The total overhead to write one item.
    /// </summary>
    public const int ItemOverhead = PreambleLength + KeyLengthLength + ValueLengthLength;

    // Preamble
    private const byte DestroyedBit = 0b1000_0000;
    private const byte MetadataBits = 0b0111_1111;

    // How many bytes are used for preamble + hash leftover
    private const int PreambleLength = 1;
    private const int KeyLengthLength = 1;
    private const int ValueLengthLength = 2;

    private ref Entry TryGetImpl(ulong hash, scoped ReadOnlySpan<byte> key)
    {
        var next = _root[hash];
        ref var pages = ref MemoryMarshal.GetReference(CollectionsMarshal.AsSpan(_pages));

        while (next != default)
        {
            ref var entry = ref GetEntry(next, ref pages);
            if (entry.hash == hash)
            {
                ref var preamble = ref GetPreamble(entry, in pages);
                if ((preamble & DestroyedBit) != DestroyedBit)
                {
                    var actual = GetKey(ref preamble);
                    if (actual.SequenceEqual(key))
                    {
                        return ref entry;
                    }
                }
            }

            next = entry.next;
        }

        return ref Unsafe.NullRef<Entry>();
    }

    private static unsafe ref Entry GetEntry(int entry, ref Page pages)
    {
        var (page, at) = Math.DivRem(entry, Entry.EntriesPerPage);
        var raw = Unsafe.Add(ref pages, page).Raw;
        return ref Unsafe.AsRef<Entry>((byte*)raw.ToPointer() + at);
    }

    private static ref byte GetPreamble(in Entry entry, in Page pages) => ref GetEntryPayload(entry, in pages);

    private static ReadOnlySpan<byte> GetKey(in Entry entry, in Page pages)
    {
        ref var payload = ref GetEntryPayload(entry, in pages);
        var length = Unsafe.Add(ref payload, PreambleLength);
        return MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref payload, PreambleLength + KeyLengthLength), length);
    }

    private static ReadOnlySpan<byte> GetKey(ref byte preamble)
    {
        var length = Unsafe.Add(ref preamble, PreambleLength);
        return MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref preamble, PreambleLength + KeyLengthLength), length);
    }

    private static ReadOnlySpan<byte> GetData(in Entry entry, in Page pages)
    {
        ref var payload = ref GetEntryPayload(entry, in pages);
        var keyLength = Unsafe.Add(ref payload, PreambleLength);

        var dataOffset = PreambleLength + KeyLengthLength + keyLength;
        ref var dataStart = ref Unsafe.Add(ref payload, dataOffset);

        var dataLength = Unsafe.ReadUnaligned<ushort>(ref dataStart);

        return MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref dataStart, ValueLengthLength), dataLength);
    }

    private static unsafe ref byte GetEntryPayload(in Entry entry, in Page pages)
    {
        var (page, at) = Math.DivRem(entry.ptr, Page.PageSize);
        var raw = (byte*)Unsafe.Add(ref Unsafe.AsRef(in pages), page).Raw.ToPointer();
        return ref Unsafe.AsRef<byte>(raw + at);
    }

    public void CopyTo(PooledSpanDictionary destination, Predicate<byte> metadataWhere, bool append = false)
    {
        foreach (var kvp in this)
        {
            if (metadataWhere(kvp.Metadata))
            {
                destination.SetImpl(kvp.Key, kvp.Hash, kvp.Value, ReadOnlySpan<byte>.Empty, kvp.Metadata, append);
            }
        }
    }

    public void Set(scoped ReadOnlySpan<byte> key, ulong hash, ReadOnlySpan<byte> data, byte metadata) =>
        SetImpl(key, hash, data, ReadOnlySpan<byte>.Empty, metadata);

    public void Set(scoped ReadOnlySpan<byte> key, ulong hash, ReadOnlySpan<byte> data0, ReadOnlySpan<byte> data1,
        byte metadata) => SetImpl(key, hash, data0, data1, metadata);

    private void SetImpl(scoped ReadOnlySpan<byte> key, ulong hash, ReadOnlySpan<byte> data0, ReadOnlySpan<byte> data1,
        byte metadata, bool append = false)
    {
        if (append == false)
        {
            ref var pages = ref MemoryMarshal.GetReference(CollectionsMarshal.AsSpan(_pages));
            ref var search = ref TryGetImpl(hash, key);

            if (Entry.Exists(search))
            {
                if (_preserveOldValues == false)
                {
                    if (TryUpdateInSitu(ref search, ref pages, data0, data1, metadata))
                    {
                        return;
                    }
                }

                // Destroy the search as it should not be visible later and move on with inserting as usual
                Destroy(search, pages);
            }
        }

        // Prepare destination first
        var dataLength = data1.Length + data0.Length;
        var size = PreambleLength + KeyLengthLength + key.Length + ValueLengthLength + dataLength;
        var destination = Write(size, out var address);

        // Pick up the root that will be updated soon
        ref var root = ref _root[hash];
        ref var entry = ref NewEntry(out var addr);

        entry = new Entry
        {
            next = root,
            hash = hash,
            ptr = address
        };

        // First write entry above, then write root back to point correctly
        root = addr;

        // Write preamble, big endian
        destination[0] = metadata;
        destination[PreambleLength] = (byte)key.Length;

        // Key
        key.CopyTo(destination.Slice(PreambleLength + KeyLengthLength));

        // Value length
        var valueStart = PreambleLength + KeyLengthLength + key.Length;

        Unsafe.WriteUnaligned(ref destination[valueStart], (ushort)dataLength);

        data0.CopyTo(destination[(valueStart + ValueLengthLength)..]);
        data1.CopyTo(destination[(valueStart + ValueLengthLength + data0.Length)..]);
    }

    private bool TryUpdateInSitu(ref Entry search, ref Page pages, ReadOnlySpan<byte> data0, ReadOnlySpan<byte> data1,
        byte metadata)
    {
        return false;
    }

    public void Destroy(scoped ReadOnlySpan<byte> key, ulong hash)
    {
        ref var entry = ref TryGetImpl(hash, key);
        ref var pages = ref MemoryMarshal.GetReference(CollectionsMarshal.AsSpan(_pages));

        if (Entry.Exists(in entry))
        {
            Destroy(entry, pages);
        }
    }

    private static void Destroy(in Entry entry, in Page pages)
    {
        ref var preamble = ref GetPreamble(entry, in pages);
        preamble = (byte)(preamble | DestroyedBit);
    }

    public Enumerator GetEnumerator() => new(this);

    /// <summary>
    /// Enumerator walks through all the values beside the ones that were destroyed in this dictionary
    /// with <see cref="PooledSpanDictionary.Destroy"/>.
    /// </summary>
    public ref struct Enumerator
    {
        private readonly ref Page _pages;
        private ref Entry _entry;

        private int _next = 0;
        private Root.Enumerator _rootEnumerator;

        /// <summary>
        /// Enumerator walks through all the values beside the ones that were destroyed in this dictionary
        /// with <see cref="PooledSpanDictionary.Destroy"/>.
        /// </summary>
        public Enumerator(PooledSpanDictionary dictionary)
        {
            _pages = ref MemoryMarshal.GetReference(CollectionsMarshal.AsSpan(dictionary._pages));
            _rootEnumerator = dictionary._root.GetEnumerator();
            if (_rootEnumerator.MoveNext())
            {
                // start enumerator
                _next = _rootEnumerator.Current;
            }

            _entry = ref Unsafe.NullRef<Entry>();
        }

        public bool MoveNext()
        {
            do
            {
                while (_next == 0)
                {
                    if (!_rootEnumerator.MoveNext())
                    {
                        return false;
                    }

                    _next = _rootEnumerator.Current;
                }

                _entry = ref GetEntry(_next, ref _pages);
                _next = _entry.next;
            } while ((GetPreamble(in _entry, ref _pages) & DestroyedBit) == DestroyedBit);

            return true;
        }

        public Item Current => new(ref _entry, ref _pages);

        public void Dispose()
        {
            // throw new ex
        }

        public readonly ref struct Item
        {
            private readonly ref readonly Entry _entry;
            private readonly ref readonly Page _pages;

            public Item(in Entry entry, in Page pages)
            {
                _entry = ref entry;
                _pages = ref pages;
            }

            public ReadOnlySpan<byte> Key => GetKey(in _entry, in _pages);


            public ReadOnlySpan<byte> Value => GetData(in _entry, in _pages);

            public ulong Hash => _entry.hash;

            public byte Metadata => (byte)(GetPreamble(in _entry, in _pages) & MetadataBits);

            public void Destroy()
            {
                ref var b = ref GetPreamble(in _entry, in _pages);
                b = (byte)(b | DestroyedBit);
            }
        }
    }

    private void AllocateNewPage()
    {
        var page = RentNewPage(false);
        _current = page;
        _currentNumber = _pages.Count - 1;
        _position = 0;
    }

    private Page RentNewPage(bool clear)
    {
        var page = _pool.Rent(clear);
        _pages.Add(page);
        return page;
    }

    private Span<byte> Write(int size, out uint addr)
    {
        if (BufferSize - _position < size)
        {
            // not enough memory
            AllocateNewPage();
        }

        // allocated before the position is changed
        var span = _current.Span.Slice(_position, size);

        addr = (uint)(_position + _currentNumber * BufferSize);
        _position += size;

        return span;
    }

    private unsafe ref Entry NewEntry(out int addr)
    {
        const int perPage = Entry.EntriesPerPage;

        if (_entryPosition >= perPage)
        {
            _entryPosition = 0;
            _entries = RentNewPage(false);
            _entryPage = _pages.Count - 1;
        }

        var position = _entryPosition;
        addr = position + _entryPage * perPage;
        _entryPosition += Entry.Size;

        return ref Unsafe.AsRef<Entry>((byte*)_entries.Raw.ToPointer() + position);
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


    [StructLayout(LayoutKind.Explicit, Size = MaxPointerSize * PageCount)]
    private readonly struct Root
    {
        private const int MaxPointerSize = 8;
        public const int PageCount = 16;
        private const int RootEntriesPerPage = Page.PageSize / sizeof(int);
        private const int EntriesTotal = RootEntriesPerPage * PageCount;
        private const uint EntryHashMask = EntriesTotal - 1;

        [FieldOffset(0)] private readonly Page _start;

        public Root(ReadOnlySpan<Page> pages)
        {
            pages.CopyTo(MemoryMarshal.CreateSpan(ref _start, PageCount));
        }

        public ref int this[ulong hash] => ref GetRef((int)(hash & EntryHashMask));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe ref int GetRef(int at)
        {
            var (pageNo, indexAtPage) = Math.DivRem(at, RootEntriesPerPage);
            var raw = Unsafe.Add(ref Unsafe.AsRef(in _start), pageNo).Raw;
            return ref Unsafe.Add(ref Unsafe.AsRef<int>(raw.ToPointer()), indexAtPage);
        }

        public Enumerator GetEnumerator() => new(this);

        public ref struct Enumerator(Root dictionary)
        {
            private int _at = -1;
            private int _pointer = 0;

            public bool MoveNext()
            {
                _at++;

                while (_at < EntriesTotal && (_pointer = dictionary.GetRef(_at)) == default)
                {
                    _at++;
                }

                return _at < EntriesTotal;
            }

            public int Current => _pointer;
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct Entry
    {
        public static bool Exists(in Entry e) => Unsafe.IsNullRef(in e) == false;

        public const int Size = 16;
        public const int EntriesPerPage = Page.PageSize / Size;

        /// <summary>
        /// The <see cref="ulong"/> hash after <see cref="PooledSpanDictionary.Mix"/>.
        /// </summary>
        public ulong hash;

        /// <summary>
        /// The next entry.
        /// </summary>
        public int next;

        /// <summary>
        /// The pointer to <see cref="PooledSpanDictionary._pages"/>, to a page then to its content, both key and the value.
        /// </summary>
        public uint ptr;
    }
}