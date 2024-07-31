using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store.Merkle;

[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct MerkleNodes
{
    private const int ConsumedNibbles = 2;
    private const int MerkleKeysPerPage = 6;

    // 4 to align to 8
    private const int Count = 4;
    private const int ActualCount = 3;
    public const int Size = DbAddress.Size * Count;

    [FieldOffset(0)] private DbAddress Nodes;

    private Span<DbAddress> Buckets => MemoryMarshal.CreateSpan(ref Nodes, ActualCount);

    /// <summary>
    /// Clears the data.
    /// </summary>
    public void Clear()
    {
        Buckets.Clear();
    }

    public bool TrySet(in NibblePath key, ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (key.Length >= ConsumedNibbles)
        {
            return false;
        }

        var id = GetId(key);
        var at = id / MerkleKeysPerPage;
        Debug.Assert(at < ActualCount);

        ref var bucket = ref Buckets[at];

        var page = GetUShortPage(batch, ref bucket);
        var map = page.Map;

        if (data.IsEmpty)
        {
            map.Delete(id);
        }
        else
        {
            map.Set(id, data);
        }

        return true;
    }

    private static UShortPage GetUShortPage(IBatchContext batch, ref DbAddress bucket)
    {
        if (!bucket.IsNull)
        {
            return new UShortPage(batch.EnsureWritableCopy(ref bucket));
        }

        // Does not exist, requires getting a new.
        // Don't clear the page, create it and then clear the underlying map only.
        var raw = batch.GetNewPage(out bucket, false);

        // Header set
        raw.Header.PageType = PageType.MerkleLeafUShort;

        var p = new UShortPage(raw);

        // Clear only map
        p.Map.Clear();

        return p;
    }

    public bool TryGet(scoped in NibblePath key, out ReadOnlySpan<byte> data, IReadOnlyBatchContext batch)
    {
        if (key.Length >= ConsumedNibbles)
        {
            data = default;
            return false;
        }

        var id = GetId(key);
        ref var bucket = ref Buckets[id / MerkleKeysPerPage];

        var page = new UShortPage(batch.GetAt(bucket));
        var map = page.Map;

        map.TryGet(id, out data);

        // Always return true as this is a check whether the component was able to proceed with the query.
        return true;
    }

    public Enumerator EnumerateAll(IReadOnlyBatchContext context) =>
        new(Buckets, context);

    public ref struct Enumerator
    {
        /// <summary>The map being enumerated.</summary>
        private readonly Span<DbAddress> _pages;

        private readonly IReadOnlyBatchContext _batch;

        private UShortSlottedArray.Enumerator _enumerator;

        /// <summary>The next index to yield.</summary>
        private sbyte _index;

        private Item _current;

        private const sbyte NotStarted = -1;

        internal Enumerator(Span<DbAddress> pages, IReadOnlyBatchContext batch)
        {
            _pages = pages;
            _batch = batch;
            _index = NotStarted;
        }

        /// <summary>Advances the enumerator to the next element of the span.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            if (_index == NotStarted)
            {
                if (TryMoveToNew())
                {
                    // Moved, can use the regular path
                    return MoveNext();
                }

                return false;
            }

            while (_enumerator.MoveNext() == false)
            {
                if (TryMoveToNew() == false)
                {
                    return false;
                }
            }

            var key = _enumerator.Current.Key;
            var path = key == EmptyKeyId ? NibblePath.Empty : NibblePath.Single((byte)(key - IdShift), 0);

            _current = new Item(path, _enumerator.Current.RawData);
            return true;
        }

        private bool TryMoveToNew()
        {
            while (_index < ActualCount - 1)
            {
                _index++;
                var addr = _pages[_index];

                if (addr.IsNull == false)
                {
                    _enumerator = new UShortPage(_batch.GetAt(addr)).Map.EnumerateAll();
                    return true;
                }
            }

            return false;
        }

        public readonly Item Current => _current;

        public readonly void Dispose()
        {
        }

        public readonly ref struct Item(NibblePath key, ReadOnlySpan<byte> rawData)
        {
            public NibblePath Key { get; } = key;
            public ReadOnlySpan<byte> RawData { get; } = rawData;
        }

        // a shortcut to not allocate, just copy the enumerator
        public readonly Enumerator GetEnumerator() => this;
    }

    private const int IdShift = 1;
    private const int EmptyKeyId = 0;
    private static ushort GetId(in NibblePath key) => (ushort)(key.IsEmpty ? EmptyKeyId : key.FirstNibble + IdShift);

    public void RegisterForFutureReuse(IBatchContext batch)
    {
        foreach (var addr in Buckets)
        {
            if (addr.IsNull == false)
            {
                batch.RegisterForFutureReuse(batch.GetAt(addr));
            }
        }
    }
}