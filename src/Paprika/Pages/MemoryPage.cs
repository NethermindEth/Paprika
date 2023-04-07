using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Serialization.Formatters.Binary;

namespace Paprika.Pages;

/// <summary>
/// Represents a min-heap of pages that can be no longer used and can be reused.
/// </summary>
public readonly struct MemoryPage : IPage
{
    /// <summary>
    /// The arity of the heap.
    /// </summary>
    private const int Arity = 4;

    /// <summary>
    /// The log2 of <see cref="Arity" />.
    /// </summary>
    private const int Log2Arity = 2;
    
    private readonly Page _page;

    [DebuggerStepThrough]
    public MemoryPage(Page page) => _page = page;

    private unsafe ref Payload Data => ref Unsafe.AsRef<Payload>(_page.Payload);

    /// <summary>
    /// Pushes the page to the registry of pages to be freed.
    /// </summary>
    /// <returns>The raw page.</returns>
    public Page Push(IBatchContext batch, DbAddress pageAddress, uint batchId)
    {
        if (_page.Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(_page);
            return new MemoryPage(writable).Push(batch, pageAddress, batchId);
        }

        if (Data.TryEnqueue(new PageInfo { Batch = batchId, Page = pageAddress }) == false)
        {
            throw new NotImplementedException("Implement overflow in free");
        }

        return _page;
    }


    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;
        private const int DataOffset = sizeof(uint);
        
        public const int MaxCount = (Size - DataOffset) / PageInfo.Size;

        /// <summary>
        /// The bit map of frames used at this page.
        /// </summary>
        [FieldOffset(0)] public int Count;

        [FieldOffset(DataOffset)] private PageInfo Item;

        /// <summary>
        /// Provides a span of all the items in the FreePage.
        /// </summary>
        private Span<PageInfo> BuildItems(int length) => MemoryMarshal.CreateSpan(ref Item, length);

        public bool TryEnqueue(PageInfo info)
        {
            if (Count >= MaxCount)
            {
                // no more place here
                return false;
            }

            Count++;
            MoveUp(info, Count - 1);
            return true;
        }
        
        public bool TryPeekBatchId(out uint batchId)
        {
            if (Count == 0)
            {
                batchId = default;
                return false;
            }

            // peek one without getting the span.
            batchId = Item.Batch;
            return true;
        }
        
        public DbAddress Dequeue()
        {
            if (Count == 0)
            {
                throw new InvalidOperationException("The page is empty! TryPeekBatchId first!");
            }

            DbAddress page = Item.Page;
            RemoveRootNode();
            return page;
        }
        
        private void MoveUp(PageInfo info, int nodeIndex)
        {
            Debug.Assert(0 <= nodeIndex && nodeIndex < Count);

            var infos = BuildItems(nodeIndex);

            while (nodeIndex > 0)
            {
                int parentIndex = GetParentIndex(nodeIndex);
                var parent = infos[parentIndex];

                if (info.Long < parent.Long)
                {
                    infos[nodeIndex] = parent;
                    nodeIndex = parentIndex;
                }
                else
                {
                    break;
                }
            }

            infos[nodeIndex] = info;
        }
        
        private void RemoveRootNode()
        {
            var lastNodeIndex = --Count;
            
            if (lastNodeIndex > 0)
            {
                ref readonly var last = ref Unsafe.Add(ref Item, Count + 1);
                MoveDown(last, 0);
            }
        }
        
        private void MoveDown(PageInfo info, int nodeIndex)
        {
            Debug.Assert(0 <= nodeIndex && nodeIndex < Count);

            var nodes = BuildItems(nodeIndex);
            int size = Count;

            int i;
            while ((i = GetFirstChildIndex(nodeIndex)) < size)
            {
                // Find the child node with the minimal priority
                var minChild = nodes[i];
                var minChildIndex = i;

                var childIndexUpperBound = Math.Min(i + Arity, size);
                while (++i < childIndexUpperBound)
                {
                    var nextChild = nodes[i];
                    if (nextChild.Long <  minChild.Long)
                    {
                        minChild = nextChild;
                        minChildIndex = i;
                    }
                }

                // Heap property is satisfied; insert node in this location.
                if (info.Long < minChild.Long)
                {
                    break;
                }

                // Move the minimal child up by one node and
                // continue recursively from its location.
                nodes[nodeIndex] = minChild;
                nodeIndex = minChildIndex;
            }

            nodes[nodeIndex] = info;
        }
        
        private static int GetParentIndex(int index) => (index - 1) >> Log2Arity;
        private static int GetFirstChildIndex(int index) => (index << Log2Arity) + 1;
    }

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct PageInfo
    {
        public const int Size = sizeof(uint) + DbAddress.Size;

        [FieldOffset(0)] public DbAddress Page;
        [FieldOffset(DbAddress.Size)] public uint Batch;

        public long Long
        {
            get
            {
                if (BitConverter.IsLittleEndian)
                {
                    return Unsafe.As<DbAddress, long>(ref Page);
                }

                return ((long)Unsafe.As<DbAddress, int>(ref Page) << 32) | Batch;
            }
        }
    }
}