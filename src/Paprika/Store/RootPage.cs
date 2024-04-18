using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// Root page is a page that contains all the needed metadata from the point of view of the database.
/// It also includes the blockchain information like block hash or block number.
/// </summary>
public readonly unsafe struct RootPage(Page root) : IPage
{
    public ref PageHeader Header => ref root.Header;

    public ref Payload Data => ref Unsafe.AsRef<Payload>(root.Payload);

    /// <summary>
    /// Represents the data of the page.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;

        /// <summary>
        /// The address of the next free page. This should be used rarely as pages should be reused
        /// with <see cref="AbandonedPage"/>.
        /// </summary>
        [FieldOffset(0)]
        public DbAddress NextFreePage;

        [FieldOffset(DbAddress.Size)]
        public DbAddress LastStorageRootPage;

        /// <summary>
        /// The first of the data pages.
        /// </summary>
        [FieldOffset(DbAddress.Size * 2)]
        public DbAddress StateRoot;

        /// <summary>
        /// Metadata of this root
        /// </summary>
        [FieldOffset(DbAddress.Size * 3)]
        public Metadata Metadata;

        /// <summary>
        /// Mapping Keccak -> DbAddress of the storage tree.
        /// </summary>
        [FieldOffset(DbAddress.Size * 3 + Metadata.Size)]
        private DbAddress IdsPayload;

        public FanOutList<FanOutPage, StorageMapping> StorageTrees =>
            new(MemoryMarshal.CreateSpan(ref IdsPayload, FanOutList.FanOut));

        public const int AbandonedStart =
            DbAddress.Size * 3 + Metadata.Size + FanOutList.Size;

        /// <summary>
        /// The start of the abandoned pages.
        /// </summary>
        [FieldOffset(AbandonedStart)] public AbandonedList AbandonedList;

        public DbAddress GetNextFreePage()
        {
            var free = NextFreePage;
            NextFreePage = NextFreePage.Next;
            return free;
        }
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver)
    {
        if (Data.StateRoot.IsNull == false)
        {
            var data = new DataPage(resolver.GetAt(Data.StateRoot));
            using var scope = visitor.On(data, Data.StateRoot);
        }

        // Data.Storage.Accept(visitor, resolver);

        // Data.AbandonedList.Accept(visitor, resolver);
    }

    /// <summary>
    /// How many id entries should be cached per readonly batch.
    /// </summary>
    public const int IdCacheLimit = 2_000;

    public bool TryGet(scoped in Key key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result)
    {
        if (key.IsState)
        {
            if (Data.StateRoot.IsNull)
            {
                result = default;
                return false;
            }

            return new DataPage(batch.GetAt(Data.StateRoot)).TryGet(batch, key.Path, out result);
        }

        var cache = batch.StorageTreeCache;
        var keccak = key.Path.UnsafeAsKeccak;

        if (cache.TryGetValue(keccak, out var id))
        {
            if (id == 0)
            {
                result = default;
                return false;
            }
        }
        else
        {
            if (Data.StorageTrees.TryGet(batch, key.Path, out var existing))
            {
                id = ReadId(existing);
                if (cache.Count < IdCacheLimit)
                {
                    cache[keccak] = id;
                }
            }
            else
            {
                // Not found, for now, not remember misses, remember miss
                // cache[keccak] = 0;
                result = default;
                return false;
            }
        }

        return new StorageRootPage(batch.GetAt(id)).TryGet(in key, batch, out result);
    }

    private static DbAddress ReadId(ReadOnlySpan<byte> id) => DbAddress.Read(id);

    private static ReadOnlySpan<byte> WriteId(Span<byte> span, DbAddress address) => address.Write(span);

    public void SetRaw(in Key key, IBatchContext batch, ReadOnlySpan<byte> rawData)
    {
        if (key.IsState)
        {
            SetAtRoot<DataPage>(batch, key.Path, rawData, ref Data.StateRoot);
        }
        else
        {
            var keccak = key.Path.UnsafeAsKeccak;
            var cache = batch.StorageTreeCache;

            if (cache.TryGetValue(keccak, out var addr) == false)
            {
                if (Data.StorageTrees.TryGet(batch, NibblePath.FromKey(keccak), out var existing))
                {
                    // exists, read and cache
                    addr = ReadId(existing);
                    cache[keccak] = addr;
                }
            }

            if (addr.IsNull == false)
            {
                // The account was previously mapped, set in the tree
                var original = new StorageRootPage(batch.GetAt(addr));
                var updated = original.Set(key, rawData, batch);

                FlushMappings(updated, batch);
                return;
            }

            // This account has not been set before. Try to put it in the existing page last storage root page.
            {
                ref var last = ref Data.LastStorageRootPage;
                if (last.IsNull || new StorageRootPage(batch.GetAt(last)).HasEmptySlot == false)
                {
                    // Last not set, allocate, then set
                    var page = batch.GetNewPage(out last, true);
                    page.Header.PageType = PageType.StorageRoot;
                    page.Header.Level = 0;

                    var storage = new StorageRootPage(page);
                    storage.Set(key, rawData, batch);

                    var span = WriteId(stackalloc byte[DbAddress.Size], last);
                    cache[keccak] = last;
                    Data.StorageTrees.Set(NibblePath.FromKey(keccak), span, batch);
                    return;
                }

                // There's room left in this page, use it for the new
                var original = new StorageRootPage(batch.GetAt(last));
                var updated = original.Set(key, rawData, batch);
                last = batch.GetAddress(updated.AsPage());

                FlushMappings(updated, batch);
            }
        }
    }

    private void FlushMappings(Page updated, IBatchContext batch)
    {
        Span<byte> span = stackalloc byte[DbAddress.Size];
        var addr = batch.GetAddress(updated);
        WriteId(span, addr);

        foreach (ref readonly var keccak in new StorageRootPage(updated).Keys)
        {
            Data.StorageTrees.Set(NibblePath.FromKey(keccak), span, batch);
            batch.StorageTreeCache[keccak] = addr;
        }
    }

    public void Destroy(IBatchContext batch, in NibblePath account)
    {
        // Destroy the Id entry about it
        Data.StorageTrees.Set(account, ReadOnlySpan<byte>.Empty, batch);

        // Destroy the account entry
        SetAtRoot<DataPage>(batch, account, ReadOnlySpan<byte>.Empty, ref Data.StateRoot);

        // Remove the cached
        batch.StorageTreeCache.Remove(account.UnsafeAsKeccak);

        // TODO: there' no garbage collection for storage
        // It should not be hard. Walk down by the mapped path, then remove all the pages underneath.
    }

    private static void SetAtRoot<TPage>(IBatchContext batch, in NibblePath path, in ReadOnlySpan<byte> rawData,
        ref DbAddress root)
        where TPage : struct, IPageWithData<TPage>
    {
        var data = batch.TryGetPageAlloc(ref root, PageType.Standard);
        var updated = TPage.Wrap(data).Set(path, rawData, batch);
        root = batch.GetAddress(updated);
    }
}

[StructLayout(LayoutKind.Sequential, Pack = sizeof(byte), Size = Size)]
public struct Metadata
{
    public const int Size = BlockNumberSize + Keccak.Size;
    private const int BlockNumberSize = sizeof(uint);

    public readonly uint BlockNumber;
    public readonly Keccak StateHash;

    public Metadata(uint blockNumber, Keccak stateHash)
    {
        BlockNumber = blockNumber;
        StateHash = stateHash;
    }
}