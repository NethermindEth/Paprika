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

        public FanOutList<FanOutPage, IdentityType> StorageTrees =>
            new(MemoryMarshal.CreateSpan(ref IdsPayload, FanOutList.FanOut));

        public const int AbandonedStart =
            DbAddress.Size * 2 + Metadata.Size + FanOutList.Size;

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

    private static DbAddress ReadId(ReadOnlySpan<byte> id) => new(BinaryPrimitives.ReadUInt32LittleEndian(id));

    private static void WriteId(Span<byte> span, DbAddress address) =>
        BinaryPrimitives.WriteUInt32LittleEndian(span, address);


    public void SetRaw(in Key key, IBatchContext batch, ReadOnlySpan<byte> rawData)
    {
        if (key.IsState)
        {
            SetAtRoot<DataPage>(batch, key.Path, rawData, ref Data.StateRoot);
        }
        else
        {
            Span<byte> span = stackalloc byte[4];
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
                else
                {
                    ref var last = ref Data.LastStorageRootPage;

                    // The key is not cached and is not mapped in the storage trees. Requires allocating a new place.
                    // Let's start with the lats memoized storage root and check if it has some place left.

                    // If the last storage root is null or has no more space, allocate new
                    if (last.IsNull || new StorageRootPage(batch.GetAt(last)).HasEmptySlot == false)
                    {
                        var storageRoot = batch.GetNewPage(out addr, true);
                        storageRoot.Header.PageType = PageType.StorageRoot;
                        storageRoot.Header.Level = 0;
                    }
                    else
                    {
                        // use the last as the address to write to
                        addr = last;
                    }

                    // The last storage root is not null and has some empty places
                    last = batch.GetAddress(new StorageRootPage(batch.GetAt(addr)).Set(key, rawData, batch));

                    // Set in trees and in cache
                    WriteId(span, last);
                    Data.StorageTrees.Set(NibblePath.FromKey(keccak), span, batch);
                    cache[keccak] = last;
                    return;
                }
            }

            // perform set
            var page = batch.GetAt(addr);
            var storage = new StorageRootPage(page);
            var updated = storage.Set(key, rawData, batch);

            if (updated.Equals(page))
            {
                // nothing to update, we wrote at the given page
                return;
            }

            // The page was different, it requires to update all the entries in cache and storage tries so that they point to the same
            WriteId(span, addr);
            foreach (var k in storage.Keys)
            {
                if (k != Keccak.Zero)
                {
                    cache[keccak] = addr;
                    Data.StorageTrees.Set(NibblePath.FromKey(k), span, batch);
                }
            }
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