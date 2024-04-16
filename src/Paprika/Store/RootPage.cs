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
/// <remarks>
/// Considerations for page types selected:
///
/// State:
/// <see cref="Payload.StateRoot"/> is <see cref="FanOutPage"/> that splits accounts into 256 buckets.
/// This makes the updates update more pages, but adds a nice fan out for fast searches.
/// Account ids:
/// <see cref="Payload.StorageTrees"/> is a <see cref="FanOutList"/> of <see cref="FanOutPage"/>s. This gives 64k buckets on two levels. Searches should search no more than 3 levels of pages.
///
/// Storage:
/// <see cref="Payload.Storage"/> is a <see cref="FanOutList"/> of <see cref="FanOutPage"/>s. This gives 64k buckets on two levels. 
/// </remarks>
public readonly unsafe struct RootPage(Page root) : IPage
{
    private const int StorageKeySize = Keccak.Size + Keccak.Size + 1;

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
        [FieldOffset(0)] public DbAddress NextFreePage;

        /// <summary>
        /// The first of the data pages.
        /// </summary>
        [FieldOffset(DbAddress.Size)]
        public DbAddress StateRoot;

        /// <summary>
        /// Metadata of this root
        /// </summary>
        [FieldOffset(DbAddress.Size * 2)]
        public Metadata Metadata;

        /// <summary>
        /// Mapping Keccak -> DbAddress of the storage tree.
        /// </summary>
        [FieldOffset(DbAddress.Size * 2 + Metadata.Size)]
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
                    cache[keccak] = ReadId(existing);
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

        return batch.GetAt(id).GetPageWithData(batch, key.StoragePath, out result);
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
                    // doest not exist, create first, memoize
                    var p = batch.GetNewPage(out addr, true);
                    p.Header.BatchId = batch.BatchId;
                    p.Header.PaprikaVersion = PageHeader.CurrentVersion;
                    p.Header.PageType = PageType.Leaf;
                    p.Header.Level = 0;

                    // set in trees and in cache
                    WriteId(span, addr);
                    Data.StorageTrees.Set(NibblePath.FromKey(keccak), span, batch);
                    cache[keccak] = addr;
                }
            }

            // perform set
            var page = batch.GetAt(addr);
            var updated = page.SetPageWithData(key.StoragePath, rawData, batch);

            if (updated.Equals(page))
            {
                // nothing to memoize
                return;
            }

            addr = batch.GetAddress(updated);

            // update in cache and set back
            cache[keccak] = addr;
            WriteId(span, addr);
            Data.StorageTrees.Set(NibblePath.FromKey(keccak), span, batch);
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