using System.Buffers.Binary;
using System.Diagnostics;
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
/// <see cref="Payload.Ids"/> is a <see cref="FanOutList"/> of <see cref="FanOutPage"/>s. This gives 64k buckets on two levels. Searches should search no more than 3 levels of pages.
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
        /// The account counter
        /// </summary>
        [FieldOffset(DbAddress.Size)] public uint AccountCounter;

        /// <summary>
        /// The first of the data pages.
        /// </summary>
        [FieldOffset(DbAddress.Size + sizeof(uint))]
        public DbAddress StateRoot;

        /// <summary>
        /// Metadata of this root
        /// </summary>
        [FieldOffset(DbAddress.Size * 2 + sizeof(uint))]
        public Metadata Metadata;

        /// <summary>
        /// Storage.
        /// </summary>
        [FieldOffset(DbAddress.Size * 2 + sizeof(uint) + Metadata.Size)]
        private DbAddress StoragePayload;

        public FanOutList<DataPage, StandardType> Storage => new(MemoryMarshal.CreateSpan(ref StoragePayload, FanOutList.FanOut));

        /// <summary>
        /// Identifiers
        /// </summary>
        [FieldOffset(DbAddress.Size * 2 + sizeof(uint) + Metadata.Size + FanOutList.Size)]
        private DbAddress IdsPayload;

        public FanOutList<FanOutPage, IdentityType> Ids => new(MemoryMarshal.CreateSpan(ref IdsPayload, FanOutList.FanOut));

        public const int AbandonedStart =
            DbAddress.Size * 2 + sizeof(uint) + Metadata.Size + FanOutList.Size * 2;

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
            var data = new FanOutPage(resolver.GetAt(Data.StateRoot));
            visitor.On(data, Data.StateRoot);
        }

        Data.AbandonedList.Accept(visitor, resolver);
    }

    public bool TryGet(scoped in Key key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result)
    {
        if (key.IsState)
        {
            if (Data.StateRoot.IsNull)
            {
                result = default;
                return false;
            }

            return new FanOutPage(batch.GetAt(Data.StateRoot)).TryGet(key.Path, batch, out result);
        }

        if (Data.Ids.TryGet(key.Path, batch, out var id) == false)
        {
            result = default;
            return false;
        }

        var path = NibblePath.FromKey(id).Append(key.StoragePath, stackalloc byte[StorageKeySize]);

        return Data.Storage.TryGet(path, batch, out result);
    }

    /// <summary>
    /// Encodes the path in a way that makes it byte-aligned and unique, but also sort:
    ///
    /// - empty path is left empty
    /// - odd-length path is padded with a single nibble with value of 0x01
    /// - even-length path is padded with a single byte (2 nibbles with value of 0x00)
    ///
    /// To ensure that <see cref="DataType.Merkle"/> and <see cref="DataType.Account"/>/<see cref="DataType.StorageCell"/>
    /// of the same length can coexist, the merkle marker is added as well.
    /// </summary>
    public static NibblePath Encode(in NibblePath path, in Span<byte> destination, DataType type)
    {
        return path;
    }

    public void SetRaw(in Key key, IBatchContext batch, ReadOnlySpan<byte> rawData)
    {
        if (key.IsState)
        {
            SetAtRoot<FanOutPage>(batch, key.Path, rawData, ref Data.StateRoot);
        }
        else
        {
            scoped NibblePath id;
            Span<byte> idSpan = stackalloc byte[sizeof(uint)];

            if (batch.IdCache.TryGetValue(key.Path.UnsafeAsKeccak, out var cachedId))
            {
                BinaryPrimitives.WriteUInt32LittleEndian(idSpan, cachedId);
                id = NibblePath.FromKey(idSpan);
            }
            else
            {
                // try fetch existing first
                if (Data.Ids.TryGet(key.Path, batch, out var existingId) == false)
                {
                    Data.AccountCounter++;
                    BinaryPrimitives.WriteUInt32LittleEndian(idSpan, Data.AccountCounter);

                    // memoize in cache
                    batch.IdCache[key.Path.UnsafeAsKeccak] = Data.AccountCounter;

                    // update root
                    Data.Ids.Set(key.Path, idSpan, batch);

                    id = NibblePath.FromKey(idSpan);
                }
                else
                {
                    // memoize in cache
                    batch.IdCache[key.Path.UnsafeAsKeccak] = BinaryPrimitives.ReadUInt32LittleEndian(existingId);
                    id = NibblePath.FromKey(existingId);
                }
            }

            var path = id.Append(key.StoragePath, stackalloc byte[StorageKeySize]);

            Data.Storage.Set(path, rawData, batch);
        }
    }

    public void Destroy(IBatchContext batch, in NibblePath account)
    {
        // Destroy the Id entry about it
        Data.Ids.Set(account, ReadOnlySpan<byte>.Empty, batch);

        // Destroy the account entry
        SetAtRoot<FanOutPage>(batch, account, ReadOnlySpan<byte>.Empty, ref Data.StateRoot);

        // Remove the cached
        batch.IdCache.Remove(account.UnsafeAsKeccak);

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

[StructLayout(LayoutKind.Explicit, Size = Size, Pack = 1)]
public struct Metadata
{
    public const int Size = BlockNumberSize + Keccak.Size;
    private const int BlockNumberSize = sizeof(uint);

    [FieldOffset(0)] public readonly uint BlockNumber;
    [FieldOffset(BlockNumberSize)] public readonly Keccak StateHash;

    public Metadata(uint blockNumber, Keccak stateHash)
    {
        BlockNumber = blockNumber;
        StateHash = stateHash;
    }
}