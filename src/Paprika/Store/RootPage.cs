using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// Root page is a page that contains all the needed metadata from the point of view of the database.
/// It also includes the blockchain information like block hash or block number
/// </summary>
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

        private const int MetadataStart = 4 * DbAddress.Size + sizeof(uint);

        /// <summary>
        /// The address of the next free page. This should be used rarely as pages should be reused
        /// with <see cref="AbandonedPage"/>.
        /// </summary>
        [FieldOffset(0)] public DbAddress NextFreePage;

        /// <summary>
        /// The first of the data pages.
        /// </summary>
        [FieldOffset(DbAddress.Size)] public DbAddress StateRoot;

        /// <summary>
        /// The first of the data pages.
        /// </summary>
        [FieldOffset(DbAddress.Size * 2)] public DbAddress StorageRoot;

        /// <summary>
        /// The root of the id pages.
        /// </summary>
        [FieldOffset(DbAddress.Size * 3)] public DbAddress IdRoot;

        /// <summary>
        /// The account counter
        /// </summary>
        [FieldOffset(DbAddress.Size * 4)] public uint AccountCounter;

        [FieldOffset(MetadataStart)] public Metadata Metadata;

        /// <summary>
        /// The start of the abandoned pages.
        /// </summary>
        [FieldOffset(AbandonedList.SpaceForRootPage)]
        public AbandonedList AbandonedList;

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

            var encoded = Encode(key.Path, stackalloc byte[key.Path.MaxByteLength], key.Type);
            return new DataPage(batch.GetAt(Data.StateRoot)).TryGet(encoded, batch, out result);
        }

        if (Data.StorageRoot.IsNull || Data.IdRoot.IsNull)
        {
            result = default;
            return false;
        }

        var ids = new FanOutPage(batch.GetAt(Data.IdRoot));
        if (ids.TryGet(key.Path, batch, out var id) == false)
        {
            result = default;
            return false;
        }

        var encodedStorage = Encode(key.StoragePath, stackalloc byte[key.Path.MaxByteLength], key.Type);
        var path = NibblePath.FromKey(id).Append(encodedStorage, stackalloc byte[StorageKeySize]);

        return new FanOutPage(batch.GetAt(Data.StorageRoot)).TryGet(path, batch, out result);
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
        var typeFlag = (byte)(type & DataType.Merkle);

        const byte oddEnd = 0x01;
        const byte evenEnd = 0x00;

        // 2 lower bits are used, for odd|even and merkle.
        // We can use 1 more bit for differentiation for even lengths.
        // This leaves 1 bit to extract potential value. This means that it can compress 2 / 16,
        // meaning 1/8 of even paths.

        const byte evenPacked = 0x04;
        const byte packedShift = 3;
        const byte maxPacked = 1;

        Debug.Assert(path.IsOdd == false, "Encoded paths should not be odd. They always start at 0");

        if (path.IsEmpty)
            return path;

        var raw = path.RawSpan;

        if (path.Length % 2 == 1)
        {
            // Odd case
            raw.CopyTo(destination);
            ref var last = ref destination[raw.Length - 1];
            last &= 0xF0;
            last |= oddEnd;
            last |= typeFlag;

            return NibblePath.FromKey(destination[..raw.Length]);
        }

        // Even case
        raw.CopyTo(destination);
        var lastByte = raw[^1];
        var lastNibble = lastByte & NibblePath.NibbleMask;
        var lastButOneNibble = lastByte & (NibblePath.NibbleMask << NibblePath.NibbleShift);

        if (lastNibble <= maxPacked)
        {
            // We can pack better
            destination[raw.Length - 1] = (byte)(lastButOneNibble | (lastNibble << packedShift) | evenPacked | typeFlag);
            return NibblePath.FromKey(destination[..raw.Length]);
        }

        destination[raw.Length] = (byte)(evenEnd | typeFlag);
        return NibblePath.FromKey(destination[..(raw.Length + 1)]);
    }

    public void SetRaw(in Key key, IBatchContext batch, ReadOnlySpan<byte> rawData)
    {
        if (key.IsState)
        {
            var encoded = Encode(key.Path, stackalloc byte[key.Path.MaxByteLength], key.Type);
            SetAtRoot<DataPage>(batch, encoded, rawData, ref Data.StateRoot);
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
                // full extraction of key possible, get it
                var ids = new FanOutPage(batch.TryGetPageAlloc(ref Data.IdRoot, PageType.Identity));

                // try fetch existing first
                if (ids.TryGet(key.Path, batch, out var existingId) == false)
                {
                    Data.AccountCounter++;
                    BinaryPrimitives.WriteUInt32LittleEndian(idSpan, Data.AccountCounter);

                    // memoize in cache
                    batch.IdCache[key.Path.UnsafeAsKeccak] = Data.AccountCounter;

                    // update root
                    Data.IdRoot = batch.GetAddress(ids.Set(key.Path, idSpan, batch));

                    id = NibblePath.FromKey(idSpan);
                }
                else
                {
                    // memoize in cache
                    batch.IdCache[key.Path.UnsafeAsKeccak] = BinaryPrimitives.ReadUInt32LittleEndian(existingId);
                    id = NibblePath.FromKey(existingId);
                }
            }

            var encoded = Encode(key.StoragePath, stackalloc byte[key.Path.MaxByteLength], key.Type);
            var path = id.Append(encoded, stackalloc byte[StorageKeySize]);

            SetAtRoot<FanOutPage>(batch, path, rawData, ref Data.StorageRoot);
        }
    }

    public void Destroy(IBatchContext batch, in NibblePath account)
    {
        // Get the id page
        var ids = new FanOutPage(batch.TryGetPageAlloc(ref Data.IdRoot, PageType.Identity));

        // Destroy the Id entry about it
        Data.IdRoot = batch.GetAddress(ids.Set(account, ReadOnlySpan<byte>.Empty, batch));

        // Destroy the account entry
        SetAtRoot<DataPage>(batch, Encode(account, stackalloc byte[account.MaxByteLength], DataType.Account),
            ReadOnlySpan<byte>.Empty, ref Data.StateRoot);

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
    private const int Size = BlockNumberSize + Keccak.Size;
    private const int BlockNumberSize = sizeof(uint);

    [FieldOffset(0)] public readonly uint BlockNumber;
    [FieldOffset(BlockNumberSize)] public readonly Keccak StateHash;

    public Metadata(uint blockNumber, Keccak stateHash)
    {
        BlockNumber = blockNumber;
        StateHash = stateHash;
    }
}