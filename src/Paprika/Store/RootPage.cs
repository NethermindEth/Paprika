using System.Buffers.Binary;
using System.Diagnostics;
using System.Numerics;
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
/// State: <see cref="Payload.StateRoot"/> 
/// Storage & Account Ids: <see cref="StorageFanOut.Level0"/>
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
        private DbAddressList.Of1024 StorageFanOut;

        public StorageFanOut.Level0 Storage => new(ref StorageFanOut);

        public const int AbandonedStart =
            DbAddress.Size * 2 + sizeof(uint) + Metadata.Size + DbAddressList.Of1024.Size;

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
        var stateRoot = Data.StateRoot;
        if (stateRoot.IsNull == false)
        {
            var builder = new NibblePath.Builder(stackalloc byte[NibblePath.Builder.DecentSize]);

            new StateRootPage(resolver.GetAt(stateRoot)).Accept(ref builder, visitor, resolver, stateRoot);

            builder.Dispose();
        }

        Data.Storage.Accept(visitor, resolver);
        Data.AbandonedList.Accept(visitor, resolver);
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

            return new StateRootPage(batch.GetAt(Data.StateRoot)).TryGet(batch, key.Path, out result);
        }

        Span<byte> idSpan = stackalloc byte[sizeof(uint)];

        ReadOnlySpan<byte> id;
        var cache = batch.IdCache;
        var keccak = key.Path.UnsafeAsKeccak;

        if (cache.TryGetValue(keccak, out var cachedId))
        {
            if (cachedId == 0)
            {
                result = default;
                return false;
            }

            WriteId(idSpan, cachedId);
            id = idSpan;
        }
        else
        {
            if (Data.Storage.TryGet(batch, key.Path, StorageFanOut.Type.Id, out id))
            {
                if (cache.Count < IdCacheLimit)
                {
                    cache[keccak] = ReadId(id);
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

        var path = NibblePath.FromKey(id).Append(key.StoragePath, stackalloc byte[StorageKeySize]);

        return Data.Storage.TryGet(batch, path, StorageFanOut.Type.Storage, out result);
    }

    private static uint ReadId(ReadOnlySpan<byte> id) => BinaryPrimitives.ReadUInt32LittleEndian(id);

    private static void WriteId(Span<byte> idSpan, uint id)
    {
        // Rotation of nibbles could help with an even spread but would make it worse for smaller networks.
        // If a chain has 16 million contracts or more, this does not matter anyway.

        // // Rotate nibbles so that small value goes first as the NibblePath reads them
        // // This will distribute buckets more properly for smaller networks as StorageFanOut consumes 5 nibbles.
        // var rotatedNibbles = ((id & 0x0F0F0F0F) << 4) | ((id & 0xF0F0F0F0) >> 4);
        // BinaryPrimitives.WriteUInt32LittleEndian(idSpan, rotatedNibbles);

        BinaryPrimitives.WriteUInt32LittleEndian(idSpan, id);
    }

    public void SetRaw(in Key key, IBatchContext batch, ReadOnlySpan<byte> rawData)
    {
        if (key.IsState)
        {
            SetAtRoot(batch, key.Path, rawData, ref Data.StateRoot);
        }
        else
        {
            scoped NibblePath id;
            Span<byte> idSpan = stackalloc byte[sizeof(uint)];

            var keccak = key.Path.UnsafeAsKeccak;

            if (batch.IdCache.TryGetValue(keccak, out var cachedId))
            {
                WriteId(idSpan, cachedId);
                id = NibblePath.FromKey(idSpan);
            }
            else
            {
                // try fetch existing first
                if (Data.Storage.TryGet(batch, key.Path, StorageFanOut.Type.Id, out var existingId) == false)
                {
                    Data.AccountCounter++;
                    WriteId(idSpan, Data.AccountCounter);

                    // memoize in cache
                    batch.IdCache[keccak] = Data.AccountCounter;

                    // update root
                    Data.Storage.Set(key.Path, StorageFanOut.Type.Id, idSpan, batch);

                    id = NibblePath.FromKey(idSpan);
                }
                else
                {
                    // memoize in cache
                    batch.IdCache[keccak] = ReadId(existingId);
                    id = NibblePath.FromKey(existingId);
                }
            }

            var path = id.Append(key.StoragePath, stackalloc byte[StorageKeySize]);
            Data.Storage.Set(path, StorageFanOut.Type.Storage, rawData, batch);
        }
    }

    public void Destroy(IBatchContext batch, in NibblePath account)
    {
        // GC for storage
        DeleteByPrefix(Key.Merkle(account), batch);

        // Destroy the Id entry about it
        Data.Storage.Set(account, StorageFanOut.Type.Id, ReadOnlySpan<byte>.Empty, batch);

        // Destroy the account entry
        SetAtRoot(batch, account, ReadOnlySpan<byte>.Empty, ref Data.StateRoot);

        // Remove the cached
        batch.IdCache.Remove(account.UnsafeAsKeccak);
    }

    public void DeleteByPrefix(in Key prefix, IBatchContext batch)
    {
        if (prefix.IsState)
        {
            var data = batch.TryGetPageAlloc(ref Data.StateRoot, PageType.StateRoot);
            var updated = new StateRootPage(data).DeleteByPrefix(prefix.Path, batch);
            Data.StateRoot = batch.GetAddress(updated);
        }
        else
        {
            scoped NibblePath id;
            Span<byte> idSpan = stackalloc byte[sizeof(uint)];

            var keccak = prefix.Path.UnsafeAsKeccak;

            if (batch.IdCache.TryGetValue(keccak, out var cachedId))
            {
                WriteId(idSpan, cachedId);
                id = NibblePath.FromKey(idSpan);
            }
            else
            {
                // Not in cache, try fetch from db
                if (Data.Storage.TryGet(batch, prefix.Path, StorageFanOut.Type.Id, out var existingId) != false)
                {
                    id = NibblePath.FromKey(existingId);
                }
                else
                {
                    // Has never been mapped, return.
                    return;
                }
            }

            var path = id.Append(prefix.StoragePath, stackalloc byte[StorageKeySize]);
            Data.Storage.DeleteByPrefix(path, batch);
        }
    }

    private static void SetAtRoot(IBatchContext batch, in NibblePath path, in ReadOnlySpan<byte> rawData,
        ref DbAddress root)
    {
        var data = batch.TryGetPageAlloc(ref root, PageType.StateRoot);
        var updated = new StateRootPage(data).Set(path, rawData, batch);
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