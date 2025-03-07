using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Xml.Serialization;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// This class is responsible for proving a good fan out for both:
/// 1. <see cref="Keccak"/> -> <see cref="uint"/> mapping needed to map contracts to their identifiers
/// 2. <see cref="uint"/> identifiers to <see cref="Level3Page"/> that provide the top of the tree for a given contract
///
/// This is done in two different ways. For identifiers, the biggest possible fan out is used
/// consuming <see cref="Level0.IdConsumedNibbles"/> from the keccak and moving it to the upper bits of the <see cref="uint"/>.
/// This distributes hashes of addresses to 64k buckets.
///
/// For storage, the identifier is kept as low as possible (counting from 0), meaning,
/// that the highest bits will be used only for large networks. This makes it small on upper levels but ensures
/// that on lower levels will distribute nicely. It still ensures a good fan out that does not blow up with the number of pages occupied.
/// </summary>
public static class StorageFanOut
{
    public const int LevelCount = 1;

    public const string ScopeIds = "Ids";
    public const string ScopeStorage = "Storage";

    public enum Type
    {
        /// <summary>
        /// Represents the mapping of Keccak->int
        /// </summary>
        Id,

        /// <summary>
        /// Represents the actual storage mapped NibblePath ->int
        /// </summary>
        Storage
    }

    private static (uint next, int index) GetIndex(uint at, int level)
    {
        const int length = DbAddressList.Of2048.Count;
        const int lengthMask = length - 1;
        var lengthBits = BitOperations.Log2(length);

        // Bits in id. Use to calculate the max size of L0 * L1 * the rest. Current max id is 67,108,864.
        const int maxValue = Level0.FanOut * Level1Page.FanOut * (Level1Page.LocalKeyNibbles * 16);
        var allowedBitsInId = BitOperations.Log2(maxValue);
        var shift = allowedBitsInId - (level + 1) * lengthBits;

        var index = (int)((at >> shift) & lengthMask);

        Debug.Assert(0 <= index && index < length);

        var nextMask = (1U << shift) - 1;
        var next = nextMask & at;

        return (next, index);
    }

    /// <summary>
    /// Provides a convenient data structure for <see cref="RootPage"/>,
    /// to hold a list of child addresses of <see cref="DbAddressList.IDbAddressList"/> but with addition of
    /// handling the updates to addresses.
    /// </summary>
    public readonly ref struct Level0(ref DbAddressList.Of2048 addresses)
    {
        private const int Level = 0;
        private readonly ref DbAddressList.Of2048 _addresses = ref addresses;

        private bool TryGet(IPageResolver batch, uint at, scoped in NibblePath key, Type type,
            out ReadOnlySpan<byte> result)
        {
            var (next, index) = GetIndex(at, Level);

            var addr = _addresses[index];
            if (addr.IsNull)
            {
                result = default;
                return false;
            }

            return Level1Page.Wrap(batch.GetAt(addr))
                .TryGet(batch, next, key, type, out result);
        }

        private void Set(IBatchContext batch, uint at, in NibblePath key, Type type, in ReadOnlySpan<byte> data)
        {
            var (next, index) = GetIndex(at, Level);
            var addr = _addresses[index];

            if (addr.IsNull)
            {
                batch.GetNewCleanPage<Level1Page>(out addr).Set(next, key, type, data, batch);
                _addresses[index] = addr;
                return;
            }

            // The page exists, update
            var updated = Level1Page.Wrap(batch.GetAt(addr)).Set(next, key, type, data, batch);
            _addresses[index] = batch.GetAddress(updated);
        }

        public void Accept(IPageVisitor visitor, IPageResolver resolver)
        {
            using var scope = visitor.Scope(nameof(StorageFanOut));

            for (var i = 0; i < FanOut; i++)
            {
                var addr = _addresses[i];
                if (!addr.IsNull)
                {
                    Level1Page.Wrap(resolver.GetAt(addr)).Accept(visitor, resolver, addr);
                }
            }
        }

        public bool TryGetId(in Keccak keccak, out uint id, IPageResolver batch)
        {
            var at = BuildIdIndex(NibblePath.FromKey(keccak), out var sliced);

            if (TryGet(batch, at, sliced, Type.Id, out var result))
            {
                id = BinaryPrimitives.ReadUInt32LittleEndian(result);
                return true;
            }

            id = default;
            return false;
        }

        public void SetId(Keccak keccak, uint id, IBatchContext batch)
        {
            var at = BuildIdIndex(NibblePath.FromKey(keccak), out var sliced);

            Span<byte> span = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(span, id);

            Set(batch, at, sliced, Type.Id, span);
        }

        // Id encoding
        private const int IdConsumedNibbles = 4;
        private const int IdNibblesToShiftUp = NibblePath.NibblePerByte * sizeof(uint) - IdConsumedNibbles;
        private const int IdShift = IdNibblesToShiftUp * NibblePath.NibbleShift;
        public const int FanOut = DbAddressList.Of2048.Count;

        private static uint BuildIdIndex(in NibblePath path, out NibblePath sliced)
        {
            // Combined 1024 at level0 + 64 at level 1 for 
            Debug.Assert(DbAddressList.Of1024.Length * DbAddressList.Of64.Length == 16 * 16 * 16 * 16,
                "Should combine properly");

            sliced = path.SliceFrom(IdConsumedNibbles);

            var combined = (path.Nibble0 << (NibblePath.NibbleShift * 3)) +
                           (path.GetAt(1) << (NibblePath.NibbleShift * 2)) +
                           (path.GetAt(2) << (NibblePath.NibbleShift * 1)) +
                           path.GetAt(IdConsumedNibbles - 1);

            return (uint)combined << IdShift;
        }

        /// <summary>
        /// A counterpart to <see cref="BuildIdIndex"/>.
        /// </summary>
        public static int NormalizeAtForId(uint at) => (int)(at >> IdShift);

        public bool TryGetStorage(uint id, scoped in NibblePath path, out ReadOnlySpan<byte> result,
            IReadOnlyBatchContext batch) =>
            TryGet(batch, id, path, Type.Storage, out result);

        public void SetStorage(uint id, scoped in NibblePath path, ReadOnlySpan<byte> data, IBatchContext batch)
        {
            Set(batch, id, path, Type.Storage, data);
        }

        public void DeleteStorageByPrefix(uint id, scoped in NibblePath prefix, IBatchContext batch)
        {
            var (next, index) = GetIndex(id, Level);
            var addr = _addresses[index];

            if (addr.IsNull)
            {
                return;
            }

            // The page exists, update
            _addresses[index] =
                batch.GetAddress(Level1Page.Wrap(batch.GetAt(addr)).DeleteByPrefix(next, prefix, batch));
        }
    }

    /// <summary>
    /// Represents a fan out for:
    /// - ids with <see cref="DbAddressList.Of4"/>
    /// - storage with <see cref="DbAddressList.Of1024"/>
    /// </summary>
    /// <param name="page"></param>
    [method: DebuggerStepThrough]
    public readonly unsafe struct Level1Page(Page page) : IPage<Level1Page>
    {
        private const int Level = 1;

        public static Level1Page Wrap(Page page) => Unsafe.As<Page, Level1Page>(ref page);
        public static PageType DefaultType => PageType.FanOutPage;

        private ref PageHeader Header => ref page.Header;

        private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

        public void Clear()
        {
            Data.Ids.Clear();
            Data.Storage.Clear();
        }

        public bool IsClean => Data.Ids.IsClean & Data.Storage.IsClean;

        public bool TryGet(IPageResolver batch, uint at, scoped in NibblePath key, Type type,
            out ReadOnlySpan<byte> result)
        {
            DbAddress addr;

            if (type == Type.Id)
            {
                addr = Data.Ids[Level0.NormalizeAtForId(at)];
                if (addr.IsNull)
                {
                    result = default;
                    return false;
                }

                return batch.GetAt(addr).TryGet(batch, key, out result);
            }

            Debug.Assert(type == Type.Storage);

            var (next, index) = GetIndex(at, Level);
            addr = Data.Storage[index];

            if (addr.IsNull)
            {
                result = default;
                return false;
            }

            var localKey = BuildLocalKey(key, (byte)next, stackalloc byte[LocalKeySize]);
            var child = batch.GetAt(addr);

            return child.TryGet(batch, localKey, out result);
        }

        public Page Set(uint at, in NibblePath key, Type type, in ReadOnlySpan<byte> data, IBatchContext batch)
        {
            if (Header.BatchId != batch.BatchId)
            {
                // the page is from another batch, meaning, it's readonly. Copy
                var writable = batch.GetWritableCopy(page);
                return new Level1Page(writable).Set(at, key, type, data, batch);
            }

            DbAddress addr;

            if (type == Type.Id)
            {
                var normalized = Level0.NormalizeAtForId(at);
                addr = Data.Ids[normalized];

                var p = addr.IsNull
                    ? batch.GetNewCleanPage<BottomPage>(out addr).AsPage()
                    : batch.EnsureWritableCopy(ref addr);

                Data.Ids[normalized] = addr;

                p.Set(key, data, batch);

                return page;
            }

            Debug.Assert(type == Type.Storage);

            var (next, index) = GetIndex(at, Level);
            addr = Data.Storage[index];

            var child = addr.IsNull
                ? batch.GetNewCleanPage<BottomPage>(out addr, StartLevel).AsPage()
                : batch.EnsureWritableCopy(ref addr);

            Data.Storage[index] = addr;

            var localKey = BuildLocalKey(key, (byte)next, stackalloc byte[LocalKeySize]);

            Debug.Assert(batch.WasWritten(addr));

            child.Set(localKey, data, batch);

            return page;
        }

        public Page DeleteByPrefix(uint at, in NibblePath prefix, IBatchContext batch)
        {
            if (Header.BatchId != batch.BatchId)
            {
                // the page is from another batch, meaning, it's readonly.
                var writable = batch.GetWritableCopy(page);
                return new Level1Page(writable).DeleteByPrefix(at, prefix, batch);
            }

            var (next, index) = GetIndex(at, Level);

            var addr = Data.Storage[index];

            if (addr.IsNull)
            {
                return page;
            }

            var child = batch.EnsureWritableCopy(ref addr);
            Data.Storage[index] = addr;

            var localPrefix = BuildLocalKey(prefix, (byte)next, stackalloc byte[LocalKeySize]);

            child.DeleteByPrefix(localPrefix, batch);

            return page;
        }

        public void Accept(IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
        {
            var builder = new NibblePath.Builder(stackalloc byte[NibblePath.Builder.DecentSize]);

            using var scope = visitor.On(ref builder, this, addr);

            using (visitor.Scope(ScopeIds))
            {
                for (var i = 0; i < DbAddressList.Of64.Count; i++)
                {
                    var bucket = Data.Ids[i];

                    if (!bucket.IsNull)
                    {
                        resolver.GetAt(bucket).Accept(ref builder, visitor, resolver, bucket);
                    }
                }
            }

            using (visitor.Scope(ScopeStorage))
            {
                for (var i = 0; i < FanOut; i++)
                {
                    var bucket = Data.Storage[i];
                    if (!bucket.IsNull)
                    {
                        resolver.GetAt(bucket).Accept(ref builder, visitor, resolver, bucket);
                    }
                }
            }

            builder.Dispose();
        }

        private const int LocalKeySize = NibblePath.KeccakNibbleCount + 2;

        /// <summary>
        /// The path oddity that is used for the local keys so that the concatenated with ease.
        /// </summary>
        /// <remarks>Not used as the level of the child page. This might be confusing at first,
        /// but we want to have the DataPage starting at even number so that it can fan out with ease.</remarks>
        private const int PathOddity = 1;

        public const int LocalKeyNibbles = 1;

        /// <summary>
        /// <see cref="PathOddity"/>
        /// </summary>
        private const int StartLevel = 0;

        public const int FanOut = DbAddressList.Of2048.Count;

        private static NibblePath BuildLocalKey(in NibblePath key, byte bucket, scoped Span<byte> workingSet)
        {
            return NibblePath.Single(bucket, PathOddity).Append(key, workingSet);
        }

        [StructLayout(LayoutKind.Explicit, Size = Size)]
        private struct Payload
        {
            private const int Size = Page.PageSize - PageHeader.Size;

            /// <summary>
            /// Ids are mapped using a single half-nibble
            /// </summary>
            [FieldOffset(0)] public DbAddressList.Of64 Ids;

            /// <summary>
            /// Storage is mapped further by another 2.5 nibble, making it 5 in total.
            /// </summary>
            [FieldOffset(DbAddressList.Of64.Size)] public DbAddressList.Of2048 Storage;
        }
    }
}