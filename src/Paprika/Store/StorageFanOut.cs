using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// Components responsible for fanning out storage heavily. This means both, id mapping as well as the actual storage.
/// </summary>
public static class StorageFanOut
{
    public const string ScopeIds = "Ids";
    public const string ScopeStorage = "Storage";

    private enum Type
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

    /// <summary>
    /// Provides a convenient data structure for <see cref="RootPage"/>,
    /// to hold a list of child addresses of <see cref="DbAddressList.IDbAddressList"/> but with addition of
    /// handling the updates to addresses.
    /// </summary>
    public readonly ref struct Level0(ref DbAddressList.Of1024 addresses)
    {
        private readonly ref DbAddressList.Of1024 _addresses = ref addresses;

        private bool TryGet(IReadOnlyBatchContext batch, uint at, scoped in NibblePath key, Type type,
            out ReadOnlySpan<byte> result)
        {
            var (next, index) = GetIndex(at);

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
            var (next, index) = GetIndex(at);
            var addr = _addresses[index];

            if (addr.IsNull)
            {
                var newPage = batch.GetNewPage(out addr, true);
                _addresses[index] = addr;

                newPage.Header.PageType = PageType.FanOutPage;
                newPage.Header.Level = 0;

                Level1Page.Wrap(newPage).Set(next, key, type, data, batch);
                return;
            }

            // The page exists, update
            var updated = Level1Page.Wrap(batch.GetAt(addr)).Set(next, key, type, data, batch);
            _addresses[index] = batch.GetAddress(updated);
        }

        private static (uint next, int index) GetIndex(uint at) =>
            ((uint next, int index))Math.DivRem(at, DbAddressList.Of1024.Length);

        public void Accept(IPageVisitor visitor, IPageResolver resolver)
        {
            using var scope = visitor.Scope(nameof(StorageFanOut));

            for (var i = 0; i < DbAddressList.Of1024.Count; i++)
            {
                var addr = _addresses[i];
                if (!addr.IsNull)
                {
                    Level1Page.Wrap(resolver.GetAt(addr)).Accept(visitor, resolver, addr);
                }
            }
        }

        public bool TryGetId(in Keccak keccak, out uint id, IReadOnlyBatchContext batch)
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

        private static uint BuildIdIndex(in NibblePath path, out NibblePath sliced)
        {
            // Combined 1024 at level0 + 64 at level 1 for 
            Debug.Assert(DbAddressList.Of1024.Length * DbAddressList.Of64.Length == 16 * 16 * 16 * 16,
                "Should combine properly");

            sliced = path.SliceFrom(4);

            return (uint)
                ((path.Nibble0 << (NibblePath.NibbleShift * 3)) +
                 (path.GetAt(1) << (NibblePath.NibbleShift * 2)) +
                 (path.GetAt(2) << (NibblePath.NibbleShift * 1)) +
                 path.GetAt(3));
        }

        public bool TryGetStorage(uint id, scoped in NibblePath path, out ReadOnlySpan<byte> result,
            IReadOnlyBatchContext batch) =>
            TryGet(batch, id, path, Type.Storage, out result);

        public void SetStorage(uint id, scoped in NibblePath path, ReadOnlySpan<byte> data, IBatchContext batch)
        {
            Set(batch, id, path, Type.Storage, data);
        }

        public void DeleteStorageByPrefix(uint id, scoped in NibblePath prefix, IBatchContext batch)
        {
            var (next, index) = GetIndex(id);
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
    private readonly unsafe struct Level1Page(Page page) : IPage
    {
        public static Level1Page Wrap(Page page) => Unsafe.As<Page, Level1Page>(ref page);

        private ref PageHeader Header => ref page.Header;

        private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

        public bool TryGet(IReadOnlyBatchContext batch, uint at, scoped in NibblePath key, Type type,
            out ReadOnlySpan<byte> result)
        {
            batch.AssertRead(Header);

            DbAddress addr;

            if (type == Type.Id)
            {
                addr = Data.Ids[(int)at];
                if (addr.IsNull)
                {
                    result = default;
                    return false;
                }

                return DataPage.Wrap(batch.GetAt(addr)).TryGet(batch, key, out result);
            }

            Debug.Assert(type == Type.Storage);

            var (next, index) = GetStorageIndex(at);
            addr = Data.Storage[index];

            if (addr.IsNull)
            {
                result = default;
                return false;
            }

            return Level2Page.Wrap(batch.GetAt(addr)).TryGet(batch, next, key, out result);
        }

        private static (uint next, int index) GetStorageIndex(uint at) =>
            ((uint next, int index))Math.DivRem(at, DbAddressList.Of1024.Length);

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
                addr = Data.Ids[(int)at];
                if (addr.IsNull)
                {
                    var p = batch.GetNewPage(out addr, false);
                    p.Header.PageType = PageType.DataPage;
                    p.Header.Level = 0;

                    new DataPage(p).Clear();
                }

                Data.Ids[(int)at] = batch.GetAddress(DataPage.Wrap(batch.GetAt(addr)).Set(key, data, batch));
                return page;
            }

            Debug.Assert(type == Type.Storage);

            var (next, index) = GetStorageIndex(at);
            addr = Data.Storage[index];

            if (addr.IsNull)
            {
                var p = batch.GetNewPage(out addr, false);
                p.Header.PageType = PageType.FanOutPage;

                var ids = new Level2Page(p);
                ids.Clear();
            }

            Data.Storage[(int)at] = batch.GetAddress(Level2Page.Wrap(batch.GetAt(addr)).Set(next, key, data, batch));

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

            var (next, index) = GetStorageIndex(at);

            var addr = Data.Storage[index];

            if (addr.IsNull)
            {
                return page;
            }

            // update after set
            Data.Storage[index] =
                batch.GetAddress(Level2Page.Wrap(batch.GetAt(addr)).DeleteByPrefix(next, prefix, batch));

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
                        DataPage.Wrap(resolver.GetAt(bucket)).Accept(ref builder, visitor, resolver, bucket);
                    }
                }
            }

            using (visitor.Scope(ScopeStorage))
            {
                for (var i = 0; i < DbAddressList.Of1024.Count; i++)
                {
                    var bucket = Data.Storage[i];
                    if (!bucket.IsNull)
                    {
                        Level2Page.Wrap(resolver.GetAt(bucket)).Accept(ref builder, visitor, resolver, bucket);
                    }
                }
            }

            builder.Dispose();
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
            [FieldOffset(DbAddressList.Of64.Size)] public DbAddressList.Of1024 Storage;
        }
    }

    [method: DebuggerStepThrough]
    public readonly unsafe struct Level2Page(Page page) : IPage
    {
        public static Level2Page Wrap(Page page) => Unsafe.As<Page, Level2Page>(ref page);

        public void Clear() => Data.Addresses.Clear();

        private ref PageHeader Header => ref page.Header;

        private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

        private static (uint next, int index) GetStorageIndex(uint at) =>
            ((uint next, int index))Math.DivRem(at, DbAddressList.Of1024.Length);

        public bool TryGet(IReadOnlyBatchContext batch, uint at, scoped in NibblePath key,
            out ReadOnlySpan<byte> result)
        {
            var (next, index) = GetStorageIndex(at);

            var addr = Data.Addresses[index];
            if (addr.IsNull)
            {
                result = default;
                return false;
            }

            return Level3Page.Wrap(batch.GetAt(addr)).TryGet(batch, next, key, out result);
        }

        public Page Set(uint at, in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
        {
            if (Header.BatchId != batch.BatchId)
            {
                // the page is from another batch, meaning, it's readonly. Copy
                var writable = batch.GetWritableCopy(page);
                return new Level2Page(writable).Set(at, key, data, batch);
            }

            var (next, index) = GetStorageIndex(at);
            var addr = Data.Addresses[index];

            if (addr.IsNull)
            {
                var p = batch.GetNewPage(out addr, false);
                p.Header.PageType = PageType.FanOutPage;

                new Level3Page(p).Clear();
            }

            Data.Addresses[(int)at] = batch.GetAddress(Level3Page.Wrap(batch.GetAt(addr)).Set(next, key, data, batch));

            return page;
        }

        public Page DeleteByPrefix(uint at, in NibblePath prefix, IBatchContext batch)
        {
            if (Header.BatchId != batch.BatchId)
            {
                // the page is from another batch, meaning, it's readonly. Copy
                var writable = batch.GetWritableCopy(page);
                return new Level2Page(writable).DeleteByPrefix(at, prefix, batch);
            }

            var (next, index) = GetStorageIndex(at);
            var addr = Data.Addresses[index];

            if (addr.IsNull)
            {
                return page;
            }

            Data.Addresses[(int)at] =
                batch.GetAddress(Level3Page.Wrap(batch.GetAt(addr)).DeleteByPrefix(next, prefix, batch));

            return page;
        }

        public void Accept(ref NibblePath.Builder builder, IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
        {
            resolver.Prefetch(Data.Addresses);

            using var scope = visitor.On(ref builder, this, addr);

            for (var i = 0; i < DbAddressList.Of1024.Length; i++)
            {
                var bucket = Data.Addresses[i];

                if (!bucket.IsNull)
                {
                    Level3Page.Wrap(resolver.GetAt(bucket)).Accept(ref builder, visitor, resolver, bucket);
                }
            }
        }

        [StructLayout(LayoutKind.Explicit, Size = Size)]
        private struct Payload
        {
            private const int Size = Page.PageSize - PageHeader.Size;

            [FieldOffset(0)] public DbAddressList.Of1024 Addresses;
        }
    }

    [method: DebuggerStepThrough]
    public readonly unsafe struct Level3Page(Page page) : IPage
    {
        public static Level3Page Wrap(Page page) => Unsafe.As<Page, Level3Page>(ref page);

        private ref PageHeader Header => ref page.Header;

        private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

        public void Clear()
        {
            // ref var iteration to do not clear copies!
            foreach (ref var bucket in Data.Buckets)
            {
                bucket.Clear();
            }
        }

        public bool TryGet(IReadOnlyBatchContext batch, uint at, in NibblePath key, out ReadOnlySpan<byte> result)
        {
            return Data.Buckets[(int)at].TryGet(batch, key, out result);
        }

        public Page Set(uint at, in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
        {
            if (Header.BatchId != batch.BatchId)
            {
                // the page is from another batch, meaning, it's readonly. Copy
                var writable = batch.GetWritableCopy(page);
                return new Level3Page(writable).Set(at, key, data, batch);
            }

            Data.Buckets[(int)at].Set(key, data, batch);

            return page;
        }

        public Page DeleteByPrefix(uint at, in NibblePath prefix, IBatchContext batch)
        {
            if (Header.BatchId != batch.BatchId)
            {
                // the page is from another batch, meaning, it's readonly. Copy
                var writable = batch.GetWritableCopy(page);
                return new Level3Page(writable).DeleteByPrefix(at, prefix, batch);
            }

            Data.Buckets[(int)at].DeleteByPrefix(prefix, batch);

            return page;
        }

        [StructLayout(LayoutKind.Explicit, Size = Size)]
        private struct Payload
        {
            private const int Size = Page.PageSize - PageHeader.Size;

            [FieldOffset(Bucket.Size * 0)] private Bucket Bucket0;

            public Span<Bucket> Buckets => MemoryMarshal.CreateSpan(ref Bucket0, Size / Bucket.Size);
        }

        [StructLayout(LayoutKind.Explicit, Size = Size)]
        private struct Bucket
        {
            public const int Size = 1016;
            private const int DataSize = Size - DbAddress.Size;

            [FieldOffset(0)] public DbAddress Root;

            [FieldOffset(DbAddress.Size)] private byte _first;

            private SlottedArray Map => new(MemoryMarshal.CreateSpan(ref _first, DataSize));

            public void Clear()
            {
                Root = default;
                Map.Clear();
            }

            public bool TryGet(IReadOnlyBatchContext batch, in NibblePath key, out ReadOnlySpan<byte> result)
            {
                if (Map.TryGet(key, out result))
                {
                    return true;
                }

                if (Root.IsNull)
                    return false;

                return new DataPage(batch.GetAt(Root)).TryGet(batch, key, out result);
            }

            public void Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
            {
                if (Map.TrySet(key, data))
                    return;

                if (Root.IsNull)
                {
                    var page = batch.GetNewPage(out Root, false);
                    page.Header.PageType = PageType.DataPage;
                    page.Header.Level = 0;

                    // clear
                    new DataPage(page).Clear();
                }

                var root = new DataPage(batch.GetAt(Root));

                if (batch.WasWritten(Root))
                {
                    // written this batch, write through
                    Map.Delete(key);
                    root.Set(key, data, batch);
                    return;
                }

                // COW
                root = new DataPage(batch.EnsureWritableCopy(ref Root));

                foreach (var item in Map.EnumerateAll())
                {
                    var result = root.Set(item.Key, item.RawData, batch);
                    Debug.Assert(result.Raw == root.AsPage().Raw);
                }

                // Clear map, all copied
                Map.Clear();

                // Set below
                root.Set(key, data, batch);
            }

            public void DeleteByPrefix(in NibblePath prefix, IBatchContext batch)
            {
                Map.DeleteByPrefix(prefix);

                if (Root.IsNull)
                    return;

                Root = batch.GetAddress(new DataPage(batch.GetAt(Root)).DeleteByPrefix(prefix, batch));
            }

            public void Accept(ref NibblePath.Builder builder, IPageVisitor visitor, IPageResolver resolver)
            {
                if (Root.IsNull)
                    return;

                new DataPage(resolver.GetAt(Root)).Accept(ref builder, visitor, resolver, Root);
            }
        }

        public void Accept(ref NibblePath.Builder builder, IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
        {
            using var scope = visitor.On(ref builder, this, addr);

            foreach (ref var bucket in Data.Buckets)
            {
                bucket.Accept(ref builder, visitor, resolver);
            }
        }
    }
}