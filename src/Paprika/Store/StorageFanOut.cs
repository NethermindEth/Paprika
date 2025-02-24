using System.Buffers.Binary;
using System.Diagnostics;
using System.Drawing;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Store;

/// <summary>
/// This class is responsible for providing a good fan out behavior for storage trees. This is done in the following manner:
///
/// 1. <see cref="Level0"/> embedded in <see cref="RootPage"/> provides an initial fan out of <see cref="DbAddressList.Of1024"/> 
/// 2. <see cref="Level1Page"/> as a separate page provides an additional fan out of <see cref="DbAddressList.Of1024"/>, giving ~ 1 million of buckets
/// 3. <see cref="Level2Page"/> acts as a mapping of the keccak to a bucket and provides a small map that amortizes initial writes.
///
/// This should allow handling lots of contracts with a good spread at the top, with locally assigned identifiers.
/// This is different from the previous design that required walking to find the keccak mapping first only then to search for the page.
/// With this, min lookup number for an account is 2 (level0 is in the root, which gives two page's lookups for level 1 and 2).
/// For bigger trees it will be nested more, but still on the same path, reducing the lookups by a few pages that previously were used for the mapping. 
/// </summary>
public static class StorageFanOut
{
    public const int LevelCount = 3;

    public const string ScopeStorage = "Storage";

    private static (int level0, int level1) Split(Keccak account)
    {
        const int mask = Level0.FanOut * Level1Page.FanOut - 1;

        var level0 = Math.DivRem(account.GetHashCode() & mask, Level0.FanOut, out var level1);
        return (level0, level1);
    }

    /// <summary>
    /// Provides a convenient data structure for <see cref="RootPage"/>,
    /// to hold a list of child addresses of <see cref="DbAddressList.IDbAddressList"/> but with addition of
    /// handling the updates to addresses.
    /// </summary>
    public readonly ref struct Level0(ref DbAddressList.Of1024 addresses)
    {
        public const int FanOut = DbAddressList.Of1024.Count;

        private readonly ref DbAddressList.Of1024 _addresses = ref addresses;

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

        public bool TryGetStorage(scoped in Keccak account, scoped in NibblePath storage,
            out ReadOnlySpan<byte> result,
            IReadOnlyBatchContext batch)
        {
            var (level0, level1) = Split(account);

            var addr = _addresses[level0];
            if (addr.IsNull)
            {
                result = default;
                return false;
            }

            return Level1Page.Wrap(batch.GetAt(addr))
                .TryGet(batch, level1, account, storage, out result);
        }

        public void SetStorage(scoped in Keccak account, scoped in NibblePath storage,
            ReadOnlySpan<byte> data, IBatchContext batch)
        {
            var (level0, level1) = Split(account);
            var addr = _addresses[level0];

            // Ensure writable to flatten the stack
            var l1 = addr.IsNull
                ? batch.GetNewCleanPage<Level1Page>(out addr)
                : Level1Page.Wrap(batch.EnsureWritableCopy(ref addr));

            // The page exists, update
            l1.Set(level1, account, storage, data, batch);
            _addresses[level0] = addr;
        }

        public void DeleteStorageByPrefix(scoped in Keccak account, scoped in NibblePath prefix,
            IBatchContext batch)
        {
            var (level0, level1) = Split(account);
            var addr = _addresses[level0];

            if (addr.IsNull)
            {
                return;
            }

            // The page exists, update
            _addresses[level0] =
                batch.GetAddress(Level1Page.Wrap(batch.GetAt(addr)).DeleteByPrefix(level1, account, prefix, batch));
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
        public const int FanOut = DbAddressList.Of1024.Count;

        public static Level1Page Wrap(Page page) => Unsafe.As<Page, Level1Page>(ref page);
        public static PageType DefaultType => PageType.FanOutPage;

        private ref PageHeader Header => ref page.Header;

        private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

        public void Clear()
        {
            Data.Storage.Clear();
        }

        public bool IsClean => Data.Storage.IsClean;

        public bool TryGet(IPageResolver batch, int at, scoped in Keccak account, scoped in NibblePath storage,
            out ReadOnlySpan<byte> result)
        {
            var addr = Data.Storage[at];

            if (addr.IsNull)
            {
                result = default;
                return false;
            }

            return Level2Page.Wrap(batch.GetAt(addr)).TryGet(batch, account, storage, out result);
        }

        public Page Set(int at, in Keccak account, in NibblePath storage, in ReadOnlySpan<byte> data,
            IBatchContext batch)
        {
            if (Header.BatchId != batch.BatchId)
            {
                // the page is from another batch, meaning, it's readonly. Copy
                var writable = batch.GetWritableCopy(page);
                return new Level1Page(writable).Set(at, account, storage, data, batch);
            }

            var addr = Data.Storage[at];

            // Ensure writable before the call to flatten the stack
            var l2 = addr.IsNull
                ? batch.GetNewCleanPage<Level2Page>(out addr)
                : Level2Page.Wrap(batch.EnsureWritableCopy(ref addr));

            l2.Set(account, storage, data, batch);

            Debug.Assert(batch.WasWritten(addr));
            Data.Storage[at] = addr;
            return page;
        }

        public Page DeleteByPrefix(int at, scoped in Keccak account, scoped in NibblePath prefix, IBatchContext batch)
        {
            if (Header.BatchId != batch.BatchId)
            {
                // the page is from another batch, meaning, it's readonly.
                var writable = batch.GetWritableCopy(page);
                return new Level1Page(writable).DeleteByPrefix(at, account, prefix, batch);
            }

            var addr = Data.Storage[at];

            if (addr.IsNull)
            {
                return page;
            }

            // update after set
            Data.Storage[at] =
                batch.GetAddress(Level2Page.Wrap(batch.GetAt(addr)).DeleteByPrefix(account, prefix, batch));

            return page;
        }

        public void Accept(IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
        {
            var builder = new NibblePath.Builder(stackalloc byte[NibblePath.Builder.DecentSize]);

            using var scope = visitor.On(ref builder, this, addr);

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
            /// Storage is mapped further by another 2.5 nibble, making it 5 in total.
            /// </summary>
            [FieldOffset(DbAddressList.Of64.Size)] public DbAddressList.Of1024 Storage;
        }
    }

    [method: DebuggerStepThrough]
    public readonly unsafe struct Level2Page(Page page) : IPage<Level2Page>
    {
        public static Level2Page Wrap(Page page) => Unsafe.As<Page, Level2Page>(ref page);
        public static PageType DefaultType => PageType.FanOutPage;

        public void Clear()
        {
            Data.Accounts.Clear();
            new SlottedArray(Data.DataSpan).Clear();
            Data.Child = default;
            Data.Overflow = default;
        }

        public bool IsClean =>
            Data.Child.IsNull &&
            Data.Overflow.IsNull &&
            new SlottedArray(Data.DataSpan).IsEmpty &&
            Data.Accounts.IndexOfAnyExcept(Keccak.Zero) < 0;

        private ref PageHeader Header => ref page.Header;

        private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

        [SkipLocalsInit]
        public bool TryGet(IPageResolver batch, scoped in Keccak account, scoped in NibblePath storage,
            out ReadOnlySpan<byte> result)
        {
            var slot = FindAccountSlot(account);

            if (slot == BucketsFull && Data.Overflow.IsNull == false)
            {
                return new Level2Page(batch.GetAt(Data.Overflow)).TryGet(batch, account, storage, out result);
            }

            if (slot < 0)
            {
                result = default;
                return false;
            }

            Span<byte> workingSet = stackalloc byte[BuildKeyAllocSize];
            var key = BuildKey(slot, storage, workingSet);

            if (Data.Child.IsNull)
            {
                return new SlottedArray(Data.DataSpan).TryGet(key, out result);
            }

            var child = batch.GetAt(Data.Child);

            return child.Header.PageType == PageType.Bottom
                ? new BottomPage(child).TryGet(batch, key, out result)
                : new DataPage(child).TryGet(batch, key, out result);
        }


        private const int BuildKeyAllocSize = NibblePath.FullKeccakByteLength + 2;

        private static NibblePath BuildKey(int slot, in NibblePath storage, Span<byte> workingSet)
        {
            Debug.Assert(slot < 256);
            return NibblePath.DoubleEven((byte)slot)
                .Append(storage, workingSet);
        }

        public Page Set(in Keccak account, in NibblePath storage, in ReadOnlySpan<byte> data, IBatchContext batch)
        {
            if (Header.BatchId != batch.BatchId)
            {
                // the page is from another batch, meaning, it's readonly. Copy
                var writable = batch.GetWritableCopy(page);
                return new Level2Page(writable).Set(account, storage, data, batch);
            }

            var slot = FindAccountSlot(account);

            if (slot == BucketsFull)
            {
                if (Data.Overflow.IsNull)
                    batch.GetNewPage<Level2Page>(out Data.Overflow).Set(account, storage, data, batch);
                else
                    new Level2Page(batch.EnsureWritableCopy(ref Data.Overflow)).Set(account, storage, data, batch);

                return page;
            }

            if (slot < 0)
            {
                // The account does not exist yet, but there's space for it.
                slot = ~slot;

                // Set account
                Data.Accounts[slot] = account;
                // The account is allocated now, move forward with setting the value.
            }

            Span<byte> workingSet = stackalloc byte[BuildKeyAllocSize];
            var key = BuildKey(slot, storage, workingSet);

            var accounts = MemoryMarshal.Cast<Keccak, byte>(Data.Accounts);
            Debug.Assert(Unsafe.IsAddressLessThan(ref accounts[^1], ref Data.DataSpan[0]));

            if (Data.Child.IsNull)
            {
                var map = new SlottedArray(Data.DataSpan);

                if (map.TrySet(key, data))
                {
                    return page;
                }

                var destination = batch.GetNewPage<BottomPage>(out Data.Child).Map;
                map.MoveNonEmptyKeysTo<NibbleSelector.All>(destination);

                // Will always fit
                destination.TrySet(key, data);
                return page;
            }

            var child = Data.Child.IsNull
                ? batch.GetNewPage<BottomPage>(out Data.Child).AsPage()
                : batch.EnsureWritableCopy(ref Data.Child);

            // It has its own page, use it
            if (child.Header.PageType == PageType.Bottom)
                new BottomPage(child).Set(key, data, batch);
            else
                new DataPage(child).Set(key, data, batch);

            return page;
        }

        public Page DeleteByPrefix(scoped in Keccak account, scoped in NibblePath prefix, IBatchContext batch)
        {
            if (Header.BatchId != batch.BatchId)
            {
                // the page is from another batch, meaning, it's readonly. Copy
                var writable = batch.GetWritableCopy(page);
                return new Level2Page(writable).DeleteByPrefix(account, prefix, batch);
            }

            var slot = FindAccountSlot(account);

            if (slot == BucketsFull)
            {
                if (Data.Overflow.IsNull)
                    batch.GetNewPage<Level2Page>(out Data.Overflow).DeleteByPrefix(account, prefix, batch);
                else
                    new Level2Page(batch.EnsureWritableCopy(ref Data.Overflow)).DeleteByPrefix(account, prefix, batch);

                return page;
            }

            if (slot < 0 || Data.Child.IsNull)
            {
                // Not found, nothing to delete
                return page;
            }

            Span<byte> workingSet = stackalloc byte[BuildKeyAllocSize];
            var key = BuildKey(slot, prefix, workingSet);

            if (Data.Child.IsNull)
            {
                var map = new SlottedArray(Data.DataSpan);
                map.DeleteByPrefix(key);
                return page;
            }

            var child = Data.Child.IsNull
                ? batch.GetNewPage<BottomPage>(out Data.Child).AsPage()
                : batch.EnsureWritableCopy(ref Data.Child);

            Debug.Assert(child.Header.PageType != PageType.None);

            // It has its own page, use it
            if (child.Header.PageType == PageType.Bottom)
                new BottomPage(child).DeleteByPrefix(key, batch);
            else
                new DataPage(child).DeleteByPrefix(key, batch);

            return page;
        }

        public void Accept(ref NibblePath.Builder builder, IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
        {
            resolver.Prefetch(Data.Overflow);

            using (visitor.On(ref builder, this, addr))
            {
                if (Data.Child.IsNull == false)
                {
                    var child = resolver.GetAt(Data.Child);
                    var type = child.Header.PageType;

                    switch (type)
                    {
                        case PageType.DataPage:
                            new DataPage(child).Accept(ref builder, visitor, resolver, Data.Child);
                            break;
                        case PageType.Bottom:
                            new BottomPage(child).Accept(ref builder, visitor, resolver, Data.Child);
                            break;
                        default:
                            throw new InvalidOperationException($"Invalid page type {type}");
                    }
                }

                if (Data.Overflow.IsNull)
                    return;

                var overflow = new Level2Page(resolver.GetAt(Data.Overflow));

                using (visitor.On(ref builder, overflow, Data.Overflow))
                {
                }
            }
        }

        /// <summary>
        /// Returns the non-negative index for existing ones and a bitwise negative index for an empty slot that is found.
        /// </summary>
        /// <param name="account"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private int FindAccountSlot(in Keccak account)
        {
            const int bucketMask = Payload.BucketCount - 1;

            var hashcode = account.GetHashCode();

            // The limit of the linear search for the space.
            const int probeLimit = 16;

            for (var i = 0; i < probeLimit; i++)
            {
                var index = (hashcode + i) & bucketMask;
                var actual = Data.Accounts[index];

                if (actual == account)
                {
                    return index;
                }

                if (actual == Keccak.Zero)
                {
                    return ~index;
                }
            }

            return BucketsFull;
        }

        private const int BucketsFull = ~(Payload.BucketCount + 1);

        [StructLayout(LayoutKind.Explicit, Size = Size)]
        private struct Payload
        {
            private const int Size = Page.PageSize - PageHeader.Size;

            private const int KeccakSize = BucketCount * Keccak.Size;

            private const int DataSize = Size - KeccakSize - DbAddress.Size * 2;
            private const int DataOffset = KeccakSize + DbAddress.Size * 2;

            public const int BucketCount = 64;

            [FieldOffset(0)] private Keccak K;

            [FieldOffset(KeccakSize)]
            public DbAddress Child;

            [FieldOffset(KeccakSize + DbAddress.Size)] public DbAddress Overflow;

            [FieldOffset(DataOffset)]
            private byte b;

            public Span<Keccak> Accounts => MemoryMarshal.CreateSpan(ref K, BucketCount);

            public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref b, DataSize);
        }
    }
}