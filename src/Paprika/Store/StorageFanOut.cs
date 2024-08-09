using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// Components responsible for fanning out storage heavily. This means both, id mapping as well as the actual storage.
/// </summary>
public static class StorageFanOut
{
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

    private const byte NibbleHalfLower = 0b0011;
    private const byte NibbleHalfHigher = 0b1100;
    private const byte NibbleHalfShift = NibblePath.NibbleShift / 2;
    private const byte TwoNibbleShift = NibblePath.NibbleShift * 2;

    /// <summary>
    /// Provides a convenient data structure for <see cref="RootPage"/>,
    /// to hold a list of child addresses of <see cref="DbAddressList.IDbAddressList"/> but with addition of
    /// handling the updates to addresses.
    /// </summary>
    public readonly ref struct Level0(ref DbAddressList.Of1024 addresses)
    {
        private readonly ref DbAddressList.Of1024 _addresses = ref addresses;

        public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, Type type,
            out ReadOnlySpan<byte> result)
        {
            var index = GetIndex(key, out var sliced);

            var addr = _addresses[index];
            if (addr.IsNull)
            {
                result = default;
                return false;
            }

            return Level1Page.Wrap(batch.GetAt(addr))
                .TryGet(batch, sliced, type, out result);
        }

        public void Set(in NibblePath key, Type type, in ReadOnlySpan<byte> data, IBatchContext batch)
        {
            var index = GetIndex(key, out var sliced);
            var addr = _addresses[index];

            if (addr.IsNull)
            {
                var newPage = batch.GetNewPage(out addr, true);
                _addresses[index] = addr;

                newPage.Header.PageType = PageType.FanOutPage;
                newPage.Header.Level = ConsumedNibbles;

                Level1Page.Wrap(newPage).Set(sliced, type, data, batch);
                return;
            }

            // The page exists, update
            var updated = Level1Page.Wrap(batch.GetAt(addr)).Set(sliced, type, data, batch);
            _addresses[index] = batch.GetAddress(updated);
        }

        private static int GetIndex(scoped in NibblePath key, out NibblePath sliced)
        {
            Debug.Assert(key.IsOdd == false);

            // Consume 2 first nibbles as raw byte, shift and add lower half
            var at = (key.UnsafeSpan << NibbleHalfShift) + (key.GetAt(2) & NibbleHalfLower);

            Debug.Assert(0 <= at && at < DbAddressList.Of1024.Count);

            sliced = key.SliceFrom(ConsumedNibbles);
            return at;
        }

        private const int ConsumedNibbles = 2;

        public void Report(IReporter reporter, IPageResolver resolver, int level, int trimmedNibbles)
        {
            var consumedNibbles = trimmedNibbles + ConsumedNibbles;

            foreach (var bucket in _addresses)
            {
                if (!bucket.IsNull)
                {
                    Level1Page.Wrap(resolver.GetAt(bucket))
                        .Report(reporter, resolver, level + 1, consumedNibbles);
                }
            }
        }

        public void Accept(IPageVisitor visitor, IPageResolver resolver)
        {
            foreach (var bucket in _addresses)
            {
                if (!bucket.IsNull)
                {
                    Level1Page.Wrap(resolver.GetAt(bucket)).Accept(visitor, resolver, bucket);
                }
            }
        }
    }

    [method: DebuggerStepThrough]
    private readonly unsafe struct Level1Page(Page page)
    {
        public static Level1Page Wrap(Page page) => Unsafe.As<Page, Level1Page>(ref page);

        private ref PageHeader Header => ref page.Header;

        private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

        public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, Type type,
            out ReadOnlySpan<byte> result)
        {
            batch.AssertRead(Header);

            var index = GetIndex(key, type, out var sliced);

            var addr = type == Type.Id ? Data.Ids[index] : Data.Storage[index];

            if (addr.IsNull)
            {
                result = default;
                return false;
            }

            return DataPage.Wrap(batch.GetAt(addr)).TryGet(batch, sliced, out result);
        }

        public Page Set(in NibblePath key, Type type, in ReadOnlySpan<byte> data, IBatchContext batch)
        {
            if (Header.BatchId != batch.BatchId)
            {
                // the page is from another batch, meaning, it's readonly. Copy
                var writable = batch.GetWritableCopy(page);
                return new Level1Page(writable).Set(key, type, data, batch);
            }

            var index = GetIndex(key, type, out var sliced);

            if (type == Type.Id)
            {
                Set(ref Data.Ids, index, sliced, data, batch, Level1ConsumedNibblesForIds);
            }
            else
            {
                Set(ref Data.Storage, index, sliced, data, batch, Level1ConsumedNibblesForStorage);
            }

            return page;
        }

        private void Set<TAddressList>(ref TAddressList list, int index, in NibblePath sliced,
            in ReadOnlySpan<byte> data,
            IBatchContext batch, int consumedNibbles)
            where TAddressList : struct, DbAddressList.IDbAddressList
        {
            var addr = list[index];

            if (addr.IsNull)
            {
                var newPage = batch.GetNewPage(out addr, true);

                list[index] = addr;

                newPage.Header.PageType = PageType.Standard;
                newPage.Header.Level = (byte)(Header.Level + consumedNibbles);

                DataPage.Wrap(newPage).Set(sliced, data, batch);
                return;
            }

            // update after set
            addr = batch.GetAddress(DataPage.Wrap(batch.GetAt(addr)).Set(sliced, data, batch));
            list[index] = addr;
        }

        private static int GetIndex(scoped in NibblePath key, Type type, out NibblePath sliced)
        {
            // Represents high part of the first nibble but lowered
            var hi = (key.FirstNibble & NibbleHalfHigher) >> NibbleHalfShift;

            Debug.Assert(0 <= hi && hi < 15);

            if (type == Type.Id)
            {
                sliced = key.SliceFrom(Level1ConsumedNibblesForIds);
                return hi;
            }

            var at = (hi << TwoNibbleShift) + // 0.5 nibble
                     (key.GetAt(1) << NibblePath.NibbleShift) + // 1 nibble 
                     key.GetAt(2); // 1 nibble
            Debug.Assert(0 <= at && at < DbAddressList.Of1024.Count);

            sliced = key.SliceFrom(Level1ConsumedNibblesForStorage);
            return at;
        }

        /// <summary>
        /// This is effectively 0.5 of the nibble as the 1.5 is consumed on the higher level.
        /// </summary>
        private const int Level1ConsumedNibblesForIds = 1;

        /// <summary>
        /// This is effectively 1.5 of the nibble as the 1.5 is consumed on the higher level.
        /// </summary>
        private const int Level1ConsumedNibblesForStorage = 3;

        public void Report(IReporter reporter, IPageResolver resolver, int pageLevel, int trimmedNibbles)
        {
            foreach (var bucket in Data.Ids)
            {
                if (!bucket.IsNull)
                {
                    DataPage.Wrap(resolver.GetAt(bucket))
                        .Report(reporter, resolver, pageLevel + 1, trimmedNibbles + Level1ConsumedNibblesForIds);
                }
            }

            foreach (var bucket in Data.Storage)
            {
                if (!bucket.IsNull)
                {
                    DataPage.Wrap(resolver.GetAt(bucket))
                        .Report(reporter, resolver, pageLevel + 1, trimmedNibbles + Level1ConsumedNibblesForStorage);
                }
            }
        }

        public void Accept(IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
        {
            using var scope = visitor.On(this, addr);

            foreach (var bucket in Data.Ids)
            {
                if (!bucket.IsNull)
                {
                    DataPage.Wrap(resolver.GetAt(bucket)).Accept(visitor, resolver, bucket);
                }
            }

            foreach (var bucket in Data.Storage)
            {
                if (!bucket.IsNull)
                {
                    DataPage.Wrap(resolver.GetAt(bucket)).Accept(visitor, resolver, bucket);
                }
            }
        }

        [StructLayout(LayoutKind.Explicit, Size = Size)]
        private struct Payload
        {
            private const int Size = Page.PageSize - PageHeader.Size;

            /// <summary>
            /// Ids are mapped using a single half-nibble
            /// </summary>
            [FieldOffset(0)] public DbAddressList.Of4 Ids;

            /// <summary>
            /// Storage is mapped further by another 2.5 nibble, making it 5 in total.
            /// </summary>
            [FieldOffset(DbAddressList.Of4.Size)] public DbAddressList.Of1024 Storage;
        }
    }
}