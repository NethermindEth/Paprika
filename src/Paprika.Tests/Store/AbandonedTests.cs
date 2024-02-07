// using FluentAssertions;
// using NUnit.Framework;
// using Paprika.Chain;
// using Paprika.Crypto;
// using Paprika.Store;
//
// namespace Paprika.Tests.Store;
//
// public class AbandonedTests : BasePageTests
// {
//     [Test]
//     public void EmptyList()
//     {
//         var list = new AbandonedList();
//         list.TryGet(out _, 1, null!)
//             .Should().BeFalse();
//     }
//
//     class BatchContext(uint batchId, PageManager manager) : BatchContextBase(batchId)
//     {
//         public override Page GetAt(DbAddress address) => manager.GetAt(address);
//
//         public override DbAddress GetAddress(Page page) => manager.GetAddress(page);
//
//         public override Page GetNewPage(out DbAddress addr, bool clear) => manager.GetNewPage(out addr, clear);
//
//         public override bool WasWritten(DbAddress addr)
//         {
//             throw new NotImplementedException();
//         }
//
//         public override void RegisterForFutureReuse(Page page)
//         {
//             throw new NotImplementedException();
//         }
//
//         public override Dictionary<Keccak, uint> IdCache => throw new NotImplementedException();
//     }
//
//     class PageManager : IDisposable
//     {
//         private readonly BufferPool _pool = new(1024);
//         
//         private Page[] _pages = Array.Empty<Page>();
//         private int _new;
//         
//         public Page GetAt(DbAddress address) => _pages[address.Raw];
//
//         public DbAddress GetAddress(Page page) => new((uint)_pages.AsSpan().IndexOf(page));
//
//         public Page GetNewPage(out DbAddress addr, bool clear)
//         {
//             _new++;
//             if (_new >= _pages.Length)
//             {
//                 const int chunk = 32;
//                 Array.Resize(ref _pages, _pages.Length + chunk);
//
//                 for (int i = 0; i < chunk; i++)
//                 {
//                     _pages[^(1 + i)] = _pool.Rent(clear);
//                 }
//                 
//             }
//
//             addr = new DbAddress((uint)_new);
//             return _pages[_new];
//         }
//
//         public void Dispose()
//         {
//             foreach (var page in _pages)
//             {
//                 _pool.Return(page);
//             }
//             
//             _pool.Dispose();
//         }
//     }
//
//     // private const int BatchId = 2;
//     //
//     // [Test]
//     // public void Simple()
//     // {
//     //     var batch = NewBatch(BatchId);
//     //     var abandoned = new AbandonedPage(batch.GetNewPage(out var addr, true));
//     //
//     //     const int fromPage = 13;
//     //     const int count = 1000;
//     //
//     //     var pages = new HashSet<uint>();
//     //
//     //     for (uint i = 0; i < count; i++)
//     //     {
//     //         var page = i + fromPage;
//     //         pages.Add(page);
//     //
//     //         abandoned.EnqueueAbandoned(batch, addr, DbAddress.Page(page));
//     //     }
//     //
//     //     for (uint i = 0; i < count; i++)
//     //     {
//     //         abandoned.TryDequeueFree(out var page).Should().BeTrue();
//     //         pages.Remove(page).Should().BeTrue($"Page {page} should have been written first");
//     //     }
//     //
//     //     pages.Should().BeEmpty();
//     // }
// }