using System.Runtime.InteropServices;
using FluentAssertions;
using Paprika.Store;

namespace Paprika.Tests.Store;

/// <summary>
/// Tests for clearable components. Whether they are properly cleared.
/// </summary>
[Parallelizable(ParallelScope.None)]
public unsafe class ClearableTests : IDisposable
{
    [Test]
    public void DataPage() => TestPage<DataPage>();

    [Test]
    public void AbandonedPage() => TestPage<AbandonedPage>();

    [Test]
    public void LeafOverflowPage() => TestPage<LeafOverflowPage>();

    [Test]
    public void StorageFanOut_Level1Page() => TestPage<StorageFanOut.Level1Page>();

    [Test]
    public void StorageFanOut_Level2Page() => TestPage<StorageFanOut.Level2Page>();

    [Test]
    public void StorageFanOut_Level3Page() => TestPage<StorageFanOut.Level3Page>();

    [Test]
    public void DbAddressList_Of4() => TestDbAddressList<DbAddressList.Of4>();

    [Test]
    public void DbAddressList_Of16() => TestDbAddressList<DbAddressList.Of16>();

    [Test]
    public void DbAddressList_Of64() => TestDbAddressList<DbAddressList.Of64>();

    [Test]
    public void DbAddressList_Of256() => TestDbAddressList<DbAddressList.Of256>();

    [Test]
    public void DbAddressList_Of1024() => TestDbAddressList<DbAddressList.Of1024>();

    private static void TestDbAddressList<TList>()
        where TList : struct, DbAddressList.IDbAddressList
    {
        var list = new TList();

        var length = TList.Length;

        for (var i = 0; i < length; i++)
        {
            list[i] = new DbAddress(1);
        }

        list.Clear();

        for (var i = 0; i < length; i++)
        {
            list[i].Should().Be(DbAddress.Null);
        }
    }

    private void TestPage<TPage>()
        where TPage : struct, IPage<TPage>
    {
        var p = new Page(_page);
        p.Span.Fill(0xFF);

        var clearable = TPage.Wrap(new(_page)); ;
        clearable.Clear();

        clearable.IsClean.Should().BeTrue();
    }

    private readonly byte* _page = (byte*)NativeMemory.AlignedAlloc(Page.PageSize, (UIntPtr)Page.PageSize);

    public void Dispose() => NativeMemory.AlignedFree(_page);
}