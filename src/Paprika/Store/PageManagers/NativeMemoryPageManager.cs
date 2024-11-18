using System.Runtime.InteropServices;

namespace Paprika.Store.PageManagers;

public sealed unsafe class NativeMemoryPageManager : PointerPageManager
{
    public NativeMemoryPageManager(long size, byte historyDepth) : base(size)
    {
        Ptr = NativeMemory.AlignedAlloc((UIntPtr)size, (UIntPtr)Page.PageSize);

        // clear first pages to make it clean
        for (var i = 0; i < historyDepth; i++)
        {
            GetAt(new DbAddress((uint)i)).Clear();
        }
    }

    protected override void* Ptr { get; }

    public override void Flush()
    {
    }

    public override void ForceFlush()
    {
    }

    public override void Dispose() => NativeMemory.AlignedFree(Ptr);

    public override ValueTask WritePages(ICollection<DbAddress> addresses, CommitOptions options) =>
        ValueTask.CompletedTask;

    protected override void ReportCopyTime(TimeSpan elapsed) { }

    public override ValueTask WriteRootPage(DbAddress rootPage, CommitOptions options) => ValueTask.CompletedTask;
}
