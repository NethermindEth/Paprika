namespace Paprika.Utils;

public static class ReaderWriterLockExtensions
{
    public static ReadLock Read(this ReaderWriterLockSlim rwl)
    {
        rwl.EnterReadLock();
        return new ReadLock(rwl);
    }

    public static WriteLock Write(this ReaderWriterLockSlim rwl)
    {
        rwl.EnterWriteLock();
        return new WriteLock(rwl);
    }

    public readonly ref struct ReadLock(ReaderWriterLockSlim rwl)
    {
        public void Dispose() => rwl.ExitReadLock();
    }

    public readonly ref struct WriteLock(ReaderWriterLockSlim rwl)
    {
        public void Dispose() => rwl.ExitWriteLock();
    }
}