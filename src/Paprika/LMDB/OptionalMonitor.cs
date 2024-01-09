namespace Paprika.LMDB;

readonly ref struct OptionalMonitor
{
    private readonly object? _lock;

    public OptionalMonitor(object? @lock)
    {
        _lock = @lock;
        if (_lock != null)
            Monitor.Enter(_lock);
    }

    public void Dispose()
    {
        if (_lock != null)
            Monitor.Exit(_lock);
    }
}