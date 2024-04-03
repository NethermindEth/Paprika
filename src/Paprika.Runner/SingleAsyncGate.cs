namespace Paprika.Runner;

public sealed class SingleAsyncGate(uint minGap)
{
    private readonly object _lock = new();
    private uint _signaled;
    private uint _awaited;
    private TaskCompletionSource? _wait;

    public Task WaitAsync(uint wait)
    {
        lock (_lock)
        {
            if (IsSatisfied(wait))
            {
                return Task.CompletedTask;
            }

            _wait = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _awaited = wait;
            return _wait.Task;
        }
    }

    private bool IsSatisfied(uint value) => _signaled + minGap >= value;

    public void Signal(uint value)
    {
        TaskCompletionSource? wait = null;
        lock (_lock)
        {
            _signaled = value;

            if (_wait != null)
            {
                if (IsSatisfied(_awaited))
                {
                    wait = _wait;
                    _wait = null;
                }
            }
        }

        wait?.TrySetResult();
    }
}
