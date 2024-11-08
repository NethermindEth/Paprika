namespace Paprika.Utils;

public class WorkProcessor : IThreadPoolWorkItem
{
    private static ParallelOptions s_parallelOptions = new()
    {
        // default to the number of processors
        MaxDegreeOfParallelism = Environment.ProcessorCount
    };

    private readonly Data _data;

    public static void For(int fromInclusive, int toExclusive, Action<int> action)
        => For(fromInclusive, toExclusive, s_parallelOptions, action);

    public static void For(int fromInclusive, int toExclusive, ParallelOptions parallelOptions, Action<int> action)
    {
        var threads = parallelOptions.MaxDegreeOfParallelism > 0 ? parallelOptions.MaxDegreeOfParallelism : Environment.ProcessorCount;

        var data = new Data(threads, fromInclusive, toExclusive, action);

        for (int i = 0; i < threads - 1; i++)
        {
            ThreadPool.UnsafeQueueUserWorkItem(new WorkProcessor(data), preferLocal: false);
        }

        new WorkProcessor(data).Execute();

        if (data.ActiveThreads > 0)
        {
            lock (data)
            {
                if (data.ActiveThreads > 0)
                {
                    // Wait for remaining to complete
                    Monitor.Wait(data);
                }
            }
        }
    }

    public static void For<TLocal>(
        int fromInclusive,
        int toExclusive,
        ParallelOptions parallelOptions,
        Func<TLocal> init,
        Func<int, TLocal, TLocal> action,
        Action<TLocal> @finally)
        => InitProcessor<TLocal>.For(fromInclusive, toExclusive, parallelOptions, init, action, @finally);

    private WorkProcessor(Data data)
    {
        _data = data;
    }

    public void Execute()
    {
        var i = _data.Index.GetNext();
        while (i < _data.ToExclusive)
        {
            _data.Action(i);
            i = _data.Index.GetNext();
        }

        _data.MarkThreadCompleted();
    }

    private class SharedCounter(int fromInclusive)
    {
        private int _index = fromInclusive;
        public int GetNext() => Interlocked.Increment(ref _index) - 1;
    }

    private class Data(int threads, int fromInclusive, int toExclusive, Action<int> action)
    {
        public SharedCounter Index { get; } = new SharedCounter(fromInclusive);
        public int ToExclusive => toExclusive;
        public Action<int> Action => action;
        public int ActiveThreads => threads;

        public int MarkThreadCompleted()
        {
            var remaining = Interlocked.Decrement(ref threads);

            if (remaining == 0)
            {
                lock (this)
                {
                    Monitor.Pulse(this);
                }
            }

            return remaining;
        }
    }

    private class InitProcessor<TLocal> : IThreadPoolWorkItem
    {
        private readonly Data<TLocal> _data;

        public static void For(
            int fromInclusive,
            int toExclusive,
            ParallelOptions parallelOptions,
            Func<TLocal> init,
            Func<int, TLocal, TLocal> action,
            Action<TLocal> @finally)
        {
            var threads = parallelOptions.MaxDegreeOfParallelism > 0 ? parallelOptions.MaxDegreeOfParallelism : Environment.ProcessorCount;

            var data = new Data<TLocal>(threads, fromInclusive, toExclusive, init, action, @finally);

            for (int i = 0; i < threads - 1; i++)
            {
                ThreadPool.UnsafeQueueUserWorkItem(new InitProcessor<TLocal>(data), preferLocal: false);
            }

            new InitProcessor<TLocal>(data).Execute();

            if (data.ActiveThreads > 0)
            {
                lock (data)
                {
                    if (data.ActiveThreads > 0)
                    {
                        // Wait for remaining to complete
                        Monitor.Wait(data);
                    }
                }
            }
        }

        private InitProcessor(Data<TLocal> data)
        {
            _data = data;
        }

        public void Execute()
        {
            var value = _data.Init();
            var i = _data.Index.GetNext();
            while (i < _data.ToExclusive)
            {
                value = _data.Action(i, value);
                i = _data.Index.GetNext();
            }

            _data.Finally(value);

            _data.MarkThreadCompleted();
        }

        private class Data<TValue>(int threads, int fromInclusive, int toExclusive, Func<TValue> init,
            Func<int, TLocal, TLocal> action,
            Action<TValue> @finally)
        {
            public SharedCounter Index { get; } = new SharedCounter(fromInclusive);
            public int ToExclusive => toExclusive;
            public Func<TValue> Init => init;
            public Func<int, TLocal, TLocal> Action => action;
            public Action<TValue> Finally => @finally;
            public int ActiveThreads => threads;

            public int MarkThreadCompleted()
            {
                var remaining = Interlocked.Decrement(ref threads);

                if (remaining == 0)
                {
                    lock (this)
                    {
                        Monitor.Pulse(this);
                    }
                }

                return remaining;
            }
        }
    }

}
