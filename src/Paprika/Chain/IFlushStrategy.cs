namespace Paprika.Chain;

public interface IFlushStrategy
{
    CommitOptions GetCommitOptions(int numberOfItemsInFlusherQueue);
}

public static class FlushStrategy
{
    /// <summary>
    /// Always flushes the data, ensuring ACI but not necessary D from ACID.
    /// </summary>
    public static readonly IFlushStrategy Always = new ConstWritingFlushStrategy(CommitOptions.FlushDataOnly);

    public static readonly IFlushStrategy Never = new ConstWritingFlushStrategy(CommitOptions.DangerNoWrite);

    public static IFlushStrategy AfterNCommits(int numberOfTimesASingleBlocksIsFlushed = 64) =>
        new CountingSingularCommitsFlushStrategy(numberOfTimesASingleBlocksIsFlushed);

    private class CountingSingularCommitsFlushStrategy(int numberOfTimesASingleBlocksIsFlushed) : IFlushStrategy
    {
        private const int Single = 1;

        private long _singleCount;

        public CommitOptions GetCommitOptions(int numberOfItemsInFlusherQueue)
        {
            _singleCount = numberOfItemsInFlusherQueue == Single ? _singleCount + 1 : 0;

            return _singleCount > numberOfTimesASingleBlocksIsFlushed
                ? CommitOptions.FlushDataOnly
                : CommitOptions.DangerNoWrite;
        }
    }

    private class ConstWritingFlushStrategy(CommitOptions options) : IFlushStrategy
    {
        public CommitOptions GetCommitOptions(int numberOfItemsInFlusherQueue) => options;
    }
}

