using FluentAssertions;
using Paprika.Chain;

namespace Paprika.Tests.Chain;

public class FlushStrategyTests
{
    [Test]
    public void AfterN()
    {
        const int single = 1;
        const int many = 2;
        const int n = 10;
        const int manyCount = 20;

        var strategy = FlushStrategy.AfterNCommits(n);

        for (var i = 0; i < manyCount; i++)
        {
            ShouldNotWrite(many);
        }

        for (var i = 0; i < n; i++)
        {
            ShouldNotWrite(single);
        }

        ShouldFlushData(single);
        ShouldFlushData(single);

        // a single value "many" resets the counter
        ShouldNotWrite(many);

        for (var i = 0; i < n; i++)
        {
            ShouldNotWrite(single);
        }

        // should write data again
        ShouldFlushData(single);

        return;

        void ShouldNotWrite(int count) => strategy.GetCommitOptions(count).Should().Be(CommitOptions.DangerNoWrite);
        void ShouldFlushData(int count) => strategy.GetCommitOptions(count).Should().Be(CommitOptions.FlushDataOnly);
    }

    [Test]
    public void Never([Values(1, 10, 1000)] int inQueue)
    {
        RunConst(inQueue, FlushStrategy.Never, CommitOptions.DangerNoWrite);
    }

    [Test]
    public void Always([Values(1, 10, 1000)] int inQueue)
    {
        RunConst(inQueue, FlushStrategy.Always, CommitOptions.FlushDataOnly);
    }

    private static void RunConst(int inQueue, IFlushStrategy strategy, CommitOptions expected)
    {
        for (var i = 0; i < 1000; i++)
        {
            strategy.GetCommitOptions(inQueue).Should().Be(expected);
        }
    }
}