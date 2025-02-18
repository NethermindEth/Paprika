using FluentAssertions;
using Paprika.Chain;

namespace Paprika.Tests.Chain;

public class StorageStatsTests
{
    [TestCase(new[] { true, true }, true, TestName = "Set & Set")]
    [TestCase(new[] { true, false }, false, TestName = "Set & Delete")]
    [TestCase(new[] { false, true }, true, TestName = "Delete & Set")]
    [TestCase(new[] { false, false }, false, TestName = "Delete & Delete")]
    public void Scenarios(bool[] operations, bool expected)
    {
        var stats = new StorageStats();
        var key = Values.Key0;

        foreach (var operation in operations)
        {
            stats.SetStorage(key, operation ? [1] : []);
        }

        (expected ? stats.Set : stats.Deleted).Should().Contain(key);
        (expected ? stats.Deleted : stats.Set).Should().BeEmpty();

        stats.Set.Intersect(stats.Deleted).Should()
            .BeEmpty("The sets of deleted and set should always have no shared elements");
    }
}