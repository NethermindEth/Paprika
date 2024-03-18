using System.Collections.Concurrent;
using System.Text;
using Paprika.Chain;
using Paprika.Data;

namespace Paprika.Tests.Merkle;

public sealed class MetricsCollector : IMetricsCollector
{
    private readonly ConcurrentDictionary<string, int> _counters = new();

    public void Clear() => _counters.Clear();

    public void OnTryGet(in Key key, ReadOnlySpan<byte> keyWritten, ulong bloom)
    {
        _counters.AddOrUpdate(key.ToString(), 1, static (_, prev) => prev + 1);
    }

    public string Report(int minToReport)
    {
        var totalReads = 0;
        var reportedRead = 0;

        var sb = new StringBuilder();

        foreach (var kvp in _counters)
        {
            totalReads += kvp.Value;

            if (kvp.Value >= minToReport)
            {
                reportedRead += kvp.Value;

                sb.Append(kvp.Key);
                sb.Append(" -> ");
                sb.Append(kvp.Value);
                sb.AppendLine();
            }
        }

        var ratio = (float)reportedRead / totalReads;

        return $"Total reads: {totalReads} with reported here being {ratio:P1}\n" + sb;

        // var metrics = _counters.ToArray();
        // Array.Sort(metrics, (m1, m2) => -m1.Value.CompareTo(m2.Value));
    }
}