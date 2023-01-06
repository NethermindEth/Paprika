using Paprika.Db;

namespace Paprika.Runner;

public static class Program
{
    private const int Count = 160_000_000;
    private const int LogEvery = 1_000_000;
    private const long Gb = 1024 * 1024 * 1024L;
    private const long DbFileSize = 20 * Gb;

    public static void Main(String[] args)
    {
        const int seed = 17;

        var dir = Directory.GetCurrentDirectory();
        var dataPath = Path.Combine(dir, "db");

        if (Directory.Exists(dataPath))
        {
            Directory.Delete(dataPath, true);
        }

        Directory.CreateDirectory(dataPath);

        var db = new MemoryMappedPagedDb(DbFileSize, dataPath, true);
        var tx = db.Begin();

        byte[] key = new byte[32];

        using (new Measure("Writing", Count))
        {
            var random = new Random(seed);

            for (var i = 0; i < Count; i++)
            {
                random.NextBytes(key);

                tx.Set(key, key);

                if (i % LogEvery == 0 && i > 0)
                {
                    var used = tx.TotalUsedPages;
                    Console.WriteLine(
                        "Wrote {0:N0} items, DB usage is at {1:P} which gives {2:F2}GB out of allocated {3}GB", i, used,
                        used * DbFileSize / Gb, DbFileSize / Gb);
                }
            }
        }

        using (new Measure("Committing to disk just data", Count))
        {
            tx.Commit(CommitOptions.FlushDataOnly);
        }

        using (new Measure("Reading", Count))
        {
            var random = new Random(seed);

            tx = db.Begin();

            for (var i = 0; i < Count; i++)
            {
                random.NextBytes(key);

                if (!tx.TryGet(key, out var v))
                {
                    throw new Exception($"Missing value at {i}!");
                }

                if (!v.SequenceEqual(key))
                {
                    throw new Exception($"Wrong value at {i}!");
                }

                if (i % LogEvery == 0 && i > 0)
                {
                    Console.WriteLine("Read {0:N0} items", i);
                }
            }
        }
    }
}