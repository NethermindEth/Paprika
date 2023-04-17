using System.Buffers.Binary;
using System.Diagnostics;
using Paprika.Crypto;
using Paprika.Db;

namespace Paprika.Runner;

public static class Program
{
    private const int BlockCount = 10000;
    private const int AccountCount = 10000;
    private const long DbFileSize = 4 * Gb;
    private const long Gb = 1024 * 1024 * 1024L;
    private const CommitOptions Commit = CommitOptions.FlushDataOnly;

    public static void Main(String[] args)
    {
        var dir = Directory.GetCurrentDirectory();
        var dataPath = Path.Combine(dir, "db");

        if (Directory.Exists(dataPath))
        {
            Directory.Delete(dataPath, true);
        }

        Directory.CreateDirectory(dataPath);

        Console.WriteLine("Initializing db of size {0}GB", DbFileSize / Gb);
        Console.WriteLine("Starting benchmark with commit level {0}", Commit);

        var db = new MemoryMappedPagedDb(DbFileSize, 64, dataPath);

        // writing
        var writing = Stopwatch.StartNew();

        for (uint block = 0; block < BlockCount; block++)
        {
            using var batch = db.BeginNextBlock();

            for (var account = 0; account < AccountCount; account++)
            {
                var key = Keccak.Zero;
                BinaryPrimitives.WriteInt32LittleEndian(key.BytesAsSpan, account);

                batch.Set(key, new Account(block, block));
            }

            batch.Commit(Commit);
        }

        Console.WriteLine("Writing state of {0} accounts through {1} blocks took {2} and used {3:F2}GB",
            AccountCount, BlockCount, writing.Elapsed, db.ActualMegabytesOnDisk / 1024);

        // reading
        var reading = Stopwatch.StartNew();
        using var read = db.BeginReadOnlyBatch();

        for (var account = 0; account < AccountCount; account++)
        {
            var key = Keccak.Zero;
            BinaryPrimitives.WriteInt32LittleEndian(key.BytesAsSpan, account);
            read.GetAccount(key);
        }

        Console.WriteLine("Reading state of {0} accounts from the last block took {1}",
            AccountCount, reading.Elapsed);
    }
}