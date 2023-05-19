using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Store;
using Paprika.Utils;
using static Paprika.Tests.Values;

namespace Paprika.Tests;

public class PrinterTests : BasePageTests
{
    [Test]
    public void Test()
    {
        const ulong size = 1 * 1024 * 1024;
        const int blocks = 3;
        const byte maxReorgDepth = 2;

        using var db = PagedDb.NativeMemoryDb(size, maxReorgDepth);

        for (int i = 0; i < blocks; i++)
        {
            Printer.Print(db, Console.Out);

            using (var block = db.BeginNextBatch())
            {
                block.Set(Key0, new Account(Balance0, (UInt256)i++));
                block.Commit(CommitOptions.FlushDataOnly);
            }
        }

        Printer.Print(db, Console.Out);
    }
}