using NUnit.Framework;

namespace Tree.Tests.Mocks;

public class MemoryDbTests
{
    [Test]
    public void Simple()
    {
        var random = new Random();
        
        var bytes1 = new byte[35];
        random.NextBytes(bytes1);
        
        var bytes2 = new byte[57];
        random.NextBytes(bytes2);

        using var db = new MemoryDb(1024 * 1024);
        
        var key1 = db.Write(bytes1);
        var key2 = db.Write(bytes2);

        var read1 = db.Read(key1);
        var read2 = db.Read(key2);
        
        CollectionAssert.AreEqual(bytes1, read1.ToArray());
        CollectionAssert.AreEqual(bytes2, read2.ToArray());
    }
}