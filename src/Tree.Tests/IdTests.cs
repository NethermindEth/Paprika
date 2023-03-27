using System.Net.Mime;
using NUnit.Framework;

namespace Tree.Tests;

public class IdTests
{
    [Test]
    public void IdTest([Values(0, 3, Id.MaxPosition)] int position,
        [Values(0, 5, Id.MaxFile)] int file,
        [Values(0, 7, Id.MaxLength)] int length)
    {
        var encoded = Id.Encode(position, length, file);
        var decoded = Id.Decode(encoded);

        Assert.AreEqual(position, decoded.Position);
        Assert.AreEqual(file, decoded.File);
        Assert.AreEqual(length, decoded.Length);
    }

    [Test]
    [Repeat(10)]
    public void IsSameFile_True()
    {
        var rand = TestContext.CurrentContext.Random;

        const int file = 4;

        var id1 = Id.Encode(rand.Next(Id.MaxPosition), rand.Next(Id.MaxLength), file);
        var id2 = Id.Encode(rand.Next(Id.MaxPosition), rand.Next(Id.MaxLength), file);

        Assert.IsTrue(Id.IsSameFile(id1, id2));
    }

    [Test]
    [Repeat(10)]
    public void IsSameFile_False()
    {
        const int file1 = 4;
        const int file2 = 8;

        var id1 = Id.Encode(1, 1, file1);
        var id2 = Id.Encode(1, 1, file2);

        Assert.IsFalse(Id.IsSameFile(id1, id2));
    }
}