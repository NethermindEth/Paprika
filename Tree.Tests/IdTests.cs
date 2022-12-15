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
}