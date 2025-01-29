using System.Runtime.InteropServices;

namespace Paprika.Tests;

/// <summary>
/// A dummy suite testing execution on different systems.
/// </summary>
[Category(Categories.OS)]
public class OsTests
{
    [Test]
    public void Test()
    {
        Console.WriteLine(RuntimeInformation.OSDescription);
    }
}