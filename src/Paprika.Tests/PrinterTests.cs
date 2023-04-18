using NUnit.Framework;
using Paprika.Pages;

namespace Paprika.Tests;

public class PrinterTests : BasePageTests
{
    [Test]
    public void Test()
    {
        var printer = new Printer();

        var root0 = new RootPage(AllocPage());
        root0.Header.BatchId = 1;

        printer.Add(root0, DbAddress.Null);

        printer.Print(Console.Out);
    }
}