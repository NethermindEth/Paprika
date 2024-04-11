using System.Diagnostics;
using NUnit.Framework.Interfaces;

namespace Paprika.Tests;

[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class |
                AttributeTargets.Interface | AttributeTargets.Assembly,
    AllowMultiple = true)]
public sealed class ReportTime : Attribute, ITestAction
{
    private const string Key = "ReportTime";

    public void BeforeTest(ITest test)
    {
        test.Properties.Set(Key, Stopwatch.StartNew());
    }

    public void AfterTest(ITest test)
    {
        var sw = (Stopwatch)test.Properties.Get(Key)!;

        Console.WriteLine($"Took: {sw.Elapsed:g}");
    }

    public ActionTargets Targets => ActionTargets.Test | ActionTargets.Suite;
}