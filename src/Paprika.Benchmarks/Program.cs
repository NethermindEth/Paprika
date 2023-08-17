// See https://aka.ms/new-console-template for more information

using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Running;

[assembly: ExcludeFromCodeCoverage]

namespace Paprika.Benchmarks;

public class Program
{
    public static void Main(string[] args)
    {
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run();
    }
}