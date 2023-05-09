// See https://aka.ms/new-console-template for more information

using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Running;

[assembly: ExcludeFromCodeCoverage]

public class Program
{
    public static void Main(string[] args)
    {
        var summary = BenchmarkRunner.Run(typeof(Program).Assembly);
    }
}