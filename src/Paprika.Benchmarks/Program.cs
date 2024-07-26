// See https://aka.ms/new-console-template for more information

using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;

[assembly: ExcludeFromCodeCoverage]

namespace Paprika.Benchmarks;

public class Program
{
    public static void Main(string[] args)
    {
        // Vector128
        // IConfig config = DefaultConfig.Instance
        //     .AddJob(Job.Default.WithEnvironmentVariable("DOTNET_EnableAVX2", "0").WithId("Vector128"));

        // Scalar, throw
        // IConfig config = DefaultConfig.Instance
        //     .AddJob(Job.Default.WithEnvironmentVariable("DOTNET_EnableHWIntrinsic", "0").WithId("Vector128"));

        IConfig? config = null;

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
    }
}
