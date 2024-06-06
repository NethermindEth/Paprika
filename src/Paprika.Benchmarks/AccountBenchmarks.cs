using BenchmarkDotNet.Attributes;
using Paprika.Crypto;

namespace Paprika.Benchmarks;

[MemoryDiagnoser]
// [DisassemblyDiagnoser]
public class AccountBenchmarks
{
    private static readonly Keccak CodeHash = Keccak.Compute(new byte[] { 0, 1, 2, 3 });
    private static readonly Keccak StorageRoot = Keccak.Compute(new byte[] { 0, 1, 2, 5, 6, 8 });

    private readonly byte[] _buffer = new byte[Account.MaxByteCount];

    [Benchmark]
    public int Small_contract_write_read()
    {
        var expected = new Account(100, 113, CodeHash, StorageRoot);
        var data = expected.WriteTo(_buffer);
        Account.ReadFrom(data, out var account);

        return data.Length + account.Balance.BitLen;
    }

    [Benchmark]
    public int Eoa_write_read()
    {
        var expected = new Account(100, 113);
        var data = expected.WriteTo(_buffer);

        Account.ReadFrom(data, out var account);

        return data.Length + account.Balance.BitLen;
    }
}
