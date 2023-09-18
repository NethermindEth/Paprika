using System.IO.Pipelines;
using Nethermind.Core.Crypto;
using Nethermind.Trie;

namespace Paprika.Importer;

public class PaprikaCopyingVisitor : ITreeLeafVisitor
{
    private long accounts;

    public PaprikaCopyingVisitor()
    {
    }

    public void VisitLeafAccount(in ValueKeccak account, Nethermind.Core.Account value)
    {
        Interlocked.Increment(ref accounts);
    }

    public void VisitLeafStorage(in ValueKeccak account, in ValueKeccak storage, ReadOnlySpan<byte> value)
    {
        throw new NotImplementedException();
    }

    public long Accounts => Volatile.Read(ref accounts);
}