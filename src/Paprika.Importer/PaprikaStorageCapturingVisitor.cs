using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Channels;
using Nethermind.Core.Crypto;
using Nethermind.Serialization.Rlp;
using Nethermind.State;
using Nethermind.Trie;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Utils;
using Keccak = Paprika.Crypto.Keccak;

namespace Paprika.Importer;

public class PaprikaStorageCapturingVisitor : ITreeLeafVisitor
{
    private readonly StringBuilder _sb = new();

    private int _accounts = 1;

    public void VisitLeafAccount(in ValueKeccak account, Nethermind.Core.Account value)
    {
        if (Interlocked.Decrement(ref _accounts) < 0)
        {
            throw new Exception("Too many account");
        }
    }

    public void VisitLeafStorage(in ValueKeccak account, in ValueKeccak storage, ReadOnlySpan<byte> value)
    {
        var span = MemoryMarshal.CreateSpan(ref MemoryMarshal.GetReference(value), value.Length);
        Rlp.ValueDecoderContext rlp = new Rlp.ValueDecoderContext(span);

        var v = storage.ToString(false) + ":" + value.ToHexString(false);

        lock (_sb)
        {
            _sb.AppendLine(v);
        }
    }

    public string Payload => _sb.ToString();
}