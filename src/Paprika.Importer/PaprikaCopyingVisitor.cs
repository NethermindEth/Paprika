using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Nethermind.Core.Crypto;
using Nethermind.Trie;
using Paprika.Chain;
using Keccak = Paprika.Crypto.Keccak;

namespace Paprika.Importer;

public class PaprikaCopyingVisitor : ITreeLeafVisitor
{
    private readonly Blockchain _blockchain;
    private readonly int _batchSize;
    private readonly Channel<(ValueKeccak, Nethermind.Core.Account)> _channel;
    private long accounts;

    public PaprikaCopyingVisitor(Blockchain blockchain, int batchSize)
    {
        _blockchain = blockchain;

        var options = new UnboundedChannelOptions
        { SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = false };
        _channel = Channel.CreateUnbounded<(ValueKeccak, Nethermind.Core.Account)>(options);

        _batchSize = batchSize;
    }

    public void VisitLeafAccount(in ValueKeccak account, Nethermind.Core.Account value)
    {
        Interlocked.Increment(ref accounts);
        Debug.Assert(_channel.Writer.TryWrite((account, value)));
    }

    public void Finish() => _channel.Writer.Complete();

    public async Task Copy()
    {
        var parent = Keccak.Zero;
        uint number = 1;

        var reader = _channel.Reader;

        while (await reader.WaitToReadAsync())
        {
            var i = 0;
            // dummy, for import only
            var child = Keccak.Compute(parent.BytesAsSpan);
            using var block = _blockchain.StartNew(parent, child, number);

            while (i < _batchSize && reader.TryRead(out var item))
            {
                i++;

                Keccak addr = default;
                item.Item1.BytesAsSpan.CopyTo(addr.BytesAsSpan);
                var v = item.Item2;

                Keccak codeHash = default;
                v.CodeHash.Bytes.CopyTo(codeHash.BytesAsSpan);

                Keccak storageRoot = default;
                v.StorageRoot.Bytes.CopyTo(storageRoot.BytesAsSpan);

                block.SetAccount(addr, new Account(v.Balance, v.Nonce, codeHash, storageRoot));
            }

            // commit & finalize
            block.Commit();
            _blockchain.Finalize(child);

            // update
            number++;
            parent = child;
        }
    }

    public void VisitLeafStorage(in ValueKeccak account, in ValueKeccak storage, ReadOnlySpan<byte> value)
    {
        throw new NotImplementedException();
    }

    public long Accounts => Volatile.Read(ref accounts);
}