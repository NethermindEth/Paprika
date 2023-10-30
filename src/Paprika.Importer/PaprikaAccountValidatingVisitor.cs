using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Nethermind.Core.Crypto;
using Nethermind.Trie;
using Paprika.Chain;
using Keccak = Paprika.Crypto.Keccak;

namespace Paprika.Importer;

public class PaprikaAccountValidatingVisitor : ITreeLeafVisitor, IDisposable
{
    struct AccountItem
    {
        private readonly ValueKeccak _account;

        // account
        private readonly Nethermind.Core.Account _accountValue;

        public bool HasStorage => _accountValue.StorageRoot != Nethermind.Core.Crypto.Keccak.EmptyTreeHash;

        public AccountItem(ValueKeccak account, Nethermind.Core.Account accountValue)
        {
            _account = account;
            _accountValue = accountValue;
        }

        public ValidationStatus Validate(IReadOnlyWorldState read)
        {
            var addr = AsPaprika(_account);
            var v = _accountValue;

            var codeHash = AsPaprika(v.CodeHash);
            var storageRoot = AsPaprika(v.StorageRoot);

            var paprika = read.GetAccount(addr);

            if (paprika.Balance != v.Balance)
                return ValidationStatus.InvalidBalance;

            if (paprika.Nonce != v.Nonce)
            {
                return ValidationStatus.InvalidNonce;
            }

            if (paprika.CodeHash != codeHash)
            {
                return ValidationStatus.InvalidCodeHash;
            }

            if (paprika.StorageRootHash != storageRoot)
            {
                return ValidationStatus.InvalidStorageRootHash;
            }

            return ValidationStatus.Valid;
        }
    }

    public enum ValidationStatus
    {
        Valid = 0,
        InvalidBalance = 1,
        InvalidNonce = 2,
        InvalidCodeHash = 3,
        InvalidStorageRootHash = 4
    }

    private readonly Blockchain _blockchain;
    private readonly Channel<AccountItem> _channel;

    public PaprikaAccountValidatingVisitor(Blockchain blockchain, int batchSize)
    {
        _blockchain = blockchain;

        var options = new BoundedChannelOptions(batchSize * 1000)
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        };

        _channel = Channel.CreateBounded<AccountItem>(options);
    }

    public void VisitLeafAccount(in ValueKeccak account, Nethermind.Core.Account value)
    {
        Add(new AccountItem(account, value));
    }

    public void VisitLeafStorage(in ValueKeccak account, in ValueKeccak storage, ReadOnlySpan<byte> value)
    {
    }

    private void Add(AccountItem accountItem)
    {
        while (_channel.Writer.TryWrite(accountItem) == false)
        {
            Thread.Sleep(TimeSpan.FromSeconds(1));
        }
    }

    public void Finish() => _channel.Writer.Complete();

    public async Task<List<Keccak>> Validate()
    {
        var statuses = new int[5];
        var withStorage = 0;
        
        using var read = _blockchain.StartReadOnlyLatestFromDb();
        var reader = _channel.Reader;

        var i = 0;

        while (await reader.WaitToReadAsync())
        {
            while (reader.TryRead(out var  item))
            {
                i++;
                var status = item.Validate(read);
                statuses[(int)status]++;

                if (item.HasStorage)
                {
                    withStorage++;
                }
            }
        }

        return new List<Keccak>();
    }

    private static Keccak AsPaprika(Nethermind.Core.Crypto.Keccak keccak)
    {
        Unsafe.SkipInit(out Keccak k);
        keccak.Bytes.CopyTo(k.BytesAsSpan);
        return k;
    }

    private static Keccak AsPaprika(ValueKeccak keccak)
    {
        Unsafe.SkipInit(out Keccak k);
        keccak.Bytes.CopyTo(k.BytesAsSpan);
        return k;
    }

    public void Dispose()
    {
    }
}