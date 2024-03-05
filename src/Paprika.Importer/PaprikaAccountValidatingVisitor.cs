// using System.Runtime.CompilerServices;
// using System.Text;
// using System.Threading.Channels;
// using Nethermind.Core.Crypto;
// using Nethermind.Trie;
// using Paprika.Chain;
// using Paprika.Data;
// using Paprika.Merkle;
// using Keccak = Paprika.Crypto.Keccak;
//
// namespace Paprika.Importer;
//
// public class PaprikaAccountValidatingVisitor : ITreeLeafVisitor, IDisposable
// {
//     struct AccountItem
//     {
//         private readonly ValueKeccak _account;
//
//         // account
//         private readonly Nethermind.Core.Account _accountValue;
//
//         public bool HasStorage => _accountValue.StorageRoot != Nethermind.Core.Crypto.Keccak.EmptyTreeHash;
//
//         public AccountItem(ValueKeccak account, Nethermind.Core.Account accountValue)
//         {
//             _account = account;
//             _accountValue = accountValue;
//         }
//
//         public ValidationStatus Validate(IReadOnlyWorldState read)
//         {
//             var addr = AccountKeccak;
//             var v = _accountValue;
//
//             var codeHash = AsPaprika(v.CodeHash);
//             var storageRoot = AsPaprika(v.StorageRoot);
//
//             var paprika = read.GetAccount(addr);
//
//             if (paprika.IsEmpty)
//                 return ValidationStatus.EmptyInPaprika;
//
//             if (paprika.Balance != v.Balance)
//                 return ValidationStatus.InvalidBalance;
//
//             if (paprika.Nonce != v.Nonce)
//             {
//                 return ValidationStatus.InvalidNonce;
//             }
//
//             if (paprika.CodeHash != codeHash)
//             {
//                 return ValidationStatus.InvalidCodeHash;
//             }
//
//             if (paprika.StorageRootHash != storageRoot)
//             {
//                 return ValidationStatus.InvalidStorageRootHash;
//             }
//
//             return ValidationStatus.Valid;
//         }
//
//         public Keccak AccountKeccak => AsPaprika(_account);
//     }
//
//     public enum ValidationStatus
//     {
//         Valid = 0,
//
//         EmptyInPaprika = 1,
//         InvalidBalance = 2,
//         InvalidNonce = 3,
//         InvalidCodeHash = 4,
//         InvalidStorageRootHash = 5
//     }
//
//     private readonly Blockchain _blockchain;
//     private readonly ComputeMerkleBehavior _merkle;
//     private readonly Channel<AccountItem> _channel;
//
//     public PaprikaAccountValidatingVisitor(Blockchain blockchain, ComputeMerkleBehavior merkle,
//         int batchSize)
//     {
//         _blockchain = blockchain;
//         _merkle = merkle;
//
//         var options = new BoundedChannelOptions(batchSize * 1000)
//         {
//             SingleReader = true,
//             SingleWriter = false,
//             AllowSynchronousContinuations = false
//         };
//
//         _channel = Channel.CreateBounded<AccountItem>(options);
//     }
//
//     public void VisitLeafAccount(in ValueKeccak account, Nethermind.Core.Account value)
//     {
//         Add(new AccountItem(account, value));
//     }
//
//     public void VisitLeafStorage(in ValueKeccak account, in ValueKeccak storage, ReadOnlySpan<byte> value)
//     {
//     }
//
//     private void Add(AccountItem accountItem)
//     {
//         while (_channel.Writer.TryWrite(accountItem) == false)
//         {
//             Thread.Sleep(TimeSpan.FromSeconds(1));
//         }
//     }
//
//     public void Finish() => _channel.Writer.Complete();
//
//     public async Task<string> Validate()
//     {
//         List<string>?[] statuses = new List<string>[6];
//
//         using var read = _blockchain.StartReadOnlyLatestFromDb();
//         var reader = _channel.Reader;
//
//         while (await reader.WaitToReadAsync())
//         {
//             while (reader.TryRead(out var item))
//             {
//                 var status = item.Validate(read);
//                 if (status != ValidationStatus.Valid)
//                 {
//                     var i = (int)status;
//                     statuses[i] ??= new();
//                     statuses[i]!.Add(item.AccountKeccak.ToString());
//                 }
//             }
//         }
//
//         var sb = new StringBuilder();
//
//         foreach (var status in Enum.GetValues<ValidationStatus>())
//         {
//             if (status == ValidationStatus.Valid)
//                 continue;
//
//             var list = statuses[(int)status];
//             if (list is { Count: > 0 })
//             {
//                 list.Sort();
//
//                 sb.AppendLine($"{status}: ");
//                 sb.AppendJoin(", ", list);
//                 sb.AppendLine();
//             }
//         }
//
//         return sb.ToString();
//     }
//
//     private static Keccak AsPaprika(Nethermind.Core.Crypto.Keccak keccak)
//     {
//         Unsafe.SkipInit(out Keccak k);
//         keccak.Bytes.CopyTo(k.BytesAsSpan);
//         return k;
//     }
//
//     private static Keccak AsPaprika(ValueKeccak keccak)
//     {
//         Unsafe.SkipInit(out Keccak k);
//         keccak.Bytes.CopyTo(k.BytesAsSpan);
//         return k;
//     }
//
//     public void Dispose()
//     {
//     }
// }
