using System.Runtime.CompilerServices;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.RLP;

namespace Paprika.Merkle;

public static partial class Node
{
    public ref partial struct Leaf
    {
        [SkipLocalsInit]
        public static void KeccakOrRlp(
            scoped in NibblePath nibblePath,
            scoped in Account account,
            out KeccakOrRlp result
        )
        {
            // Stage 1: accountRlp = RLP(accountBalance, accountNonce, codeHash, storageRootHash)
            var accountRlpLength =
                Rlp.LengthOf(account.Balance)
                + Rlp.LengthOf(account.Nonce)
                + Rlp.LengthOfKeccakRlp // CodeHash
                + Rlp.LengthOfKeccakRlp; // StorageRootHash

            Span<byte> accountRlp = stackalloc byte[Rlp.LengthOfSequence(accountRlpLength)];
            new RlpStream(accountRlp)
                .StartSequence(accountRlpLength)
                .Encode(account.Nonce)
                .Encode(account.Balance)
                .Encode(account.StorageRootHash)
                .Encode(account.CodeHash);

            // Stage 2: result = KeccakOrRlp(nibblePath, accountRlp)
            Span<byte> hexPath = stackalloc byte[nibblePath.HexEncodedLength];
            nibblePath.HexEncode(hexPath, true);

            var encodedLength = Rlp.LengthOf(hexPath) + Rlp.LengthOf(accountRlp);
            var totalLength = Rlp.LengthOfSequence(encodedLength);

            Span<byte> accountAndPathRlp = stackalloc byte[totalLength];
            result = new RlpStream(accountAndPathRlp)
                .StartSequence(encodedLength)
                .Encode(hexPath)
                .Encode(accountRlp)
                .ToKeccakOrRlp();
        }
    }
}
