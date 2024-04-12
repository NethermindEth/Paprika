using System.Runtime.CompilerServices;
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

            Span<byte> valueRlp = stackalloc byte[Rlp.LengthOfSequence(accountRlpLength)];
            var rlp = new RlpStream(valueRlp);
            rlp.StartSequence(accountRlpLength);
            rlp.Encode(account.Nonce);
            rlp.Encode(account.Balance);
            rlp.Encode(account.StorageRootHash);
            rlp.Encode(account.CodeHash);

            Finalize(nibblePath, valueRlp, out result);
        }

        [SkipLocalsInit]
        private static void Finalize(scoped in NibblePath nibblePath, scoped Span<byte> valueRlp,
            out KeccakOrRlp result)
        {
            // Stage 2: result = KeccakOrRlp(nibblePath, accountRlp)
            Span<byte> hexPath = stackalloc byte[nibblePath.HexEncodedLength];
            nibblePath.HexEncode(hexPath, true);

            var encodedLength = Rlp.LengthOf(hexPath) + Rlp.LengthOf(valueRlp);
            var totalLength = Rlp.LengthOfSequence(encodedLength);

            Span<byte> accountAndPathRlp = stackalloc byte[totalLength];
            var rlp = new RlpStream(accountAndPathRlp);
            rlp.StartSequence(encodedLength);
            rlp.Encode(hexPath);
            rlp.Encode(valueRlp);
            rlp.ToKeccakOrRlp(out result);
        }

        [SkipLocalsInit]
        public static void KeccakOrRlp(scoped in NibblePath nibblePath, scoped ReadOnlySpan<byte> value, out KeccakOrRlp result)
        {
            var valueLength = Rlp.LengthOf(value);
            Span<byte> valueRlp = stackalloc byte[valueLength];
            new RlpStream(valueRlp).Encode(value);

            Finalize(nibblePath, valueRlp, out result);
        }
    }
}
