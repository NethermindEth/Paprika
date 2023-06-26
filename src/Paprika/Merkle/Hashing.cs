using Paprika.Crypto;
using Paprika.Data;
using Paprika.RLP;

namespace Paprika.Merkle;

public static partial class Node
{
    public ref partial struct Leaf
    {
        public static void KeccakOrRlp(NibblePath nibblePath, Account account, out KeccakOrRlp result) =>
            KeccakOrRlp(nibblePath, account, Keccak.OfAnEmptyString, Keccak.EmptyTreeHash, out result);

        public static void KeccakOrRlp(
            NibblePath nibblePath,
            Account account,
            Keccak codeHash,
            Keccak storageRootHash,
            out KeccakOrRlp result
        )
        {
            /*
            // Stage 1: value = RLP(accountBalance, accountNonce, codeHash, storageRootHash)
            var rlpStream = RlpOfAccount(account, codeHash, storageRootHash);

            // Stage 2: res = KeccakOrRlp(nibblePath, value)
            var result = Encode(nibblePath, rlpStream.Data);
            */

            // Stage 1: value = RLP(accountBalance, accountNonce, codeHash, storageRootHash)
            var contentLength =
                Rlp.LengthOf(account.Balance)
                + Rlp.LengthOf(account.Nonce)
                + Rlp.LengthOfKeccakRlp // CodeHash
                + Rlp.LengthOfKeccakRlp; // StorageRootHash

            Span<byte> rlpBuffer = stackalloc byte[Rlp.LengthOfSequence(contentLength)];
            var rlpStream = new RlpStream(rlpBuffer);

            rlpStream.StartSequence(contentLength)
                .Encode(account.Nonce)
                .Encode(account.Balance)
                .Encode(storageRootHash)
                .Encode(codeHash);

            // Stage 2: res = KeccakOrRlp(nibblePath, value)
            Span<byte> hexPath = stackalloc byte[nibblePath.HexEncodedLength];
            nibblePath.HexEncode(hexPath, true);

            var contentLength2 = Rlp.LengthOf(hexPath) + Rlp.LengthOf(rlpBuffer);
            var totalLength = Rlp.LengthOfSequence(contentLength2);

            Span<byte> data = new byte[totalLength];
            RlpStream rlp = new(data);

            rlp.StartSequence(contentLength2);
            rlp.Encode(hexPath);
            rlp.Encode(rlpBuffer);

            result = rlp.ToKeccakOrRlp();
        }
    }
}
