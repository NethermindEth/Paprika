using Paprika.Crypto;
using Paprika.Data;
using Paprika.RLP;

namespace Paprika.Merkle;

public static partial class Node
{
    public ref partial struct Leaf
    {
        public static KeccakOrRlp KeccakOrRlp(NibblePath nibblePath, Account account) =>
            KeccakOrRlp(nibblePath, account, Keccak.OfAnEmptyString, Keccak.EmptyTreeHash);

        public static KeccakOrRlp KeccakOrRlp(
            NibblePath nibblePath,
            Account account,
            Keccak codeHash,
            Keccak storageRootHash
        )
        {
            // Stage 1: value = RLP(accountBalance, accountNonce, codeHash, storageRootHash)
            var rlpStream = RlpOfAccount(account, codeHash, storageRootHash);

            // Stage 2: res = KeccakOrRlp(nibblePath, value)
            var result = Encode(nibblePath, rlpStream.Data);

            return result;
        }

        private static RlpStream RlpOfAccount(Account account, Keccak codeHash, Keccak storageRootHash)
        {
            var contentLength =
                Rlp.LengthOf(account.Balance)
                + Rlp.LengthOf(account.Nonce)
                + Rlp.LengthOfKeccakRlp // CodeHash
                + Rlp.LengthOfKeccakRlp; // StorageRootHash

            Span<byte> rlpBuffer = new byte[Rlp.LengthOfSequence(contentLength)];
            var rlpStream = new RlpStream(rlpBuffer);

            rlpStream.StartSequence(contentLength)
                .Encode(account.Nonce)
                .Encode(account.Balance)
                .Encode(storageRootHash)
                .Encode(codeHash);

            return rlpStream;
        }

        private static KeccakOrRlp Encode(NibblePath path, Span<byte> value)
        {
            Span<byte> hexPath = new byte[path.HexEncodedLength];
            path.HexEncode(hexPath, true);

            var contentLength = Rlp.LengthOf(hexPath) + Rlp.LengthOf(value);
            var totalLength = Rlp.LengthOfSequence(contentLength);

            Span<byte> data = new byte[totalLength];
            RlpStream rlp = new(data);

            rlp.StartSequence(contentLength);
            rlp.Encode(hexPath);
            rlp.Encode(value);

            return RLP.KeccakOrRlp.WrapRlp(rlp.Data);
        }
    }
}
