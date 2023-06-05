using Paprika.Crypto;
using Paprika.Data;
using Paprika.RLP;

namespace Paprika.Tree;

public static class Leaf
{
    public static KeccakOrRlp ComputeKeccakOrRlp(NibblePath nibblePath, Account account) =>
        ComputeKeccakOrRlp(nibblePath, account, Keccak.OfAnEmptyString, Keccak.EmptyTreeHash);

    public static KeccakOrRlp ComputeKeccakOrRlp(
        NibblePath nibblePath,
        Account account,
        Keccak codeHash,
        Keccak storageRootHash
    )
    {
        // Stage 1: value = RLP(accountBalance, accountNonce, codeHash, storageRootHash)
        var rlpStream = RlpOfAccount(account, codeHash, storageRootHash);

        // Stage 2: res = KeccakOrRlp(nibblePath, value)
        var result = EncodeLeaf(nibblePath, rlpStream.Data);

        return result;
    }

    private static RlpStream RlpOfAccount(Account account, Keccak codeHash, Keccak storageRootHash)
    {
        var contentLength =
            Rlp.LengthOf(account.Balance)
            + Rlp.LengthOf(account.Balance)
            + Rlp.LengthOfKeccakRlp // CodeHash
            + Rlp.LengthOfKeccakRlp; // StorageRootHash

        Span<byte> rlpBuffer = new byte[Rlp.LengthOfSequence(contentLength)];
        RlpStream rlpStream = new(rlpBuffer);

        rlpStream.StartSequence(contentLength);
        rlpStream.Encode(account.Nonce);
        rlpStream.Encode(account.Balance);
        rlpStream.Encode(storageRootHash);
        rlpStream.Encode(codeHash);

        return rlpStream;
    }

    private static KeccakOrRlp EncodeLeaf(NibblePath path, Span<byte> value)
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

        return KeccakOrRlp.WrapRlp(rlp.Data);
    }
}
