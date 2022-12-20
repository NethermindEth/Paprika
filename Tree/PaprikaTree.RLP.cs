using Tree.Crypto;
using Tree.Rlp;

namespace Tree;

public partial class PaprikaTree
{
    private static Span<byte> GetNodeKeccakOrRlp(IDb db, long id)
    {
        var node = db.Read(id);
        ref var first = ref node[0];

        if ((first & HasKeccak) == HasKeccak)
        {
            // keccak, 32 bytes, already set, return
            return node.Slice(TypePrefixLength, KeccakLength);
        }

        if ((first & HasRlp) == HasRlp)
        {
            // rlp, 31 bytes or less, already set, return
            var rlpLength = node[TypePrefixLength];
            return node.Slice(TypePrefixLength + RlpLenghtOfLength, rlpLength);
        }

        var keccakOrRpl = node.Slice(TypePrefixLength, KeccakLength);
        byte hasRlpOrKeccak = 0;
        
        if ((first & TypeMask) == LeafType)
        {
            var leaf = node.Slice(PrefixTotalLength);
            var value = ReadLeaf(leaf, out var path);
            hasRlpOrKeccak = EncodeLeaf(path, value, keccakOrRpl);
        }

        // if ((first & TypeMask) == BranchType)
        // {
        //     var jump = Branch.Find(node, keyPath.FirstNibble);
        //     if (jump != Null)
        //     {
        //         keyPath = keyPath.SliceFrom(1);
        //         current = jump;
        //         continue;
        //     }
        //
        //     value = default;
        //     return false;
        // }

        if ((first & TypeMask) == ExtensionType)
        {
            Extension.Read(node, out var path, out var jumpTo);
            var childNodeOrKeccak = GetNodeKeccakOrRlp(db, jumpTo);
            hasRlpOrKeccak = EncodeExtension(path, childNodeOrKeccak, keccakOrRpl);
        }

        // rlp or keccak are computed now, check what it is and return
        
        if ((hasRlpOrKeccak & HasKeccak) == HasKeccak)
        {
            // mark as computed, return up
            first |= HasKeccak;
            return keccakOrRpl;
        }

        // mark as computed, return up a slice based on the first parameter
        first |= HasRlp;
        return keccakOrRpl.Slice(RlpLenghtOfLength, keccakOrRpl[0]);
    }
    
    public static byte EncodeLeaf(NibblePath path, ReadOnlySpan<byte> value, Span<byte> destination)
    {
        Span<byte> hexPath = stackalloc byte [path.HexEncodedLength];
        path.HexEncode(hexPath, true);
        
        var contentLength = Rlp.Rlp.LengthOf(hexPath) + Rlp.Rlp.LengthOf(value);
        var totalLength = Rlp.Rlp.LengthOfSequence(contentLength);

        Span<byte> data = stackalloc byte[totalLength];
        
        RlpStream rlp = new(data);
        
        rlp.StartSequence(contentLength);
        rlp.Encode(hexPath);
        rlp.Encode(value);

        return WrapRlp(rlp, destination);
    }
    
    public static byte EncodeExtension(NibblePath path, ReadOnlySpan<byte> childKeccakOrRlp, Span<byte> destination)
    {
        Span<byte> hexPath = stackalloc byte [path.HexEncodedLength];
        path.HexEncode(hexPath, true);
        
        var contentLength = Rlp.Rlp.LengthOf(hexPath) + Rlp.Rlp.LengthOf(childKeccakOrRlp);
        var totalLength = Rlp.Rlp.LengthOfSequence(contentLength);

        Span<byte> data = stackalloc byte[totalLength];
        
        RlpStream rlp = new(data);
        
        rlp.StartSequence(contentLength);
        rlp.Encode(hexPath);
        
        if (childKeccakOrRlp.Length < KeccakLength)
        {
            // I think it can only happen if we have a short extension to a branch with a short extension as the only child?
            // so |
            // so |
            // so E - - - - - - - - - - - - - - -
            // so |
            // so |
            rlp.Write(childKeccakOrRlp);
        }
        else
        {
            rlp.EncodeKeccak(childKeccakOrRlp);
        }

        return WrapRlp(rlp, destination);
    }

    private static byte WrapRlp(in RlpStream rlp, Span<byte> destination)
    {
        var data = rlp.Data;

        if (data.Length < 32)
        {
            destination[0] = (byte)data.Length;
            data.CopyTo(destination.Slice(RlpLenghtOfLength));
            return HasRlp;
        }

        var keccak = ValueKeccak.Compute(data);
        keccak.BytesAsSpan.CopyTo(destination);
        return HasKeccak;
    }
}