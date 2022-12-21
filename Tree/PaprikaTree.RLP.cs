using System.Buffers;
using Tree.Crypto;
using Tree.Rlp;

namespace Tree;

public partial class PaprikaTree
{
    private const int KeccakRlpLength = 33;
    private const int MaxBranchRlpLength = MaxLengthOfLengths + Branch.BranchCount * KeccakRlpLength + 1; // 1 for null value
    private const int MaxLengthOfLengths = 4;

    internal const byte HasKeccak = 0b0010_0000;
    internal const byte HasRlp = 0b0001_0000;
    
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

        if ((first & TypeMask) == BranchType)
        {
            var branch = Branch.Read(node);
            hasRlpOrKeccak = EncodeBranch(branch, db, keccakOrRpl);
        }

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

    internal static byte EncodeBranch(in Branch branch, IDb db, in Span<byte> destination)
    {
        var pool = ArrayPool<byte>.Shared;
        var bytes = pool.Rent(MaxBranchRlpLength);

        try
        {
            RlpStream rlp = new (bytes);
            rlp.Position = MaxLengthOfLengths;
            
            for (var i = 0; i < Branch.BranchCount; i++)
            {
                unsafe
                {
                    var child = branch.Branches[i];
                    if (child == Null)
                    {
                        rlp.EncodeEmptyArray();
                    }
                    else
                    {
                        var childKeccakOrRlp = GetNodeKeccakOrRlp(db, child);
                        if (childKeccakOrRlp.Length < KeccakLength)
                        {
                            rlp.Write(childKeccakOrRlp);
                        }
                        else
                        {
                            rlp.EncodeKeccak(childKeccakOrRlp);
                        }    
                    }
                }
            }
            
            // write empty value 
            rlp.EncodeEmptyArray();

            // write length
            var pos = rlp.Position;
            var length = pos - MaxLengthOfLengths;
            var sequenceLength = Rlp.Rlp.LengthOfLength(length);
            var actualStart = MaxLengthOfLengths - sequenceLength;
            
            rlp.Position = actualStart;
            rlp.StartSequence(length);
            rlp.Position = pos;

            return WrapRlp(bytes.AsSpan(actualStart, length + sequenceLength), destination);
        }
        finally
        {
            pool.Return(bytes);
        }
    }

    internal static byte EncodeLeaf(NibblePath path, ReadOnlySpan<byte> value, Span<byte> destination)
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

        return WrapRlp(rlp.Data, destination);
    }
    
    internal static byte EncodeExtension(NibblePath path, ReadOnlySpan<byte> childKeccakOrRlp, Span<byte> destination)
    {
        Span<byte> hexPath = stackalloc byte [path.HexEncodedLength];
        path.HexEncode(hexPath, false);

        var contentLength = Rlp.Rlp.LengthOf(hexPath) + (childKeccakOrRlp.Length == KeccakLength
            ? KeccakRlpLength
            : childKeccakOrRlp.Length);
        
        var totalLength = Rlp.Rlp.LengthOfSequence(contentLength);

        Span<byte> data = stackalloc byte[totalLength];
        
        RlpStream rlp = new(data);
        
        rlp.StartSequence(contentLength);
        rlp.Encode(hexPath);
        
        if (childKeccakOrRlp.Length < KeccakLength)
        {
            rlp.Write(childKeccakOrRlp);
        }
        else
        {
            rlp.EncodeKeccak(childKeccakOrRlp);
        }

        return WrapRlp(rlp.Data, destination);
    }

    private static byte WrapRlp(in Span<byte> data, Span<byte> destination)
    {
        if (data.Length < 32)
        {
            destination[0] = (byte)data.Length;
            data.CopyTo(destination.Slice(RlpLenghtOfLength));
            return HasRlp;
        }

        KeccakHash.ComputeHash(data, destination);
        return HasKeccak;
    }
}