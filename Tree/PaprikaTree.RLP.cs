using System.Buffers;
using System.Reflection.Metadata.Ecma335;
using Tree.Crypto;
using Tree.Rlp;

namespace Tree;

public partial class PaprikaTree
{
    private const int RlpLengthOfLength = 1;
    private const int KeccakRlpLength = 33;
    
    public enum KeccakOrRlp
    {
        Keccak = 1,
        Rlp = 2
    }
    
    private static KeccakOrRlp CalculateKeccakOrRlp(IDb db, long id, Span<byte> destination)
    {
        throw new NotImplementedException();
        
        var node = db.Read(id);
        ref var first = ref node[0];
        
        if ((first & TypeMask) == LeafType)
        {
            var leaf = node.Slice(PrefixTotalLength);
            var value = ReadLeaf(leaf, out var path);
            return EncodeLeaf(path, value, destination);
        }
        
        // if ((first & TypeMask) == BranchType)
        // {
        //     var branch = Branch.Read(node);
        //     hasRlpOrKeccak = EncodeBranch(branch, db, keccakOrRpl, parallel);
        // }
        
        if ((first & TypeMask) == ExtensionType)
        {
            // for extensions do not memoize the child keccak,
            // if something changed underneath, this hash will change as well
            // so there's not that much value in storing it.
            Extension.Read(node, out var path, out var jumpTo);
            Span<byte> child = stackalloc byte[32];
            var rlpOrKeccak = CalculateKeccakOrRlp(db, jumpTo, child);
            return EncodeExtension(path, TrimToType(child, rlpOrKeccak), destination);
        }
        
        
    }

    /// <summary>
    /// Trims the keccak or RLP according to the type, according to the way <see cref="WrapRlp"/> encodes it.
    /// </summary>
    /// <param name="rlpOrKeccak"></param>
    /// <param name="type"></param>
    /// <returns></returns>
    private static Span<byte> TrimToType(Span<byte> rlpOrKeccak, KeccakOrRlp type)
    {
        return type == KeccakOrRlp.Keccak ? rlpOrKeccak : rlpOrKeccak.Slice(1, rlpOrKeccak[0]);
    }

    // internal static byte EncodeBranch(in Branch branch, IDb db, in Span<byte> destination, bool parallel = false)
    // {
    //     var pool = ArrayPool<byte>.Shared;
    //     var bytes = pool.Rent(MaxBranchRlpLength);
    //
    //     try
    //     {
    //         if (parallel)
    //         {
    //             // parallel, precalculate children
    //             var children = new List<long>(Branch.BranchCount);
    //             for (var i = 0; i < Branch.BranchCount; i++)
    //             {
    //                 unsafe
    //                 {
    //                     var child = branch.Branches[i];
    //                     if (child != Null)
    //                     {
    //                         children.Add(child);
    //                     }
    //                 }
    //             }
    //
    //             Parallel.ForEach(children, id => CalculateKeccakOrRlp(db, id));
    //         }
    //         
    //         RlpStream rlp = new (bytes);
    //         rlp.Position = MaxLengthOfLengths;
    //         
    //         for (var i = 0; i < Branch.BranchCount; i++)
    //         {
    //             unsafe
    //             {
    //                 var child = branch.Branches[i];
    //                 if (child == Null)
    //                 {
    //                     rlp.EncodeEmptyArray();
    //                 }
    //                 else
    //                 {
    //                     var childKeccakOrRlp = CalculateKeccakOrRlp(db, child);
    //                     if (childKeccakOrRlp.Length < KeccakLength)
    //                     {
    //                         rlp.Write(childKeccakOrRlp);
    //                     }
    //                     else
    //                     {
    //                         rlp.EncodeKeccak(childKeccakOrRlp);
    //                     }    
    //                 }
    //             }
    //         }
    //         
    //         // write empty value 
    //         rlp.EncodeEmptyArray();
    //
    //         // write length
    //         var pos = rlp.Position;
    //         var length = pos - MaxLengthOfLengths;
    //         var sequenceLength = Rlp.Rlp.LengthOfLength(length);
    //         var actualStart = MaxLengthOfLengths - sequenceLength;
    //         
    //         rlp.Position = actualStart;
    //         rlp.StartSequence(length);
    //         rlp.Position = pos;
    //
    //         return WrapRlp(bytes.AsSpan(actualStart, length + sequenceLength), destination);
    //     }
    //     finally
    //     {
    //         pool.Return(bytes);
    //     }
    // }

    internal static KeccakOrRlp EncodeLeaf(NibblePath path, ReadOnlySpan<byte> value, Span<byte> destination)
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
    
    internal static KeccakOrRlp EncodeExtension(NibblePath path, ReadOnlySpan<byte> childKeccakOrRlp, Span<byte> destination)
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

    private static KeccakOrRlp WrapRlp(in Span<byte> data, Span<byte> destination)
    {
        if (data.Length < 32)
        {
            destination[0] = (byte)data.Length;
            data.CopyTo(destination.Slice(RlpLengthOfLength));
            return KeccakOrRlp.Rlp;
        }

        KeccakHash.ComputeHash(data, destination);
        return KeccakOrRlp.Keccak;
    }
}