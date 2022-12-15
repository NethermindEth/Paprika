// using System.Security.Cryptography;
// using Tree.Crypto;
// using Tree.Rlp;
//
// namespace Tree;
//
// public partial class PaprikaTree
// {
//     private static void GetNodeKeccak(IDb db, long id)
//     {
//         
//     }
//     
//     private static int EncodeLeaf(NibblePath path, ReadOnlySpan<byte> value, Span<byte> destination)
//     {
//         Span<byte> hexPath = stackalloc byte [path.HexEncodedLength];
//         path.HexEncode(hexPath, true);
//         
//         var contentLength = Rlp.Rlp.LengthOf(hexPath) + Rlp.Rlp.LengthOf(value);
//         var totalLength = Rlp.Rlp.LengthOfSequence(contentLength);
//
//         Span<byte> data = stackalloc byte[totalLength];
//         
//         RlpStream rlp = new(data);
//         
//         rlp.StartSequence(contentLength);
//         rlp.Encode(hexPath);
//         rlp.Encode(value);
//
//         return WrapRlp(rlp, destination);
//     }
//
//     private static int WrapRlp(in RlpStream rlp, Span<byte> destination)
//     {
//         var data = rlp.Data;
//         
//         if (data.Length >= 32)
//         {
//             var keccak = ValueKeccak.Compute(data);
//             keccak.BytesAsSpan.CopyTo(destination);
//             return 32;
//         }
//
//         data.CopyTo(destination);
//         return data.Length;
//     }
// }