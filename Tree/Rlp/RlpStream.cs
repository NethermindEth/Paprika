// namespace Tree.Rlp;
//
// public ref struct RlpStream
// {
//     public RlpStream(Span<byte> data)
//     {
//         Data = data;
//     }
//
//     public void StartSequence(int contentLength)
//     {
//         byte prefix;
//         if (contentLength < 56)
//         {
//             prefix = (byte)(192 + contentLength);
//             WriteByte(prefix);
//         }
//         else
//         {
//             prefix = (byte)(247 + Rlp.LengthOfLength(contentLength));
//             WriteByte(prefix);
//             WriteEncodedLength(contentLength);
//         }
//     }
//
//     private void WriteEncodedLength(int value)
//     {
//         switch (value)
//         {
//             case < 1 << 8:
//                 WriteByte((byte)value);
//                 return;
//             case < 1 << 16:
//                 WriteByte((byte)(value >> 8));
//                 WriteByte((byte)value);
//                 return;
//             case < 1 << 24:
//                 WriteByte((byte)(value >> 16));
//                 WriteByte((byte)(value >> 8));
//                 WriteByte((byte)value);
//                 return;
//             default:
//                 WriteByte((byte)(value >> 24));
//                 WriteByte((byte)(value >> 16));
//                 WriteByte((byte)(value >> 8));
//                 WriteByte((byte)value);
//                 return;
//         }
//     }
//
//     public void WriteByte(byte byteToWrite)
//     {
//         Data[Position++] = byteToWrite;
//     }
//
//     public void Write(ReadOnlySpan<byte> bytesToWrite)
//     {
//         bytesToWrite.CopyTo(Data.Slice(Position, bytesToWrite.Length));
//         Position += bytesToWrite.Length;
//     }
//
//     public readonly Span<byte> Data;
//
//     public int Position { get; set; }
//
//     public int Length => Data.Length;
//
//     public void Encode(Keccak? keccak)
//     {
//         if (keccak == null)
//         {
//             WriteByte(EmptyArrayByte);
//         }
//         else if (ReferenceEquals(keccak, Keccak.EmptyTreeHash))
//         {
//             Write(Rlp.OfEmptyTreeHash.Bytes);
//         }
//         else if (ReferenceEquals(keccak, Keccak.OfAnEmptyString))
//         {
//             Write(Rlp.OfEmptyStringHash.Bytes);
//         }
//         else
//         {
//             WriteByte(160);
//             Write(keccak.Bytes);
//         }
//     }
//
//     public void Encode(Keccak[] keccaks)
//     {
//         if (keccaks == null)
//         {
//             EncodeNullObject();
//         }
//         else
//         {
//             var length = Rlp.LengthOf(keccaks);
//             StartSequence(length);
//             for (int i = 0; i < keccaks.Length; i++)
//             {
//                 Encode(keccaks[i]);
//             }
//         }
//     }
//         
//     public void Encode(Rlp? rlp)
//     {
//         if (rlp == null)
//         {
//             WriteByte(EmptyArrayByte);
//         }
//         else
//         {
//             Write(rlp.Bytes);
//         }
//     }
//
//     void WriteZero(int length)
//     {
//         Position += 256;
//     }
//
//     public void Encode(byte value)
//     {
//         if (value == 0)
//         {
//             WriteByte(128);
//         }
//         else if (value < 128)
//         {
//             WriteByte(value);
//         }
//         else
//         {
//             WriteByte(129);
//             WriteByte(value);
//         }
//     }
//
//     public void Encode(bool value)
//     {
//         Encode(value ? (byte)1 : (byte)0);
//     }
//
//     public void Encode(ReadOnlySpan<byte> input)
//     {
//         if (input.Length == 0)
//         {
//             WriteByte(EmptyArrayByte);
//         }
//         else if (input.Length == 1 && input[0] < 128)
//         {
//             WriteByte(input[0]);
//         }
//         else if (input.Length < 56)
//         {
//             byte smallPrefix = (byte)(input.Length + 128);
//             WriteByte(smallPrefix);
//             Write(input);
//         }
//         else
//         {
//             int lengthOfLength = Rlp.LengthOfLength(input.Length);
//             byte prefix = (byte)(183 + lengthOfLength);
//             WriteByte(prefix);
//             WriteEncodedLength(input.Length);
//             Write(input);
//         }
//     }
//     public void EncodeNullObject()
//     {
//         WriteByte(EmptySequenceByte);
//     }
//
//     public void EncodeEmptyByteArray()
//     {
//         WriteByte(EmptyArrayByte);
//     }
//
//     private const byte EmptyArrayByte = 128;
//
//     private const byte EmptySequenceByte = 192;
//
//     public override string ToString()
//     {
//         return $"[{nameof(RlpStream)}|{Position}/{Length}]";
//     }
// }