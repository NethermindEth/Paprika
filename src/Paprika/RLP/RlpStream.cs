using Paprika.Crypto;
using Nethermind.Int256;
using Paprika.Utils;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Paprika.RLP;

public ref struct RlpStream
{
    private const byte EmptyArrayByte = 128;

    public readonly Span<byte> Data;
    public int Position { get; set; }
    public int Length => Data.Length;

    public RlpStream(Span<byte> data)
    {
        Data = data;
    }

    public RlpStream StartSequence(int contentLength)
    {
        byte prefix;
        if (contentLength < 56)
        {
            prefix = (byte)(192 + contentLength);
            WriteByte(prefix);
        }
        else
        {
            prefix = (byte)(247 + Rlp.LengthOfLength(contentLength));
            WriteByte(prefix);
            WriteEncodedLength(contentLength);
        }

        return this;
    }

    public void WriteByte(byte byteToWrite)
    {
        Data[Position++] = byteToWrite;
    }

    public void Write(scoped ReadOnlySpan<byte> bytesToWrite)
    {
        bytesToWrite.CopyTo(Data.Slice(Position, bytesToWrite.Length));
        Position += bytesToWrite.Length;
    }

    public void EncodeKeccak(in ReadOnlySpan<byte> keccak)
    {
        WriteByte(160);
        Write(keccak);
    }

    public void EncodeEmptyArray() => WriteByte(EmptyArrayByte);

    public RlpStream Encode(scoped ReadOnlySpan<byte> input)
    {
        if (input.Length == 0)
        {
            WriteByte(EmptyArrayByte);
        }
        else if (input.Length == 1 && input[0] < 128)
        {
            WriteByte(input[0]);
        }
        else if (input.Length < 56)
        {
            byte smallPrefix = (byte)(input.Length + 128);
            WriteByte(smallPrefix);
            Write(input);
        }
        else
        {
            int lengthOfLength = Rlp.LengthOfLength(input.Length);
            byte prefix = (byte)(183 + lengthOfLength);
            WriteByte(prefix);
            WriteEncodedLength(input.Length);
            Write(input);
        }

        return this;
    }

    public RlpStream Encode(in UInt256 value)
    {
        if (value.IsZero)
        {
            WriteByte(EmptyArrayByte);
        }
        else
        {
            Span<byte> bytes = stackalloc byte[32];
            value.ToBigEndian(bytes);
            Encode(bytes.WithoutLeadingZeros());
        }

        return this;
    }

    public RlpStream Encode(in Keccak keccak)
    {
        WriteByte(160);

        // Fast unaligned write instead of byte copy
        Unsafe.WriteUnaligned(ref Data[Position], keccak);
        Position += Keccak.Size;

        return this;
    }

    private void WriteEncodedLength(int value)
    {
        switch (value)
        {
            case < 1 << 8:
                WriteByte((byte)value);
                return;
            case < 1 << 16:
                WriteByte((byte)(value >> 8));
                WriteByte((byte)value);
                return;
            case < 1 << 24:
                WriteByte((byte)(value >> 16));
                WriteByte((byte)(value >> 8));
                WriteByte((byte)value);
                return;
            default:
                WriteByte((byte)(value >> 24));
                WriteByte((byte)(value >> 16));
                WriteByte((byte)(value >> 8));
                WriteByte((byte)value);
                return;
        }
    }

    public override string ToString() => $"[{nameof(RlpStream)}|{Position}/{Length}]";

    public static UInt256 DecodeUInt256(ReadOnlySpan<byte> span)
    {
        byte byteValue = span[0];
        if (byteValue == 0)
        {
            ThrowNonCanonical();
        }

        if (byteValue < 128)
        {
            return byteValue;
        }

        ReadOnlySpan<byte> byteSpan = DecodeByteArraySpan(span);

        if (byteSpan.Length > 32)
        {
            ThrowWrongSize();
        }

        if (byteSpan.Length > 1 && byteSpan[0] == 0)
        {
            ThrowNonCanonical();
        }

        return new UInt256(byteSpan, true);

        ReadOnlySpan<byte> DecodeByteArraySpan(ReadOnlySpan<byte> data)
        {
            int prefix = data[0];
            ReadOnlySpan<byte> span = SingleBytes();
            if ((uint)prefix < (uint)span.Length)
            {
                return span.Slice(prefix, 1);
            }

            if (prefix == 128)
            {
                return default;
            }

            if (prefix <= 183)
            {
                int length = prefix - 128;
                ReadOnlySpan<byte> buffer = data.Slice(1, length);
                if (buffer.Length == 1 && buffer[0] < 128)
                {
                    ThrowUnexpectedValue(buffer[0]);
                }

                return buffer;
            }

            ThrowUnexpectedValue(prefix);
            return default;

            static ReadOnlySpan<byte> SingleBytes() => new byte[128] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127 };

            [DoesNotReturn]
            [StackTraceHidden]
            static void ThrowUnexpectedValue(int buffer0)
            {
                throw new Exception($"Unexpected byte value {buffer0}");
            }
        }

        [DoesNotReturn]
        [StackTraceHidden]
        static void ThrowNonCanonical()
        {
            throw new Exception($"Non-canonical UInt256 (leading zero bytes)");
        }

        [DoesNotReturn]
        [StackTraceHidden]
        static void ThrowWrongSize()
        {
            throw new Exception("UInt256 cannot be longer than 32 bytes");
        }
    }
}
