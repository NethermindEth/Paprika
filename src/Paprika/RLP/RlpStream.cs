using Paprika.Crypto;
using Nethermind.Int256;
using Paprika.Utils;

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
        // TODO: If keccak is a known one like `Keccak.OfAnEmptyString` or `Keccak.OfAnEmptySequenceRlp`
        // we can cache those `Rlp`s to be reused

        WriteByte(160);
        Write(keccak.BytesAsSpan);

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
}
