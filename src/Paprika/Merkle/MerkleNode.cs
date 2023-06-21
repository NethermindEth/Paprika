using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Merkle;

public static class Node
{
    public enum Type : byte
    {
        Leaf,
        Extension,
        Branch,
    }

    private static void ValidateHeaderNodeType(Header header, Type expected)
    {
        if (header.NodeType != expected)
        {
            throw new ArgumentException($"Expected Header with {nameof(Type)} {expected}, got {header.NodeType}");
        }
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
    public readonly struct Header
    {
        public const int Size = sizeof(byte);

        private const byte IsDirtyMask = 0b0001;
        private const byte NodeTypeMask = 0b0110;

        [FieldOffset(0)]
        private readonly byte _header;

        public bool IsDirty => (_header & IsDirtyMask) != 0;
        public Type NodeType => (Type)((_header & NodeTypeMask) >> 1);

        public Header(Type nodeType, bool isDirty = true)
        {
            _header = (byte)((byte)nodeType << 1 | (isDirty ? IsDirtyMask : 0));
        }

        public Span<byte> WriteTo(Span<byte> output)
        {
            output[0] = _header;
            return output.Slice(Size);
        }

        public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Header header)
        {
            var isDirty = (source[0] & IsDirtyMask) != 0;
            var nodeType = (Type)((source[0] & NodeTypeMask) >> 1);
            header = new Header(nodeType, isDirty);

            return source.Slice(Size);
        }

        public bool Equals(in Header other)
        {
            return _header.Equals(other._header);
        }

        public override string ToString() =>
            $"{nameof(Header)} {{ " +
            $"{nameof(IsDirty)}: {IsDirty}, " +
            $"{nameof(Type)}: {NodeType} " +
            $"}}";
    }

    public readonly ref struct Leaf
    {
        public int MaxByteLength => Header.Size + Path.MaxByteLength + Keccak.Size;

        public readonly Header Header;
        public readonly NibblePath Path;
        public readonly Keccak Keccak;

        private Leaf(Header header, NibblePath path, Keccak keccak)
        {
            ValidateHeaderNodeType(header, Type.Leaf);
            Header = header;
            Path = path;
            Keccak = keccak;
        }

        public Leaf(NibblePath path, Keccak keccak)
        {
            Header = new Header(Type.Leaf);
            Path = path;
            Keccak = keccak;
        }

        public Span<byte> WriteTo(Span<byte> output)
        {
            var leftover = Header.WriteTo(output);
            leftover = Path.WriteToWithLeftover(leftover);
            leftover = Keccak.WriteTo(leftover);

            return leftover;
        }

        public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Leaf leaf)
        {
            var leftover = Header.ReadFrom(source, out var header);
            leftover = NibblePath.ReadFrom(leftover, out var path);
            leftover = Keccak.ReadFrom(leftover, out var keccak);

            leaf = new Leaf(header, path, keccak);
            return leftover;
        }

        public bool Equals(in Leaf other) =>
            Header.Equals(other.Header)
            && Path.Equals(other.Path)
            && Keccak.Equals(other.Keccak);

        public override string ToString() =>
            $"{nameof(Leaf)} {{ " +
            $"{nameof(Header)}: {Header.ToString()}, " +
            $"{nameof(Path)}: {Path.ToString()}, " +
            $"{nameof(Keccak)}: {Keccak} " +
            $"}}";
    }

    public readonly ref struct Extension
    {
        public int MaxByteLength => Header.Size + Path.MaxByteLength;

        public readonly Header Header;
        public readonly NibblePath Path;

        private Extension(Header header, NibblePath path)
        {
            ValidateHeaderNodeType(header, Type.Extension);
            Header = header;
            Path = path;
        }

        public Extension(NibblePath path)
        {
            Header = new Header(Type.Extension);
            Path = path;
        }

        public Span<byte> WriteTo(Span<byte> output)
        {
            var leftover = Header.WriteTo(output);
            leftover = Path.WriteToWithLeftover(leftover);

            return leftover;
        }

        public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Extension extension)
        {
            var leftover = Header.ReadFrom(source, out var header);
            leftover = NibblePath.ReadFrom(leftover, out var path);

            extension = new Extension(header, path);
            return leftover;
        }

        public bool Equals(in Extension other) =>
            Header.Equals(other.Header)
            && Path.Equals(other.Path);

        public override string ToString() =>
            $"{nameof(Extension)} {{ " +
            $"{nameof(Header)}: {Header.ToString()}, " +
            $"{nameof(Path)}: {Path.ToString()}, " +
            $"}}";
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
    public readonly ref struct Branch
    {
        public const int Size = 35;
        private const int NibbleBitSetSize = sizeof(ushort);

        [FieldOffset(0)]
        public readonly Header Header;

        [FieldOffset(1)]
        public readonly ushort NibbleBitSet;

        [FieldOffset(3)]
        public readonly Keccak Keccak;

        // TODO: What interface do we want to expose for nibbles?
        // Options:
        // - `IEnumerable<byte>` with all nibbles is not possible
        // - `byte[]` with all nibbles
        // - `bool HasNibble(byte nibble)` to lookup a single nibble at a time
        public bool HasNibble(byte nibble) => (NibbleBitSet & (1 << nibble)) != 0;

        private Branch(Header header, ushort nibbleBitSet, Keccak keccak)
        {
            ValidateHeaderNodeType(header, Type.Branch);
            Header = header;
            NibbleBitSet = nibbleBitSet;
            Keccak = keccak;
        }

        public Branch(ushort nibbleBitSet, Keccak keccak)
        {
            Header = new Header(Type.Branch);
            NibbleBitSet = nibbleBitSet;
            Keccak = keccak;
        }

        public Span<byte> WriteTo(Span<byte> output)
        {
            var leftover = Header.WriteTo(output);

            BinaryPrimitives.WriteUInt16LittleEndian(leftover, NibbleBitSet);
            leftover = leftover.Slice(NibbleBitSetSize);

            leftover = Keccak.WriteTo(leftover);

            return leftover;
        }

        public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Branch branch)
        {
            var leftover = Header.ReadFrom(source, out var header);

            var nibbleBitSet = BinaryPrimitives.ReadUInt16LittleEndian(leftover);
            leftover = leftover.Slice(NibbleBitSetSize);

            leftover = Keccak.ReadFrom(leftover, out var keccak);

            branch = new Branch(header, nibbleBitSet, keccak);

            return leftover;
        }

        public bool Equals(in Branch other)
        {
            return Header.Equals(other.Header)
                   && NibbleBitSet.Equals(other.NibbleBitSet)
                   && Keccak.Equals(other.Keccak);
        }

        public override string ToString() =>
            $"{nameof(Branch)} {{ " +
            $"{nameof(Header)}: {Header.ToString()}, " +
            $"{nameof(NibbleBitSet)}: {NibbleBitSet}, " +
            $"{nameof(Keccak)}: {Keccak} " +
            $"}}";
    }
}
