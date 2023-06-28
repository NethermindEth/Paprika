using System.Buffers.Binary;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Merkle;

public static partial class Node
{
    public enum Type : byte
    {
        Leaf,
        Extension,
        Branch,
    }

    public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Type nodeType, out Leaf leaf, out Extension extension, out Branch branch)
    {
        leaf = new Leaf();
        extension = new Extension();
        branch = new Branch();

        // TODO: It would be nice to not read the header twice:
        // - Once to get the header
        // - Again to read each Node type
        Header.ReadFrom(source, out var header);
        switch (header.NodeType)
        {
            case Type.Leaf:
                nodeType = Type.Leaf;
                return Leaf.ReadFrom(source, out leaf);
            case Type.Extension:
                nodeType = Type.Extension;
                return Extension.ReadFrom(source, out extension);
            case Type.Branch:
                nodeType = Type.Branch;
                return Branch.ReadFrom(source, out branch);
            default:
                throw new ArgumentOutOfRangeException($"Could not decode {nameof(Header)}");
        }
    }

    private static Header ValidateHeaderNodeType(Header header, Type expected)
    {
        Debug.Assert(header.NodeType == expected, $"Expected {nameof(Header)} with {nameof(Type)} {expected}, got {header.NodeType}");

        return header;
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
    public readonly struct Header
    {
        public const int Size = sizeof(byte);

        private const byte IsDirtyMask = 0b0001;
        private const int DirtyMaskShift = 0;
        private const byte Dirty = 0b0001;
        private const byte NotDirty = 0b0000;

        private const byte NodeTypeMask = 0b0110;
        private const int NodeTypeMaskShift = 1;

        private const byte MetadataMask = 0b1111_0000;
        private const int MetadataMaskShift = 4;

        [FieldOffset(0)]
        private readonly byte _header;

        public bool IsDirty => (_header & IsDirtyMask) >> DirtyMaskShift == Dirty;
        public Type NodeType => (Type)((_header & NodeTypeMask) >> NodeTypeMaskShift);
        public byte Metadata => (byte)((_header & MetadataMask) >> MetadataMaskShift);

        public Header(Type nodeType, bool isDirty = true, byte metadata = 0b0000)
        {
            _header = (byte)(metadata << MetadataMaskShift);
            _header |= (byte)((byte)nodeType << NodeTypeMaskShift);
            _header |= isDirty ? Dirty : NotDirty;
        }

        private Header(byte header)
        {
            _header = header;
        }

        public Span<byte> WriteTo(Span<byte> output)
        {
            var leftover = WriteToWithLeftover(output);
            return output.Slice(0, output.Length - leftover.Length);
        }

        public Span<byte> WriteToWithLeftover(Span<byte> output)
        {
            output[0] = _header;
            return output.Slice(Size);
        }

        public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Header header)
        {
            header = new Header(source[0]);
            return source.Slice(Size);
        }

        public bool Equals(in Header other)
        {
            return _header.Equals(other._header);
        }

        public override string ToString() =>
            $"{nameof(Header)} {{ " +
            $"{nameof(IsDirty)}: {IsDirty}, " +
            $"{nameof(Type)}: {NodeType}, " +
            $"{nameof(Metadata)}: 0b{Convert.ToString(Metadata, 2).PadLeft(4, '0')}" +
            $" }}";
    }

    public readonly ref partial struct Leaf
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
            var leftover = WriteToWithLeftover(output);
            return output.Slice(0, output.Length - leftover.Length);
        }

        public Span<byte> WriteToWithLeftover(Span<byte> output)
        {
            var leftover = Header.WriteToWithLeftover(output);
            leftover = Path.WriteToWithLeftover(leftover);
            leftover = Keccak.WriteToWithLeftover(leftover);

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
            var leftover = WriteToWithLeftover(output);
            return output.Slice(0, output.Length - leftover.Length);
        }

        public Span<byte> WriteToWithLeftover(Span<byte> output)
        {
            var leftover = Header.WriteToWithLeftover(output);
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

    public readonly ref struct Branch
    {
        public int MaxByteLength => Header.Size + sizeof(ushort) + (HeaderHasKeccak(Header) ? Keccak.Size : 0);
        private const int NibbleBitSetSize = sizeof(ushort);

        private const int HeaderMetadataKeccakMask = 0b0001;
        private const int NoKeccak = 0;
        private const int HasKeccak = 1;

        public readonly Header Header;
        public readonly ushort NibbleBitSet;
        public readonly Keccak Keccak;

        // TODO: What interface do we want to expose for nibbles?
        // Options:
        // - `IEnumerable<byte>` with all nibbles is not possible
        // - `byte[]` with all nibbles
        // - `bool HasNibble(byte nibble)` to lookup a single nibble at a time
        public bool HasNibble(byte nibble) => (NibbleBitSet & (1 << nibble)) != 0;

        private Branch(Header header, ushort nibbleBitSet, Keccak keccak)
        {
            Header = ValidateHeaderKeccak(ValidateHeaderNodeType(header, Type.Branch), shouldHaveKeccak: true);
            NibbleBitSet = ValidateNibbleBitSet(nibbleBitSet);
            Keccak = keccak;
        }

        private Branch(Header header, ushort nibbleBitSet)
        {
            Header = ValidateHeaderKeccak(header, shouldHaveKeccak: false);
            NibbleBitSet = ValidateNibbleBitSet(nibbleBitSet);
            Keccak = default;
        }

        public Branch(ushort nibbleBitSet, Keccak keccak)
        {
            Header = new Header(Type.Branch, metadata: HasKeccak);
            NibbleBitSet = ValidateNibbleBitSet(nibbleBitSet);
            Keccak = keccak;
        }

        public Branch(ushort nibbleBitSet)
        {
            Header = new Header(Type.Branch, metadata: NoKeccak);
            NibbleBitSet = ValidateNibbleBitSet(nibbleBitSet);
            Keccak = default;
        }

        private static ushort ValidateNibbleBitSet(ushort nibbleBitSet)
        {
            var count = BitOperations.PopCount(nibbleBitSet);
            if (count < 2)
            {
                throw new ArgumentException("At least two nibbles should be set, but only {count} were found", nameof(nibbleBitSet));
            }

            return nibbleBitSet;
        }

        private static Header ValidateHeaderKeccak(Header header, bool shouldHaveKeccak)
        {
            var expected = shouldHaveKeccak ? HasKeccak : NoKeccak;
            var actual = header.Metadata & HeaderMetadataKeccakMask;

            Debug.Assert(actual == expected, $"Expected {nameof(Header)} to have {nameof(Keccak)} = {shouldHaveKeccak}, got {!shouldHaveKeccak}");

            return header;
        }

        private static bool HeaderHasKeccak(Header header) =>
            (header.Metadata & HeaderMetadataKeccakMask) == HasKeccak;

        public Span<byte> WriteTo(Span<byte> output)
        {
            var leftover = WriteToWithLeftover(output);
            return output.Slice(0, output.Length - leftover.Length);
        }

        public Span<byte> WriteToWithLeftover(Span<byte> output)
        {
            var leftover = Header.WriteToWithLeftover(output);

            BinaryPrimitives.WriteUInt16LittleEndian(leftover, NibbleBitSet);
            leftover = leftover.Slice(NibbleBitSetSize);

            if (HeaderHasKeccak(Header))
            {
                leftover = Keccak.WriteToWithLeftover(leftover);
            }

            return leftover;
        }

        public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Branch branch)
        {
            var leftover = Header.ReadFrom(source, out var header);

            var nibbleBitSet = BinaryPrimitives.ReadUInt16LittleEndian(leftover);
            leftover = leftover.Slice(NibbleBitSetSize);

            if (HeaderHasKeccak(header))
            {
                leftover = Keccak.ReadFrom(leftover, out var keccak);
                branch = new Branch(header, nibbleBitSet, keccak);
            }
            else
            {
                branch = new Branch(header, nibbleBitSet);
            }

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
