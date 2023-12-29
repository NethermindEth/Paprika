using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Data;
using static System.Diagnostics.Debug;

namespace Paprika.Merkle;

public static partial class Node
{
    public enum Type : byte
    {
        Leaf = 0,
        Extension = 1,
        Branch = 2,
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

    private static void ValidateHeaderNodeType(Header header, Type expected)
    {
        Assert(header.NodeType == expected, $"Expected {nameof(Header)} with {nameof(Type)} {expected}, got {header.NodeType}");
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
    public readonly struct Header
    {
        public const int Size = sizeof(byte);

        private const byte NodeTypeMask = 0b0000_0011;
        private const int NodeTypeMaskShift = 0;

        private const byte MetadataMask = 0b1111_1100;
        private const int MetadataMaskShift = 2;

        [FieldOffset(0)]
        private readonly byte _header;

        public Type NodeType => (Type)((_header & NodeTypeMask) >> NodeTypeMaskShift);
        public byte Metadata => (byte)((_header & MetadataMask) >> MetadataMaskShift);

        public Header(Type nodeType, byte metadata = 0b0000)
        {
            _header = (byte)(metadata << MetadataMaskShift);
            _header |= (byte)((byte)nodeType << NodeTypeMaskShift);
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

        public static Header Peek(ReadOnlySpan<byte> source) => new(source[0]);

        public bool Equals(in Header other) => _header.Equals(other._header);

        public override string ToString() =>
            $"{nameof(Header)} {{ " +
            $"{nameof(Type)}: {NodeType}, " +
            $"{nameof(Metadata)}: 0b{Convert.ToString(Metadata, 2).PadLeft(4, '0')}" +
            $" }}";
    }

    public readonly ref partial struct Leaf
    {
        public int MaxByteLength => Header.Size + Path.MaxByteLength;

        public readonly Header Header;
        public readonly NibblePath Path;

        private Leaf(Header header, NibblePath path)
        {
            ValidateHeaderNodeType(header, Type.Leaf);
            Header = header;
            Path = path;
        }

        public Leaf(NibblePath path)
        {
            // leaves shall never be marked as dirty or not. This information will be held by branch
            Header = new Header(Type.Leaf);
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

        public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Leaf leaf)
        {
            var leftover = Header.ReadFrom(source, out var header);
            leftover = NibblePath.ReadFrom(leftover, out var path);

            leaf = new Leaf(header, path);
            return leftover;
        }

        public bool Equals(in Leaf other) =>
            Header.Equals(other.Header)
            && Path.Equals(other.Path);

        public override string ToString() =>
            $"{nameof(Leaf)} {{ " +
            $"{nameof(Path)}: {Path.ToString()} " +
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
        public int MaxByteLength => Header.Size +
                                    (HeaderHasAllSet(Header) ? 0 : NibbleSet.MaxByteSize) +
                                    (HeaderHasKeccak(Header) ? Keccak.Size : 0) +
                                    (HeaderHasEmbeddedLeafs(Header) ? Leafs.MaxByteSize : 0);

        private const byte HeaderMetadataKeccakMask = 0b0000_0001;
        private const byte HeaderMetadataAllChildrenSetMask = 0b0000_0010;
        private const byte HeaderMetadataEmbeddedLeafsMask = 0b0000_0100;

        public readonly Header Header;
        public readonly NibbleSet.Readonly Children;
        public readonly Keccak Keccak;
        public readonly EmbeddedLeafs Leafs;

        private Branch(Header header, NibbleSet.Readonly children, scoped in Keccak keccak, EmbeddedLeafs leafs)
        {
            ValidateHeaderNodeType(header, Type.Branch);
            Header = header;
            Children = children;
            Keccak = keccak;
            Leafs = leafs;
        }

        public Branch(NibbleSet.Readonly children, scoped in Keccak keccak, scoped in EmbeddedLeafs leafs = default)
        {
            var allSet = children.AllSet ? HeaderMetadataAllChildrenSetMask : 0;
            var hasKeccak = keccak == default ? 0 : HeaderMetadataKeccakMask;
            var hasEmbeddedLeafs = (leafs.IsEmpty == false) ? HeaderMetadataEmbeddedLeafsMask : 0;

            Header = new Header(Type.Branch, metadata: (byte)(hasKeccak | allSet | hasEmbeddedLeafs));

            Assert(children);

            Children = children;
            Leafs = leafs;
            Keccak = keccak;
        }

        private static void Assert(NibbleSet.Readonly set)
        {
            if (set.SetCount < 2)
            {
                throw new ArgumentException($"At least two nibbles should be set, but only {set.SetCount} were found");
            }
        }

        public bool HasKeccak => HeaderHasKeccak(Header);

        private static bool HeaderHasKeccak(Header header) =>
            (header.Metadata & HeaderMetadataKeccakMask) == HeaderMetadataKeccakMask;

        private static bool HeaderHasEmbeddedLeafs(Header header) =>
            (header.Metadata & HeaderMetadataEmbeddedLeafsMask) == HeaderMetadataEmbeddedLeafsMask;

        private static bool HeaderHasAllSet(Header header) =>
            (header.Metadata & HeaderMetadataAllChildrenSetMask) == HeaderMetadataAllChildrenSetMask;

        public Span<byte> WriteTo(Span<byte> output)
        {
            var leftover = WriteToWithLeftover(output);
            return output.Slice(0, output.Length - leftover.Length);
        }

        public Span<byte> WriteToWithLeftover(Span<byte> output)
        {
            var leftover = Header.WriteToWithLeftover(output);

            if (!Children.AllSet)
            {
                // write children only if not all set. All set is encoded in the header
                leftover = Children.WriteToWithLeftover(leftover);
            }

            if (HeaderHasKeccak(Header))
            {
                leftover = Keccak.WriteToWithLeftover(leftover);
            }

            if (HeaderHasEmbeddedLeafs(Header))
            {
                leftover = Leafs.WriteToWithLeftover(leftover);
            }

            return leftover;
        }

        public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Branch branch)
        {
            var leftover = Header.ReadFrom(source, out var header);

            NibbleSet.Readonly children;
            if (HeaderHasAllSet(header))
            {
                children = NibbleSet.Readonly.All;
            }
            else
            {
                leftover = NibbleSet.Readonly.ReadFrom(leftover, out children);
            }

            Keccak keccak = default;
            if (HeaderHasKeccak(header))
            {
                leftover = Keccak.ReadFrom(leftover, out keccak);
            }

            EmbeddedLeafs leafs = default;
            if (HeaderHasEmbeddedLeafs(header))
            {
                leftover = EmbeddedLeafs.ReadFrom(leftover, out leafs);
            }

            branch = new Branch(header, children, keccak, leafs);

            return leftover;
        }

        public bool Equals(in Branch other)
        {
            return Header.Equals(other.Header)
                   && Children.Equals(other.Children)
                   && Keccak.Equals(other.Keccak);
        }

        public override string ToString() =>
            $"{nameof(Branch)} {{ " +
            $"{nameof(Header)}: {Header.ToString()}, " +
            $"{nameof(Children)}: {Children}, " +
            $"{nameof(Keccak)}: {Keccak} " +
            $"}}";
    }
}
