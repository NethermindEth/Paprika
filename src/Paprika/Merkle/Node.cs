using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Data;
using static System.Diagnostics.Debug;

namespace Paprika.Merkle;

public static partial class Node
{
    public enum Type : byte
    {
        Leaf = 2,

        /// <summary>
        /// The extension is used both, to encode the Extension but also to encode the <see cref="Boundary"/> nodes for the snap sync.
        /// The fact that no extension can have a nibble path of length of 64 is used here.
        /// </summary>
        Extension = 1,

        Branch = 0,
    }

    [SkipLocalsInit]
    public static Type ReadFrom(out Leaf leaf, out Extension extension, ReadOnlySpan<byte> source)
    {
        leaf = default;
        extension = default;

        // TODO: It would be nice to not read the header twice:
        // - Once to get the header
        // - Again to read each Node type

        var nodeType = Header.GetTypeFrom(source);
        switch (nodeType)
        {
            case Type.Leaf:
                Leaf.ReadFrom(source, out leaf);
                break;
            case Type.Extension:
                Extension.ReadFrom(source, out extension);
                break;
            case Type.Branch:
                // Do nothing
                break;
            default:
                ThrowUnknownNodeType(nodeType);
                break;
        }

        return nodeType;

        [DoesNotReturn]
        [StackTraceHidden]
        static void ThrowUnknownNodeType(Type nodeType)
        {
            throw new ArgumentOutOfRangeException($"Unacceptable extension type {nodeType}");
        }
    }

    public static ReadOnlySpan<byte> ReadFrom(out Type nodeType, out Leaf leaf, out Extension extension,
        out Branch branch, ReadOnlySpan<byte> source)
    {
        leaf = default;
        extension = default;
        branch = default;

        // TODO: It would be nice to not read the header twice:
        // - Once to get the header
        // - Again to read each Node type

        switch (nodeType = Header.GetTypeFrom(source))
        {
            case Type.Leaf:
                return Leaf.ReadFrom(source, out leaf);
            case Type.Extension:
                return Extension.ReadFrom(source, out extension);
            case Type.Branch:
                return Branch.ReadFrom(source, out branch);
            default:
                ThrowUnknownNodeType();
                return default;
        }

        [DoesNotReturn]
        [StackTraceHidden]
        static void ThrowUnknownNodeType()
        {
            throw new ArgumentOutOfRangeException($"Could not decode {nameof(Header)}");
        }
    }

    private static Header ValidateHeaderNodeType(Header header, Type expected)
    {
        Assert(header.NodeType == expected,
            $"Expected {nameof(Header)} with {nameof(Type)} {expected}, got {header.NodeType}");

        return header;
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
    public readonly struct Header
    {
        public const int Size = sizeof(byte);

        private const byte HighestBit = 0b1000_0000;

        private const byte NodeTypeMask = 0b1100_0000;
        private const int NodeTypeMaskShift = 6;

        private const byte MetadataMask = 0b0011_1111;
        private const int MetadataMaskShift = 0;

        [FieldOffset(0)] private readonly byte _header;

        public Type NodeType
        {
            get
            {
                var value = (_header & NodeTypeMask) >> NodeTypeMaskShift;

                // 2 eliminates cases where a leaf takes 7bits for metadata
                return value >= 2 ? Type.Leaf : (Type)value;
            }
        }

        /// <summary>
        /// The part ((_header & HighestBit) >> 1)) allows for node types with the highest bit set
        /// <see cref="Type.Leaf"/> to have 7 bits of metadata.
        /// </summary>
        public byte Metadata => (byte)((((_header & MetadataMask) | ((_header & HighestBit) >> 1)) & _header) >> MetadataMaskShift);

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

        public static Type GetTypeFrom(ReadOnlySpan<byte> source)
        {
            return new Header(source[0]).NodeType;
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
        public int MaxByteLength => Header.Size + Path.RawSpan.Length;

        private const byte OddPathMetadata = 0b0100_0000;
        private const byte OddPathMetadataShift = 6;
        private const byte LengthMetadata = 0b0011_1111;

        /// <summary>
        /// There's a single 64-nibble long path.
        /// There are two 63-nibble long paths:
        /// 1. an odd, encoded: 0b0111_1110
        /// 2. an even, encoded: 0b0011_1110
        /// aw we subtract 1 from length as no leafs with 0 paths are stored.
        /// This make it unique addressing and allows to save one byte for the nibble path length. 
        /// </summary>
        private const byte FullPathMetadata = 0b0111_1111;

        public const int MinimalLeafPathLength = 1;

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
            // Leaves shall never be marked as dirty or not. This information will be held by branch

            Assert(path.Length >= MinimalLeafPathLength);

            if (path.Length == NibblePath.KeccakNibbleCount)
            {
                Header = new Header(Type.Leaf, FullPathMetadata);
            }
            else
            {
                var oddity = path.IsOdd ? OddPathMetadata : default;
                var length = path.Length - MinimalLeafPathLength;
                Header = new Header(Type.Leaf, (byte)(oddity | length));
            }

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
            Path.RawSpan.CopyTo(leftover);
            return leftover[Path.RawSpan.Length..];
        }

        public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Leaf leaf)
        {
            var leftover = Header.ReadFrom(source, out var header);

            if (header.Metadata == FullPathMetadata)
            {
                leaf = new Leaf(header, NibblePath.FromKey(leftover[..Keccak.Size]));
                return leftover[Keccak.Size..];
            }

            var length = (header.Metadata & LengthMetadata) + MinimalLeafPathLength;
            var oddity = (header.Metadata & OddPathMetadata) >> OddPathMetadataShift;

            var actualLength = (length + oddity + MinimalLeafPathLength) / 2;

            var path = NibblePath.FromKey(leftover.Slice(0, actualLength))
                .SliceFrom(oddity)
                .SliceTo(length);

            leaf = new Leaf(header, path);
            return leftover[actualLength..];
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
                                    (HeaderHasKeccak(Header) ? Keccak.Size : 0);

        private const byte HeaderMetadataKeccakMask = 0b0000_0001;
        private const byte HeaderMetadataAllChildrenSetMask = 0b0000_0010;

        public readonly Header Header;
        public readonly NibbleSet.Readonly Children;
        public readonly Keccak Keccak;

        private Branch(Header header, NibbleSet.Readonly children, Keccak keccak)
        {
            Header = ValidateHeaderKeccak(ValidateHeaderNodeType(header, Type.Branch), shouldHaveKeccak: true);
            Children = children;
            Keccak = keccak;
        }

        private Branch(Header header, NibbleSet.Readonly children)
        {
            Header = ValidateHeaderKeccak(header, shouldHaveKeccak: false);
            Children = children;
            Keccak = default;
        }

        public Branch(NibbleSet.Readonly children, Keccak keccak)
        {
            var allSet = children.AllSet ? HeaderMetadataAllChildrenSetMask : 0;
            var hasKeccak = keccak == default ? 0 : HeaderMetadataKeccakMask;

            Header = new Header(Type.Branch, metadata: (byte)(hasKeccak | allSet));

            Assert(children);

            Children = children;
            Keccak = keccak;
        }

        private static void Assert(NibbleSet.Readonly set)
        {
            if (set.SetCount < 2)
            {
                throw new ArgumentException($"At least two nibbles should be set, but only {set.SetCount} were found");
            }
        }

        public Branch(NibbleSet.Readonly children)
        {
            Header = new Header(Type.Branch, metadata: (byte)(children.AllSet ? HeaderMetadataAllChildrenSetMask : 0));

            Assert(children);

            Children = children;
            Keccak = default;
        }

        private static Header ValidateHeaderKeccak(Header header, bool shouldHaveKeccak)
        {
            var expected = shouldHaveKeccak ? HeaderMetadataKeccakMask : 0;
            var actual = header.Metadata & HeaderMetadataKeccakMask;

            Debug.Assert(actual == expected,
                $"Expected {nameof(Header)} to have {nameof(Keccak)} = {shouldHaveKeccak}, got {!shouldHaveKeccak}");

            return header;
        }

        public bool HasKeccak => HeaderHasKeccak(Header);

        private static bool HeaderHasKeccak(Header header) =>
            (header.Metadata & HeaderMetadataKeccakMask) == HeaderMetadataKeccakMask;

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

            if (HeaderHasKeccak(header))
            {
                leftover = Keccak.ReadFrom(leftover, out var keccak);
                branch = new Branch(header, children, keccak);
            }
            else
            {
                branch = new Branch(header, children);
            }

            return leftover;
        }

        /// <summary>
        /// Gets only branch data cleaning any leftovers.
        /// </summary>
        public static ReadOnlySpan<byte> GetOnlyBranchData(ReadOnlySpan<byte> source)
        {
            Header.ReadFrom(source, out var header);
            return source[..GetBranchDataLength(header)];
        }

        private static int GetBranchDataLength(Header header) =>
            Header.Size +
            (HeaderHasAllSet(header) ? 0 : NibbleSet.MaxByteSize) +
            (HeaderHasKeccak(header) ? Keccak.Size : 0);

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
