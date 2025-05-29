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

    [StructLayout(LayoutKind.Sequential, Pack = sizeof(byte), Size = Size)]
    public readonly struct Header
    {
        public const int Size = sizeof(byte);

        private const byte HighestBit = 0b1000_0000;

        private const byte NodeTypeMask = 0b1100_0000;
        private const int NodeTypeMaskShift = 6;

        private const byte MetadataMask = 0b0011_1111;
        private const int MetadataMaskShift = 0;

        private readonly byte _header;

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

        public static Type GetTypeFrom(ReadOnlySpan<byte> source) => new Header(source[0]).NodeType;

        public static Header Peek(ReadOnlySpan<byte> source) => new(source[0]);

        public bool Equals(in Header other) => _header.Equals(other._header);

        public override string ToString() =>
            $"{nameof(Header)} {{ " +
            $"{nameof(Type)}: {NodeType}, " +
            $"{nameof(Metadata)}: 0b{Convert.ToString(Metadata, 2).PadLeft(4, '0')}" +
            $" }}";
    }

    /// <remarks>
    /// Leafs are one of the biggest items in the database (Ethereum, mainnet, 40GB total).
    /// To encode them efficiently, the following structure is used. If the length of the leaf is:
    ///
    /// 1. even, the header contains even info and the raw bytes are added after the header.
    /// 2. odd, the header contains even info, then the first odd nibble is written, and the rest as in the above
    /// </remarks>
    public readonly ref partial struct Leaf
    {
        public const int MaxByteLength = Header.Size + NibblePath.KeccakNibbleCount;

        private const byte OddPathMetadata = 0b0001_0000;
        private const int MinimalLeafPathLength = 1;

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
            Assert(path.Length >= MinimalLeafPathLength,
                "Leaves that have empty path should not be persisted. They should be stored only in the branch");
            Assert((path.Odd + path.Length) % 2 == 0,
                "If path is odd, length should be odd as well. If even, even");

            var metadata = path.IsOdd ? (byte)(OddPathMetadata | path.Nibble0) : 0;
            Header = new Header(Type.Leaf, (byte)metadata);

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
            var span = Path.RawSpan;
            if (Path.IsOdd)
            {
                // consume first byte as it's odd and the odd nibble is in the header
                span = span[1..];
            }

            if (span.Length > 0)
            {
                span.CopyTo(leftover);
            }

            return leftover[span.Length..];
        }

        public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Leaf leaf)
        {
            var leftover = Header.ReadFrom(source, out var header);

            NibblePath path;
            if ((header.Metadata & OddPathMetadata) == OddPathMetadata)
            {
                // Construct path by wrapping the source and slicing by one to move to first nibble.
                path = NibblePath.FromKey(source, 1);
            }
            else
            {
                path = NibblePath.FromKey(leftover);
            }

            leaf = new Leaf(header, path);
            return ReadOnlySpan<byte>.Empty;
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
        public const int MaxByteLength = Header.Size + NibblePath.KeccakNibbleCount;

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
        public const int MaxByteLength = Header.Size + NibbleSet.MaxByteSize;

        private const byte HeaderMetadataAllChildrenSetMask = 0b0010_0000;
        private const byte HeaderMetadataWithoutOneChildSetMask = 0b0001_0000;
        private const byte HeaderMetadataWithTwoOrThree = 0b0011_0000;
        private const byte OneNibbleHeaderMask = 0b0000_1111;

        private const byte HeaderMetadataCustomFormat = HeaderMetadataAllChildrenSetMask |
                                                        HeaderMetadataWithoutOneChildSetMask |
                                                        HeaderMetadataWithTwoOrThree;

        private const byte ChildCount3 = 3;
        private const byte ChildCount2 = 2;
        private const byte BytesConsumedBy2Or3Children = 1;

        public readonly Header Header;
        public readonly NibbleSet.Readonly Children;

        private Branch(Header header, NibbleSet.Readonly children)
        {
            Header = ValidateHeaderNodeType(header, Type.Branch);
            Children = children;
        }

        private static void Assert(NibbleSet.Readonly set)
        {
            if (set.SetCount < 2)
            {
                ThrowAssertFail(set);
            }

            [DoesNotReturn]
            [StackTraceHidden]
            static void ThrowAssertFail(NibbleSet.Readonly set)
            {
                throw new ArgumentException($"At least two nibbles should be set, but only {set.SetCount} were found");
            }
        }

        public Branch(NibbleSet.Readonly children)
        {
            byte metadata = children.SetCount switch
            {
                NibbleSet.NibbleCount => HeaderMetadataAllChildrenSetMask,
                NibbleSet.NibbleCount - 1 => (byte)(HeaderMetadataWithoutOneChildSetMask | children.SmallestNibbleNotSet),
                ChildCount3 => (byte)(HeaderMetadataWithTwoOrThree | children.SmallestNibbleSet),
                ChildCount2 => (byte)(HeaderMetadataWithTwoOrThree | children.SmallestNibbleSet),
                _ => 0
            };

            Header = new Header(Type.Branch, metadata: metadata);

            Assert(children);

            Children = children;
        }

        private static bool HeaderHasCustomFormat(Header header) => (header.Metadata & HeaderMetadataCustomFormat) != 0;

        public Span<byte> WriteTo(Span<byte> output)
        {
            var leftover = WriteToWithLeftover(output);
            return output.Slice(0, output.Length - leftover.Length);
        }

        public Span<byte> WriteToWithLeftover(Span<byte> output)
        {
            var leftover = Header.WriteToWithLeftover(output);

            if (!HeaderHasCustomFormat(Header))
            {
                // write children only if not all set. All set is encoded in the header
                return Children.WriteToWithLeftover(leftover);
            }

            var custom = Header.Metadata & HeaderMetadataCustomFormat;
            if (custom is HeaderMetadataWithoutOneChildSetMask or HeaderMetadataAllChildrenSetMask)
            {
                return leftover;
            }

            // 2 or 3 children case, use one more byte, the smallest is stored already in the header
            if (Children.SetCount == ChildCount2)
            {
                leftover[0] = (byte)(Children.BiggestNibbleSet | (Children.BiggestNibbleSet << NibblePath.NibbleShift));
            }
            else
            {
                Debug.Assert(Children.SetCount == ChildCount3);

                // remove the smallest nibble set and then get the smallest
                var mid = Children.Remove(Children.SmallestNibbleSet).SmallestNibbleSet;
                leftover[0] = (byte)(mid | (Children.BiggestNibbleSet << NibblePath.NibbleShift));
            }

            return leftover[BytesConsumedBy2Or3Children..];
        }

        public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Branch branch)
        {
            var leftover = Header.ReadFrom(source, out var header);

            NibbleSet.Readonly children;
            if (HeaderHasCustomFormat(header))
            {
                var meta = header.Metadata & HeaderMetadataCustomFormat;

                if (meta == HeaderMetadataAllChildrenSetMask)
                {
                    children = NibbleSet.Readonly.All;
                }
                else if (meta == HeaderMetadataWithoutOneChildSetMask)
                {
                    children = NibbleSet.Readonly.AllWithout((byte)(header.Metadata & OneNibbleHeaderMask));
                }
                else
                {
                    var b = leftover[0];
                    children = new NibbleSet((byte)(header.Metadata & OneNibbleHeaderMask),
                        (byte)(b & NibblePath.NibbleMask),
                        (byte)((b >> NibblePath.NibbleShift) & NibblePath.NibbleMask));
                    leftover = leftover[BytesConsumedBy2Or3Children..];
                }
            }
            else
            {
                leftover = NibbleSet.Readonly.ReadFrom(leftover, out children);
            }

            branch = new Branch(header, children);

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

        private static int GetBranchDataLength(Header header)
        {
            return (header.Metadata & HeaderMetadataCustomFormat) switch
            {
                HeaderMetadataAllChildrenSetMask => Header.Size,
                HeaderMetadataWithoutOneChildSetMask => Header.Size,
                HeaderMetadataWithTwoOrThree => Header.Size + BytesConsumedBy2Or3Children,
                _ => Header.Size + NibbleSet.MaxByteSize
            };
        }

        public bool Equals(in Branch other)
        {
            return Header.Equals(other.Header)
                   && Children.Equals(other.Children);
        }

        public override string ToString() =>
            $"{nameof(Branch)} {{ " +
            $"{nameof(Header)}: {Header.ToString()}, " +
            $"{nameof(Children)}: {Children} }}";
    }
}
