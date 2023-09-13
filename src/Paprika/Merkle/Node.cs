using System.Diagnostics;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Data;
using static System.Diagnostics.Debug;

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
        Assert(header.NodeType == expected, $"Expected {nameof(Header)} with {nameof(Type)} {expected}, got {header.NodeType}");

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

        public static Header Peek(ReadOnlySpan<byte> source) => new(source[0]);

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
            Header = new Header(Type.Leaf, false);
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
                                    NibbleSet.MaxByteSize +
                                    (HeaderHasKeccak(Header) ? Keccak.Size : 0);

        private const int HeaderMetadataKeccakMask = 0b0001;
        private const int NoKeccak = 0;
        private const int KeccakSet = 1;

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
            Header = new Header(Type.Branch, metadata: KeccakSet);

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
            Header = new Header(Type.Branch, metadata: NoKeccak);

            Assert(children);

            Children = children;
            Keccak = default;
        }

        private static Header ValidateHeaderKeccak(Header header, bool shouldHaveKeccak)
        {
            var expected = shouldHaveKeccak ? KeccakSet : NoKeccak;
            var actual = header.Metadata & HeaderMetadataKeccakMask;

            Debug.Assert(actual == expected,
                $"Expected {nameof(Header)} to have {nameof(Keccak)} = {shouldHaveKeccak}, got {!shouldHaveKeccak}");

            return header;
        }

        public bool HasKeccak => HeaderHasKeccak(Header);

        private static bool HeaderHasKeccak(Header header) =>
            (header.Metadata & HeaderMetadataKeccakMask) == KeccakSet;

        public Span<byte> WriteTo(Span<byte> output)
        {
            var leftover = WriteToWithLeftover(output);
            return output.Slice(0, output.Length - leftover.Length);
        }

        public Span<byte> WriteToWithLeftover(Span<byte> output)
        {
            var leftover = Header.WriteToWithLeftover(output);
            leftover = Children.WriteToWithLeftover(leftover);

            if (HeaderHasKeccak(Header))
            {
                leftover = Keccak.WriteToWithLeftover(leftover);
            }

            return leftover;
        }

        public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Branch branch)
        {
            var leftover = Header.ReadFrom(source, out var header);

            leftover = NibbleSet.Readonly.ReadFrom(leftover, out var children);

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
        /// Skips the branch at source, returning just the leftover.
        /// </summary>
        public static ReadOnlySpan<byte> Skip(ReadOnlySpan<byte> source)
        {
            Header.ReadFrom(source, out var header);
            return source.Slice(GetBranchDataLength(header));
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
            Header.Size + NibbleSet.MaxByteSize + (HeaderHasKeccak(header) ? Keccak.Size : 0);

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
