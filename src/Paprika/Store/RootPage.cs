using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;

namespace Paprika.Store;

/// <summary>
/// Root page is a page that contains all the needed metadata from the point of view of the database.
/// It also includes the blockchain information like block hash or block number
/// </summary>
public readonly unsafe struct RootPage(Page root) : IPage
{
    public ref PageHeader Header => ref root.Header;

    public ref Payload Data => ref Unsafe.AsRef<Payload>(root.Payload);

    /// <summary>
    /// Represents the data of the page.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;

        private const int MetadataStart = 4 * DbAddress.Size + sizeof(uint);

        /// <summary>
        /// The address of the next free page. This should be used rarely as pages should be reused
        /// with <see cref="AbandonedPage"/>.
        /// </summary>
        [FieldOffset(0)] public DbAddress NextFreePage;

        /// <summary>
        /// The first of the data pages.
        /// </summary>
        [FieldOffset(DbAddress.Size)] public DbAddress StateRoot;

        /// <summary>
        /// The first of the data pages.
        /// </summary>
        [FieldOffset(DbAddress.Size * 2)] public DbAddress StorageRoot;

        /// <summary>
        /// The root of the id pages.
        /// </summary>
        [FieldOffset(DbAddress.Size * 3)] public DbAddress IdRoot;

        /// <summary>
        /// The account counter
        /// </summary>
        [FieldOffset(DbAddress.Size * 4)] public uint AccountCounter;

        [FieldOffset(MetadataStart)]
        public Metadata Metadata;

        /// <summary>
        /// The start of the abandoned pages.
        /// </summary>
        [FieldOffset(AbandonedList.SpaceForRootPage)]
        public AbandonedList AbandonedList;

        public DbAddress GetNextFreePage()
        {
            var free = NextFreePage;
            NextFreePage = NextFreePage.Next;
            return free;
        }
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver)
    {
        if (Data.StateRoot.IsNull == false)
        {
            var data = new DataPage(resolver.GetAt(Data.StateRoot));
            visitor.On(data, Data.StateRoot);
        }

        Data.AbandonedList.Accept(visitor, resolver);


    }
}

[StructLayout(LayoutKind.Explicit, Size = Size, Pack = 1)]
public struct Metadata
{
    public const int Size = BlockNumberSize + Keccak.Size;
    private const int BlockNumberSize = sizeof(uint);

    [FieldOffset(0)] public readonly uint BlockNumber;
    [FieldOffset(BlockNumberSize)] public readonly Keccak StateHash;

    public Metadata(uint blockNumber, Keccak stateHash)
    {
        BlockNumber = blockNumber;
        StateHash = stateHash;
    }

    public static Metadata ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.IsEmpty)
        {
            return new Metadata(0, Keccak.EmptyTreeHash);
        }

        var blockNumber = BinaryPrimitives.ReadUInt32LittleEndian(source);
        var keccak = default(Keccak);

        source.Slice(BlockNumberSize).CopyTo(keccak.BytesAsSpan);

        return new Metadata(blockNumber, keccak);
    }

    public void WriteTo(Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(destination, BlockNumber);
        StateHash.Span.CopyTo(destination.Slice(BlockNumberSize));
    }
}