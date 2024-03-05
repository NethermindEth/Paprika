using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Chain;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Merkle;

public class Trie(ICommit commit, BufferPool pool)
{
    private Addr _root = default;
    private readonly List<Page> _pages = new();
    private const int ItemsPerPage = Page.PageSize / Item.Size;

    public void MarkPathDirty(in NibblePath path)
    {
        _root = Dirty(_root, path, NibblePath.Empty);
    }

    private Addr Dirty(Addr current, in NibblePath original, in NibblePath path)
    {
        if (current.IsNull)
        {
            ref var node = ref Alloc(out var addr);
            node.Type = Node.Type.Leaf;
            node.Path = original.SliceFrom(path.Length);
            return addr;
        }

        if (current.IsUnresolved)
        {
            current = Resolve(path);
            // Fallback to getting the current
        }

        ref var item = ref GetAt(current);
        switch (item.Type)
        {
            case Node.Type.Leaf:
                break;
            case Node.Type.Extension:
                break;
            case Node.Type.Branch:
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    private Addr Resolve(in NibblePath path)
    {
        // Node exists but is unresolved, resolve it
        using var owner = commit.Get(Key.Merkle(path));

        if (owner.IsEmpty)
        {
            throw new Exception("Empty node!");
        }

        Node.ReadFrom(owner.Span, out var type, out var leaf, out var ext, out var branch);

        ref var node = ref Alloc(out var current);
        switch (type)
        {
            case Node.Type.Leaf:
                node.Type = Node.Type.Leaf;
                node.Path = leaf.Path;
                break;
            case Node.Type.Extension:
                node.Type = Node.Type.Extension;
                node.Path = ext.Path;
                node.ExtensionNext = Addr.Unresolved;
                break;
            case Node.Type.Branch:
                node.Type = Node.Type.Branch;
                for (byte i = 0; i < NibbleSet.NibbleCount; i++)
                {
                    node[i] = branch.Children[i] ? Addr.Unresolved : default;
                }
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }

        return current;
    }

    private ref Item GetAt(Addr addr)
    {
        Debug.Assert(addr.Value >= Addr.StartFrom);

        // TODO: potentially optimize

        var (page, at) = Math.DivRem((int)addr.Value, ItemsPerPage);
        var items = MemoryMarshal.Cast<byte, Item>(_pages[page].Span);
        return ref items[at];
    }

    private ref Item Alloc(out Addr unknown)
    {
        throw new NotImplementedException();
    }

    public void Delete(in NibblePath path)
    {

    }

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Item
    {
        public const int Size = 64;
        private const int SpanLength = Size - PathOffset;

        [FieldOffset(0)] private byte _start;

        [FieldOffset(0)] private uint _addr;

        [FieldOffset(PathOffset)] private byte _path;

        private const int TypeShift = 6;

        private const uint AddrMask = uint.MaxValue / 4; // two bits to serve the type
        private const int PathOffset = 4;

        public Node.Type Type
        {
            get => (Node.Type)(_start >> TypeShift);

            // destroy previous content
            set => _start = (byte)value;
        }

        /// <summary>
        /// Branch children.
        /// </summary>
        public Addr this[byte child]
        {
            get
            {
                Debug.Assert(child < 16);
                return new Addr(Unsafe.Add(ref _addr, child) & AddrMask);
            }
            set
            {
                ref var at = ref Unsafe.Add(ref _addr, child);
                at = (at & ~AddrMask) | value.Value;
            }
        }

        /// <summary>
        /// Extension next.
        /// </summary>
        public Addr ExtensionNext
        {
            get => new(_addr & AddrMask);
            set => _addr = (_addr & ~AddrMask) | value.Value;
        }

        /// <summary>
        /// Extension next.
        /// </summary>
        public NibblePath Path
        {
            get
            {
                NibblePath.ReadFrom(Span, out var path);
                return path;
            }

            set => value.WriteTo(Span);
        }

        private Span<byte> Span => MemoryMarshal.CreateSpan(ref _path, SpanLength);
    }


    public readonly struct Addr(uint value)
    {
        public uint Value { get; } = value;
        public const uint StartFrom = 2;

        public bool IsNull => Value == Null.Value;
        public bool IsUnresolved => Value == Unresolved.Value;

        public static readonly Addr Null = new(0);
        public static readonly Addr Unresolved = new(StartFrom - 1);
    }
}
