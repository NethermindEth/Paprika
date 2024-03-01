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
    private List<Page> _pages = new();

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
            // Query for the node
            using var owner = commit.Get(Key.Merkle(path));
            if (owner.IsEmpty)
            {
                // No value set now, create one.
                commit.SetLeaf(key, leftoverPath);
                return;
            }
        }
    }

    private ref Item Alloc(out Addr unknown)
    {
        throw new NotImplementedException();
    }

    public void Delete(in NibblePath path)
    {
        
    }
    
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Item
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

        public bool IsNull => value == default;
        public bool IsUnresolved => value == StartFrom - 1;
    }
}