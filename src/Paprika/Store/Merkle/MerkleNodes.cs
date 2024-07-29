using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store.Merkle;

[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct MerkleNodes
{
    private const int ConsumedNibbles = 2;
    private const int MerkleKeysPerPage = 6;
    
    // 4 to align to 8
    private const int Count = 4;
    public const int Size = DbAddress.Size * Count;

    [FieldOffset(0)] private DbAddress Nodes;

    private Span<DbAddress> Buckets => MemoryMarshal.CreateSpan(ref Nodes, Count);

    public bool TrySet(in NibblePath key, ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (key.Length >= ConsumedNibbles)
        {
            return false;
        }

        var id = GetId(key);
        ref var bucket = ref Buckets[id / MerkleKeysPerPage];

        var page = new UShortPage(batch.EnsureWritableExists(ref bucket));
        var map = page.Map;

        if (data.IsEmpty)
        {
            map.Delete(id);
        }
        else
        {
            map.Set(id, data);
        }

        return true;
    }
    
    public bool TryGet(scoped in NibblePath key, out ReadOnlySpan<byte> data, IReadOnlyBatchContext batch)
    {
        if (key.Length >= ConsumedNibbles)
        {
            data = default;
            return false;
        }
        
        var id = GetId(key);
        ref var bucket = ref Buckets[id / MerkleKeysPerPage];

        var page = new UShortPage(batch.GetAt(bucket));
        var map = page.Map;

        map.TryGet(id, out data);
        
        // Always return true as this is a check whether the component was able to proceed with the query.
        return true;
    }

    private static ushort GetId(in NibblePath key) => (ushort)(key.IsEmpty ? 0 : key.FirstNibble + 1);
}