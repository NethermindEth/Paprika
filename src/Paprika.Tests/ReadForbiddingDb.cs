using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Tests;

public class ReadForbiddingDb(IDb db) : IDb
{
    private Key.Predicate? _forbidden;
    public IBatch BeginNextBatch() => db.BeginNextBatch();

    public IReadOnlyBatch BeginReadOnlyBatch(string name = "") => new ReadOnlyBatch(db.BeginReadOnlyBatch(name), this);

    public void Flush() => db.Flush();

    public IReadOnlyBatch BeginReadOnlyBatchOrLatest(in Keccak stateHash, string name = "") =>
        new ReadOnlyBatch(db.BeginReadOnlyBatchOrLatest(in stateHash, name), this);

    public IReadOnlyBatch[] SnapshotAll() => db.SnapshotAll();

    public bool HasState(in Keccak keccak) => db.HasState(in keccak);
    public int HistoryDepth => db.HistoryDepth;
    public void ForceFlush() => db.ForceFlush();

    private class ReadOnlyBatch(IReadOnlyBatch batch, ReadForbiddingDb parent) : IReadOnlyBatch
    {
        public Metadata Metadata => batch.Metadata;

        public bool TryGet(scoped in Key key, out ReadOnlySpan<byte> result)
        {
            parent.OnTryGet(key, this);

            return batch.TryGet(in key, out result);
        }

        public void Dispose() => batch.Dispose();
    }

    public void ForbidReads(Key.Predicate predicate)
    {
        _forbidden = predicate;
    }

    public void AllowAllReads()
    {
        _forbidden = null;
    }

    private void OnTryGet(in Key key, IReadOnlyBatch reads)
    {
        if (_forbidden != null && _forbidden(key))
        {
            throw new Exception($"The key {key.ToString()} cannot be get as reads have been forbidden. At {reads.Metadata.BlockNumber}");
        }
    }
}