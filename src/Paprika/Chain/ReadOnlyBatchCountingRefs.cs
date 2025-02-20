using Paprika.Data;
using Paprika.Store;
using Paprika.Utils;

namespace Paprika.Chain;

/// <summary>
/// A ref counting wrapper over a <see cref="IReadOnlyBatch"/>.
/// </summary>
/// <param name="batch">The batch to be ref-counted.</param>
public class ReadOnlyBatchCountingRefs(IReadOnlyBatch batch) : RefCountingDisposable, IReadOnlyBatch
{
    protected override void CleanUp() => batch.Dispose();

    public Metadata Metadata { get; } = batch.Metadata;

    public bool TryGet(scoped in Key key, out ReadOnlySpan<byte> result) => batch.TryGet(key, out result);

    public RootPage Root => batch.Root;

    public uint BatchId => batch.BatchId;

    public void VerifyNoPagesMissing() => batch.VerifyNoPagesMissing();

    public override string ToString() => base.ToString() + $", Batch :{batch}";
}
