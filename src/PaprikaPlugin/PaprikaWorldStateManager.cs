// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Autofac.Features.AttributeFilters;
using Nethermind.Api;
using Nethermind.Core;
using Nethermind.Core.Extensions;
using Nethermind.Db;
using Nethermind.Evm.State;
using Nethermind.State;
using Nethermind.State.SnapServer;
using Nethermind.Trie.Pruning;
using Paprika.Chain;
using Paprika.Merkle;
using Paprika.Store;

namespace PaprikaPlugin;

public class PaprikaWorldStateManager: IWorldStateManager, IAsyncDisposable
{
    private Blockchain _paprikaBlockchain;
    private PagedDb _paprikaDb;
    private PaprikaStateReader _stateReader;

    private ComputeMerkleBehavior _merkleBehavior;
    private IWorldStateScopeProvider _mainScopeProvider;
    private readonly IDb _codeDb;

    public PaprikaWorldStateManager(IInitConfig initConfig, [KeyFilter(DbNames.Code)] IDb codeDb)
    {
        var dbPath = initConfig.BaseDbPath + "/paprika/";

        _paprikaDb = PagedDb.MemoryMappedDb(
            200.GiB(),
            128, // TODO: CHange history depht to more than a byte
            dbPath,
            true);

        _codeDb = codeDb;
        _merkleBehavior = new ComputeMerkleBehavior(ComputeMerkleBehavior.ParallelismNone);
        _paprikaBlockchain = new Blockchain(
            _paprikaDb,
            _merkleBehavior,
            minFlushDelay: TimeSpan.FromMinutes(1),
            cacheBudgetStateAndStorage: CacheBudget.Options.None,
            cacheBudgetPreCommit: CacheBudget.Options.None,
            finalizationQueueLimit: 1000,
            int.MaxValue,
            null);

        var readOnlyWorldStateAccessor = _paprikaBlockchain.BuildReadOnlyAccessor();
        _stateReader = new PaprikaStateReader(readOnlyWorldStateAccessor, codeDb);

        _mainScopeProvider =
            new PaprikaWorldStateScopeProvider(_paprikaBlockchain, codeDb);
    }

    public IWorldStateScopeProvider GlobalWorldState => _mainScopeProvider;
    public IStateReader GlobalStateReader => _stateReader;
    public ISnapServer? SnapServer => null;
    public IReadOnlyKeyValueStore? HashServer => null;
    public IWorldStateScopeProvider CreateResettableWorldState()
    {
        return new PaprikaReadOnlyStateScopeProvider(_paprikaBlockchain, _codeDb);
    }

    public event EventHandler<ReorgBoundaryReached>? ReorgBoundaryReached; // TODO: How?
    public IOverridableWorldScope CreateOverridableWorldScope()
    {
        throw new NotImplementedException();
    }

    public bool VerifyTrie(BlockHeader stateAtBlock, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public void FlushCache(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async ValueTask DisposeAsync()
    {
        _merkleBehavior.Dispose();
        await _paprikaBlockchain.DisposeAsync();
        _paprikaDb.Dispose();
    }
}
