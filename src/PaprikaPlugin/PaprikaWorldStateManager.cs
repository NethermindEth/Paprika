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
    private Paprika.IDb _paprikaDb;
    private PaprikaStateReader _stateReader;

    private ComputeMerkleBehavior _merkleBehavior;
    private IWorldStateScopeProvider _mainScopeProvider;
    private readonly IDb _codeDb;
    private SemaphoreSlim _scopeLock = new SemaphoreSlim(1);

    public PaprikaWorldStateManager(
        Paprika.IDb paprikaDb,
        [KeyFilter(DbNames.Code)] IDb codeDb,
        ComputeMerkleBehavior computeMerkleBehavior,
        Blockchain paprikaBlockchain)
    {
        _paprikaDb = paprikaDb;
        _codeDb = codeDb;
        _merkleBehavior = computeMerkleBehavior;
        _paprikaBlockchain = paprikaBlockchain;

        var readOnlyWorldStateAccessor = _paprikaBlockchain.BuildReadOnlyAccessor();
        _stateReader = new PaprikaStateReader(readOnlyWorldStateAccessor, codeDb);

        _mainScopeProvider =
            new PaprikaWorldStateScopeProvider(_paprikaBlockchain, codeDb, _scopeLock);
    }

    public IWorldStateScopeProvider GlobalWorldState => _mainScopeProvider;
    public IStateReader GlobalStateReader => _stateReader;
    public ISnapServer? SnapServer => null;
    public IReadOnlyKeyValueStore? HashServer => null;
    public IWorldStateScopeProvider CreateResettableWorldState()
    {
        return new PaprikaReadOnlyStateScopeProvider(_paprikaBlockchain, _codeDb, _scopeLock);
    }

    public event EventHandler<ReorgBoundaryReached>? ReorgBoundaryReached; // TODO: How?
    public IOverridableWorldScope CreateOverridableWorldScope()
    {
        return new PaprikaOverridableWorldScope(
            new PaprikaReadOnlyStateScopeProvider(_paprikaBlockchain, _codeDb, _scopeLock), _stateReader);

    }

    public bool VerifyTrie(BlockHeader stateAtBlock, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public void FlushCache(CancellationToken cancellationToken)
    {
        // TODO: Flush
    }

    public async ValueTask DisposeAsync()
    {
        _merkleBehavior.Dispose();
        await _paprikaBlockchain.DisposeAsync();
    }

    // Just a fake one
    private class PaprikaOverridableWorldScope(IWorldStateScopeProvider worldStateScopeProvider, IStateReader stateReader) : IOverridableWorldScope
    {
        public IWorldStateScopeProvider WorldState => worldStateScopeProvider;
        public IStateReader GlobalStateReader => stateReader;
        public void ResetOverrides()
        {
        }
    }
}
