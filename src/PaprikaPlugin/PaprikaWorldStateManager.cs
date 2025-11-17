// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Evm.State;
using Nethermind.State;
using Nethermind.State.SnapServer;
using Nethermind.Trie.Pruning;

namespace PaprikaPlugin;

public class PaprikaWorldStateManager: IWorldStateManager
{
    public IWorldStateScopeProvider GlobalWorldState { get; }
    public IStateReader GlobalStateReader { get; }
    public ISnapServer? SnapServer { get; }
    public IReadOnlyKeyValueStore? HashServer { get; }
    public IWorldStateScopeProvider CreateResettableWorldState()
    {
        throw new NotImplementedException();
    }

    public event EventHandler<ReorgBoundaryReached>? ReorgBoundaryReached;
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
}
