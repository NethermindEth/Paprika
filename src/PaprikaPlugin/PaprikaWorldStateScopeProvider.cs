// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Evm.State;
using Nethermind.State;
using Paprika.Chain;
using IWorldState = Paprika.Chain.IWorldState;

namespace PaprikaPlugin;

public class PaprikaWorldStateScopeProvider(Blockchain paprikaBlockchain, IKeyValueStoreWithBatching codeDb): IWorldStateScopeProvider
{
    public bool HasRoot(BlockHeader? baseBlock)
    {
        return paprikaBlockchain.HasState((baseBlock?.StateRoot ?? Nethermind.Core.Crypto.Keccak.EmptyTreeHash).ToPaprikaKeccak());
    }

    public IWorldStateScopeProvider.IScope BeginScope(BlockHeader? baseBlock)
    {
        IWorldState paprikaWorldState = paprikaBlockchain.StartNew(
            (baseBlock?.StateRoot ?? Nethermind.Core.Crypto.Keccak.EmptyTreeHash).ToPaprikaKeccak());

        return new PaprikaScope(paprikaWorldState, new KeyValueWithBatchingBackedCodeDb(codeDb));
    }
}

public class PaprikaReadOnlyStateScopeProvider(Blockchain paprikaBlockchain, IKeyValueStoreWithBatching codeDb): IWorldStateScopeProvider
{
    public bool HasRoot(BlockHeader? baseBlock)
    {
        return paprikaBlockchain.HasState((baseBlock?.StateRoot ?? Nethermind.Core.Crypto.Keccak.EmptyTreeHash).ToPaprikaKeccak());
    }

    public IWorldStateScopeProvider.IScope BeginScope(BlockHeader? baseBlock)
    {
        IReadOnlyWorldState paprikaWorldState = paprikaBlockchain.StartReadOnly(
            (baseBlock?.StateRoot ?? Nethermind.Core.Crypto.Keccak.EmptyTreeHash).ToPaprikaKeccak());

        return new ReadOnlyPaprikaScope(paprikaWorldState, new KeyValueWithBatchingBackedCodeDb(codeDb));
    }
}
