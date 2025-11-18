// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Evm.State;
using Nethermind.State;
using Paprika.Chain;
using IWorldState = Paprika.Chain.IWorldState;

namespace PaprikaPlugin;

public class PaprikaWorldStateScopeProvider(
    Blockchain paprikaBlockchain,
    IKeyValueStoreWithBatching codeDb,
    SemaphoreSlim scopeLock): IWorldStateScopeProvider
{
    public bool HasRoot(BlockHeader? baseBlock)
    {
        return paprikaBlockchain.HasState((baseBlock?.StateRoot ?? Nethermind.Core.Crypto.Keccak.EmptyTreeHash).ToPaprikaKeccak());
    }

    public IWorldStateScopeProvider.IScope BeginScope(BlockHeader? baseBlock)
    {
        scopeLock.Wait();

        IWorldState paprikaWorldState = paprikaBlockchain.StartNew(
            (baseBlock?.StateRoot ?? Nethermind.Core.Crypto.Keccak.EmptyTreeHash).ToPaprikaKeccak());

        return new PaprikaScope(paprikaWorldState, new KeyValueWithBatchingBackedCodeDb(codeDb), scopeLock, false);
    }
}

public class PaprikaReadOnlyStateScopeProvider(
    Blockchain paprikaBlockchain,
    IKeyValueStoreWithBatching codeDb,
    SemaphoreSlim scopeLock): IWorldStateScopeProvider
{
    public bool HasRoot(BlockHeader? baseBlock)
    {
        return paprikaBlockchain.HasState((baseBlock?.StateRoot ?? Nethermind.Core.Crypto.Keccak.EmptyTreeHash).ToPaprikaKeccak());
    }

    public IWorldStateScopeProvider.IScope BeginScope(BlockHeader? baseBlock)
    {
        IWorldState paprikaWorldState = paprikaBlockchain.StartNew(
            (baseBlock?.StateRoot ?? Nethermind.Core.Crypto.Keccak.EmptyTreeHash).ToPaprikaKeccak());

        return new PaprikaScope(paprikaWorldState, new KeyValueWithBatchingBackedCodeDb(codeDb), scopeLock, true);
    }
}
