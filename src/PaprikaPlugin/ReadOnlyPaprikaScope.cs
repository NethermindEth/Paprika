// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Evm.State;
using Nethermind.Int256;
using Paprika.Chain;

namespace PaprikaPlugin;

internal class ReadOnlyPaprikaScope(IReadOnlyWorldState paprikaWorldState, IWorldStateScopeProvider.ICodeDb codeDb) : IWorldStateScopeProvider.IScope
{
    public void Dispose()
    {
        paprikaWorldState.Dispose();
    }

    public Hash256 RootHash => paprikaWorldState.Hash.ToNethHash();

    public void UpdateRootHash()
    {
        // TODO: Need to call pre commit behaviour manually maybe
    }

    public Account? Get(Address address)
    {
        return paprikaWorldState.GetAccount(address.ToAccountPath.ToPaprikaKeccak()).ToNethAccount();
    }

    public void HintGet(Address address, Account? account)
    {
        // TODO: Probably not needed
    }

    public IWorldStateScopeProvider.ICodeDb CodeDb => codeDb;
    public IWorldStateScopeProvider.IStorageTree CreateStorageTree(Address address)
    {
        return new PaprikaStorageTree(paprikaWorldState, address.ToPaprikaKeccak());
    }

    public IWorldStateScopeProvider.IWorldStateWriteBatch StartWriteBatch(int estimatedAccountNum)
    {
        throw new Exception("rite not supported");
    }

    public void Commit(long blockNumber)
    {
        throw new Exception("commit not supported");
    }

    private class PaprikaStorageTree(IStateStorageAccessor paprikaWorldState, Paprika.Crypto.Keccak address) : IWorldStateScopeProvider.IStorageTree
    {
        public Hash256 RootHash => paprikaWorldState.GetAccount(address).CodeHash.ToNethHash();
        public byte[] Get(in UInt256 index)
        {
            Span<byte> buffer = stackalloc byte[32];
            Paprika.Crypto.Keccak key = index.SlotToPaprikaKeccak();
            paprikaWorldState.GetStorage(address, key, buffer);
            return buffer.IsEmpty ? null : buffer.ToArray();
        }

        public void HintGet(in UInt256 index, byte[]? value)
        {
        }

        public byte[] Get(in ValueHash256 hash)
        {
            return null;
        }
    }
}
