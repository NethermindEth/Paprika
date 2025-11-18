// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Evm.State;
using Nethermind.Int256;
using Paprika.Chain;
using IWorldState = Paprika.Chain.IWorldState;

namespace PaprikaPlugin;

internal class PaprikaScope(
    IWorldState paprikaWorldState,
    IWorldStateScopeProvider.ICodeDb codeDb,
    SemaphoreSlim scopeLock,
    bool isReadOnly,
    Blockchain paprikaBlockchain) : IWorldStateScopeProvider.IScope
{
    public void Dispose()
    {
        paprikaWorldState.Dispose();
        scopeLock.Release();
    }

    public Hash256 RootHash => paprikaWorldState.Hash.ToNethHash();

    public void UpdateRootHash()
    {
        // Paprika auto update on `Hash` getter
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
        return new PaprikaWriteBatcher(paprikaWorldState);
    }

    public void Commit(long blockNumber)
    {
        if (!isReadOnly)
        {
            paprikaWorldState.Commit((uint)blockNumber);

            if (blockNumber == 0)
            {
                paprikaBlockchain.Finalize(paprikaWorldState.Hash);
            }
        }
    }

    private class PaprikaStorageTree(IWorldState paprikaWorldState, Paprika.Crypto.Keccak address) : IWorldStateScopeProvider.IStorageTree
    {
        public Hash256 RootHash => paprikaWorldState.GetAccount(address).CodeHash.ToNethHash();
        public byte[] Get(in UInt256 index)
        {
            Span<byte> buffer = stackalloc byte[32];
            Paprika.Crypto.Keccak key = index.SlotToPaprikaKeccak();
            buffer = paprikaWorldState.GetStorage(address, key, buffer);
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

    private class PaprikaWriteBatcher(IWorldState paprikaWorldState) : IWorldStateScopeProvider.IWorldStateWriteBatch
    {
        public void Dispose()
        {
        }

        public event EventHandler<IWorldStateScopeProvider.AccountUpdated>? OnAccountUpdated; // TODO: Umm....
        public void Set(Address key, Account? account)
        {
            paprikaWorldState.SetAccount(key.ToPaprikaKeccak(), (account ?? Account.TotallyEmpty).ToPaprikaAccount());
        }

        public IWorldStateScopeProvider.IStorageWriteBatch CreateStorageWriteBatch(Address key, int estimatedEntries)
        {
            return new PaprikaStorageWriter(
                paprikaWorldState.GetStorageSetter(key.ToPaprikaKeccak()),
                paprikaWorldState,
                key.ToPaprikaKeccak()
            );
        }
    }

    private class PaprikaStorageWriter(IStorageSetter setter, IWorldState worldState, Paprika.Crypto.Keccak account) : IWorldStateScopeProvider.IStorageWriteBatch
    {
        public void Dispose()
        {
        }

        public void Set(in UInt256 index, byte[] value)
        {
            setter.SetStorage(index.SlotToPaprikaKeccak(), value);
        }

        public void Clear()
        {
            worldState.DestroyAccount(account);
        }
    }
}
