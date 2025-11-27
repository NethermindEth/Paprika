// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Extensions;
using Nethermind.Evm.State;
using Nethermind.Int256;
using Paprika.Chain;
using RocksDbSharp;
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
        public Hash256 RootHash => Keccak.Zero;
        // public Hash256 RootHash => paprikaWorldState.GetAccount(address).StorageRootHash.ToNethHash();

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
        private Lock _writeLock = new Lock();

        public void Dispose()
        {
        }

        public event EventHandler<IWorldStateScopeProvider.AccountUpdated>? OnAccountUpdated; // TODO: Umm....
        public void Set(Address key, Account? account)
        {
            using var _ = _writeLock.EnterScope();
            var paprikaKeccak = key.ToPaprikaKeccak();
            Paprika.Account originalAccount = paprikaWorldState.GetAccount(paprikaKeccak);

            // I dont know man.... I'm just trying things out.
            paprikaWorldState.SetAccount(key.ToPaprikaKeccak(),
                new Paprika.Account(
                    account?.Balance ?? originalAccount.Balance,
                    account?.Nonce ?? originalAccount.Nonce,
                    account?.CodeHash?.ToPaprikaKeccak() ?? originalAccount.CodeHash,
                    originalAccount.StorageRootHash)
            );
        }

        public IWorldStateScopeProvider.IStorageWriteBatch CreateStorageWriteBatch(Address key, int estimatedEntries)
        {
            return new PaprikaStorageWriter(
                paprikaWorldState.GetStorageSetter(key.ToPaprikaKeccak()),
                paprikaWorldState,
                key.ToPaprikaKeccak(),
                paprikaWorldState.GetAccount(key.ToPaprikaKeccak()).IsEmpty,
                _writeLock
            );
        }
    }

    private class PaprikaStorageWriter(
        IStorageSetter setter,
        IWorldState worldState,
        Paprika.Crypto.Keccak account,
        bool originallyEmpty,
        Lock writeLock
    ) : IWorldStateScopeProvider.IStorageWriteBatch
    {
        public void Dispose()
        {
        }

        public void Set(in UInt256 index, byte[] value)
        {
            using var _ = writeLock.EnterScope();
            setter.SetStorage(index.SlotToPaprikaKeccak(), value);
        }

        public void Clear()
        {
            using var _ = writeLock.EnterScope();
            worldState.DestroyAccount(account);
        }
    }
}
