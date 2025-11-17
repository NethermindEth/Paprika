// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Int256;
using Nethermind.State;
using Nethermind.Trie;

namespace PaprikaPlugin;

public class PaprikaStateReader: IStateReader
{
    public bool TryGetAccount(BlockHeader? baseBlock, Address address, out AccountStruct account)
    {
        throw new NotImplementedException();
    }

    public ReadOnlySpan<byte> GetStorage(BlockHeader? baseBlock, Address address, in UInt256 index)
    {
        throw new NotImplementedException();
    }

    public byte[]? GetCode(Hash256 codeHash)
    {
        throw new NotImplementedException();
    }

    public byte[]? GetCode(in ValueHash256 codeHash)
    {
        throw new NotImplementedException();
    }

    public void RunTreeVisitor<TCtx>(ITreeVisitor<TCtx> treeVisitor, Hash256 stateRoot, VisitingOptions? visitingOptions = null) where TCtx : struct, INodeContext<TCtx>
    {
        throw new NotImplementedException();
    }

    public bool HasStateForBlock(BlockHeader? baseBlock)
    {
        throw new NotImplementedException();
    }
}
