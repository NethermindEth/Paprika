﻿using Nethermind.Int256;
using Paprika.Crypto;

namespace Paprika.Pages;

/// <summary>
/// Represents an intent to set the account data.
/// </summary>
public readonly ref struct SetContext
{
    public readonly Keccak Key;
    public readonly IBatchContext Batch;
    public readonly UInt256 Balance;
    public readonly UInt256 Nonce;

    public SetContext(in Keccak keccak, in UInt256 balance, in UInt256 nonce, IBatchContext batch)
    {
        Key = keccak;
        Batch = batch;
        Balance = balance;
        Nonce = nonce;
    }
}