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
    public readonly UInt128 Balance;
    public readonly uint Nonce;

    public SetContext(in Keccak keccak, in UInt256 balance, in UInt256 nonce, IBatchContext batch)
    {
        Key = keccak;
        Batch = batch;
        Balance = new UInt128(balance[1], balance[0]);
        Nonce = (uint)nonce;
    }

    public SetContext(in Keccak keccak, in UInt128 balance, uint nonce, IBatchContext batch)
    {
        Key = keccak;
        Batch = batch;
        Balance = balance;
        Nonce = nonce;
    }
}