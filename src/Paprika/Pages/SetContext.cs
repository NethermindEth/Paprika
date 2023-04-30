using Nethermind.Int256;
using Paprika.Crypto;

namespace Paprika.Pages;

/// <summary>
/// Represents an intent to set the account data.
/// </summary>
public readonly ref struct SetContext
{
    public readonly NibblePath Path;
    public readonly IBatchContext Batch;
    public readonly UInt256 Balance;
    public readonly UInt256 Nonce;

    public SetContext(NibblePath path, UInt256 balance, UInt256 nonce, IBatchContext batch)
    {
        Path = path;
        Batch = batch;
        Balance = balance;
        Nonce = nonce;
    }

    public SetContext TrimPath(int nibbleCount) => new(Path.SliceFrom(nibbleCount), Balance, Nonce, Batch);
}