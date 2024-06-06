using Nethermind.Int256;
using Paprika.Crypto;

namespace Paprika.Tests;

public static class Values
{
    public static readonly Keccak Key0 = new(new byte[]
        { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, });

    public static readonly Keccak KeyLen6 = Keccak.Compute(
        new byte[] { 0, 1, 2, 3, 4, 5, });

    public static readonly Keccak Key1A = new(new byte[]
        { 1, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, });

    public static readonly Keccak Key1 = new(new byte[]
        { 1, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, });

    public static readonly Keccak Key1B = new(new byte[]
        { 1, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 8 });

    public static readonly Keccak Key2 = new(new byte[]
        { 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6 });

    public static readonly Keccak Key3 = Keccak.Compute(new byte[] { 234, 34, 23 });
    public static readonly Keccak Key4 = Keccak.Compute(new byte[] { 1, 2, 3 });
    public static readonly Keccak Key5 = Keccak.Compute(new byte[] { 35, 234, 33 });

    public static readonly UInt256 Balance0 = 13;
    public static readonly UInt256 Balance1 = 17;
    public static readonly UInt256 Balance2 = 19;

    public static readonly UInt256 Nonce0 = 23;
    public static readonly UInt256 Nonce1 = 29;
    public static readonly UInt256 Nonce2 = 31;
}
