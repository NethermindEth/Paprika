using System.Numerics;
using System.Runtime.CompilerServices;
using Nethermind.Int256;

namespace Paprika.Pages;

/// <summary>
/// The result of getting a state under a key.
/// </summary>
public readonly ref struct GetContext
{
    public readonly UInt256 Balance;
    public readonly UInt256 Nonce;

    public GetContext(in UInt128 balance, in uint nonce)
    {
        Convert(balance, out Balance);
        Nonce = nonce;
    }

    private static void Convert<TFrom>(in TFrom from, out UInt256 to)
        where TFrom : IBinaryInteger<UInt128>
    {
        Span<byte> span = stackalloc byte[32];
        from.WriteLittleEndian(span);
        to = new UInt256(span, false);
    }
}