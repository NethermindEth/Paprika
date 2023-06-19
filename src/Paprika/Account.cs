using Nethermind.Int256;

namespace Paprika;

/// <summary>
/// A presentation of an account.
/// </summary>
public readonly struct Account : IEquatable<Account>
{
    public static readonly Account Empty = default;

    public readonly UInt256 Balance;
    public readonly UInt256 Nonce;

    public Account(UInt256 balance, UInt256 nonce)
    {
        Balance = balance;
        Nonce = nonce;
    }

    public bool Equals(Account other) => Balance.Equals(other.Balance) && Nonce == other.Nonce;

    public override bool Equals(object? obj) => obj is Account other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(Balance, Nonce);

    public static bool operator ==(Account left, Account right) => left.Equals(right);

    public static bool operator !=(Account left, Account right) => !left.Equals(right);

    public override string ToString() => $"{nameof(Nonce)}: {Nonce}, {nameof(Balance)}: {Balance}";
}