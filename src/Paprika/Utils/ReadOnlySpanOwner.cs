namespace Paprika.Utils;

/// <summary>
/// Provides a <see cref="ReadOnlySpan{T}"/> under ownership.
/// </summary>
public readonly ref struct ReadOnlySpanOwner<T>(ReadOnlySpan<T> span, IDisposable? owner)
{
    public readonly ReadOnlySpan<T> Span = span;

    public bool IsEmpty => Span.IsEmpty;

    public bool HasOwner => owner != null;

    /// <summary>
    /// Disposes the owner provided as <see cref="IDisposable"/> once.
    /// </summary>
    public void Dispose() => owner?.Dispose();

    public bool IsOwnedBy(ISpanOwner potentialSpanOwner) => potentialSpanOwner.Owns(owner);
}

public interface ISpanOwner
{
    /// <summary>
    /// Checks whether this owner is capable of owning spans
    /// owned by <paramref name="actualSpanOwner"/>.
    /// </summary>
    public bool Owns(object? actualSpanOwner) => ReferenceEquals(actualSpanOwner, this);
}