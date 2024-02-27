namespace Paprika.Utils;

/// <summary>
/// Provides a <see cref="ReadOnlySpan{T}"/> under ownership.
/// </summary>
public readonly ref struct ReadOnlySpanOwner<T>(ReadOnlySpan<T> span, IDisposable? owner)
{
    public readonly ReadOnlySpan<T> Span = span;

    public bool IsEmpty => Span.IsEmpty;

    /// <summary>
    /// Disposes the owner provided as <see cref="IDisposable"/> once.
    /// </summary>
    public void Dispose() => owner?.Dispose();

    /// <summary>
    /// Answers whether this span is owned and provided by <paramref name="owner1"/>.
    /// </summary>
    public bool IsOwnedBy(object owner1) => ReferenceEquals(owner1, owner);
}