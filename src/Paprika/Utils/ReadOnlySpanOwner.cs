namespace Paprika.Utils;

/// <summary>
/// Provides a <see cref="ReadOnlySpan{T}"/> under ownership.
/// </summary>
/// <typeparam name="T"></typeparam>
public readonly ref struct ReadOnlySpanOwner<T>
{
    public readonly ReadOnlySpan<T> Span;
    private readonly IDisposable? _owner;

    public ReadOnlySpanOwner(ReadOnlySpan<T> span, IDisposable? owner)
    {
        Span = span;
        _owner = owner;
    }

    public bool IsEmpty => Span.IsEmpty;

    /// <summary>
    /// Disposes the owner provided as <see cref="IDisposable"/> once.
    /// </summary>
    public void Dispose() => _owner?.Dispose();
}