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

    /// <summary>
    /// Whether the owner is empty.
    /// </summary>
    public bool IsEmpty => _owner == null;

    /// <summary>
    /// Disposes the owner provided as <see cref="IDisposable"/> once.
    /// </summary>
    public void Dispose()
    {
        if (_owner != null)
            _owner.Dispose();
    }
}