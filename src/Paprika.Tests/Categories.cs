namespace Paprika.Tests;

public static class Categories
{
    /// <summary>
    /// Long running tests that should be skipped by CI when running in default settings.
    /// </summary>
    public const string LongRunning = nameof(LongRunning);
}