namespace Paprika.Tests;

public static class Categories
{
    /// <summary>
    /// Long running tests that should be skipped by CI when running in default settings.
    /// </summary>
    public const string LongRunning = nameof(LongRunning);

    /// <summary>
    /// The test asserts the memory using <see cref="JetBrains.dotMemoryUnit"/> capabilities.
    /// </summary>
    public const string Memory = nameof(Memory);

    /// <summary>
    /// An operating system dependent API.
    /// </summary>
    public const string OS = nameof(OS);
}
