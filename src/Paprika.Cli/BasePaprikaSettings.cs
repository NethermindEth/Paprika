using Paprika.Store;
using Spectre.Console.Cli;

namespace Paprika.Cli;

public abstract class BasePaprikaSettings : CommandSettings
{
    [CommandArgument(0, "<path>")]
    public string Path { get; set; }

    [CommandArgument(1, "<size>")]
    public int Size { get; set; }

    [CommandArgument(1, "<historyDepth>")]
    public byte HistoryDepth { get; set; }

    public const int NextArgPosition = 2;

    protected PagedDb BuildDb()
    {
        const long Gb = 1024 * 1024 * 1024;
        return PagedDb.MemoryMappedDb(Size * Gb, HistoryDepth, Path, false);
    }
}