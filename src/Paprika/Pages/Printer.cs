using System.Text;
using Paprika.Crypto;

namespace Paprika.Pages;

public class Printer
{
    const char UL = '┌';
    const char UR = '┐';
    const char DL = '└';
    const char DR = '┐';

    const char LineH = '─';
    const char LineV = '─';

    const char CrossU = '┬';
    const char CrossD = '┴';

    private const int BodyHeight = 9;
    private const int PageHeight = BodyHeight + 2;

    private readonly SortedDictionary<uint, (string, string)[]> _printable = new();

    private static readonly (string, string)[] Empty =
    {
        ("", ""),
        ("", ""),
        ("EMPTY", "EMPTY"),
        ("", ""),
        ("", "")
    };

    public void Add(RootPage page, DbAddress addr)
    {
        if (page.Header.BatchId == default)
        {
            PrintEmpty(addr);
        }
        else
        {
            var p = new[]
            {
                ("BatchId", page.Header.BatchId.ToString()),
                ("BlockNumber", page.Data.BlockNumber.ToString()),
                ("StateRootHash", Abbr(page.Data.StateRootHash)),
                ("NextFreePage", page.Data.NextFreePage.ToString()),
                ("Abandoned", ListPages(page.Data.AbandonedPages))
            };

            _printable.Add(addr.Raw, p);
        }
    }

    public void Print(TextWriter writer)
    {
        uint current = 0; // start with zero

        foreach (var page in _printable)
        {
            if (page.Key != current)
            {
                throw new Exception($"Missing page at address {DbAddress.Page(current)}");
            }

            // TODO

            current++;
        }
    }

    private static string Abbr(Keccak hash) => hash.ToString(true).Substring(0, 6) + "...";

    private static string ListPages(Span<DbAddress> addresses) => "[" + string.Join(",",
        addresses.ToArray().Where(addr => addr.IsNull == false).Select(addr => addr.Raw)) + "]";

    private void PrintEmpty(DbAddress addr) => _printable.Add(addr.Raw, Empty);
}