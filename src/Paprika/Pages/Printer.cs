using System.Text;
using Paprika.Crypto;
using Paprika.Db;

namespace Paprika.Pages;

public class Printer : IPageVisitor
{
    const char UL = '┌';
    const char UR = '┐';
    const char DL = '└';
    const char DR = '┘';

    const char LineH = '─';
    const char LineV = '│';

    const char CrossU = '┬';
    const char CrossD = '┴';

    private const string Type = "Type";

    private const int Margins = 2;
    private const int BodyHeight = 9;
    private const int PageHeight = BodyHeight + Margins;

    private const int BodyWidth = 30;

    private const int LastBuilder = PageHeight - 1;

    private static readonly string SpaceOnlyLine = new(' ', BodyWidth);
    private static readonly string HorizontalLine = new(LineH, BodyWidth);

    private readonly SortedDictionary<uint, (string, string)[]> _printable = new();


    private static readonly (string, string)[] Empty =
    {
        ("EMPTY", "EMPTY"),
    };

    private static readonly (string, string)[] Unknown =
    {
        ("UNKNOWN", "PAGE TYPE"),
    };

    public static void Print(PagedDb db, TextWriter writer)
    {
        var printer = new Printer();
        db.Accept(printer);

        writer.WriteLine();
        writer.WriteLine();
        writer.WriteLine("Batch id: {0}", printer._maxRootBatchId);

        printer.Print(writer);
    }

    private uint _maxRootBatchId;

    public void On(RootPage page, DbAddress addr)
    {
        if (page.Header.BatchId > _maxRootBatchId)
        {
            _maxRootBatchId = page.Header.BatchId;
        }

        if (page.Header.BatchId == default)
        {
            PrintEmpty(addr);
        }
        else
        {
            var p = new[]
            {
                (Type, "Root"),
                ("BatchId", page.Header.BatchId.ToString()),
                ("BlockNumber", page.Data.BlockNumber.ToString()),
                ("StateRootHash", Abbr(page.Data.StateRootHash)),
                ("DataPages", ListPages(page.Data.AccountPages)),
                ("NextFreePage", page.Data.NextFreePage.ToString()),
                ("Abandoned", ListPages(page.Data.AbandonedPages))
            };

            _printable.Add(addr.Raw, p);
        }
    }

    public void On(AbandonedPage page, DbAddress addr)
    {
        var p = new[]
        {
            (Type, "Abandoned"),
            ("AbandonedAt", page.AbandonedAtBatch.ToString()),
            ("Pages", ListPages(page.Abandoned)),
            ("Next", page.Next.ToString())
        };

        _printable.Add(addr.Raw, p);
    }

    public void On(DataPage page, DbAddress addr)
    {
        var nextPages = page.Data.Buckets.ToArray().Where(a => a.IsNull == false && a.IsValidPageAddress).ToArray();
        var p = new[]
        {
            (Type, "DataPage"),
            ("BatchId", page.Header.BatchId.ToString()),
            ("Points Down To Pages",ListPages(nextPages)),
        };

        _printable.Add(addr.Raw, p);
    }

    public void On(FanOut256Page page, DbAddress addr)
    {
        var nextPages = page.Data.Buckets.ToArray().Where(a => a.IsNull == false && a.IsValidPageAddress).ToArray();
        var p = new[]
        {
            (Type, "FanOut256Page"),
            ("BatchId", page.Header.BatchId.ToString()),
            ("Points Down To Pages", ListPages(nextPages)),
        };

        _printable.Add(addr.Raw, p);
    }

    public void Print(TextWriter writer)
    {
        var builders = Enumerable.Range(0, PageHeight)
            .Select(i => new StringBuilder()).ToArray();

        ColumnStart(builders);

        uint current = 0; // start with zero

        foreach (var page in _printable)
        {
            while (page.Key != current)
            {
                // the page is unknown, print anything 
                WritePage(Unknown, current, builders);
                current++;
            }

            WritePage(page.Value, current, builders);
            current++;
        }

        TrimOneEnd(builders);
        ColumnEnd(builders);

        foreach (var builder in builders)
        {
            writer.WriteLine(builder);
        }
    }

    private static string Abbr(Keccak hash) => hash.ToString(true).Substring(0, 6) + "...";

    private static string ListPages(ReadOnlySpan<DbAddress> addresses) => "[" + string.Join(",",
        addresses.ToArray().Where(addr => addr.IsNull == false).Select(addr => addr.Raw)) + "]";

    private static void TrimOneEnd(StringBuilder[] builders)
    {
        foreach (var builder in builders)
        {
            builder.Remove(builder.Length - 1, 1);
        }
    }

    private static void WritePage((string, string)[] page, uint address, StringBuilder[] builders)
    {
        // frame up and down
        builders[0].Append(HorizontalLine);
        builders[LastBuilder].Append(HorizontalLine);

        // write type and address in first line and follow with a line
        const int addrSize = 3;
        var type = page[0].Item2;

        builders[1].AppendFormat("{0}{1}{2, 10}", address.ToString().PadLeft(addrSize), LineV,
            type.PadLeft(BodyWidth - addrSize - 1));
        builders[2].Append(HorizontalLine);

        // fill with spaces for sure
        const int startFromLine = 3;
        for (int i = startFromLine; i < LastBuilder; i++)
        {
            var pageProperty = i - startFromLine + 1;
            if (pageProperty < page.Length)
            {
                var (name, value) = page[pageProperty];
                builders[i].AppendFormat("{0}: {1}", name, value.PadRight(BodyWidth - 2 - name.Length));
            }
            else
            {
                builders[i].Append(SpaceOnlyLine);
            }
        }

        ColumnMiddle(builders);
    }

    private static void ColumnStart(StringBuilder[] builders) => Column(UL, LineV, DL, builders);
    private static void ColumnMiddle(StringBuilder[] builders) => Column(CrossU, LineV, CrossD, builders);
    private static void ColumnEnd(StringBuilder[] builders) => Column(UR, LineV, DR, builders);


    private static void Column(char first, char middle, char last, StringBuilder[] builders)
    {
        builders[0].Append(first);

        for (int i = 1; i < LastBuilder; i++)
        {
            builders[i].Append(middle);
        }

        builders[LastBuilder].Append(last);
    }

    private void PrintEmpty(DbAddress addr) => _printable.Add(addr.Raw, Empty);
}