using Autofac;
using Autofac.Core;
using Nethermind.Api;
using Nethermind.Api.Extensions;
using Nethermind.Core;
using Nethermind.Core.Extensions;
using Nethermind.PaprikaPlugin;
using Nethermind.State;
using Paprika;
using Paprika.Store;

namespace PaprikaPlugin;

public class PaprikaPlugin(IPaprikaConfig paprikaConfig): INethermindPlugin
{
    public string Name => "Paprika";
    public string Description => "Paprika State Db";
    public string Author => "Nethermind";
    public bool Enabled => paprikaConfig.Enabled;
    public IModule Module { get; } = new PaprikaPluginModule();
}

public class PaprikaPluginModule : Module
{
    protected override void Load(ContainerBuilder builder)
    {
        base.Load(builder);

        builder
            .AddSingleton<IWorldStateManager, PaprikaWorldStateManager>()
            .AddSingleton<IDb, IInitConfig, IPaprikaConfig>(ConfigureDb)
            ;
    }

    private IDb ConfigureDb(IInitConfig initConfig, IPaprikaConfig paprikaConfig)
    {
        if (initConfig.DiagnosticMode == DiagnosticMode.MemDb)
        {
            return PagedDb.NativeMemoryDb(
                paprikaConfig.SizeGb.GiB(),
                paprikaConfig.HistoryDepth);
        }

        var dbPath = initConfig.BaseDbPath + "/paprika/";

        return PagedDb.MemoryMappedDb(
            paprikaConfig.SizeGb.GiB(),
            paprikaConfig.HistoryDepth, // TODO: CHange history depht to more than a byte
            dbPath,
            paprikaConfig.FlushToDisk);
    }
}
