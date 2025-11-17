using Autofac;
using Nethermind.Api.Extensions;
using Nethermind.Core;
using Nethermind.State;

namespace PaprikaPlugin;

public class PaprikaPlugin: INethermindPlugin
{
    public string Name => "Paprika";
    public string Description => "Paprika State Db";
    public string Author => "Nethermind";
    public bool Enabled => true; // TODO: Make configurable
}

public class PaprikaPluginModule : Module
{
    protected override void Load(ContainerBuilder builder)
    {
        base.Load(builder);

        builder
            .AddSingleton<IWorldStateManager, PaprikaWorldStateManager>()
            .AddSingleton<IStateReader, PaprikaStateReader>()
            ;
    }
}
