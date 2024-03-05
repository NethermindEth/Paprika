using System.Diagnostics.CodeAnalysis;

namespace Paprika.Utils;

/// <summary>
/// Informs that in a given member there's an optimization opportunity.
/// </summary>
[ExcludeFromCodeCoverage]
[AttributeUsage(AttributeTargets.All, AllowMultiple = true)]
public class OptimizationOpportunityAttribute : Attribute
{
    public OptimizationType Type { get; }
    public string Comment { get; }

    public OptimizationOpportunityAttribute(OptimizationType type, string comment)
    {
        Type = type;
        Comment = comment;
    }
}

[Flags]
public enum OptimizationType
{
    DiskSpace,
    CPU,
}
