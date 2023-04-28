namespace Paprika;

/// <summary>
/// Informs that in a given member there's an optimization opportunity.
/// </summary>
[AttributeUsage(AttributeTargets.All)]
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