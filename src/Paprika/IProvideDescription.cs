namespace Paprika;

public interface IProvideDescription
{
    /// <summary>
    /// Provides an in-depth of this object. May allocate a lot and take a long period of time to make it.
    /// </summary>
    string Describe();
}