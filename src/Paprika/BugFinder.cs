using System.Diagnostics;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika;

public static class BugFinder
{
    private static readonly Keccak Missing =
        new(Convert.FromHexString("2f31c20a56f5e9fc1a3873c84d5f80daf7d52e12c5ad5260dc6c32eb41e5fc4d"));
    
    public static void Search(in Keccak account)
    {
        if (Missing == account)
        {
            Debugger.Launch();
        }
    }
    
    public static void Search(in Key key)
    {
        if (key.Path.Length != 64) 
            return;
        
        if (key.Path.UnsafeAsKeccak == Missing)
        {
            Debugger.Launch();    
        }
    }
}