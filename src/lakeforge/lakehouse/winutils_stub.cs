using System;

internal static class WinUtilsStub
{
    private static int Main(string[] args)
    {
        if (args.Length > 0 && args[0] == "ls")
        {
            Console.WriteLine("-rwxrwxrwx 1 lakeforge lakeforge");
        }
        return 0;
    }
}
