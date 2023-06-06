
using Cdy.Tag;
using DBRuntimeServer;
using RuntimeServiceImp;
using System.Diagnostics;
using System.Management;
using System.Xml.Linq;
public class Program
{
    static int Port = 14000;

    /// <summary>
    /// 
    /// </summary>
    /// <returns></returns>
    static int ReadServerPort()
    {
        try
        {
            string spath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location), "Config", "DbRuntimeServer.cfg");
            if (System.IO.File.Exists(spath))
            {
                XElement xx = XElement.Load(spath);
                return int.Parse(xx.Attribute("ServerPort")?.Value);
            }
        }
        catch
        {

        }
        return 14000;
    }

    static void Main(string[] args)
    {
        WindowConsolHelper.DisbleQuickEditMode();
        Console.WriteLine("准备启动....");
        Port = ReadServerPort();

        if (!Console.IsInputRedirected)
        {
            Console.Title = "DBGuardian";
        }
       
        if (args.Contains("/m"))
        {
            WindowConsolHelper.MinWindow("DBGuardian");
        }
        RuntimeServiceManager.Instance.Start();
        DBWebRunner.Instance.Start(Port);
        while(true)
        {
           string cmd = Console.In.ReadLine();
            if(cmd=="exit")
            {
                break;
            }
        }
        //Console.WriteLine("启动完成....");
    }

}
