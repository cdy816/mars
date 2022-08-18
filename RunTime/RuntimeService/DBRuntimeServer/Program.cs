
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
        try
        {
            Port = ReadServerPort();
            Console.Title = "DbRuntimeServer";
            if (args.Contains("/m"))
            {
                WindowConsolHelper.MinWindow("DbRuntimeServer");
            }

            RuntimeServiceManager.Instance.Start();
            //foreach (var vv in args)
            //{
            //    if (vv != "/m")
            //    {
            //        Startup.Server = vv;
            //        break;
            //    }
            //}
            CreateHostBuilder(args).Build().Run();
        }
        catch (Exception ex)
        {
            if (ex.Message.Contains("dev-certs"))
            {
                EnableDevCerts();
                Console.Write("    由于安装证书需要，请重新启动服务程序!      ", ConsoleColor.Yellow);
            }
            else
            {
                Console.WriteLine(ex.Message);
            }
            Console.WriteLine("  输入任意字符串退出.  ");
            Console.ReadLine();
        }
    }

    static void EnableDevCerts()
    {
        Process.Start(new ProcessStartInfo() { FileName = "dotnet", Arguments = "dev-certs https --trust" });
    }

    /// <summary>
    /// 
    /// </summary>
    static bool IsWin7
    {
        get
        {
            return Environment.OSVersion.Version.Major < 8 && Environment.OSVersion.Platform == PlatformID.Win32NT;
        }
    }

    // Additional configuration is required to successfully run gRPC on macOS.
    // For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682
    static IHostBuilder CreateHostBuilder(string[] args) =>

        Host.CreateDefaultBuilder(args)
            .ConfigureLogging(logging =>
            {
                logging.AddFilter("Grpc", LogLevel.Warning);
                logging.SetMinimumLevel(LogLevel.Warning);
            })
            .ConfigureWebHostDefaults(webBuilder =>
            {
                if (IsWin7)
                {
                    //Win 7 的情况下使用 不支持TLS 的 HTTP/2
                    webBuilder.ConfigureKestrel(options =>
                    {
                        options.Listen(System.Net.IPAddress.Parse("0.0.0.0"), Port, a => a.Protocols =
                         Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2);
                    });
                }
                else
                {
                    webBuilder.UseUrls("https://0.0.0.0:" + Port);
                }

                webBuilder.UseStartup<Startup>();
            });
}
