using Cdy.Tag;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DirectAccessGrpc
{
    public class Program
    {
        static int Port = 14338;

        public static void Main(string[] args)
        {
            ServiceLocator.Locator.Registor<ILog>(new ConsoleLogger());

            Port = DirectAccessProxy.Proxy.Load().ServerPort;
            Console.Title = "DirectAccessGrpcApi";
            if (args.Contains("/m"))
            {
                WindowConsolHelper.MinWindow("DirectAccessGrpcApi");
            }
            CreateHostBuilder(args).Build().Run();
        }

        /// <summary>
        /// 
        /// </summary>
        public static bool IsWin7
        {
            get
            {
                return Environment.OSVersion.Version.Major < 8 && Environment.OSVersion.Platform == PlatformID.Win32NT;
            }
        }

        // Additional configuration is required to successfully run gRPC on macOS.
        // For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682
        public static IHostBuilder CreateHostBuilder(string[] args) =>
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
}
