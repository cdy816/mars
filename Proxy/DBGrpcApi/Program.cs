using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Xml.Linq;
using Cdy.Tag;
using Cdy.Tag.Consume;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DBGrpcApi
{
    public class Program : IEmbedProxy
    {
        static int Port = 14333;

        private static int ReadServerPort()
        {
            try
            {
                string spath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location), "Config", "DBGrpcApi.cfg");
                if (System.IO.File.Exists(spath))
                {
                    XElement xx = XElement.Load(spath);
                    if (xx.Element("Allow") != null)
                        Cdy.Tag.Common.ClientAuthorization.Instance.LoadAllowFromXML(xx.Element("Allow"));

                    if (xx.Element("Forbidden") != null)
                        Cdy.Tag.Common.ClientAuthorization.Instance.LoadForbiddenFromXML(xx.Element("Forbidden"));

                    return int.Parse(xx.Attribute("ServerPort")?.Value);
                }
            }
            catch
            {

            }
            return 14333;
        }

        public static void Main(string[] args)
        {
            try
            {
                Port = ReadServerPort();

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    WindowConsolHelper.DisbleQuickEditMode();
                    if (!Console.IsInputRedirected)
                        Console.Title = "DBGrpcApi";
                    if (args.Contains("/m"))
                    {
                        WindowConsolHelper.MinWindow("DBGrpcApi");
                    }
                }
                Cdy.Tag.Common.ProcessMemoryInfo.Instances.StartMonitor("DBGrpcApi");
                foreach (var vv in args)
                {
                    if (vv != "/m")
                    {
                        Startup.Server = vv;
                        break;
                    }
                }
                CreateHostBuilder(args).Build().Run();
            }
            catch(Exception ex)
            {
                if (ex.Message.Contains("dev-certs"))
                {
                    EnableDevCerts();
                    Console.Write("    由于安装证书需要，请重新启动服务程序!      ",ConsoleColor.Yellow);
                }
                else
                {
                    Console.WriteLine(ex.Message);
                }
                Console.WriteLine("  输入任意字符串退出.  ");
                Console.ReadLine();
            }
        }

        private static void EnableDevCerts()
        {
            Process.Start(new ProcessStartInfo() { FileName = "dotnet", Arguments = "dev-certs https --trust" });
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
                        string spath = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "mars.pfx");
                        if (System.IO.File.Exists(spath))
                        {
                            webBuilder.UseKestrel(options =>
                            {
                                options.ListenAnyIP(Port, listenOps =>
                                {
                                    listenOps.UseHttps(callback =>
                                    {
                                        callback.AllowAnyClientCertificate();
                                        callback.ServerCertificate = new System.Security.Cryptography.X509Certificates.X509Certificate2(spath, "mars");
                                    });
                                });
                            });
                        }
                        else
                        {
                            webBuilder.UseUrls("https://0.0.0.0:" + Port);
                        }
                    }

                    webBuilder.UseStartup<Startup>();
                });

        #region IEmbedProxy
        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public void Init()
        {
            Port=ReadServerPort();
            Startup.IsRunningEmbed=true;
        }
        private IHost host;
        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            Task.Run(() => {
                try
                {
                    host = CreateHostBuilder(null).Build();
                    host.Run();
                }
                catch
                {

                }
            });
          
        }

        public void Stop()
        {
            host?.StopAsync();
            host= null;
        }
        #endregion

    }
}
