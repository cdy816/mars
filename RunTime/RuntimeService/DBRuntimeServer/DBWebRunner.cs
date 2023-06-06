using System.Diagnostics;

namespace DBRuntimeServer
{
    public class DBWebRunner
    {
        /// <summary>
        /// 
        /// </summary>
        public static DBWebRunner Instance = new DBWebRunner();

        /// <summary>
        /// 
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="port"></param>
        public void Start(int port)
        {
            Port = port;
            Console.WriteLine("启动Web服务:" + port);
            Task.Run(() => {
                CreateHostBuilder(null).Build().Run();
            });
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
        IHostBuilder CreateHostBuilder(string[] args) =>

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
    }
}
