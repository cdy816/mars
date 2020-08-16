using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DBGrpcApi
{
    public class Program
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
            Port = ReadServerPort();
            CreateHostBuilder(args).Build().Run();
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
                    webBuilder.UseUrls("https://0.0.0.0:"+ Port);
                    webBuilder.UseStartup<Startup>();
                });
    }
}
