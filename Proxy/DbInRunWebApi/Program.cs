using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Cdy.Tag;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DbInRunWebApi
{
    /// <summary>
    /// 
    /// </summary>
    public class Program
    {
        static int Port = 14331;

        private static int ReadServerPort()
        {
            try
            {
                string spath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location), "Config", "DbWebApi.cfg");
                if (System.IO.File.Exists(spath))
                {
                    XElement xx = XElement.Load(spath);
                    return int.Parse(xx.Attribute("ServerPort")?.Value);
                }
            }
            catch
            {

            }
            return 14331;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="args"></param>
        public static void Main(string[] args)
        {
            Port = ReadServerPort();
            WindowConsolHelper.DisbleQuickEditMode();
            CreateHostBuilder(args).Build().Run();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseUrls("https://0.0.0.0:"+Port);
                    webBuilder.UseStartup<WebAPIStartup>();
                });
    }
}
