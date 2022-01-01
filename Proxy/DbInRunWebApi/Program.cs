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
        static bool UseHttps = false;

        private static void ReadServerParameter()
        {
            try
            {
                string spath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location), "Config", "DbWebApi.cfg");
                if (System.IO.File.Exists(spath))
                {
                    XElement xx = XElement.Load(spath);
                    if(xx.Attribute("UseHttps")!=null)
                    {
                        UseHttps = bool.Parse(xx.Attribute("UseHttps").Value);
                    }
                    Port = int.Parse(xx.Attribute("ServerPort")?.Value);
                }
            }
            catch
            {

            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="args"></param>
        public static void Main(string[] args)
        {
             ReadServerParameter();
            WindowConsolHelper.DisbleQuickEditMode();
            Console.Title = "DbWebApi";
            if (args.Contains("/m"))
            {
                WindowConsolHelper.MinWindow("DbWebApi");
            }
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
                    if (UseHttps)
                    {
                        webBuilder.UseUrls("https://0.0.0.0:" + Port);
                    }
                    else
                    {
                        webBuilder.UseUrls("http://0.0.0.0:" + Port);
                    }
                    webBuilder.UseStartup<WebAPIStartup>();
                });
    }
}
