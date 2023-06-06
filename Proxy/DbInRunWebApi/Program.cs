using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Xml.Linq;
using Cdy.Tag;
using Cdy.Tag.Consume;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DbInRunWebApi
{
    /// <summary>
    /// 
    /// </summary>
    public class Program : IEmbedProxy
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
                    if (xx.Element("Allow") != null)
                        Cdy.Tag.Common.ClientAuthorization.Instance.LoadAllowFromXML(xx.Element("Allow"));

                    if (xx.Element("Forbidden") != null)
                        Cdy.Tag.Common.ClientAuthorization.Instance.LoadForbiddenFromXML(xx.Element("Forbidden"));

                    if (xx.Attribute("UseHttps")!=null)
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
            
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                WindowConsolHelper.DisbleQuickEditMode();
                if (!Console.IsInputRedirected)
                    Console.Title = "DbWebApi";
                Cdy.Tag.Common.ProcessMemoryInfo.Instances.StartMonitor("DbWebApi");
                if (args.Contains("/m"))
                {
                    WindowConsolHelper.MinWindow("DbWebApi");
                }
            }

            try
            {
                CreateHostBuilder(args).Build().Run();
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
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
                    else
                    {
                        webBuilder.UseUrls("http://0.0.0.0:" + Port);
                    }
                    webBuilder.UseStartup<WebAPIStartup>();
                });

        #region IEmbedProxy
        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public void Init()
        {
             ReadServerParameter();
            WebAPIStartup.IsRunningEmbed = true;
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
            host = null;
        }
        #endregion
    }
}
