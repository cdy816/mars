using Cdy.Tag;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using System.Net.WebSockets;
using Microsoft.AspNetCore.Http;
using System.Threading;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;

namespace DBDevelopService
{
    public class WebAPIDBService
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static WebAPIDBService Service = new WebAPIDBService();
        private IHost mhost;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Start(int port)
        {
            try
            {
                LoggerService.Service.Info("WebAPIDBService", "Ready to start to WebAPI DBService.....");
                StartAsync("0.0.0.0", port);
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("WebAPIDBService", ex.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ip"></param>
        /// <param name="port"></param>
        public async void StartAsync(string ip, int port)
        {
            string sip = ip;
            if (!sip.StartsWith("http://"))
            {
                sip = "http://" + ip;
            }
            sip += ":" + port;
            mhost = CreateHostBuilder(sip).Build();
            LoggerService.Service.Info("WebAPIDBService", "启动服务:"+ sip);
            await mhost.StartAsync();
        }

        /// <summary>
        /// 
        /// </summary>
        public async void StopAsync()
        {
            LoggerService.Service.Info("WebAPIDBService", "关闭服务:");
            await mhost.StopAsync();
            mhost.Dispose();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="serverUrl"></param>
        /// <returns></returns>
        public static IHostBuilder CreateHostBuilder(string serverUrl) =>
           Host.CreateDefaultBuilder()
                .ConfigureLogging(logging =>
                {
                    logging.AddFilter("WebApi", LogLevel.Warning);
                    logging.SetMinimumLevel(LogLevel.Warning);
                })
               .ConfigureWebHostDefaults(webBuilder =>
               {
                   webBuilder.UseStartup<WebAPIStartup>();
                   webBuilder.UseUrls(serverUrl);
               });

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
