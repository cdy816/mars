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
using Cdy.Tag.Consume;

namespace DbInRunWebApi
{
    public class WebAPIDBService: IConsumeDriver
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static WebAPIDBService Service = new WebAPIDBService();
        private IHost mhost;

        public string Name => "WebApi";
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
            StartAsync("0.0.0.0", port);
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
               .ConfigureWebHostDefaults(webBuilder =>
               {
                   webBuilder.UseStartup<WebAPIStartup>();
                   webBuilder.UseUrls(serverUrl);
               });

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Start()
        {
            Start(8000);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Stop()
        {
            this.StopAsync();
            return true;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
