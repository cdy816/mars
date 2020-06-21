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

namespace DBDevelopService
{
    public class GrpcDBService
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static GrpcDBService Service = new GrpcDBService();
        private IHost mhost;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        ///// <summary>
        ///// 
        ///// </summary>
        //public GrpcDBService()
        //{
        //    DBDevelopService.SecurityManager.Manager.Init();
        //    //注册日志
        //    ServiceLocator.Locator.Registor<ILog>(new ConsoleLogger());
        //}

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
            if (!sip.StartsWith("https://"))
            {
                sip = "https://" + ip;
            }
            sip += ":" + port;
            mhost = CreateHostBuilder(sip).Build();
           
            LoggerService.Service.Info("DBService", "启动服务:"+ sip);
            await mhost.StartAsync();
        }

        /// <summary>
        /// 
        /// </summary>
        public async void StopAsync()
        {
            LoggerService.Service.Info("DBService", "关闭服务:");
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
                   webBuilder.UseStartup<Startup>();
                   webBuilder.UseUrls(serverUrl);
               });

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
