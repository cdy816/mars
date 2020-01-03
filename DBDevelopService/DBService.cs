using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DBDevelopService
{
    public class DBService
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static DBService Service = new DBService();
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
            await mhost.StartAsync();
        }

        /// <summary>
        /// 
        /// </summary>
        public async void StopAsync()
        {
           await mhost.StopAsync();
        }


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
