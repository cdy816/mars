using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DBDevelopService
{
    public class Service
    {

        #region ... Variables  ...
        
        private GrpcDBService grpcDBService = new GrpcDBService();
        private WebAPIDBService webDBService = new WebAPIDBService();

        /// <summary>
        /// 
        /// </summary>
        public static Service Instanse = new Service();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        public Service()
        {
            DBDevelopService.SecurityManager.Manager.Init();
            //驱动初始化
            Cdy.Tag.DriverManager.Manager.Init();
            //注册日志
            ServiceLocator.Locator.Registor<ILog>(new ConsoleLogger());
        }

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Start(int grpcPort = 5001, int webSocketPort = 8000, bool isEnableGrpc = true, bool isEnableWebApi = true)
        {
            DbManager.Instance.Load();
            if (isEnableGrpc)
                grpcDBService.Start(grpcPort);

            if (isEnableWebApi)
                webDBService.Start(webSocketPort);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            grpcDBService.StopAsync();
            webDBService.StopAsync();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
