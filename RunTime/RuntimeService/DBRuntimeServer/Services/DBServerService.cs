using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Cdy.Tag;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace DBRuntimeServer
{
    public class DBServerService : DBServer.DBServerBase
    {
        private readonly ILogger<DBServerService> _logger;
        public DBServerService(ILogger<DBServerService> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<CheckDatabaseIsRunningResponse> CheckDatabaseIsRunning(DatabaseCommonRequest request, ServerCallContext context)
        {
            if (!UserManager.Manager.CheckLogin(request.Token)) return Task.FromResult(new CheckDatabaseIsRunningResponse() { IsRunning = false });
            var vv = Cdy.Tag.ServiceLocator.Locator.Resolve<DBRuntimeAPI.IDatabaseService>().CheckStart(request.Database);
            return Task.FromResult(new CheckDatabaseIsRunningResponse() { IsRunning =vv});
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetDatabseSettingResponse> GetDatabseSetting(DatabaseCommonRequest request, ServerCallContext context)
        {
            if (!UserManager.Manager.CheckLogin(request.Token)) return Task.FromResult(new GetDatabseSettingResponse() { Result = false });

            var res = Cdy.Tag.ServiceLocator.Locator.Resolve<DBRuntimeAPI.IDatabaseService>().GetDatabaseAPI(request.Database);
            GetDatabseSettingResponse re = new GetDatabseSettingResponse();
            foreach(var item in res)
            {
                re.Apis.Add(new ApiInfo() { Name = item.Key, Port = int.Parse(item.Value) });
            }
            return Task.FromResult(re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetDiskResponse> GetDisk(DatabaseCommonRequest request, ServerCallContext context)
        {
            if (!UserManager.Manager.CheckLogin(request.Token)) return Task.FromResult(new GetDiskResponse() { Result = false });

            var res = Cdy.Tag.ServiceLocator.Locator.Resolve<DBRuntimeAPI.IDatabaseService>().HisDataDisk(request.Database);
            var resb = Cdy.Tag.ServiceLocator.Locator.Resolve<DBRuntimeAPI.IDatabaseService>().BackHisDataDisk(request.Database);
            GetDiskResponse gds = new GetDiskResponse();
            gds.Disks.Add(new DiskInfo() { Total = res.Item2.ToString(), Used = res.Item1.ToString(), UsedFor = "HisData",Label= res.Item3 });
            if (resb != null)
                gds.Disks.Add(new DiskInfo() { Total = resb.Item2.ToString(), Used = resb.Item1.ToString(), UsedFor = "BackHisData",Label=resb.Item3 });
            return Task.FromResult(gds);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<StartDatabseResponse> HotStartDatabse(DatabaseCommonRequest request, ServerCallContext context)
        {
            if (!UserManager.Manager.CheckLogin(request.Token)) return Task.FromResult(new StartDatabseResponse() { Result = false });

            var res = Cdy.Tag.ServiceLocator.Locator.Resolve<DBRuntimeAPI.IDatabaseService>().ReStartDatabase(request.Database);
            return Task.FromResult(new StartDatabseResponse() { Result = res});
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<ListDatabaseResponse> ListDatabase(CommonRequest request, ServerCallContext context)
        {
            var res = Cdy.Tag.ServiceLocator.Locator.Resolve<DBRuntimeAPI.IDatabaseService>().ListDatabse();
            ListDatabaseResponse lds = new ListDatabaseResponse();
            lds.Databases.AddRange(res);
            return Task.FromResult(lds);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<StartDatabseResponse> StartDatabse(DatabaseCommonRequest request, ServerCallContext context)
        {
            if (!UserManager.Manager.CheckLogin(request.Token)) return Task.FromResult(new StartDatabseResponse() { Result = false });

            var res = Cdy.Tag.ServiceLocator.Locator.Resolve<DBRuntimeAPI.IDatabaseService>().RunDatabase(request.Database);
            return Task.FromResult(new StartDatabseResponse() { Result = res });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<StartDatabseResponse> StopDatabse(DatabaseCommonRequest request, ServerCallContext context)
        {
            if (!UserManager.Manager.CheckLogin(request.Token)) return Task.FromResult(new StartDatabseResponse() { Result = false });
            var res = Cdy.Tag.ServiceLocator.Locator.Resolve<DBRuntimeAPI.IDatabaseService>().StopDatabase(request.Database);
            return Task.FromResult(new StartDatabseResponse() { Result = res });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<HasAlarmResponse> HasAlarm(DatabaseCommonRequest request, ServerCallContext context)
        {
            if (!UserManager.Manager.CheckLogin(request.Token)) return Task.FromResult(new HasAlarmResponse() { Result = false });
            string spath = System.IO.Path.Combine(PathHelper.helper.AppPath, "Ant");
            if(System.IO.Directory.Exists(spath))
            {
                string apitype="";
                int port = 0;
                string sspath = System.IO.Path.Combine(PathHelper.helper.DataPath, request.Database, request.Database + ".adbs");
                if (System.IO.File.Exists(sspath))
                {
                    XElement xx = XElement.Load(sspath);
                    if (xx.Element("Proxy") != null)
                    {
                        var xxx = xx.Element("Proxy");
                        apitype = xxx.Attribute("Type") != null ? xxx.Attribute("Type").Value : "";
                        if (!string.IsNullOrEmpty(apitype) && xxx.Element(apitype) != null)
                        {
                            port = int.Parse(xxx.Element(apitype).Attribute("Port").Value);
                        }
                    }
                }
                return Task.FromResult(new HasAlarmResponse() { Result = true,Grpc=apitype.ToLower().Contains("grpc"),Port=port });
            }
            return Task.FromResult(new HasAlarmResponse() { Result = false });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<CheckDatabaseIsRunningResponse> CheckApiIsRunning(DatabaseApiRequest request, ServerCallContext context)
        {
            if (!UserManager.Manager.CheckLogin(request.Token)) return Task.FromResult(new CheckDatabaseIsRunningResponse() { IsRunning=false });
            var apis = Process.GetProcessesByName(request.Api);
            if(apis!=null && apis.Length>0)
            {
               
                return Task.FromResult(new CheckDatabaseIsRunningResponse() { IsRunning = true });
            }
            else
            {
                return Task.FromResult(new CheckDatabaseIsRunningResponse() { IsRunning = false });
            }
        }

    }
}
