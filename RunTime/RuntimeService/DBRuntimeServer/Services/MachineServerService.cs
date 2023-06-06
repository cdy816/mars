using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Cdy.Tag;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace DBRuntimeServer
{
    public class MachineServerService : MachineServer.MachineServerBase
    {
        private readonly ILogger<MachineServerService> _logger;
        /// <summary>
        /// 
        /// </summary>
        /// <param name="logger"></param>
        public MachineServerService(ILogger<MachineServerService> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetMachineInfoResponse> GetMachineInfo(GetLocalResourceRequest request, ServerCallContext context)
        {
            if (!UserManager.Manager.CheckLogin(request.Token)) return Task.FromResult(new GetMachineInfoResponse() { Result = false });
            var rs = ServiceLocator.Locator.Resolve<DBRuntimeAPI.ILocalResourceService>();
            GetMachineInfoResponse res = new GetMachineInfoResponse() { Result = true, OSVersion = rs.OSVersion, DotnetVersion = rs.DotnetVersion, Is64Bit = rs.Is64Bit ? 1 : 0, MachineName = rs.MachineName, ProcessCount = rs.ProcessCount };
            return Task.FromResult<GetMachineInfoResponse>(res);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<LoginResponse> Login(LoginRequest request, ServerCallContext context)
        {
            return Task.FromResult(new LoginResponse() { Token = UserManager.Manager.Login(request.Database,request.Username,request.Password),Timeout = UserManager.Manager.TimeOut}) ;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolReponse> Logout(GetLocalResourceRequest request, ServerCallContext context)
        {
            UserManager.Manager.Logout(request.Token);
            return Task.FromResult(new BoolReponse() { Result=true});
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolReponse> Hart(GetLocalResourceRequest request, ServerCallContext context)
        {
            UserManager.Manager.Fresh(request.Token);
            return Task.FromResult(new BoolReponse() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolReponse> CheckProcessRun(CheckProcessRunRequest request, ServerCallContext context)
        {
            if (!UserManager.Manager.CheckLogin(request.Token)) return Task.FromResult(new BoolReponse() { Result = false });
            CheckProcessStart(request.ProcessName);
            return Task.FromResult(new BoolReponse() { Result = true });
        }

        private bool CheckProcesses(string name)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return Process.GetProcessesByName(name).Length > 0;
            else
            {
                return Cdy.Tag.Common.ProcessMemoryInfo.IsExist(name);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        private void CheckProcessStart(string name)
        {
            if(!CheckProcesses(name))
            {
                var file = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), name);
                if (Environment.OSVersion.Platform == PlatformID.Win32NT)
                {
                    if (System.IO.File.Exists(file + ".exe"))
                    {
                        var vfile = file;
                        ProcessStartInfo pinfo = new ProcessStartInfo();
                        pinfo.FileName = vfile + ".exe";
                        pinfo.Arguments = "/m";
                        pinfo.RedirectStandardOutput = false;
                        pinfo.RedirectStandardInput = false;
                        pinfo.UseShellExecute = true;
                        pinfo.WindowStyle = ProcessWindowStyle.Minimized;
                        Process.Start(pinfo);
                    }
                }
                else
                {
                    if (System.IO.File.Exists(file + ".dll"))
                    {
                        ProcessStartInfo info = new ProcessStartInfo("dotnet", $"{file}.dll /m") { RedirectStandardOutput = false, RedirectStandardInput = false, RedirectStandardError = false, UseShellExecute = true,WindowStyle = ProcessWindowStyle.Minimized };
                        Process.Start(info);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetLocalResourceResponse> GetLocalResource(GetLocalResourceRequest request, ServerCallContext context)
        {
            if (!UserManager.Manager.CheckLogin(request.Token)) return Task.FromResult(new GetLocalResourceResponse() { Result = false });

            GetLocalResourceResponse re = new GetLocalResourceResponse() { Network = new NetworkInfo()};
            var rs = ServiceLocator.Locator.Resolve<DBRuntimeAPI.ILocalResourceService>();
            re.CPU = rs.CPU().ToString();
            var nk = rs.Network();
            re.Network.Receive = nk.Item1.ToString();
            re.Network.Send = nk.Item2.ToString();

            var mm = rs.Memory();
            re.MemoryUsed = mm.Item1.ToString();
            re.MemoryTotal = mm.Item2.ToString();

            return Task.FromResult(re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetProcessInfoResponse> GetProcessInfo(GetProcessInfoRequest request, ServerCallContext context)
        {
            if (!UserManager.Manager.CheckLogin(request.Token)) return Task.FromResult(new GetProcessInfoResponse() { Result = false });

            GetProcessInfoResponse re = new GetProcessInfoResponse() { Result = true };
            DBRuntimeImp.ProcessMonitorManager.Manager.Update();

            foreach(var vv in DBRuntimeImp.ProcessMonitorManager.Manager.ListProcess())
            {
                if (vv.IsOpened)
                {
                    var vp = new ProcessInfo() { Name = vv.Name, CPU = vv.Info.CPU, Memory = vv.Info.TotalMemory, StartTime = vv.Info.StartTime.ToString(), TotalCPU = vv.Info.TotalCPU, ThreadCount = vv.Info.ThreadCount,IsOpened=true };
                    foreach (var vvv in vv.Info.Clients) vp.Clients.Add(new RemoteClient() { DateTime = vvv.Value.Time.ToString(), Ip = new IPAddress(vvv.Value.Ip).ToString(), Port = vvv.Value.Port });
                    re.Infos.Add(vp);
                }
                else
                {
                    var vp = new ProcessInfo() { Name = vv.Name, IsOpened = false };
                    re.Infos.Add(vp);
                }
            }

            return Task.FromResult(re);
        }
    }
}
