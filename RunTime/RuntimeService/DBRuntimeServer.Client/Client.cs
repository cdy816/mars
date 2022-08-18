using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Reflection;
using System.Linq;

namespace DBRuntimeServer
{
    /// <summary>
    /// 
    /// </summary>
    public class Client : IDisposable
    {

        #region ... Variables  ...

        private string mLoginId = string.Empty;

        private DBServer.DBServerClient mDBClient;
        private LogServer.LogServerClient mLogClient;
        private MachineServer.MachineServerClient mMachineClient;


        private Grpc.Net.Client.GrpcChannel grpcChannel;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...



        /// <summary>
        /// 
        /// </summary>
        /// <param name="ip"></param>
        /// <param name="port"></param>
        public Client(string ip, int port)
        {
            Ip = ip;
            Port = port;
            Init();
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 使用TLS加密
        /// </summary>
        public bool UseTls { get; set; } = true;

        public string Ip { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsLogined
        {
            get
            {
                return !string.IsNullOrEmpty(mLoginId);
            }
        }

        /// <summary>
        /// 超时时间
        /// </summary>
        public int TimeOut { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime LoginTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string UserName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Password { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public Client Init()
        {
            try
            {
                var httpClientHandler = new HttpClientHandler();
                httpClientHandler.ServerCertificateCustomValidationCallback =
                    HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
                var httpClient = new HttpClient(httpClientHandler);
                if (UseTls &&!IsWin7)
                    grpcChannel = Grpc.Net.Client.GrpcChannel.ForAddress(@"https://" + Ip + ":" + Port, new GrpcChannelOptions { HttpClient = httpClient });
                else
                {
                    grpcChannel = Grpc.Net.Client.GrpcChannel.ForAddress(@"http://" + Ip + ":" + Port, new GrpcChannelOptions { HttpClient = httpClient });
                }

                mDBClient=new DBRuntimeServer.DBServer.DBServerClient(grpcChannel);
                mLogClient=new DBRuntimeServer.LogServer.LogServerClient(grpcChannel);
                mMachineClient=new DBRuntimeServer.MachineServer.MachineServerClient(grpcChannel);

            }
            catch (Exception ex)
            {
                //LoggerService.Service.Erro("DevelopService", ex.Message);
            }
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        static bool IsWin7
        {
            get
            {
                return Environment.OSVersion.Version.Major < 8 && Environment.OSVersion.Platform == PlatformID.Win32NT;
            }
        }

        /// <summary>
        /// 登录
        /// </summary>
        /// <param name="username">用户名</param>
        /// <param name="password">密码</param>
        /// <returns></returns>
        public bool Login(string username, string password,string database)
        {
            if (mMachineClient != null)
            {
                try
                {
                    var re = mMachineClient.Login(new LoginRequest() { Username = username, Password = password,Database=database });
                    TimeOut = re.Timeout;
                    LoginTime = DateTime.Now;
                    mLoginId = re.Token;
                }
                catch
                {
                    return false;
                }
            }
            return false;
        }

        /// <summary>
        /// 登出
        /// </summary>
        public void Logout()
        {
            if (mMachineClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    mMachineClient.Logout(new GetLocalResourceRequest() { Token = mLoginId });
                    mLoginId = string.Empty;
                }
                catch
                {

                }
            }
        }

        /// <summary>
        /// 定时心跳，维持登录
        /// </summary>
        /// <returns></returns>
        public bool Hart()
        {
            try
            {
                if (mMachineClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mMachineClient.Hart(new GetLocalResourceRequest() { Token = mLoginId }).Result;
                }
            }
            catch
            {

            }
            return false;
        }

       

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<string> ListDatabase()
        {
            List<string> re = new List<string>();
            try
            {
              
                var res = mDBClient.ListDatabase(new CommonRequest() { Token = mLoginId });
                if (res != null)
                {
                    foreach (var vv in res.Databases)
                    {
                        re.Add(vv);
                    }
                }
            }
            catch
            {

            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool IsDatabaseRun(string database)
        {
            if(!IsLogined) return false;
            return mDBClient.CheckDatabaseIsRunning(new DatabaseCommonRequest() { Database = database, Token = mLoginId}).IsRunning;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool StartDatabase(string database)
        {
            if (!IsLogined) return false;
            return mDBClient.StartDatabse(new DatabaseCommonRequest() { Database = database, Token = mLoginId }).Result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool CheckDbDevelopServerIsRun()
        {
            if (!IsLogined) return false;
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool StopDatabase(string database)
        {
            if (!IsLogined) return false;
            return mDBClient.StopDatabse(new DatabaseCommonRequest() { Database = database, Token = mLoginId }).Result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool ReStart(string database)
        {
            if (!IsLogined) return false;
            return mDBClient.HotStartDatabse(new DatabaseCommonRequest() { Database = database, Token = mLoginId }).Result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="api"></param>
        /// <returns></returns>
        public bool IsApiRun(string database,string api)
        {
            if (!IsLogined) return false;
            return mDBClient.CheckApiIsRunning(new DatabaseApiRequest() { Database=database,Token=mLoginId,Api=api }).IsRunning;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool CheckProcessStart(string name)
        {
            if (!IsLogined) return false;
            return mMachineClient.CheckProcessRun(new CheckProcessRunRequest() { ProcessName=name,Token=mLoginId}).Result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public List<ApiItem> GetApiSetting(string database)
        {
            List<ApiItem> re = new List<ApiItem>();
            if (!IsLogined) return re;
            var res = mDBClient.GetDatabseSetting(new DatabaseCommonRequest() { Token=mLoginId,Database=database});
            if(res!=null)
            {
                foreach(var vv in res.Apis)
                {
                    re.Add(new ApiItem() { Name=vv.Name,Port=vv.Port });
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public List<DiskInfoItem> GetDiskInfo(string database)
        {
            List<DiskInfoItem> re = new List<DiskInfoItem>();
            if (!IsLogined) return re;
            var res = mDBClient.GetDisk(new DatabaseCommonRequest() { Database = database, Token = mLoginId});
            if(res!= null)
            {
                foreach(var vv in res.Disks)
                {
                    re.Add(new DiskInfoItem() { Label = vv.Label, Total = double.Parse(vv.Total), Used = double.Parse(vv.Used), UsedFor = vv.UsedFor });
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public MachineInfo? GetMachineInfo()
        {
            if (!IsLogined) return null;
            var res = mMachineClient.GetMachineInfo(new GetLocalResourceRequest() { Token = mLoginId });
            if(res!=null && res.Result)
            {
                return new MachineInfo() { DotnetVersion = res.DotnetVersion, MachineName = res.MachineName, Is64Bit = res.Is64Bit>0, OSVersion = res.OSVersion, ProcessCount = res.ProcessCount };
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public List<string> GetLogs(string database,DateTime starttime,DateTime endtime,string type)
        {
            List<string> res = new List<string>();
            if (!IsLogined) return res;
            var re = mLogClient.GetLog2(new GetLogRequest2() { Database = database, LogType = type, Token=mLoginId, StartTime = starttime.ToString(),EndTime=endtime.ToString() });
            res.Add(UnZipString(re.Msg.Memory.ToArray()));
            return res;
        }

        public string UnZipString(byte[] msg)
        {
            List<string> re = new List<string>();
            using (MemoryStream ms = new MemoryStream(msg))
            {
                using (System.IO.Compression.BrotliStream bs = new System.IO.Compression.BrotliStream(ms,System.IO.Compression.CompressionMode.Decompress))
                {
                    using(System.IO.StreamReader sr = new StreamReader(bs))
                    {
                        return sr.ReadToEnd();
                    }
                }
            }
            return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="starttime"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public List<string> GetLogs(string database, DateTime starttime, string type)
        {
            List<string> res = new List<string>();
            if (!IsLogined) return res;
            var re = mLogClient.GetLog(new GetLogRequest() { Database = database, LogType = type,Token=mLoginId, StartTime = starttime.ToString() });
            res.Add(UnZipString(re.Msg.Memory.ToArray()));
            return res;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public MachineResourceItem GetHostResource()
        {
            if (!IsLogined) return new MachineResourceItem();

            var res = mMachineClient.GetLocalResource(new GetLocalResourceRequest() { Token = mLoginId });
            if(res != null)
            {
                return new MachineResourceItem() { CPU = double.Parse(res.CPU),MemoryTotal=double.Parse(res.MemoryTotal),MemoryUsed=double.Parse(res.MemoryUsed),Network=new NetworkInfo() { Send=res.Network.Send,Receive=res.Network.Receive } };
            }
            else
            {
                return new MachineResourceItem();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            mLoginId = string.Empty;
            grpcChannel.Dispose();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public class MachineInfo
    {
        /// <summary>
        /// 
        /// </summary>
        public string OSVersion { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string DotnetVersion { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int ProcessCount { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool Is64Bit { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string MachineName { get; set; }
    }



    /// <summary>
    /// 
    /// </summary>
    public struct ApiItem
    {
        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public int Port { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public struct DiskInfoItem
    {
        public string Label { get; set; }
        public double Used { get; set; }
        public double Total { get; set; }
        public string UsedFor { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public struct MachineResourceItem
    {
        /// <summary>
        /// 
        /// </summary>
        public double CPU { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public double MemoryTotal { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public double MemoryUsed { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public NetworkInfo Network { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public struct NetworkInfoItem
    {
        public double Send { get; set; }
        public double Receive { get; set; }
    }

}
