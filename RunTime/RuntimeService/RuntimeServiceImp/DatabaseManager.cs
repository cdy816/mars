using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Pipes;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace RuntimeServiceImp
{
    public class DatabaseManager : DBRuntimeAPI.IDatabaseService,DBRuntimeAPI.ILogService
    {

        /// <summary>
        /// 
        /// </summary>
        public static DatabaseManager Instance = new DatabaseManager();

        /// <summary>
        /// 
        /// </summary>
        public string DatabasePath { get; set; }

        private object mLockObj = new object();

        /// <summary>
        /// 
        /// </summary>
        public IDictionary<string, DatabaseItem> mDatabaseItems=new Dictionary<string, DatabaseItem>();

        /// <summary>
        /// 
        /// </summary>
        public DatabaseManager()
        {
            DatabasePath = PathHelper.helper.DataPath;
           // DatabasePath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "Data");
        }

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            mDatabaseItems.Clear();
            if (System.IO.Directory.Exists(DatabasePath))
            {
                foreach (var item in System.IO.Directory.EnumerateDirectories(DatabasePath))
                {
                    DatabaseItem ditem = new DatabaseItem();
                    ditem.Name = new DirectoryInfo(item).Name;
                    ditem.Path = item;
                    if (!mDatabaseItems.ContainsKey(ditem.Name))
                    {
                        mDatabaseItems.Add(ditem.Name, ditem);
                    }
                    ditem.Init();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>

        public Tuple<double, double,string> BackHisDataDisk(string database)
        {
            if (mDatabaseItems.ContainsKey(database))
            {
                string spath = mDatabaseItems[database].BackHisDataPath;
                if (!string.IsNullOrEmpty(spath))
                {
                    System.IO.DriveInfo dinfo = new System.IO.DriveInfo(System.IO.Path.GetPathRoot(spath));
                    return new Tuple<double, double,string>((dinfo.TotalSize - dinfo.AvailableFreeSpace)/1024.0/1024/1024, dinfo.TotalSize / 1024.0 / 1024 / 1024,dinfo.Name);
                }
            }
            return new Tuple<double, double,string>(0, 0,"");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public Dictionary<string, string> GetDatabaseAPI(string database)
        {
            Dictionary<string, string> api = new Dictionary<string, string>();
            if(mDatabaseItems.ContainsKey(database))
            {
                var vv = mDatabaseItems[database];
                //if(vv.EnableGRPCAPI)
                {
                    api.Add("GrpcAPI",vv.GRPCPort.ToString());
                }

                //if (vv.EnableWebAPI)
                {
                    api.Add("WebApi", vv.WebAPIPort.ToString());
                }

                //if (vv.EnableHighApi)
                {
                    api.Add("HighAPI", vv.HighPort.ToString());
                }

                api.Add("DbOpcServer", vv.OPCUAServerPort.ToString());
            }
            return api;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public Tuple<double, double,string> HisDataDisk(string database)
        {
            if (mDatabaseItems.ContainsKey(database))
            {
                string spath = mDatabaseItems[database].HisDataPath;
                System.IO.DriveInfo dinfo = new System.IO.DriveInfo(System.IO.Path.GetPathRoot(spath));
                return new Tuple<double, double,string>((dinfo.TotalSize - dinfo.AvailableFreeSpace) / 1024.0 / 1024 / 1024, dinfo.TotalSize / 1024.0 / 1024 / 1024,dinfo.Name);
               
            }
            return new Tuple<double, double,string>(0,0,"");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public  bool CheckStart(string name)
        {
            lock (mLockObj)
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    if (IsDatabaseRun(name, out bool isdbrun))
                    {
                        return true;
                    }
                    else if (!isdbrun) return false;
                }

                using (var client = new NamedPipeClientStream(".", name, PipeDirection.InOut))
                {
                    try
                    {
                        client.Connect(1000);
                        client.Close();
                        return true;
                    }
                    catch (Exception)
                    {
                        return false;
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool IsDatabaseRun(string database, out bool isdbrun)
        {
            var pps = System.Diagnostics.Process.GetProcessesByName("DbInRun");
            if(pps!=null && pps.Length > 0)
            {
                foreach(var p in pps)
                {
                    if(string.Compare(p.MainWindowTitle, "DbInRun-" + database,true)==0)
                    {
                        isdbrun = true;
                        return true;
                    }
                }
                isdbrun = true;
            }
            else
            {
                isdbrun = false;
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> ListDatabse()
        {
            return mDatabaseItems.Keys;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public bool ReStartDatabase(string database)
        {
           return ReLoadDatabase(database);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool RunDatabase(string database)
        {
            return StartDb(database);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public bool StopDatabase(string database)
        {
            return StopDatabaseInner(database);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        private  bool StartDb(string name)
        {
            try
            {
                if (CheckStart(name)) return false;

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    var info = new ProcessStartInfo() { FileName = "DBInRun.exe" };
                    info.UseShellExecute = true;
                    info.Arguments = "start " + name;
                    info.WorkingDirectory = System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location);
                    Process.Start(info).WaitForExit(1000);
                }
                else
                {
                    var info = new ProcessStartInfo() { FileName = "dotnet" };
                    info.UseShellExecute = true;
                    info.CreateNoWindow = false;
                    info.Arguments = "./DBInRun.dll start " + name;
                    info.WorkingDirectory = System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location);
                    Process.Start(info).WaitForExit(1000);
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public bool ReLoadDatabase(string name)
        {
            lock (mLockObj)
            {
                using (var client = new NamedPipeClientStream(".", name, PipeDirection.InOut))
                {
                    try
                    {
                        client.Connect(2000);
                        client.WriteByte(1);
                        client.FlushAsync();

                        var res = 0;
                        if (OperatingSystem.IsWindows())
                        {
                            client.WaitForPipeDrain();
                            res = client.ReadByte();
                        }
                        else
                        {
                            int count = 0;
                            res = client.ReadByte();
                            while (res == -1)
                            {
                                res = client.ReadByte();
                                count++;
                                if (count > 20) break;
                                Thread.Sleep(100);
                            }
                        }
                        if (res == 1)
                        {
                            return true;
                        }
                        

                    }
                    catch (Exception)
                    {
                    }
                    return false;
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        private  bool StopDatabaseInner(string name)
        {
            using (var client = new NamedPipeClientStream(".", name, PipeDirection.InOut))
            {
                try
                {
                    client.Connect(2000);
                    client.WriteByte(0);
                    client.FlushAsync();

                    if (OperatingSystem.IsWindows())
                    {
                        client.WaitForPipeDrain();
                    }

                    if (client.IsConnected)
                    {
                        var res = client.ReadByte();
                        int count = 0;
                        while (res == -1)
                        {
                            res = client.ReadByte();
                            count++;
                            if (count > 20) break;
                            Thread.Sleep(100);
                        }
                        if (res == 1)
                        {
                            return true;
                        }
                    }
                }
                catch (Exception ex)
                {
                    
                }
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datebase"></param>
        /// <exception cref="NotImplementedException"></exception>
        public IEnumerable<string> EnumLogType(string datebase)
        {
            return new List<string>() { "DBInRun", "DBHighApi", "DBGrpcApi", "DbWebApi", "DbOpcServer", "DBInStudioServer", "Ant", "Spider" };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="type"></param>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public IEnumerable<string> ReadLogs(string database, string type, DateTime starttime, DateTime endtime)
        {
            if (type == "DBInRun" || type == "DBHighApi" || type == "DBGrpcApi")
            {
                if (mDatabaseItems.ContainsKey(database))
                {
                    return mDatabaseItems[database].ReadLog(type, starttime, endtime);
                }
            }
            else if(type== "DbWebApi")
            {
                return ReadWebApiServer(database, starttime, endtime);
            }
            else if (type == "DbOpcServer")
            {
                return ReadDbOpcServer(database, starttime, endtime);
            }
            else if(type== "Ant")
            {

            }
            else if(type== "Spider")
            {

            }
            else if(type== "DBInStudioServer")
            {
                return ReadStudioServer(starttime, endtime);
            }
            return new List<string>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <returns></returns>
        private IEnumerable<string> ReadStudioServer(DateTime starttime,DateTime endtime)
        {
            List<string> log = new List<string>();
            DateTime start = starttime.Date;
            while (start <= endtime)
            {
                string sfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "Log", "DBInStudioServer_" + start.ToString("yyyyMMdd") + ".txt");
                log.AddRange(sfile.ReadLogs(starttime, endtime));
                start = start.AddDays(1);
            }
            return log;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <returns></returns>
        private IEnumerable<string> ReadWebApiServer(string database,DateTime starttime, DateTime endtime)
        {
            List<string> log = new List<string>();
            DateTime start = starttime.Date;
            while (start <= endtime)
            {
                string sfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "DbWebApi","Data",database, "Log", "DbWebApi_" + start.ToString("yyyyMMdd") + ".txt");
                log.AddRange(sfile.ReadLogs(starttime, endtime));
                start = start.AddDays(1);
            }
            return log;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <returns></returns>
        private IEnumerable<string> ReadDbOpcServer(string database, DateTime starttime, DateTime endtime)
        {
            List<string> log = new List<string>();
            DateTime start = starttime.Date;
            while (start <= endtime)
            {
                string sfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "DbOpcServer", "Data", database, "Log", "DbWebApi_" + start.ToString("yyyyMMdd") + ".txt");
                log.AddRange(sfile.ReadLogs(starttime, endtime));
                start = start.AddDays(1);
            }
            return log;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public bool CheckDatabaseUser(string database,string username, string password)
        {
            if(mDatabaseItems.ContainsKey(database))
            {
                return mDatabaseItems[database].CheckUser(username,password);
            }
            return false;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class DatabaseItem
    {
        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Path { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string HisDataPath { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string BackHisDataPath { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsRunning { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool EnableWebAPI { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public int WebAPIPort { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int OPCUAServerPort { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool EnableGRPCAPI { get; set; }
        
        /// <summary>
        /// 
        /// </summary>
        public int GRPCPort { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool EnableHighApi { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int HighPort { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Tuple<int,int> SpiderPortRange { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Tuple<int, int> DirectAccessDriverPortRange { get; set; }

        public void Init()
        {
            try
            {
                string sfile = System.IO.Path.Combine(Path, this.Name + ".hdb");
                if (File.Exists(sfile))
                {
                    var fs = File.Open(sfile, FileMode.Open);
                    using (var sr = new StreamReader(fs))
                    {
                        while (!sr.EndOfStream)
                        {
                            var vss = sr.ReadLine();
                            if (!string.IsNullOrEmpty(vss) && vss.Contains("HisSetting"))
                            {
                                XElement xx = XElement.Parse(vss);
                                if (xx.Attribute("HisDataPathBack") != null)
                                {
                                    this.BackHisDataPath = xx.Attribute("HisDataPathBack").Value;
                                }
                                else
                                {
                                    this.BackHisDataPath = "";
                                }

                                if (xx.Attribute("HisDataPath") != null)
                                {
                                    this.HisDataPath = xx.Attribute("HisDataPath").Value;
                                }
                                else
                                {
                                    this.HisDataPath = System.IO.Path.Combine(Path, "HisData");
                                }
                                break;
                            }
                            
                        }
                    }
                    fs.Close();
                }
            }
            catch
            {

            }

            try {
                 string sfile = System.IO.Path.Combine(Path, this.Name + ".db");
                if(System.IO.File.Exists(sfile))
                {
                    XElement fs = XElement.Load(sfile);
                    var ss = fs.Element("Setting");
                    if (ss != null)
                    {
                        if(ss.Attribute("EnableWebApi")!=null)
                        this.EnableWebAPI = bool.Parse(ss.Attribute("EnableWebApi").Value);
                        if (ss.Attribute("EnableGrpcApi") != null)
                            this.EnableGRPCAPI = bool.Parse(ss.Attribute("EnableGrpcApi").Value);
                        if (ss.Attribute("EnableHighApi") != null)
                            this.EnableHighApi = bool.Parse(ss.Attribute("EnableHighApi").Value);
                    }
                }
            }
            catch
            {

            }

            string basepath = System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location);

            string configpath = System.IO.Path.Combine(basepath, "Config");
            //if (this.EnableGRPCAPI)
            {
               string sfile = System.IO.Path.Combine(configpath, "DBGrpcApi.cfg");
                if(System.IO.File.Exists (sfile))
                {
                    XElement xx = XElement.Load(sfile);
                    if(xx.Attribute("ServerPort")!=null)
                    {
                        GRPCPort = int.Parse(xx.Attribute("ServerPort").Value);
                    }
                }
            }

            //if (this.EnableHighApi)
            {
                string sfile = System.IO.Path.Combine(configpath, "DBHighApi.cfg");
                if (System.IO.File.Exists(sfile))
                {
                    XElement xx = XElement.Load(sfile);
                    if (xx.Attribute("ServerPort") != null)
                    {
                        HighPort = int.Parse(xx.Attribute("ServerPort").Value);
                    }
                }
            }

            //if (this.EnableWebAPI)
            {
                configpath = System.IO.Path.Combine(basepath, "DbWebApi", "Config");
                string sfile = System.IO.Path.Combine(configpath, "DbWebApi.cfg");
                if (System.IO.File.Exists(sfile))
                {
                    XElement xx = XElement.Load(sfile);
                    if (xx.Attribute("ServerPort") != null)
                    {
                        WebAPIPort = int.Parse(xx.Attribute("ServerPort").Value);
                    }
                }
            }

            {
                configpath = System.IO.Path.Combine(basepath, "DbOpcServer", "Config");
                var sfile = System.IO.Path.Combine(configpath, "DBOpcServer.cfg");
                if (System.IO.File.Exists(sfile))
                {
                    XElement xx = XElement.Load(sfile);
                    if (xx.Attribute("ServerPort") != null)
                    {
                        OPCUAServerPort = int.Parse(xx.Attribute("ServerPort").Value);
                    }
                }
            }

            InitDriver();
        }

        private void InitDriver()
        {
            string sfile = System.IO.Path.Combine(Path, "SpiderDriver.cfg");
            if (System.IO.File.Exists(sfile))
            {
                XElement xx = XElement.Load(sfile);
                int sport = int.Parse(xx.Element("Server").Attribute("StartPort").Value);
                int eport = int.Parse(xx.Element("Server").Attribute("EndPort").Value);
                SpiderPortRange = new Tuple<int, int>(sport, eport);
            }
            else
            {
                var configpath = System.IO.Path.Combine(new DirectoryInfo(this.Path).Parent.FullName, "Config");
                sfile = System.IO.Path.Combine(configpath, "SpiderDriver.cfg");
                if (System.IO.File.Exists(sfile))
                {
                    XElement xx = XElement.Load(sfile);
                    int sport = int.Parse(xx.Element("Server").Attribute("StartPort").Value);
                    int eport = int.Parse(xx.Element("Server").Attribute("EndPort").Value);
                    SpiderPortRange = new Tuple<int, int>(sport, eport);
                }
            }


            sfile = System.IO.Path.Combine(Path, "DirectAccessDriver.cfg");
            if (System.IO.File.Exists(sfile))
            {
                XElement xx = XElement.Load(sfile);
                int sport = int.Parse(xx.Element("Server").Attribute("StartPort").Value);
                int eport = int.Parse(xx.Element("Server").Attribute("EndPort").Value);
                DirectAccessDriverPortRange = new Tuple<int, int>(sport, eport);
            }
            else
            {
                var configpath = System.IO.Path.Combine(new DirectoryInfo(this.Path).Parent.FullName, "Config");
                sfile = System.IO.Path.Combine(Path, "DirectAccessDriver.cfg");
                if (System.IO.File.Exists(sfile))
                {
                    XElement xx = XElement.Load(sfile);
                    int sport = int.Parse(xx.Element("Server").Attribute("StartPort").Value);
                    int eport = int.Parse(xx.Element("Server").Attribute("EndPort").Value);
                    DirectAccessDriverPortRange = new Tuple<int, int>(sport, eport);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <returns></returns>
        public IEnumerable<string> ReadLog(string type, DateTime starttime, DateTime endtime)
        {
            List<string> log = new List<string>();
            DateTime start = starttime.Date;
            while(start<=endtime)
            {
                string sfile = System.IO.Path.Combine(Path, "Log",type+"_"+start.ToString("yyyyMMdd")+".txt");
                log.AddRange(sfile.ReadLogs(starttime,endtime));
                start =start.AddDays(1);
            }
            return log;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="usrname"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public bool CheckUser(string usrname,string password)
        {
            string sfile = System.IO.Path.Combine(Path, this.Name + ".sdb");
            if(System.IO.File.Exists(sfile))
            {
                var doc = new SecuritySerise().Load(sfile);
                var user = doc.User.GetUser(usrname);
                if(user != null)
                {
                    if(user.Password==password)
                    {
                        if(user.SuperUser)
                        {
                            return true;
                        }
                        else
                        {
                            foreach (var vpp in user.Permissions)
                            {
                                if (!string.IsNullOrEmpty(vpp))
                                {
                                    if (doc.Permission.Permissions[vpp].SuperPermission)
                                    {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return false;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public static class LogFileExtend
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="startime"></param>
        /// <param name="endtime"></param>
        /// <returns></returns>
        public static IEnumerable<string> ReadLogs(this string file,DateTime startime,DateTime endtime)
        {
            List<string> re = new List<string>();
            if (System.IO.File.Exists(file))
            {
                using (var fs = new System.IO.StreamReader(System.IO.File.Open(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!fs.EndOfStream)
                    {
                        var vss = fs.ReadLine();
                        if (!string.IsNullOrEmpty(vss))
                        {
                            if (DateTime.TryParse(vss.Substring(0, 23), out DateTime dt))
                            {
                                if (dt >= startime && dt <= endtime)
                                    re.Add(vss);
                            }
                        }
                    }
                }
            }
            return re;
        }
    }
}
