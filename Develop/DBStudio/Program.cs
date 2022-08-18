using Cdy.Tag;
using System;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using System.IO.Pipes;
using System.Threading;
using Microsoft.Extensions.Logging;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Reflection;
using System.Xml.Linq;
using DBDevelopService;

namespace DBStudio
{
    class Program:DBDevelopService.IDatabaseManager
    {
        /// <summary>
        /// 
        /// </summary>
        static bool mIsExited = false;

        static object mLockObj = new object();

        /// <summary>
        /// 
        /// </summary>
        public class Config
        {

            #region ... Variables  ...

            /// <summary>
            /// 
            /// </summary>
            public static Config Instance = new Config();

            #endregion ...Variables...

            #region ... Events     ...

            #endregion ...Events...

            #region ... Constructor...


            #endregion ...Constructor...

            #region ... Properties ...

            /// <summary>
            /// 
            /// </summary>
            public bool IsWebApiEnable { get; set; }

            /// <summary>
            /// 
            /// </summary>
            public int WebApiPort { get; set; } = 9000;

            /// <summary>
            /// 
            /// </summary>
            public bool IsGrpcEnable { get; set; } = true;

            /// <summary>
            /// 
            /// </summary>
            public int GrpcPort { get; set; } = 5001;

            #endregion ...Properties...

            #region ... Methods    ...

            /// <summary>
            /// 
            /// </summary>
            public void Load()
            {
                try
                {
                    string spath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "DbInStudioServer.cfg");

                    LoggerService.Service.Info("Config", "Start to load config");

                    if (System.IO.File.Exists(spath))
                    {
                        var xxx = XElement.Load(spath);
                        this.IsWebApiEnable = bool.Parse(xxx.Attribute("IsWebApiEnable")?.Value);
                        this.IsGrpcEnable = bool.Parse(xxx.Attribute("IsGrpcEnable")?.Value);
                        this.WebApiPort = int.Parse(xxx.Attribute("WebApiPort")?.Value);
                        this.GrpcPort = int.Parse(xxx.Attribute("GrpcPort")?.Value);
                    }
                }
                catch(Exception ex)
                {
                    LoggerService.Service.Erro("Config", ex.Message);
                }
            }

            #endregion ...Methods...

            #region ... Interfaces ...

            #endregion ...Interfaces...
        }


        static void Main(string[] args)
        {
            LogoHelper.Print();

            Console.Title = "DBInStudioServer";

            //注册日志
            ServiceLocator.Locator.Registor<ILog>(new ConsoleLogger());

            Program pg = new Program();
            Config.Instance.Load();
            ServiceLocator.Locator.Registor(typeof(DBDevelopService.IDatabaseManager), pg);

            ValueConvertManager.manager.Init();

            if (!DBDevelopService.DbManager.Instance.IsLoaded)
                DBDevelopService.DbManager.Instance.PartLoad();

            int port = Config.Instance.GrpcPort;
            int webPort = Config.Instance.WebApiPort;

            bool isNeedMinMode = false;

            if (args.Length > 0)
            {
                if (args[0] == "/m")
                {
                    isNeedMinMode = true;
                }
                else
                {
                    port = int.Parse(args[0]);
                }
            }

            if (args.Length > 1)
            {
                webPort = int.Parse(args[1]);
            }
            WindowConsolHelper.DisbleQuickEditMode();

            if (isNeedMinMode)
            {
                WindowConsolHelper.MinWindow("DBInStudioServer");
            }

            Console.CancelKeyPress += Console_CancelKeyPress;

            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;

            
            DBDevelopService.Service.Instanse.Start(port, webPort,Config.Instance.IsGrpcEnable,Config.Instance.IsWebApiEnable);

            Thread.Sleep(100);

            OutByLine("", Res.Get("HelpMsg"));
            while (!mIsExited)
            {
                OutInLine("", "");
                var vv = Console.ReadLine();
                
                if (vv != null)
                {
                    string[] cmd = vv.Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);
                    if (cmd.Length == 0) continue;

                    string cmsg = cmd[0].ToLower();

                    if (cmsg == "exit")
                    {
                        OutByLine("", Res.Get("AppExitHlp"));
                        cmd = Console.ReadLine().Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);
                        if (cmd.Length == 0) continue;
                        if (cmd[0].ToLower() == "y")
                            break;
                    }
                    else if (cmsg == "db")
                    {
                        if (cmd.Length > 1)
                            ProcessDatabaseCreate(cmd[1]);
                    }
                    else if (cmsg == "list")
                    {
                        ListDatabase();
                    }
                    else if (cmsg == "h")
                    {
                        OutByLine("", GetHelpString());
                    }
                    else if (cmsg == "**")
                    {
                        LogoHelper.PrintAuthor();
                    }
                }
            }
            DBDevelopService.Service.Instanse.Stop();

        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            LoggerService.Service.Erro("GrpcDBService", e.ExceptionObject.ToString());
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            mIsExited = true;
            e.Cancel = true;
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(Res.Get("AnyKeyToExit"));
            Console.ResetColor();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="prechar"></param>
        /// <param name="msg"></param>
        private static void OutByLine(string prechar, string msg)
        {
            Console.WriteLine(prechar + ">" + msg);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="prechar"></param>
        /// <param name="msg"></param>
        private static void OutInLine(string prechar, string msg)
        {
            Console.Write(prechar + ">" + msg);
        }

        /// <summary>
        /// 
        /// </summary>
        private static void ListDatabase()
        {
            //if (!DBDevelopService.DbManager.Instance.IsLoaded)
            //    DBDevelopService.DbManager.Instance.PartLoad();

            StringBuilder sb = new StringBuilder();
            foreach (var vdd in DBDevelopService.DbManager.Instance.ListDatabase())
            {
                sb.Append(vdd + ",");
            }
            sb.Length = sb.Length > 1 ? sb.Length - 1 : sb.Length;
            OutByLine("", sb.ToString());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="msg"></param>
        private static void ProcessDatabaseCommand(Database db, string msg)
        {
            try
            {
                string[] cmd = msg.Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);
                string cmsg = cmd[0].ToLower();
                if (cmsg == "save")
                {
                    new DatabaseSerise() { Dbase = db }.Save();
                    DbManager.Instance.ReLoad(db.Name);
                }
                else if (cmsg == "add")
                {
                    AddTag(db, cmd[1].ToLower(), cmd[2], cmd[3], int.Parse(cmd[4]));
                }
                else if (cmsg == "remove")
                {
                    RemoveTag(db, cmd[1].ToLower());
                }
                else if (cmsg == "update")
                {
                    UpdateTag(db, cmd[1].ToLower(), cmd[2], cmd[3]);
                }
                else if (cmsg == "updatehis")
                {
                    UpdateHisTag(db, cmd[1].ToLower(), cmd[2], cmd[3]);
                }
                else if (cmsg == "import")
                {
                    Import(db, cmd[1].ToLower());
                }
            }
            catch
            {

            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="prechar"></param>
        /// <param name="description"></param>
        /// <param name="waitstring"></param>
        /// <returns></returns>
        private static bool WaitForInput(string prechar,string description, string waitstring)
        {
            OutByLine(prechar, description);
            var cmd = Console.ReadLine().Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);
            if (cmd.Length > 0 && string.Compare(waitstring,cmd[0],true)==0) return true;
            else return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        private static void ProcessDatabaseCreate(string name)
        {
            Database db = DBDevelopService.DbManager.Instance.GetDatabase(name);

            if (db == null)
            {
                if(WaitForInput("",string.Format(Res.Get("NewDatabase"),name),"y"))
                {
                    db = DbManager.Instance.NewDB(name, name);
                }
                else
                {
                    return;
                }
            }
            else
            {
                DBDevelopService.DbManager.Instance.CheckAndContinueLoadDatabase(db);
            }

            OutByLine(name, Res.Get("HelpMsg"));
            while (true)
            {
                OutInLine(name, "");
                string[] cmd = Console.ReadLine().Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);
                if (cmd.Length == 0) continue;
                string cmsg = cmd[0].ToLower();
                try
                {
                    if (cmsg == "save")
                    {
                        new DatabaseSerise() { Dbase = db }.Save();
                        //DBDevelopService.DbManager.Instance.AddDatabase(db.Name, db);
                    }
                    else if (cmsg == "add")
                    {
                        AddTag(db, cmd[1].ToLower(), cmd[2], cmd[3], int.Parse(cmd[4]));
                    }
                    else if (cmsg == "remove")
                    {
                        RemoveTag(db, cmd[1].ToLower());
                    }
                    else if (cmsg == "clear")
                    {
                        ClearTag(db);
                    }
                    else if (cmsg == "update")
                    {
                        UpdateTag(db, cmd[1].ToLower(), cmd[2], cmd[3]);
                        UpdateHisTag(db, cmd[1].ToLower(), cmd[2], cmd[3]);
                    }
                    else if (cmsg == "start")
                    {
                        if (!CheckStart(db.Name))
                        {
                            StartDb(db.Name);
                        }
                        else
                        {
                            Console.WriteLine(string.Format(Res.Get("databaseinrunningHlp"), db.Name));
                        }
                    }
                    //else if (cmsg == "rerun")
                    else if (cmsg == "restart")
                    {
                        if (!CheckStart(db.Name))
                        {
                            StartDb(db.Name);
                        }
                        else
                        {
                            ReLoadDatabase(db.Name);
                        }
                    }
                    //else if (cmsg == "restart")
                    //{
                    //    StopDatabase(db.Name);
                    //    while (CheckStart(db.Name)) Thread.Sleep(100);
                    //    StartDb(db.Name);
                    //}
                    else if (cmsg == "isstarted")
                    {
                        if(CheckStart(db.Name))
                        {
                            Console.WriteLine("database "+db.Name+" is start.");
                        }
                        else
                        {
                            Console.WriteLine("database " + db.Name + " is stop.");
                        }
                    }
                    else if (cmsg == "stop")
                    {
                        StopDatabase(db.Name);
                    }
                    //else if (cmsg == "updatehis")
                    //{
                    //    UpdateHisTag(db, cmd[1].ToLower(), cmd[2], cmd[3]);
                    //}
                    else if (cmsg == "import")
                    {
                        if (cmd.Length > 1)
                            Import(db, cmd[1].ToLower());
                        else
                        {
                            Import(db, name + ".csv");
                        }
                    }
                    else if (cmsg == "export")
                    {
                        if (cmd.Length > 1)
                            ExportToCSV(db, cmd[1].ToLower());
                        else
                        {
                            ExportToCSV(db, name + ".csv");
                        }
                    }
                    else if (cmsg == "list")
                    {
                        string ctype = cmd.Length > 1 ? cmd[1] : "";
                        ListDatabase(db, ctype);
                    }
                    else if (cmsg == "h")
                    {
                        if (cmd.Length == 1)
                        {
                            Console.WriteLine(GetDbManagerHelpString());
                        }
                    }
                    else if (cmsg == "sp")
                    {
                        Sp(db,int.Parse(cmd[1]),int.Parse(cmd[2]),int.Parse(cmd[3]), cmd.Skip(4).ToArray());
                    }
                    else if (cmsg == "exit")
                    {
                        break;
                    }
                }
                catch(Exception ex)
                {
                    OutByLine(name, Res.Get("ErroParameter")+" " +ex.Message);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        private static bool StartDb(string name)
        {
            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    var info = new ProcessStartInfo() { FileName = "DBInRun.exe" };
                    info.UseShellExecute = true;
                    info.Arguments = "start " + name;
                    info.WorkingDirectory = System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location);
                    Process.Start(info).WaitForExit(1000);
                }
                else
                {
                    var info = new ProcessStartInfo() { FileName = "dotnet" };
                    info.UseShellExecute = true;
                    info.CreateNoWindow = false;
                    info.Arguments = "./DBInRun.dll start " + name;
                    info.WorkingDirectory = System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location);
                    Process.Start(info).WaitForExit(1000);
                }


                Console.WriteLine(string.Format(Res.Get("StartdatabaseSucessful"), name));
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="headname"></param>
        /// <param name="group"></param>
        /// <param name="startId"></param>
        /// <param name="realDatabase"></param>
        /// <returns></returns>
        private static string GetAvaiableName(string headname,string group,ref int startId,RealDatabase realDatabase)
        {
            for(int i=startId;i<int.MaxValue;i++)
            {
                string sname = group +"."+ headname + i;
                if(!realDatabase.NamedTags.ContainsKey(sname))
                {
                    startId = i+1;
                    return headname + i;
                }
            }
            return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="paras"></param>
        private static void Sp(Database db,int rtp,int ctp,int addressType, params string[] paras)
        {
            Cdy.Tag.RealDatabase test = db.RealDatabase;
          
            Cdy.Tag.HisDatabase htest = db.HisDatabase;
            
            Cdy.Tag.RecordType rrtp = (RecordType)(rtp);

            int idstart = 0;

            string address = "";
            if (paras.Length > 0)
            {
                int dcount = int.Parse(paras[0]);
                for (int i = 0; i < dcount; i++)
                {
                    if(addressType==0)
                    {
                        address = "Spider:";
                    }
                    else
                    {
                        if (i % 3 == 0)
                        {
                            address = "Sim:sin";
                        }
                        else if (i % 3 == 1)
                        {
                            address = "Sim:cos";
                        }
                        else
                        {
                            address = "Sim:step";
                        }
                    }
                    var vtag = new Cdy.Tag.DoubleTag() { Name = GetAvaiableName("Double", "Double",ref idstart,test), Group = "Double", LinkAddress = address };
                    test.Append(vtag);
                    htest.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = Cdy.Tag.TagType.Double, Circle = 1000, Type = rrtp, CompressType = ctp });
                }
            }

            if (paras.Length > 1)
            {
                int fcount = int.Parse(paras[1]);
                for (int i = 0; i < fcount; i++)
                {
                    if (addressType == 0)
                    {
                        address = "Spider:";
                    }
                    else
                    {
                        if (i % 3 == 0)
                        {
                            address = "Sim:sin";
                        }
                        else if (i % 3 == 1)
                        {
                            address = "Sim:cos";
                        }
                        else
                        {
                            address = "Sim:step";
                        }
                    }
                    var vtag = new Cdy.Tag.FloatTag() { Name = GetAvaiableName("Float", "Float", ref idstart, test), Group = "Float", LinkAddress = address };
                    test.Append(vtag);
                    htest.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = Cdy.Tag.TagType.Float, Circle = 1000, Type = rrtp, CompressType = ctp });
                }
            }

            if (paras.Length > 2)
            {
                int fcount = int.Parse(paras[2]);
                if (addressType == 0)
                {
                    address = "Spider:";
                }
                else
                {
                    address = "Sim:step";
                }
                for (int i = 0; i < fcount; i++)
                {
                    var vtag = new Cdy.Tag.LongTag() { Name = GetAvaiableName("Long", "Long", ref idstart, test), Group = "Long", LinkAddress = address };
                    test.Append(vtag);
                    htest.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = Cdy.Tag.TagType.Long, Circle = 1000, Type = rrtp, CompressType = ctp });
                }
            }

            if (paras.Length > 3)
            {
                if (addressType == 0)
                {
                    address = "Spider:";
                }
                else
                {
                    address = "Sim:step";
                }
                int fcount = int.Parse(paras[3]);
                for (int i = 0; i < fcount; i++)
                {
                    var vtag = new Cdy.Tag.IntTag() { Name = GetAvaiableName("Int", "Int", ref idstart, test), Group = "Int", LinkAddress = address };
                    test.Append(vtag);
                    htest.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = Cdy.Tag.TagType.Int, Circle = 1000, Type = rrtp, CompressType = ctp });
                }
            }

            if (paras.Length > 4)
            {
                if (addressType == 0)
                {
                    address = "Spider:";
                }
                else
                {
                    address = "Sim:square";
                }
                int fcount = int.Parse(paras[4]);
                for (int i = 0; i < fcount; i++)
                {
                    var vtag = new Cdy.Tag.BoolTag() { Name = GetAvaiableName("Bool", "Bool", ref idstart, test), Group = "Bool", LinkAddress = address };
                    test.Append(vtag);
                    htest.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = Cdy.Tag.TagType.Bool, Circle = 1000, Type = rrtp, CompressType = ctp });
                }
            }


            if (paras.Length > 5)
            {
                if (addressType == 0)
                {
                    address = "Spider:";
                }
                else
                {
                    address = "Sim:steppoint";
                }
                int fcount = int.Parse(paras[5]);
                for (int i = 0; i < fcount; i++)
                {
                    var vtag = new Cdy.Tag.IntPointTag() { Name = GetAvaiableName("IntPoint", "IntPoint", ref idstart, test), Group = "IntPoint", LinkAddress = address };
                    test.Append(vtag);
                    htest.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = Cdy.Tag.TagType.IntPoint, Circle = 1000, Type = rrtp, CompressType = ctp });
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private static string GetDbManagerHelpString()
        {
            string str = "{0,-10} {1,-50} {2}";
            StringBuilder re = new StringBuilder();
            re.AppendLine();
            re.AppendLine(string.Format(str, "save","","// "+Res.Get("SaveDatabaseHlp")));
            re.AppendLine(string.Format(str, "start","","// " + Res.Get("StartDatabaseHlp")));
            re.AppendLine(string.Format(str, "restart","","// " + Res.Get("ReStartDatabaseHlp")));
            re.AppendLine(string.Format(str, "stop","","// " + Res.Get("StopDatabaseHlp")));
            re.AppendLine(string.Format(str, "add","[tagtype] [tagname] [linkaddress] [repeat]","// ") + Res.Get("AddTagHlp"));
            re.AppendLine(string.Format(str, "remove","[tagname]","// ") + Res.Get("AddTagHlp"));
            re.AppendLine(string.Format(str, "clear","","// " + Res.Get("ClearTagHlp")));
            re.AppendLine(string.Format(str, "update","[tagname] [propertyname] [propertyvalue]","// " + Res.Get("UpdateTagHlp")));
            //re.AppendLine(string.Format(str, "updatehis","[tagname] [propertyname] [propertyvalue]","// update value of a poperty in a tag's his config"));
            re.AppendLine(string.Format(str, "import","[filename]","// " + Res.Get("ImportHlp")));
            re.AppendLine(string.Format(str, "export","[filename]","// " + Res.Get("ExportHlp")));
            re.AppendLine(string.Format(str, "list","[tagtype]","// " + Res.Get("ListTagHlp")));
            re.AppendLine(string.Format(str, "exit", "", "// " + Res.Get("ExitDatabaseHlp")));
            re.AppendLine(string.Format(str, "sp","[recordType] [compressType] [enable sim address][double tag number] [float tag number] [long tag number] [int tag number] [bool tag number] [intpoint tag number]"," //" + Res.Get("QuickGeneraterTagHlp")));
            

            return re.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="type"></param>
        private static void ListDatabase(Database database, string type="")
        {
            if(!string.IsNullOrEmpty(type))
            {
                int count = database.RealDatabase.Tags.Values.Where(e => e.Type == (TagType)Enum.Parse(typeof(TagType),type)).Count();
                OutByLine(database.Name,string.Format(Res.Get("TagMsg"), count, type));
            }
            else
            {
                foreach (TagType vv in Enum.GetValues(typeof(TagType)))
                {
                    int count = database.RealDatabase.Tags.Values.Where(e => e.Type == vv).Count();
                    OutByLine(database.Name, string.Format(Res.Get("TagMsg"), count, vv.ToString()));
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="file"></param>
        private static void ExportToCSV(Database database, string file)
        {
            string sfile = file;

            if (!System.IO.Path.IsPathRooted(sfile))
            {
                sfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location), sfile);
            }

            var stream = new StreamWriter(File.Open(sfile, FileMode.Create, FileAccess.ReadWrite));
            foreach(var vv in database.RealDatabase.Tags)
            {
                if(database.HisDatabase.HisTags.ContainsKey(vv.Key))
                {
                    stream.WriteLine(SaveToCSVString(vv.Value, database.HisDatabase.HisTags[vv.Key]));
                }
                else
                {
                    stream.WriteLine(SaveToCSVString(vv.Value, null));

                }
            }
            stream.Close();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mRealTagMode"></param>
        /// <param name="mHisTagMode"></param>
        /// <returns></returns>
        public static string SaveToCSVString(Tagbase mRealTagMode,HisTag mHisTagMode)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(mRealTagMode.Id + ",");
            sb.Append(mRealTagMode.Name + ",");
            sb.Append(mRealTagMode.Desc + ",");
            sb.Append(mRealTagMode.Group + ",");
            sb.Append(mRealTagMode.Type + ",");
            sb.Append(mRealTagMode.LinkAddress + ",");
            sb.Append((int)mRealTagMode.ReadWriteType + ",");
            if (mRealTagMode.Conveter != null)
                sb.Append(mRealTagMode.Conveter.SeriseToString() + ",");
            else
            {
                sb.Append(",");
            }
            if (mRealTagMode is NumberTagBase)
            {
                sb.Append((mRealTagMode as NumberTagBase).MaxValue.ToString() + ",");
                sb.Append((mRealTagMode as NumberTagBase).MinValue.ToString() + ",");
            }
            else
            {
                sb.Append(",");
                sb.Append(",");
            }
            if (mRealTagMode is FloatingTagBase)
            {
                sb.Append((mRealTagMode as FloatingTagBase).Precision + ",");
            }
            else
            {
                sb.Append(",");
            }
            sb.Append(mRealTagMode.Parent + ",");

            if (mRealTagMode is ComplexTag)
            {
                sb.Append((mRealTagMode as ComplexTag).LinkComplexClass + ",");
            }
            else
            {
                sb.Append(",");
            }

            if (mHisTagMode != null)
            {
                sb.Append(mHisTagMode.Type + ",");
                sb.Append(mHisTagMode.Circle + ",");
                sb.Append(mHisTagMode.CompressType + ",");
                sb.Append(mHisTagMode.MaxValueCountPerSecond + ",");
                if (mHisTagMode.Parameters != null)
                {
                    foreach (var vv in mHisTagMode.Parameters)
                    {
                        sb.Append(vv.Key + ",");
                        sb.Append(vv.Value + ",");
                    }
                }
            }
            sb.Length = sb.Length > 0 ? sb.Length - 1 : sb.Length;
            return sb.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static Tuple<Tagbase,HisTag> LoadFromCSVString(string val)
        {
            string[] stmp = val.Split(new char[] { ',' });
            Cdy.Tag.TagType tp = (Cdy.Tag.TagType)Enum.Parse(typeof(Cdy.Tag.TagType), stmp[4]);
            var realtag = TagTypeExtends.GetTag(tp);

            realtag.Id = int.Parse(stmp[0]);
            realtag.Name = stmp[1];
            realtag.Desc = stmp[2];
            realtag.Group = stmp[3];
            realtag.LinkAddress = stmp[5];
            realtag.ReadWriteType = (ReadWriteMode)(int.Parse(stmp[6]));
            if (stmp[7] != null)
            {
                realtag.Conveter = stmp[7].DeSeriseToValueConvert();
            }

            if (realtag is NumberTagBase)
            {
                (realtag as NumberTagBase).MaxValue = double.Parse(stmp[8], System.Globalization.NumberStyles.Any);
                (realtag as NumberTagBase).MinValue = double.Parse(stmp[9], System.Globalization.NumberStyles.Any);
            }

            if (realtag is FloatingTagBase)
            {
                (realtag as FloatingTagBase).Precision = byte.Parse(stmp[10]);
            }
            realtag.Parent = stmp[11];

            if (realtag is ComplexTag)
            {
                (realtag as ComplexTag).LinkComplexClass = stmp[12];
            }

            if (stmp.Length > 13)
            {
                Cdy.Tag.HisTag histag = new HisTag();
                histag.Type = (Cdy.Tag.RecordType)Enum.Parse(typeof(Cdy.Tag.RecordType), stmp[13]);

                histag.Circle = int.Parse(stmp[14]);
                histag.CompressType = int.Parse(stmp[15]);
                histag.Parameters = new Dictionary<string, double>();
                histag.TagType = realtag.Type;
                histag.Id = realtag.Id;
                histag.MaxValueCountPerSecond = short.Parse(stmp[16]);

                for (int i = 17; i < stmp.Length; i++)
                {
                    string skey = stmp[i];
                    if (string.IsNullOrEmpty(skey))
                    {
                        break;
                    }
                    double dval = double.Parse(stmp[i + 1]);

                    if (!histag.Parameters.ContainsKey(skey))
                    {
                        histag.Parameters.Add(skey, dval);
                    }

                    i++;
                }
                return new Tuple<Tagbase, HisTag>(realtag, histag);
            }

            return new Tuple<Tagbase, HisTag>(realtag, null);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="file"></param>
        private static void Import(Database database, string file)
        {
            string sfile = file;

            if (!System.IO.Path.IsPathRooted(sfile))
            {
                sfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location), sfile);
            }
            if (System.IO.File.Exists(sfile))
            {
                var reader = new System.IO.StreamReader(System.IO.File.Open(sfile, System.IO.FileMode.Open));
                while (reader.Peek() > 0)
                {
                    var cmd = reader.ReadLine();
                    if (sfile.EndsWith(".cmd"))
                    {
                        ProcessDatabaseCommand(database, cmd);
                    }
                    else if (sfile.EndsWith(".csv"))
                    {
                        var vres = LoadFromCSVString(cmd);
                        database.RealDatabase.AddOrUpdate(vres.Item1);
                        if(vres.Item2!=null)
                        {
                            database.HisDatabase.AddOrUpdate(vres.Item2);
                        }
                    }
                   
                }
            }

            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tag"></param>
        /// <param name="parameterName"></param>
        /// <param name="value"></param>
        private static void UpdateTag(Database database, string tag, string parameterName, string value)
        {
            var vv = database.RealDatabase.GetTagByName(tag);
            if (vv != null)
            {
                UpdateTagParameter(vv, parameterName, value);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="parameterName"></param>
        /// <param name="value"></param>
        private static void UpdateTagParameter(Tagbase tag, string parameterName, string value)
        {
            switch (parameterName)
            {
                case "linkaddress":
                    tag.LinkAddress = value;
                    break;
                case "group":
                    tag.Group = value;
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tag"></param>
        /// <param name="parameterName"></param>
        /// <param name="value"></param>
        private static void UpdateHisTag(Database database, string tag,string parameterName,string value)
        {
            var vv = database.RealDatabase.GetTagByName(tag);
            if (vv != null)
            {
                if (database.HisDatabase.HisTags.ContainsKey(vv.Id))
                {
                    UpdateHisTagParameter(database.HisDatabase.HisTags[vv.Id], parameterName, value);
                }
                else
                {
                    HisTag vtag = new HisTag() { Id = vv.Id, TagType = vv.Type };
                    database.HisDatabase.AddHisTags(vtag);
                    UpdateHisTagParameter(vtag, parameterName, value);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="parameterName"></param>
        /// <param name="value"></param>
        private static void UpdateHisTagParameter(HisTag tag,string parameterName,string value)
        {
            switch(parameterName)
            {
                case "type":
                    tag.Type = (RecordType)(int.Parse(value));
                    break;
                case "circle":
                    tag.Circle = int.Parse(value);
                    break;
                case "maxvaluecountpersecond":
                    tag.MaxValueCountPerSecond = short.Parse(value);
                    break;
                case "compresstype":
                    tag.CompressType = int.Parse(value);
                    break;
                case "parameters":
                    var vals = value.Split(new char[';']);
                    foreach(var vv in vals)
                    {
                        string[] sval = vv.Split(new char['=']);
                        tag.Parameters.Add(sval[0], double.Parse(sval[1]));
                    }
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        private static void ClearTag(Database database)
        {
            database.RealDatabase.Tags.Clear();
            database.RealDatabase.MaxId = -1;
            database.HisDatabase.HisTags.Clear();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tag"></param>
        private static void RemoveTag(Database database, string tag)
        {
            var vv = database.RealDatabase.GetTagByName(tag);
            if(vv!=null)
            {
                database.RealDatabase.Remove(vv.Id);
                database.HisDatabase.RemoveHisTag(vv.Id);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="type"></param>
        /// <param name="tag"></param>
        /// <param name="link"></param>
        /// <param name="repeat"></param>
        private static void AddTag(Database database ,string type,string tag,string link,int repeat)
        {
            if (type == "double")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.DoubleTag() { Name = tag, LinkAddress = link,Group="" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Double, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.DoubleTag() { Name = tag+j, LinkAddress = link,Group="" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Double, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "float")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.FloatTag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Float, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.FloatTag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Float, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "int")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.IntTag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Int, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.IntTag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Int, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "uint")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.UIntTag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UInt, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.UIntTag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UInt, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "long")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.LongTag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Long, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.LongTag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Long, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "ulong")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.ULongTag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.ULong, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.ULongTag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.ULong, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "short")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.ShortTag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Short, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.ShortTag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Short, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "ushort")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.UShortTag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UShort, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.UShortTag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UShort, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "bool")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.BoolTag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Bool, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.BoolTag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Bool, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "string")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.StringTag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.String, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.StringTag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.String, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "datetime")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.DateTimeTag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.String, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.DateTimeTag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.String, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "intpoint")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.IntPointTag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.IntPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.IntPointTag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.IntPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "uintpoint")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.UIntPointTag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UIntPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.UIntPointTag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UIntPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "intpoint3")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.IntPoint3Tag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.IntPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.IntPoint3Tag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.IntPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "uintpoint3")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.UIntPoint3Tag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UIntPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.UIntPoint3Tag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UIntPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "longpoint3")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.LongPoint3Tag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.LongPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.LongPoint3Tag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.LongPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "longpoint")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.LongPointTag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.LongPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.LongPointTag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.LongPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "ulongpoint3")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.ULongPoint3Tag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.ULongPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.ULongPoint3Tag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.ULongPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "ulongpoint")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.ULongPointTag() { Name = tag, LinkAddress = link, Group = "" };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.ULongPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.ULongPointTag() { Name = tag + j, LinkAddress = link, Group = "" };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.ULongPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private static string GetHelpString()
        {
            string str = "{0,-10} {1,-16} {2}";
            StringBuilder re = new StringBuilder();
            re.AppendLine();
            re.AppendLine( string.Format(str, "db","[databasename]",@"// " + Res.Get("GDMsgHlp")));
            re.AppendLine(string.Format(str, "list","","// "+Res.Get("ListDatabaseHlp")));
            re.AppendLine(string.Format(str, "exit","","// " + Res.Get("Exit")));
            re.AppendLine(string.Format(str, "h","","// " + Res.Get("HMsg")));
            return re.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public static bool StopDatabase(string name)
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
                            Console.WriteLine(string.Format(Res.Get("StopdatabaseSucessful"), name));
                        }
                    }
                    return true;
                }
                catch(Exception ex)
                {
                    Console.WriteLine( string.Format(Res.Get("Stopdatabasefail"),name)+ex.Message);
                }
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public static bool ReLoadDatabase(string name)
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
                            Console.WriteLine(string.Format(Res.Get("RerundatabaseSucessful"), name));
                        }
                        return true;

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(string.Format(Res.Get("Rerundatabasefail"), name));
                        //Console.WriteLine("ReRun database " + name + "  failed. " + ex.Message + "  " + ex.StackTrace);
                    }
                    return false;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public static bool CheckStart(string name)
        {
            lock (mLockObj)
            {
                if(IsDatabaseRun(name,out bool isdbrun))
                {
                    return true;
                }
                else if(!isdbrun) return false;

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
        /// <param name="isdbrun"></param>
        /// <returns></returns>
        public static bool IsDatabaseRun(string database, out bool isdbrun)
        {
            var pps = System.Diagnostics.Process.GetProcessesByName("DbInRun");
            if (pps != null && pps.Length > 0)
            {
                foreach (var p in pps)
                {
                    if (string.Compare(p.MainWindowTitle, "DbInRun-" + database, true) == 0)
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
        /// <param name="name"></param>
        public bool Start(string name)
        {
            if (!CheckStart(name))
            {
              return  StartDb(name);
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool Stop(string name)
        {
            //if (CheckStart(name))
            {
               return StopDatabase(name);
            }
            //return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool Rerun(string name)
        {
            if (!CheckStart(name))
            {
              return  StartDb(name);
            }
            else
            {
               return ReLoadDatabase(name);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool IsRunning(string name)
        {
            return CheckStart(name);
        }
    }
}

