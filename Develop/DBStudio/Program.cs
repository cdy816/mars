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

namespace DBStudio
{
    class Program
    {
        static bool mIsExited = false;
        static void Main(string[] args)
        {
            int port = 5001;
            int webPort = 9000;
            if (args.Length > 0)
            {
                port = int.Parse(args[0]);
            }

            if (args.Length > 1)
            {
                webPort = int.Parse(args[1]);
            }
            WindowConsolHelper.DisbleQuickEditMode();

            Console.CancelKeyPress += Console_CancelKeyPress;

            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;

            LogoHelper.Print();
            DBDevelopService.Service.Instanse.Start(port, webPort);
            //OutByLine("", "输入exit退出服务");
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
                        OutByLine("", "确定要退出?输入y确定,输入其他任意字符取消");
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
            if (!DBDevelopService.DbManager.Instance.IsLoaded)
                DBDevelopService.DbManager.Instance.Load();

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
        /// <param name="name"></param>
        private static void ProcessDatabaseCreate(string name)
        {
            if (!DBDevelopService.DbManager.Instance.IsLoaded)
                DBDevelopService.DbManager.Instance.Load();

            Database db = DBDevelopService.DbManager.Instance.GetDatabase(name);

            if (db == null)
            {
                db = Database.New(name);
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
                    }
                    else if (cmsg == "start")
                    {
                        if (!CheckStart(db.Name))
                        {
                            StartDb(db.Name);
                        }
                        else
                        {
                            Console.WriteLine("database " + db.Name + " is in running.");
                        }
                    }
                    else if (cmsg == "restart")
                    {
                        StopDatabase(db.Name);
                        while (CheckStart(db.Name)) Thread.Sleep(100);
                        StartDb(db.Name);
                    }
                    else if (cmsg == "isstart")
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
                    else if (cmsg == "updatehis")
                    {
                        UpdateHisTag(db, cmd[1].ToLower(), cmd[2], cmd[3]);
                    }
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
                        Sp(db,int.Parse(cmd[1]),int.Parse(cmd[2]), cmd.Skip(3).ToArray());
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
        private static void StartDb(string name)
        {
            var info = new ProcessStartInfo() { FileName = "DbInRun.exe" };
            info.UseShellExecute = true;
            info.Arguments = "start "+name;
            info.WorkingDirectory = System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location);
            Process.Start(info);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="paras"></param>
        private static void Sp(Database db,int ctp,int addressType, params string[] paras)
        {
            Cdy.Tag.RealDatabase test = db.RealDatabase;
          
            Cdy.Tag.HisDatabase htest = db.HisDatabase;

            string address = "";
            if (paras.Length > 0)
            {
                int dcount = int.Parse(paras[0]);
                for (int i = 0; i < dcount; i++)
                {
                    if(addressType==0)
                    {
                        address = "";
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
                    var vtag = new Cdy.Tag.DoubleTag() { Name = "Double" + i, Group = "Double", LinkAddress = address };
                    test.Append(vtag);
                    htest.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = Cdy.Tag.TagType.Double, Circle = 1000, Type = Cdy.Tag.RecordType.Timer, CompressType = ctp });
                }
            }

            if (paras.Length > 1)
            {
                int fcount = int.Parse(paras[1]);
                for (int i = 0; i < fcount; i++)
                {
                    if (addressType == 0)
                    {
                        address = "";
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
                    var vtag = new Cdy.Tag.FloatTag() { Name = "Float" + i, Group = "Float", LinkAddress = address };
                    test.Append(vtag);
                    htest.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = Cdy.Tag.TagType.Float, Circle = 1000, Type = Cdy.Tag.RecordType.Timer, CompressType = ctp });
                }
            }

            if (paras.Length > 2)
            {
                int fcount = int.Parse(paras[2]);
                if (addressType == 0)
                {
                    address = "";
                }
                else
                {
                    address = "Sim:step";
                }
                for (int i = 0; i < fcount; i++)
                {
                    var vtag = new Cdy.Tag.LongTag() { Name = "Long" + i, Group = "Long", LinkAddress = address };
                    test.Append(vtag);
                    htest.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = Cdy.Tag.TagType.Long, Circle = 1000, Type = Cdy.Tag.RecordType.Timer, CompressType = ctp });
                }
            }

            if (paras.Length > 3)
            {
                if (addressType == 0)
                {
                    address = "";
                }
                else
                {
                    address = "Sim:step";
                }
                int fcount = int.Parse(paras[3]);
                for (int i = 0; i < fcount; i++)
                {
                    var vtag = new Cdy.Tag.IntTag() { Name = "Int" + i, Group = "Int", LinkAddress = address };
                    test.Append(vtag);
                    htest.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = Cdy.Tag.TagType.Int, Circle = 1000, Type = Cdy.Tag.RecordType.Timer, CompressType = ctp });
                }
            }

            if (paras.Length > 4)
            {
                if (addressType == 0)
                {
                    address = "";
                }
                else
                {
                    address = "Sim:square";
                }
                int fcount = int.Parse(paras[4]);
                for (int i = 0; i < fcount; i++)
                {
                    var vtag = new Cdy.Tag.BoolTag() { Name = "Bool" + i, Group = "Bool", LinkAddress = address };
                    test.Append(vtag);
                    htest.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = Cdy.Tag.TagType.Bool, Circle = 1000, Type = Cdy.Tag.RecordType.Timer, CompressType = ctp });
                }
            }


            if (paras.Length > 5)
            {
                if (addressType == 0)
                {
                    address = "";
                }
                else
                {
                    address = "Sim:steppoint";
                }
                int fcount = int.Parse(paras[5]);
                for (int i = 0; i < fcount; i++)
                {
                    var vtag = new Cdy.Tag.IntPointTag() { Name = "IntPoint" + i, Group = "IntPoint", LinkAddress = address };
                    test.Append(vtag);
                    htest.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = Cdy.Tag.TagType.IntPoint, Circle = 1000, Type = Cdy.Tag.RecordType.Timer, CompressType = ctp });
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private static string GetDbManagerHelpString()
        {
            StringBuilder re = new StringBuilder();
            re.AppendLine();
            re.AppendLine("save                                                 // save database ");
            re.AppendLine("start                                                 // start database ");
            re.AppendLine("restart                                                 // restart database ");
            re.AppendLine("stop                                                 // stop database ");
            re.AppendLine("add       [tagtype] [tagname] [linkaddress] [repeat] // add numbers tag to database ");
            re.AppendLine("remove    [tagname]                                  // remove a tag");
            re.AppendLine("clear                                                // clear all tags in database");
            re.AppendLine("update    [tagname] [propertyname] [propertyvalue]   // update value of a poperty in a tag");
            re.AppendLine("updatehis [tagname] [propertyname] [propertyvalue]   // update value of a poperty in a tag's his config");
            re.AppendLine("import    [filename]                                 //import tags from a csvfile");
            re.AppendLine("export    [filename]                                 //export tags to a csvfile");
            re.AppendLine("list      [tagtype]                                  //the sumery info of specical type tags or all tags");
            re.AppendLine("sp        [compressType] [enable sim address][double tag number] [float tag number] [long tag number] [int tag number] [bool tag number] [intpoint tag number]   //Quickly generate a specified number of tags for test purposes");
            re.AppendLine("exit                                                 //exit and back to parent");

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

            var stream = new StreamWriter(File.Open(sfile, FileMode.OpenOrCreate, FileAccess.ReadWrite));
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
            if(mRealTagMode.Conveter!=null)
            sb.Append(mRealTagMode.Conveter.SeriseToString() + ",");
            else
            {
                sb.Append(",");
            }
            if (mRealTagMode is NumberTagBase)
            {
                sb.Append((mRealTagMode as  NumberTagBase).MaxValue.ToString() + ",");
                sb.Append((mRealTagMode as NumberTagBase).MinValue.ToString() + ",");
            }
            else
            {
                sb.Append(",");
                sb.Append(",");
            }
            if(mRealTagMode is FloatingTagBase)
            {
                sb.Append((mRealTagMode as FloatingTagBase).Precision + ",");
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
            if(stmp[7]!=null)
            {
                realtag.Conveter = stmp[7].DeSeriseToValueConvert();
            }

            if(realtag is NumberTagBase)
            {
                (realtag as NumberTagBase).MaxValue = double.Parse(stmp[8], System.Globalization.NumberStyles.Any);
                (realtag as NumberTagBase).MinValue = double.Parse(stmp[9], System.Globalization.NumberStyles.Any);
            }

            if (realtag is FloatingTagBase)
            {
                (realtag as FloatingTagBase).Precision = byte.Parse(stmp[10]);
            }

            if (stmp.Length > 11)
            {
                Cdy.Tag.HisTag histag = new HisTag();
                histag.Type = (Cdy.Tag.RecordType)Enum.Parse(typeof(Cdy.Tag.RecordType), stmp[11]);

                histag.Circle = long.Parse(stmp[12]);
                histag.CompressType = int.Parse(stmp[13]);
                histag.Parameters = new Dictionary<string, double>();
                histag.TagType = realtag.Type;
                histag.Id = realtag.Id;

                for (int i = 14; i < stmp.Length; i++)
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
                    tag.Circle = long.Parse(value);
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
            database.RealDatabase.MaxId = 0;
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
                    var vtag = new Cdy.Tag.DoubleTag() { Name = tag, LinkAddress = link };
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
                    var vtag = new Cdy.Tag.FloatTag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Float, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.FloatTag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Float, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "int")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.IntTag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Int, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.IntTag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Int, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "uint")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.UIntTag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UInt, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.UIntTag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UInt, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "long")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.LongTag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Long, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.LongTag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Long, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "ulong")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.ULongTag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.ULong, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.ULongTag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.ULong, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "short")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.ShortTag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Short, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.ShortTag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Short, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "ushort")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.UShortTag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UShort, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.UShortTag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UShort, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "bool")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.BoolTag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Bool, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.BoolTag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.Bool, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "string")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.StringTag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.String, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.StringTag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.String, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "datetime")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.DateTimeTag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.String, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.DateTimeTag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.String, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "intpoint")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.IntPointTag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.IntPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.IntPointTag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.IntPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "uintpoint")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.UIntPointTag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UIntPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.UIntPointTag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UIntPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "intpoint3")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.IntPoint3Tag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.IntPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.IntPoint3Tag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.IntPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "uintpoint3")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.UIntPoint3Tag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UIntPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.UIntPoint3Tag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.UIntPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "longpoint3")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.LongPoint3Tag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.LongPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.LongPoint3Tag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.LongPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "longpoint")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.LongPointTag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.LongPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.LongPointTag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.LongPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "ulongpoint3")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.ULongPoint3Tag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.ULongPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.ULongPoint3Tag() { Name = tag + j, LinkAddress = link };
                        database.RealDatabase.Append(vtag);
                        database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.ULongPoint3, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                    }
                }
            }
            else if (type == "ulongpoint")
            {
                if (repeat == 1)
                {
                    var vtag = new Cdy.Tag.ULongPointTag() { Name = tag, LinkAddress = link };
                    database.RealDatabase.Append(vtag);
                    database.HisDatabase.AddHisTags(new Cdy.Tag.HisTag() { Id = vtag.Id, TagType = TagType.ULongPoint, Type = RecordType.Timer, Circle = 1000, CompressType = 0 });
                }
                else
                {
                    for (int j = 0; j < repeat; j++)
                    {
                        var vtag = new Cdy.Tag.ULongPointTag() { Name = tag + j, LinkAddress = link };
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
            StringBuilder re = new StringBuilder();
            re.AppendLine();
            re.AppendLine("db    [databasename]  // " + Res.Get("GDMsg"));
            re.AppendLine("list                  // List all exist database");
            re.AppendLine("exit                  // " + Res.Get("Exit"));
            re.AppendLine("h                     // " + Res.Get("HMsg"));
            return re.ToString();
        }

        public static void StopDatabase(string name)
        {
            using (var client = new NamedPipeClientStream(".", name, PipeDirection.InOut))
            {
                try
                {
                    client.Connect(2000);
                    client.WriteByte(0);
                    client.WaitForPipeDrain();
                    var res = client.ReadByte();
                    if (res == 1)
                    {
                        Console.WriteLine("Stop database" + name + " sucessfull.");
                    }
                }
                catch
                {
                    Console.WriteLine("Stop database " + name + "  failed.");
                }
            }
        }

        public static bool CheckStart(string name)
        {
            using (var client = new NamedPipeClientStream(".", name, PipeDirection.InOut))
            {
                try
                {
                    client.Connect(1000);
                    client.Close();
                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }
    
       
    }
}

