using Cdy.Tag;
using System;
using System.Text;

namespace DBStudio
{
    class Program
    {
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

            DBDevelopService.Service.Instanse.Start(port, webPort);
            Console.WriteLine("输入exit退出服务");
            Console.WriteLine(Res.Get("HelpMsg"));
            while (true)
            {
                Console.Write(">");
                string[] cmd = Console.ReadLine().Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);
                if (cmd.Length == 0) continue;

                string cmsg = cmd[0].ToLower();

                if (cmsg == "exit")
                {
                    Console.WriteLine("确定要退出?输入y确定,输入其他任意字符取消");
                    cmd = Console.ReadLine().Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);
                    if (cmd.Length == 0) continue;
                    if (cmd[0].ToLower() == "y")
                        break;
                }
                else if (cmsg == "db")
                {
                    if (cmd.Length > 1)
                        ProcessDatabaseCreat(cmd[1]);
                }
                else if (cmsg == "h")
                {
                    Console.WriteLine(GetHelpString());
                }
            }
            DBDevelopService.Service.Instanse.Stop();

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="msg"></param>
        private static void ProcessDatabaseCommand(Database db,string msg)
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
        private static void ProcessDatabaseCreat(string name)
        {
            if (!DBDevelopService.DbManager.Instance.IsLoaded)
                DBDevelopService.DbManager.Instance.Load();

            Database db = DBDevelopService.DbManager.Instance.GetDatabase(name);
            
            if (db == null)
            {
                db = Database.New(name);
            }

            Console.WriteLine(Res.Get("HelpMsg"));
            while (true)
            {
                Console.Write(name+">"); 
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
                    else if (cmsg == "list")
                    {
                        ListDatabase(db, cmd[1].ToLower());
                    }
                    else if (cmsg == "h")
                    {

                    }
                    else if (cmsg == "exit")
                    {
                        break;
                    }
                }
                catch
                {
                    Console.Write(name + ">" + Res.Get("ErroParameter"));
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="type"></param>
        private static void ListDatabase(Database database, string type)
        {
            if(string.IsNullOrEmpty(type))
            {
                
            }
            else
            {

            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="file"></param>
        private static void Import(Database database,string file)
        {
            string sfile = file;
            if(!System.IO.Path.IsPathRooted(sfile))
            {
                sfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location), sfile);
            }
            if(System.IO.File.Exists(sfile))
            {
                var reader = new System.IO.StreamReader(System.IO.File.Open(sfile, System.IO.FileMode.Open));
                while(reader.Peek()>0)
                {
                    var cmd = reader.ReadLine();
                    ProcessDatabaseCommand(database, cmd);
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
                        var vtag = new Cdy.Tag.DoubleTag() { Name = tag+j, LinkAddress = link };
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
        }

        private static string GetHelpString()
        {
            StringBuilder re = new StringBuilder();
            re.AppendLine();
            re.AppendLine("db    [databasename] // " + Res.Get("GDMsg"));
            re.AppendLine("exit             // " + Res.Get("Exit"));
            re.AppendLine("h                // " + Res.Get("HMsg"));
            return re.ToString();
        }
    }
}

