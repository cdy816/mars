using Cdy.Tag;
using System;
using System.Text;

namespace Mars
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("***************欢迎来到Mars实时数据库****************");
            if (args.Length>0 && args[0]== "start")
            {
                if (args.Length > 1)
                {
                    Cdy.Tag.Runner.RunInstance.StartAsync(args[1]);
                }
                else
                {
                    Cdy.Tag.Runner.RunInstance.Start();
                }
            }

            Console.WriteLine("输入h获取命令帮助信息");
            while (true)
            {
                Console.Write(">");
                string[] cmd = Console.ReadLine().Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);

                if (cmd.Length == 0) continue;

                string scmd = cmd[0].ToLower();
                switch (scmd)
                {
                    case "exit":
                        if (Cdy.Tag.Runner.RunInstance.IsStarted)
                        {
                            Cdy.Tag.Runner.RunInstance.Stop();
                        }
                        break;
                    case "start":
                        if (cmd.Length > 1)
                        {
                            Cdy.Tag.Runner.RunInstance.StartAsync(cmd[1]);
                        }
                        else
                        {
                            Cdy.Tag.Runner.RunInstance.Start();
                        }
                        break;
                    case "stop":
                        Cdy.Tag.Runner.RunInstance.Stop();
                        break;
                    case "gd":
                        switch (cmd.Length)
                        {
                            case 1:
                                GeneratorTestDatabase("local");
                                break;
                            case 2:
                                GeneratorTestDatabase(cmd[1]);
                                break;
                            case 3:
                                GeneratorTestDatabase(cmd[1], int.Parse(cmd[2]));
                                break;
                            case 4:
                                GeneratorTestDatabase(cmd[1], int.Parse(cmd[2]), int.Parse(cmd[3]));
                                break;
                            case 5:
                                GeneratorTestDatabase(cmd[1], int.Parse(cmd[2]), int.Parse(cmd[3]), int.Parse(cmd[4]));
                                break;
                            case 6:
                                GeneratorTestDatabase(cmd[1], int.Parse(cmd[2]), int.Parse(cmd[3]), int.Parse(cmd[4]), int.Parse(cmd[5]));
                                break;
                            case 7:
                                GeneratorTestDatabase(cmd[1], int.Parse(cmd[2]), int.Parse(cmd[3]), int.Parse(cmd[4]), int.Parse(cmd[5]), int.Parse(cmd[6]));
                                break;
                        }
                        break;
                    case "h":
                        Console.WriteLine(GetHelpString());
                        break;
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
            re.AppendLine("start [database] // start to run a databse,ignor database name to run default 'local' database ");
            re.AppendLine("stop             // stop  a databse");
            re.AppendLine("gd    [databasename] [double tag count] [float tag count] [long tag count] [int tag count] [bool tag count] // generate a sample databse with special number tags");
            re.AppendLine("h                //display command list");
            return re.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="dcount"></param>
        /// <param name="fcount"></param>
        /// <param name="lcount"></param>
        /// <param name="icount"></param>
        /// <param name="bcount"></param>
        static void GeneratorTestDatabase(string databaseName,int dcount=0, int fcount = 0, int lcount = 0, int icount=0,int bcount=0)
        {
            //Cdy.Tag.PathHelper.helper.SetDataBasePath(databaseName);

            Database db =  Database.New(databaseName); 

            Cdy.Tag.RealDatabase test = db.RealDatabase;
            db.RealDatabase = test;

            for (int i=0;i<dcount;i++)
            {
                test.Append(new Cdy.Tag.DoubleTag() { Name = "Double" + i ,Group="Double"});
            }

            for (int i = 0; i < fcount; i++)
            {
                test.Append(new Cdy.Tag.FloatTag() { Name = "Float" + i, Group = "Float" });
            }

            for (int i = 0; i < lcount; i++)
            {
                test.Append(new Cdy.Tag.LongTag() { Name = "Long" + i, Group = "Long" });
            }

            for (int i = 0; i < icount; i++)
            {
                test.Append(new Cdy.Tag.IntTag() { Name = "Int" + i, Group = "Int" });
            }

            for (int i = 0; i < bcount; i++)
            {
                test.Append(new Cdy.Tag.BoolTag() { Name = "Bool" + i, Group = "Bool" });
            }

           // new Cdy.Tag.RealDatabaseManager() { Database = test }.Save();

            Cdy.Tag.HisDatabase htest =db.HisDatabase;
            int id = 0;
            for (int i = 0; i < dcount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Double, Circle = 1000,Type = Cdy.Tag.RecordType.Timer });
                id++;
            }

            for (int i = 0; i < fcount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Float, Circle = 1000, Type = Cdy.Tag.RecordType.Timer });
                id++;
            }

            for (int i = 0; i < lcount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Long, Circle = 1000, Type = Cdy.Tag.RecordType.Timer });
                id++;
            }

            for (int i = 0; i < icount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Int, Circle = 1000, Type = Cdy.Tag.RecordType.Timer });
                id++;
            }

            for (int i = 0; i < bcount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Bool, Circle = 1000, Type = Cdy.Tag.RecordType.Timer });
                id++;
            }
            db.HisDatabase = htest;

            new DatabaseSerise() { Dbase = db }.Save();

            //new Cdy.Tag.HisDatabaseManager() { Database = htest }.Save();

        }

    }
}
