using Cdy.Tag;
using System;
using System.Text;

namespace DBInRun
{
    class Program
    {
        //static MarshalMemoryBlock block;
        static void Main(string[] args)
        {
            bool mIsClosed = false;
            Console.WriteLine(Res.Get("WelcomeMsg"));
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

            Console.WriteLine(Res.Get("HelpMsg"));
            while (!mIsClosed)
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
                        mIsClosed = true;
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
                    //case "mtest":
                    //    block = new MarshalMemoryBlock((long)(1024 * 1024 * 1024)*2);
                    //    //block.Clear();
                    //    break;
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
            re.AppendLine("start [database] // "+Res.Get("StartMsg"));
            re.AppendLine("stop             // " + Res.Get("StopMsg"));
            re.AppendLine("gd    [databasename] [double tag count] [float tag count] [long tag count] [int tag count] [bool tag count] // "+Res.Get("GDMsg"));
            re.AppendLine("h                // " + Res.Get("HMsg"));
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

            string address = "";

            for (int i=0;i<dcount;i++)
            {
                if(i%3==0)
                {
                    address = "Sim:sin";
                }
                else if(i%3==1)
                {
                    address = "Sim:cos";
                }
                else
                {
                    address = "Sim:step";
                }
                test.Append(new Cdy.Tag.DoubleTag() { Name = "Double" + i ,Group="Double",LinkAddress= address });
            }

            for (int i = 0; i < fcount; i++)
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
                test.Append(new Cdy.Tag.FloatTag() { Name = "Float" + i, Group = "Float", LinkAddress = address });
            }

            for (int i = 0; i < lcount; i++)
            {
                test.Append(new Cdy.Tag.LongTag() { Name = "Long" + i, Group = "Long",LinkAddress= "Sim:step" });
            }

            for (int i = 0; i < icount; i++)
            {
                test.Append(new Cdy.Tag.IntTag() { Name = "Int" + i, Group = "Int" });
            }

            for (int i = 0; i < bcount; i++)
            {
                test.Append(new Cdy.Tag.BoolTag() { Name = "Bool" + i, Group = "Bool" });
            }

            

            Cdy.Tag.HisDatabase htest =db.HisDatabase;
            int id = 0;
            for (int i = 0; i < dcount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Double, Circle = 1000,Type = Cdy.Tag.RecordType.Timer,CompressType=0 });
                id++;
            }

            for (int i = 0; i < fcount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Float, Circle = 1000, Type = Cdy.Tag.RecordType.Timer, CompressType = 0 });
                id++;
            }

            for (int i = 0; i < lcount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Long, Circle = 1000, Type = Cdy.Tag.RecordType.Timer, CompressType = 0 });
                id++;
            }

            for (int i = 0; i < icount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Int, Circle = 1000, Type = Cdy.Tag.RecordType.Timer, CompressType = 0 });
                id++;
            }

            for (int i = 0; i < bcount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Bool, Circle = 1000, Type = Cdy.Tag.RecordType.Timer, CompressType = 0 });
                id++;
            }
            db.HisDatabase = htest;

            new DatabaseSerise() { Dbase = db }.Save();


        }

    }
}
