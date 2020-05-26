using Cdy.Tag;
using System;
using System.IO;
using System.IO.Pipes;
using System.Text;
using System.Threading.Tasks;

namespace DBInRun
{
    class Program
    {
        static bool mIsClosed = false;
        //static MarshalMemoryBlock block;
        static void Main(string[] args)
        {
            Console.CancelKeyPress += Console_CancelKeyPress;
            AppDomain.CurrentDomain.ProcessExit += CurrentDomain_ProcessExit;

            WindowConsolHelper.DisbleQuickEditMode();

            Console.WriteLine(Res.Get("WelcomeMsg"));
            PrintLogo();
            if (args.Length>0 && args[0]== "start")
            {
                if (args.Length > 1)
                {
                    int port = 14330;
                    if (args.Length > 2)
                    {
                        int.TryParse(args[2], out port);
                    }
                    Cdy.Tag.Runner.RunInstance.StartAsync(args[1], port);
                }
                else
                {
                    Cdy.Tag.Runner.RunInstance.Start();
                }
                Task.Run(() => {
                    StartMonitor(args.Length > 1 ? args[1] : "local");
                });
                
            }

            Console.WriteLine(Res.Get("HelpMsg"));
            while (!mIsClosed)
            {
                Console.Write(">");

                string smd = Console.ReadLine();
                if (mIsClosed)
                {
                    break;
                }
                if (string.IsNullOrEmpty(smd)) continue;

                string[] cmd = smd.Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);

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
                        string dbname = "local";
                        if (cmd.Length > 1)
                        {
                            Cdy.Tag.Runner.RunInstance.StartAsync(cmd[1]);
                            Console.Title = "DbInRun-" + cmd[1];
                            dbname = cmd[1];
                        }
                        else
                        {
                            Cdy.Tag.Runner.RunInstance.Start();
                            Console.Title = "DbInRun-local";
                            dbname = "local";
                        }
                        Task.Run(() => {
                            StartMonitor(dbname);
                        });
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
        private static void PrintLogo()
        {
            LogoHelper.Print();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void CurrentDomain_ProcessExit(object sender, EventArgs e)
        {
            if (Cdy.Tag.Runner.RunInstance.IsStarted)
            {
                Cdy.Tag.Runner.RunInstance.Stop();
            }
        }

        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            if (Cdy.Tag.Runner.RunInstance.IsStarted)
            {
                Cdy.Tag.Runner.RunInstance.Stop();
            }
            mIsClosed = true;
            e.Cancel = true;
            Console.WriteLine(Res.Get("AnyKeyToExit"));

        }

        private static void StartMonitor(string name)
        {
            try
            {
                while (!mIsClosed)
                {
                    using (var server = new NamedPipeServerStream(name, PipeDirection.InOut))
                    {
                        server.WaitForConnection();
                        while (!mIsClosed)
                        {
                            try
                            {
                                if (!server.IsConnected) break;
                                var cmd = server.ReadByte();
                                if (cmd == 0)
                                {
                                    if (Cdy.Tag.Runner.RunInstance.IsStarted)
                                    {
                                        Cdy.Tag.Runner.RunInstance.Stop();
                                    }
                                    mIsClosed = true;
                                    server.WriteByte(1);
                                    server.WaitForPipeDrain();
                                    Console.WriteLine(Res.Get("AnyKeyToExit"));
                                    break;
                                    //退出系统
                                }
                                else
                                {

                                }
                            }
                            catch
                            {
                                break;
                            }
                        }
                    }
                    
                }
                
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
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
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Double, Circle = 1000,Type = Cdy.Tag.RecordType.Timer,CompressType=1 });
                id++;
            }

            for (int i = 0; i < fcount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Float, Circle = 1000, Type = Cdy.Tag.RecordType.Timer, CompressType = 1 });
                id++;
            }

            for (int i = 0; i < lcount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Long, Circle = 1000, Type = Cdy.Tag.RecordType.Timer, CompressType = 1 });
                id++;
            }

            for (int i = 0; i < icount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Int, Circle = 1000, Type = Cdy.Tag.RecordType.Timer, CompressType = 1 });
                id++;
            }

            for (int i = 0; i < bcount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Bool, Circle = 1000, Type = Cdy.Tag.RecordType.Timer, CompressType = 1 });
                id++;
            }
            db.HisDatabase = htest;

            new DatabaseSerise() { Dbase = db }.Save();


        }

    }
}
