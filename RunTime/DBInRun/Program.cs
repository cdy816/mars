using Cdy.Tag;
using System;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DBInRun
{
    class Program
    {
        static bool mIsClosed = false;

        static void Main(string[] args)
        {
            Console.CancelKeyPress += Console_CancelKeyPress;
            AppDomain.CurrentDomain.ProcessExit += CurrentDomain_ProcessExit;
            AppDomain.CurrentDomain.FirstChanceException += CurrentDomain_FirstChanceException;
            WindowConsolHelper.DisbleQuickEditMode();

            Runner3.RunInstance.Init();

            Console.WriteLine(Res.Get("WelcomeMsg"));
            PrintLogo();

            Console.WriteLine(Res.Get("HelpMsg"));

            if (args.Length>0 && args[0]== "start")
            {
                Task.Run(() => {
                    StartMonitor(args.Length > 1 ? args[1] : "local");
                });

                if (args.Length > 1)
                {
                    int port = 14330;
                    if (args.Length > 2)
                    {
                        int.TryParse(args[2], out port);
                    }
                    Cdy.Tag.Runner3.RunInstance.StartAsync(args[1], port);
                    Console.Title = "DbInRun-" + args[1];
                }
                else
                {
                    Cdy.Tag.Runner3.RunInstance.Start();
                }
            }
         
            while (!mIsClosed)
            {
                Console.Write(">");
                
                while (!Console.KeyAvailable)
                {
                    if(mIsClosed)
                    {
                        break;
                    }
                    Thread.Sleep(100);
                }
                
                if (mIsClosed)
                {
                    break;
                }

                string smd = Console.ReadLine();

                if (string.IsNullOrEmpty(smd))
                {
                    LoggerService.Service.EnableLogger =! LoggerService.Service.EnableLogger;
                    if(LoggerService.Service.EnableLogger)
                    {
                        Console.WriteLine("Log is enabled.");
                    }
                    else
                    {
                        Console.WriteLine("Log is disabled.");
                    }
                    continue;
                }

                string[] cmd = smd.Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);

                if (cmd.Length == 0) continue;

                string scmd = cmd[0].ToLower();
                switch (scmd)
                {
                    case "exit":
                        if (Cdy.Tag.Runner3.RunInstance.IsStarted)
                        {
                            Cdy.Tag.Runner3.RunInstance.Stop();
                        }
                        mIsClosed = true;
                        break;
                    case "start":
                        string dbname = "local";
                        if (cmd.Length > 1)
                        {
                            Cdy.Tag.Runner3.RunInstance.StartAsync(cmd[1]);
                            Console.Title = "DbInRun-" + cmd[1];
                            dbname = cmd[1];
                        }
                        else
                        {
                            Cdy.Tag.Runner3.RunInstance.Start();
                            Console.Title = "DbInRun-local";
                            dbname = "local";
                        }
                        Task.Run(() => {
                            StartMonitor(dbname);
                        });
                        break;
                    case "stop":
                        Cdy.Tag.Runner3.RunInstance.Stop();
                        break;
                    case "switch":
                        LoggerService.Service.EnableLogger = true;
                        if (cmd.Length > 1)
                        {
                            var cd = cmd[1].ToLower();
                            if (cd == "primary")
                            {
                                if (!Cdy.Tag.Runner3.RunInstance.Switch(WorkState.Primary))
                                {
                                    LoggerService.Service.Erro("RDDCManager", "Failed to switch to primary!");
                                }
                            }
                            else if (cd == "standby")
                            {
                                if (!Cdy.Tag.Runner3.RunInstance.Switch(WorkState.Standby))
                                {
                                    LoggerService.Service.Erro("RDDCManager", "Failed to switch to standby!");
                                }
                            }
                        }
                        break;
                    case "restart":
                        LoggerService.Service.EnableLogger = true;
                        Task.Run(() => {
                            Cdy.Tag.Runner3.RunInstance.ReStartDatabase();
                        });
                        break;
                    case "dynamicload":
                        LoggerService.Service.EnableLogger = true;
                        Task.Run(() => {
                            Cdy.Tag.Runner3.RunInstance.DynamicLoadTags();
                        });
                        break;
                    case "list":
                        ListDatabase();
                        break;
                    case "setdatapath":
                        if (cmd.Length > 1)
                            SeriseEnginer4.HisDataPathPrimary = cmd[1];
                        break;
                    case "setdatabackuppath":
                        if (cmd.Length > 1)
                            SeriseEnginer4.HisDataPathBack = cmd[1];
                        break;
                    case "h":
                        Console.WriteLine(GetHelpString());
                        break;
                    case "**":
                        LogoHelper.PrintAuthor();
                        break;
                }
            }
        }

        private static void CurrentDomain_FirstChanceException(object sender, System.Runtime.ExceptionServices.FirstChanceExceptionEventArgs e)
        {
            Console.WriteLine(e.Exception.StackTrace);
            e.Exception.HResult = 0;
        }

        /// <summary>
        /// 
        /// </summary>
        private static void ListDatabase()
        {
            string spath = System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location);
            spath = System.IO.Path.Combine(spath, "Data");
            StringBuilder sb = new StringBuilder();
            string stemp = "{0} {1}";
            foreach(var vv in System.IO.Directory.EnumerateDirectories(spath))
            {
                var vvn = new System.IO.DirectoryInfo(vv).Name;
                string sdb = System.IO.Path.Combine(vv, vvn+".db");
                sb.AppendLine(string.Format(stemp, vvn, System.IO.File.GetLastWriteTime(sdb)));
            }
            Console.WriteLine(sb.ToString());
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
            if (Cdy.Tag.Runner3.RunInstance.IsStarted)
            {
                Cdy.Tag.Runner3.RunInstance.Stop();
            }
        }

        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            if (Cdy.Tag.Runner3.RunInstance.IsStarted)
            {
                Cdy.Tag.Runner3.RunInstance.Stop();
            }
            mIsClosed = true;
            e.Cancel = true;

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
                                    if (Cdy.Tag.Runner3.RunInstance.IsStarted)
                                    {
                                        Cdy.Tag.Runner3.RunInstance.Stop();
                                    }
                                    mIsClosed = true;
                                    server.WriteByte(1);
                                    server.FlushAsync();
                                    //server.WaitForPipeDrain();
                                    Console.WriteLine(Res.Get("AnyKeyToExit")+".....");
                                    break;
                                    //退出系统
                                }
                                else if(cmd==1)
                                {
                                    Console.WriteLine("Start to restart database.......");
                                    Task.Run(() => {
                                        Cdy.Tag.Runner3.RunInstance.ReStartDatabase();
                                    });
                                    server.WriteByte(1);
                                    server.FlushAsync();
                                   // server.WaitForPipeDrain();
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
                LoggerService.Service.Info("Programe", ex.Message);
                //Console.WriteLine(ex.Message);
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
            re.AppendLine("start [database]        // "+Res.Get("StartMsg"));
            re.AppendLine("stop                    // " + Res.Get("StopMsg"));
            re.AppendLine("restart                 // " + Res.Get("RestartMsg"));
            re.AppendLine("dynamicload             // " + Res.Get("DynamicloadMsg"));
            re.AppendLine("list                    // " + Res.Get("ListMsg"));
            re.AppendLine("switch primary/standby  // " + Res.Get("RddcSwitch"));
            re.AppendLine("setdatapath [path]      // " + Res.Get("sethisdatapath"));
            re.AppendLine("setdatabackuppath [path]// " + Res.Get("sethisdatapath"));
            re.AppendLine("h                       // " + Res.Get("HMsg"));
            return re.ToString();
        }
    }
}
