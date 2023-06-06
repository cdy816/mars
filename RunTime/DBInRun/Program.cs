using Cdy.Tag;
using System;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Xml.Linq;

namespace DBInRun
{
    class Program
    {
        static bool mIsClosed = false;

        static DateTime mStartTime = DateTime.Now;

        static void ReadDefaultConfig()
        {
            try
            {
                string scfg = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location), "DbInRun.cfg");
                if (System.IO.File.Exists(scfg))
                {
                    XElement xx = XElement.Load(scfg);
                    if (xx.Attribute("EmbedMode") != null)
                    {
                        Runner4.EmbedMode = bool.Parse(xx.Attribute("EmbedMode").Value);
                    }

                    if (xx.Attribute("EnableMachineMonitor") != null)
                    {
                        Runner4.EnableMachineMonitor = bool.Parse(xx.Attribute("EnableMachineMonitor").Value);
                    }

                    if (xx.Attribute("HideLocalApiWindow") != null)
                    {
                        Runner4.HideLocalApiWindow = bool.Parse(xx.Attribute("HideLocalApiWindow").Value);
                    }

                    if(xx.Attribute("CachFlashTime")!=null)
                    {
                        LogStorageManager.LogSleepTime = int.Parse(xx.Attribute("CachFlashTime").Value);
                        if (LogStorageManager.LogSleepTime <= 0)
                        {
                            LogStorageManager.LogSleepTime = 100;
                        }
                    }

                    if (xx.Attribute("CachFlashSize") != null)
                    {
                        LogStorageManager.CachFlashSize = int.Parse(xx.Attribute("CachFlashSize").Value);
                        if(LogStorageManager.CachFlashSize==0)
                        {
                            LogStorageManager.CachFlashSize = 5;
                        }
                    }

                    if(xx.Attribute("CompressMode")!=null)
                    {
                        SeriseEnginer7.CompressMode = (SeriseEnginer7.ZipCompressMode)(int.Parse(xx.Attribute("CompressMode").Value));
                    }

                    //
                }
            }
            catch
            {

            }
        }

        static void Main(string[] args)
        {
            ReadDefaultConfig();

            Console.CancelKeyPress += Console_CancelKeyPress;
            AppDomain.CurrentDomain.ProcessExit += CurrentDomain_ProcessExit;
            AppDomain.CurrentDomain.FirstChanceException += CurrentDomain_FirstChanceException;
            WindowConsolHelper.DisbleQuickEditMode();
            WindowConsolHelper.DiableCloseMenu();

            Runner4.RunInstance.Init();

            Console.WriteLine(Res.Get("WelcomeMsg"));
            PrintLogo();
            Cdy.Tag.Common.ProcessMemoryInfo.Instances.StartMonitor("DBInRun");
            Console.WriteLine(Res.Get("HelpMsg"));

            if (args.Contains("/s"))
            {
                Cdy.Tag.Runner4.HideLocalApiWindow = false;
            }

            if (args.Contains("/e"))
            {
                Runner4.EmbedMode = true;
            }

            if(args.Contains("/g"))
            {
                Runner4.EnableMachineMonitor = true;
            }

            if (args.Length>0 && args[0]== "start")
            {
                string stile = "";

                Task.Run(() => {
                    StartMonitor(args.Length > 1 ? args[1] : "local");
                });

                if (args.Length > 1)
                {
                    int port = 14330;
                    if (args.Length > 2)
                    {
                        if(int.TryParse(args[2], out int lport))
                        {
                            port= lport;
                        }
                    }
                    Cdy.Tag.Runner4.RunInstance.StartAsync(args[1], port);
                    stile = "DbInRun-" + args[1];
                }
                else
                {
                    Cdy.Tag.Runner4.RunInstance.Start();
                    stile = "DbInRun-local";
                }

                Console.Title = stile;

                if(args.Contains("/m"))
                {
                    WindowConsolHelper.MinWindow(stile);
                }
                

                //Console.WriteLine("test main window title:", Process.GetCurrentProcess().MainWindowTitle);
            }

            


            while (!mIsClosed)
            {
                Console.Write(">");
                if (!Console.IsInputRedirected)
                {
                    while (!Console.KeyAvailable)
                    {
                        if (mIsClosed)
                        {
                            break;
                        }
                        Thread.Sleep(100);
                    }
                }
                
                if (mIsClosed)
                {
                    break;
                }

                string smd = Console.In.ReadLine();

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
                        if (Cdy.Tag.Runner4.RunInstance.IsStarted)
                        {
                            Cdy.Tag.Runner4.RunInstance.Stop();
                        }
                        mIsClosed = true;
                        break;
                    case "start":
                        mStartTime = DateTime.Now;
                        string dbname = "local";
                        if (cmd.Length > 1)
                        {
                            Cdy.Tag.Runner4.RunInstance.StartAsync(cmd[1]);
                            Console.Title = "DbInRun-" + cmd[1];
                            dbname = cmd[1];
                        }
                        else
                        {
                            Cdy.Tag.Runner4.RunInstance.Start();
                            Console.Title = "DbInRun-local";
                            dbname = "local";
                        }
                        //Console.WriteLine("test main window title:", Process.GetCurrentProcess().MainWindowTitle);
                        Task.Run(() => {
                            StartMonitor(dbname);
                        });
                        break;
                    case "stop":
                        Cdy.Tag.Runner4.RunInstance.Stop(false);
                        break;
                    case "switch":
                        LoggerService.Service.EnableLogger = true;
                        if (cmd.Length > 1)
                        {
                            var cd = cmd[1].ToLower();
                            if (cd == "primary")
                            {
                                if (!Cdy.Tag.Runner4.RunInstance.Switch(WorkState.Primary))
                                {
                                    LoggerService.Service.Erro("RDDCManager", "Failed to switch to primary!");
                                }
                            }
                            else if (cd == "standby")
                            {
                                if (!Cdy.Tag.Runner4.RunInstance.Switch(WorkState.Standby))
                                {
                                    LoggerService.Service.Erro("RDDCManager", "Failed to switch to standby!");
                                }
                            }
                        }
                        break;
                    case "restart":
                        LoggerService.Service.EnableLogger = true;
                        Task.Run(() => {
                            Cdy.Tag.Runner4.RunInstance.ReStartDatabase();
                        });
                        break;
                    case "dynamicload":
                        LoggerService.Service.EnableLogger = true;
                        Task.Run(() => {
                            Cdy.Tag.Runner4.RunInstance.DynamicLoadTags();
                        });
                        break;
                    case "list":
                        ListDatabase();
                        break;
                    case "setdatapath":
                        if (cmd.Length > 1)
                            SeriseEnginer5.HisDataPathPrimary = cmd[1];
                        break;
                    case "setdatabackuppath":
                        if (cmd.Length > 1)
                            SeriseEnginer5.HisDataPathBack = cmd[1];
                        break;
                    case "h":
                        Console.WriteLine(GetHelpString());
                        break;
                    case "time":
                        Console.WriteLine((DateTime.Now - mStartTime).TotalSeconds);
                        break;
                    case "**":
                        LogoHelper.PrintAuthor();
                        break;
                }
            }
        }

        private static void CurrentDomain_FirstChanceException(object sender, System.Runtime.ExceptionServices.FirstChanceExceptionEventArgs e)
        {
            if ((e.Exception is System.Net.Sockets.SocketException)||(e.Exception.Message.Contains("System.Net.Sockets.Socket")))
            {
                //
            }
            else
            {
                Console.WriteLine(e.Exception.Message + " -> " + e.Exception.StackTrace);
            }
            e.Exception.HResult = 0;
        }

        /// <summary>
        /// 
        /// </summary>
        private static void ListDatabase()
        {
            //string spath = System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location);
            //spath = System.IO.Path.Combine(spath, "Data");
            string spath = PathHelper.helper.DataPath;
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
            if (Cdy.Tag.Runner4.RunInstance.IsStarted)
            {
                Cdy.Tag.Runner4.RunInstance.Stop();
            }
        }

        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            if (Cdy.Tag.Runner4.RunInstance.IsStarted)
            {
                Cdy.Tag.Runner4.RunInstance.Stop();
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
                    try
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
                                        if (Cdy.Tag.Runner4.RunInstance.IsStarted)
                                        {
                                            Cdy.Tag.Runner4.RunInstance.Stop();
                                        }
                                        mIsClosed = true;
                                        server.WriteByte(1);
                                        server.FlushAsync();
                                        //server.WaitForPipeDrain();
                                        Console.WriteLine(Res.Get("AnyKeyToExit") + ".....");
                                       
                                        break;
                                        //退出系统
                                    }
                                    else if (cmd == 1)
                                    {
                                        Console.WriteLine("Start to restart database.......");
                                        if (!Cdy.Tag.Runner4.RunInstance.IsRestartBusy)
                                        {
                                            Task.Run(() =>
                                            {
                                                Cdy.Tag.Runner4.RunInstance.ReStartDatabase();
                                            });
                                        }
                                        server.WriteByte(1);
                                        server.FlushAsync();
                                        // server.WaitForPipeDrain();
                                    }
                                    else
                                    {

                                    }
                                }
                                catch(Exception)
                                {
                                    break;
                                }
                            }
                        }
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine("StartMonitor"+ ex.Message);
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
