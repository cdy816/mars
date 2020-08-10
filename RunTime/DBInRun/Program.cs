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

        static Thread mainThread;

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
                    Cdy.Tag.Runner.RunInstance.StartAsync(args[1], port);
                }
                else
                {
                    Cdy.Tag.Runner.RunInstance.Start();
                }
            }

            mainThread = Thread.CurrentThread;

            Console.WriteLine(Res.Get("HelpMsg"));
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
                    case "restart":
                        Task.Run(() => {
                            Cdy.Tag.Runner.RunInstance.ReStartDatabase();
                        });
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
                                    Console.WriteLine(Res.Get("AnyKeyToExit")+".....");

                                    //Task.Run(() => {
                                    //    Thread.Sleep(5000);

                                    //    System.Diagnostics.Process.GetCurrentProcess().Kill();
                                    //});

                                    break;
                                    //退出系统
                                }
                                else if(cmd==1)
                                {
                                    Cdy.Tag.Runner.RunInstance.ReStartDatabase();
                                    server.WriteByte(1);
                                    server.WaitForPipeDrain();
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
            re.AppendLine("h                // " + Res.Get("HMsg"));
            return re.ToString();
        }
    }
}
