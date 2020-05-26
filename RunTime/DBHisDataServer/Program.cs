using Cdy.Tag;
using System;
using System.IO.Pipes;
using System.Text;
using System.Threading.Tasks;

namespace DBHisDataServer
{
    class Program
    {
        static bool mIsClosed = false;

        static void Main(string[] args)
        {
            Console.CancelKeyPress += Console_CancelKeyPress;
            AppDomain.CurrentDomain.ProcessExit += CurrentDomain_ProcessExit;
            WindowConsolHelper.DisbleQuickEditMode();
            Console.WriteLine(Res.Get("WelcomeMsg"));
            if (args.Length > 0 && args[0] == "start")
            {
                if (args.Length > 1)
                {
                    Runner.Instance.Start(args[1]);
                }
                else
                {
                    Runner.Instance.Start("local");
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
                        if (Runner.Instance.IsStarted)
                        {
                            Runner.Instance.Stop();
                        }
                        mIsClosed = true;
                        break;
                    case "start":
                        if (cmd.Length > 1)
                        {
                            Runner.Instance.Start(cmd[1]);
                        }
                        else
                        {
                            Runner.Instance.Start("local");
                        }
                        Task.Run(() => {
                            StartMonitor(cmd.Length > 1 ? cmd[1] : "local");
                        });
                        break;
                    case "stop":
                        Runner.Instance.Stop();
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
            re.AppendLine("start [database] // " + Res.Get("StartMsg"));

            re.AppendLine("stop             // " + Res.Get("StopMsg"));
            re.AppendLine("exit              // ");
            re.AppendLine("h                // " + Res.Get("HMsg"));
            return re.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void CurrentDomain_ProcessExit(object sender, EventArgs e)
        {
            if (Runner.Instance.IsStarted)
            {
                Runner.Instance.Stop();
            }
        }

        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            if (Runner.Instance.IsStarted)
            {
                Runner.Instance.Stop();
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
                    using (var server = new NamedPipeServerStream(name+"h", PipeDirection.InOut))
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
                                    if (Runner.Instance.IsStarted)
                                    {
                                        Runner.Instance.Stop();
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
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
