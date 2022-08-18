using Cdy.Tag;
using DBRuntime.Proxy;
using System.Xml.Linq;

namespace DBOpcServer
{
    internal class Program
    {
        private static bool mIsClosed = false;

        private static bool mNoneSecurityMode=false;

        private static int ReadServerPort()
        {
            try
            {
                string spath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location), "Config", "DBOpcServer.cfg");
                if (System.IO.File.Exists(spath))
                {
                    XElement xx = XElement.Load(spath);

                    mNoneSecurityMode = bool.Parse(xx.Attribute("NoneSecurityMode").Value);

                    return int.Parse(xx.Attribute("ServerPort")?.Value);
                }
            }
            catch
            {

            }
            return 14332;
        }

        static void Main(string[] args)
        {
            Console.CancelKeyPress += Console_CancelKeyPress;
            AppDomain.CurrentDomain.ProcessExit += CurrentDomain_ProcessExit;
            WindowConsolHelper.DisbleQuickEditMode();

            Console.Title = "DBOpcServer";
            if (args.Contains("/m"))
            {
                WindowConsolHelper.MinWindow("DBOpcServer");
            }

            OPCServer.Server.Port = ReadServerPort();

            Start();
            while (!mIsClosed)
            {
                string smd = Console.ReadLine();
                if (mIsClosed)
                {
                    break;
                }
                switch (smd)
                {
                    case "exit":
                        Stop();
                        mIsClosed = true;
                        break;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private static void Start()
        {
            DatabaseRunner.Manager.Load();
            DatabaseRunner.Manager.Start();
            DatabaseRunner.Manager.IsReadyEvent += Manager_IsReadyEvent;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        private static void Manager_IsReadyEvent(bool value)
        {
            Task.Run(() => {
                if (value)
                {
                    OPCServer.Server.NoneSecurityMode = mNoneSecurityMode;
                    OPCServer.Server.Start();
                    //start opc server
                }
                else
                {
                    //stop opc server
                    OPCServer.Server.Stop();
                }
            });
            
        }


        /// <summary>
        /// 
        /// </summary>
        private static void Stop()
        {
            DatabaseRunner.Manager.Close();
            //stop opc server
            OPCServer.Server.Stop();
            DatabaseRunner.Manager.IsReadyEvent -= Manager_IsReadyEvent;
            mIsClosed = true;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void CurrentDomain_ProcessExit(object sender, EventArgs e)
        {
            Stop();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            Stop();
        }
    }
}