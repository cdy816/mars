using Cdy.Tag;
using DBHighApi.Api;
using DBRuntime.Proxy;
using System;
using System.Linq;
using System.Runtime.InteropServices;
using System.Xml.Linq;

namespace DBHighApi
{
    class Program
    {
        private static bool mIsClosed = false;


        private static int ReadServerPort()
        {
            try
            {
                string spath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location), "Config", "DBHighApi.cfg");
                if (System.IO.File.Exists(spath))
                {
                    XElement xx = XElement.Load(spath);
                    if (xx.Element("Allow") != null)
                        Cdy.Tag.Common.ClientAuthorization.Instance.LoadAllowFromXML(xx.Element("Allow"));

                    if (xx.Element("Forbidden") != null)
                        Cdy.Tag.Common.ClientAuthorization.Instance.LoadForbiddenFromXML(xx.Element("Forbidden"));
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
            //if (!Console.IsInputRedirected)
            {
                Console.CancelKeyPress += Console_CancelKeyPress;
                AppDomain.CurrentDomain.ProcessExit += CurrentDomain_ProcessExit;
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                WindowConsolHelper.DisbleQuickEditMode();
                if (!Console.IsInputRedirected)
                    Console.Title = "DBHighApi";

                if (args.Contains("/m"))
                {
                    WindowConsolHelper.MinWindow("DBHighApi");
                }
            }

            Cdy.Tag.Common.ProcessMemoryInfo.Instances.StartMonitor("DBHighApi");

            Start();
            while (!mIsClosed)
            {
                string smd = Console.In.ReadLine();
                if (mIsClosed)
                {
                    break;
                }
                switch (smd)
                {
                    case "exit":
                        Console.WriteLine("DBHighApi","Ready to Exit....");
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
            if (value)
            {

                int port = ReadServerPort();

                Cdy.Tag.LoggerService.Service.Info("DBHighAPI", string.Format(Res.Get("serverstartmsg"), port));
                DataService.Service.Start(port);
            }
            else
            {
                DataService.Service.Pause(true);
                //DataService.Service.Stop();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private static void Stop()
        {
            DatabaseRunner.Manager.Close();
            DataService.Service.Stop();
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
