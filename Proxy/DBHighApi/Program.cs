﻿using Cdy.Tag;
using DBHighApi.Api;
using DBRuntime.Proxy;
using System;
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
            if (value)
            {

                int port = ReadServerPort();

                Cdy.Tag.LoggerService.Service.Info("DBHighAPI", string.Format(Res.Get("serverstartmsg"), port));
                DataService.Service.Start("127.0.0.1", port);
            }
            else
            {
                DataService.Service.Stop();
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
