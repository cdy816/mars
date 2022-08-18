//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/1/20 9:03:14.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Cdy.Tag
{
    public class ConsoleLogger : ILog
    {

        #region ... Variables  ...

        string debugFormate = "Debug  {0,-16} {1,-26} {2}";

        string infoFormate  = "Info   {0,-16} {1,-26} {2}";

        string erroFormate  = "Erro   {0,-16} {1,-26} {2}";

        string warnFormate  = "Warn   {0,-16} {1,-26} {2}";

        string recordFormate = "{0,-16} {1,-26} {2}";

        private bool mStarted=false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public ConsoleLogger()
        {

        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public DateTime LastTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Queue<RecordLog> Buffer { get; set; } = new Queue<RecordLog>(100);


        /// <summary>
        /// 
        /// </summary>
        public bool IsRecordStart { get { return mStarted; } }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="msg"></param>
        public void Debug(string name, string msg)
        {
            Console.WriteLine(string.Format(debugFormate, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"), name, msg));
            Record(name, msg);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="msg"></param>
        /// <param name="parameter"></param>
        public void Debug(string name, string msg, object parameter)
        {
            Console.ForegroundColor = (ConsoleColor)(parameter);
            Console.WriteLine(string.Format(debugFormate, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"), name, msg));
            Console.ResetColor();
            Record(name, msg);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="msg"></param>
        public void Erro(string name, string msg)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(string.Format(erroFormate, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"), name, msg));
            Console.ResetColor();
            Record(name, msg);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="msg"></param>
        public void Info(string name, string msg)
        {
            Console.WriteLine(string.Format(infoFormate, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"), name, msg));
            Record(name, msg);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="msg"></param>
        /// <param name="parameter"></param>
        public void Info(string name, string msg, object parameter)
        {
            Console.ForegroundColor = (ConsoleColor)(parameter);
            Console.WriteLine(string.Format(infoFormate, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"), name, msg));
            Console.ResetColor();
            Record(name, msg);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="msg"></param>
        /// <exception cref="NotImplementedException"></exception>
        public void Record(string name, string msg)
        {
            lock (Buffer)
            {
                DateTime dnow = DateTime.Now;
                Buffer.Enqueue(new RecordLog() { Time = dnow, Message = string.Format(recordFormate, dnow.ToString("yyyy-MM-dd HH:mm:ss.fff"), name, msg.TrimStart()) });
            }
        }


        private Thread mSaveThread;
        private bool mIsExit = false;
        private DateTime mLastSaveDate;
        private System.IO.StreamWriter mLogWriter;

        /// <summary>
        /// 
        /// </summary>
        public void StartRecord()
        {
            mStarted = true;
            mSaveThread = new Thread(CheckSave);
            mSaveThread.IsBackground = true;
            mSaveThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void StopRecord()
        {
            mIsExit = true;
            mStarted = false;
            while (mSaveThread.IsAlive) ;
        }

        private void CheckSave()
        {
            RecordLog vd;
            while (!mIsExit)
            {
                if(Buffer.Count > 0)
                {
                    while(Buffer.Count > 0)
                    {
                        lock(Buffer)
                        vd = Buffer.Dequeue();
                        Save(vd);
                    }
                    mLogWriter?.Flush();
                }
                Thread.Sleep(2000);
            }
        }

        private void Save(RecordLog log)
        {
            if (mLastSaveDate != log.Time.Date)
            {
                mLastSaveDate = log.Time.Date;
                if (mLogWriter != null) mLogWriter.Close();

                string spath = string.IsNullOrEmpty(LoggerService.LogPath) ? System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "Log") : LoggerService.LogPath;

                if (!System.IO.Directory.Exists(spath))
                {
                    System.IO.Directory.CreateDirectory(spath);
                }
                spath = System.IO.Path.Combine(spath, LoggerService.Name +"_"+ log.Time.Date.ToString("yyyyMMdd") + ".txt");
                mLogWriter = new System.IO.StreamWriter(System.IO.File.Open(spath, System.IO.FileMode.OpenOrCreate, System.IO.FileAccess.ReadWrite, System.IO.FileShare.ReadWrite));
                mLogWriter.BaseStream.Position = mLogWriter.BaseStream.Length;
            }

            mLogWriter.WriteLine(log.Message);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="msg"></param>
        public void Warn(string name, string msg)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(string.Format(warnFormate, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"), name, msg));
            Console.ResetColor();
        }

        

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }

    public struct RecordLog
    {
        /// <summary>
        /// 
        /// </summary>
        public DateTime Time { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string Message { get; set; }
    }
}
