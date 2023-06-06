//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/1/20 8:55:18.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class LoggerService
    {

        #region ... Variables  ...

        private ILog mLogger;

        /// <summary>
        /// 
        /// </summary>
        public static LoggerService Service = new LoggerService();

        private object mLockObj = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        public LoggerService()
        {
            mLogger = ServiceLocator.Locator.Resolve<ILog>();
            EnableLogger = true;

        }

        static LoggerService()
        {
            Name = System.Reflection.Assembly.GetEntryAssembly()?.GetName().Name;
            if (string.IsNullOrEmpty(Name))
            {
                Name = Assembly.GetExecutingAssembly()?.GetName().Name;
            }
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public bool EnableLogger { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public static string LogPath = "";

        /// <summary>
        /// 名称
        /// </summary>
        public static string Name = "";

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void GetLogger()
        {
            mLogger = ServiceLocator.Locator.Resolve<ILog>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="msg"></param>
        public void Debug(string name, string msg)
        {
            lock (mLockObj)
            {
                CheckStartRecord();
                if (EnableLogger)
                    mLogger?.Debug(name, msg);
                else
                {
                    Record(name, msg);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="msg"></param>
        /// <param name="parameter"></param>
        public void Debug(string name, string msg, object parameter)
        {
            lock (mLockObj)
            {
                CheckStartRecord();
                if (EnableLogger)
                    mLogger?.Debug(name, msg,parameter);
                else
                {
                    Record(name, msg);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="msg"></param>
        public void Warn(string name, string msg)
        {
            lock (mLockObj)
            {
                CheckStartRecord();
                if (EnableLogger)
                    mLogger?.Warn(name, msg);
                else
                {
                    Record(name, msg);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="msg"></param>
        public void Erro(string name, string msg)
        {
            lock (mLockObj)
            {
                CheckStartRecord();
                if (EnableLogger)
                    mLogger?.Erro(name, msg);
                else
                {
                    Record(name, msg);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="msg"></param>
        public void Info(string name, string msg)
        {
            lock (mLockObj)
            {
                CheckStartRecord();
                if (EnableLogger)
                    mLogger?.Info(name, msg);
                else
                {
                    Record(name, msg);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="msg"></param>
        /// <param name="parameter"></param>
        public void Info(string name,string msg,object parameter)
        {
            lock (mLockObj)
            {
                CheckStartRecord();
                if (EnableLogger)
                {
                    mLogger?.Info(name, msg, parameter);
                }
                else
                {
                    Record(name, msg);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="msg"></param>
        public void Record(string name,string msg)
        {
            lock (mLockObj)
            {
                CheckStartRecord();
                mLogger?.Record(name, msg);
            }
        }
        
        private void CheckStartRecord()
        {
            if (mLogger != null && !mLogger.IsRecordStart)
                mLogger.StartRecord();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            mLogger?.StopRecord();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
