﻿//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/4 12:16:14.
//  Version 1.0
//  种道洋
//==============================================================

using DBRuntime.His;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading;

namespace Cdy.Tag
{
    /// <summary>
    /// 日志管理系统
    /// </summary>
    public class LogManager3
    {

        #region ... Variables  ...
        private Thread mSaveThread;
        private bool mIsExit = false;

        private DateTime mStartTime;
        private DateTime mEndTime;
        private DateTime mBaseTime;

        private ManualResetEvent resetEvent = new ManualResetEvent(false);

        private ManualResetEvent closedEvent = new ManualResetEvent(false);

        private HisDataMemoryBlockCollection3 mNeedSaveMemory1;

        private string mLogDirector = string.Empty;

        private string mDatabase = string.Empty;

        private bool mIsChanged = false;

        private object mLocker = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public LogManager3()
        {

        }
        #endregion ...Constructor...

        #region ... Properties ...

        public IHisEngine3 Parent { get; set; }

        /// <summary>
        /// 文件时长
        /// </summary>
        public ushort TimeLen { get; set; } = 1;


        /// <summary>
        /// 
        /// </summary>
        public string Database { get { return mDatabase; } set { mDatabase = value; CheckLogDirector(); } }

        /// <summary>
        /// 
        /// </summary>
        public bool IsChanged 
        {
            get { return mIsChanged; }
            set
            {
                lock (mLocker)
                    mIsChanged = value;
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        

        /// <summary>
        /// 
        /// </summary>
        private void CheckLogDirector()
        {
            mLogDirector = PathHelper.helper.GetDataPath(mDatabase, "Log");
            if(!System.IO.Directory.Exists(mLogDirector))
            {
                System.IO.Directory.CreateDirectory(mLogDirector);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private string GetLogFilePath(DateTime starttime,DateTime endtime)
        {
            return System.IO.Path.Combine(mLogDirector, starttime.ToString("yyyyMMddHHmmss")+  ((int)((mEndTime - starttime).TotalSeconds)).ToString("D3")+".log"); ;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="memory"></param>
        public void RequestToSave(DateTime startTime,DateTime endTime,DateTime baseTime, HisDataMemoryBlockCollection3 memory)
        {
            mNeedSaveMemory1 = memory;
            mStartTime = startTime;
            mEndTime = endTime;
            mBaseTime = baseTime;
            resetEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            mSaveThread = new Thread(SaveProcess);
            mSaveThread.IsBackground = true;
            mSaveThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            mIsExit = true;
            resetEvent.Set();
            closedEvent.WaitOne();
        }

        /// <summary>
        /// 
        /// </summary>
        private void SaveProcess()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            while (!mIsExit)
            {
                resetEvent.WaitOne();
                resetEvent.Reset();
                if (mIsExit) break;

                Stopwatch sw = new Stopwatch();
                sw.Start();
                if(mNeedSaveMemory1!=null)
                {
                    mNeedSaveMemory1.MakeMemoryBusy();
                    RecordToFile();
                    mNeedSaveMemory1.MakeMemoryNoBusy();
                    ClearMemoryHisData(mNeedSaveMemory1);
                }
                CheckRemoveOldFiles();
                sw.Stop();
                LoggerService.Service.Info("LogManager", "记录"+ mNeedSaveMemory1.Name +"到日志文件 耗时" + sw.ElapsedMilliseconds + " ");
            }
            closedEvent.Set();
            LoggerService.Service.Info("LogManager", "退出!");

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public void ClearMemoryHisData(HisDataMemoryBlockCollection3 memory)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            while (memory.IsBusy()) ;
            memory.Clear();
            sw.Stop();
            LoggerService.Service.Info("Record", memory.Name + "清空数据区耗时:" + sw.ElapsedMilliseconds);
        }

        /// <summary>
        /// 
        /// </summary>
        private void RecordToFile()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            string fileName = GetLogFilePath(mStartTime, mEndTime);
            LoggerService.Service.Info("LogManager", "开始记日志录文件：" + fileName);

            var manager = DataFileSeriserManager.manager.GetDefaultFileSersie();
            manager.CreatOrOpenFile(fileName);
            var stream = manager.GetStream();

            //using (var stream = System.IO.File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
            //{
            stream.Write(BitConverter.GetBytes(TimeLen));         //写入文件时长

            stream.Write(BitConverter.GetBytes(mBaseTime.ToBinary()));

            long ltmp = stream.Position;
            int headsize = mNeedSaveMemory1.TagAddress.Count * 12 + 4;

            stream.Position = ltmp + headsize;
            //stream.Write(memory.Buffer, 0, memory.Position);      //写入文件头

            var vtmp = mNeedSaveMemory1.RecordDataToLog(stream);                 //写入内存数据

            //写入文件头数据
            stream.Position = ltmp;

            vtmp.WriteToStream(stream, 0, headsize);

            vtmp.Dispose();

            //}
            manager.Dispose();

            sw.Stop();
            LoggerService.Service.Info("LogManager", "日志文件：" + fileName + " 记录完成! 耗时:" + sw.ElapsedMilliseconds);

        }

        /// <summary>
        /// 
        /// </summary>
        private void CheckRemoveOldFiles()
        {
            System.IO.DirectoryInfo dinfo = new System.IO.DirectoryInfo(mLogDirector);
            Dictionary<DateTime, string> logFiles = new Dictionary<DateTime, string>();
            if(dinfo.Exists)
            {
                foreach(var vv in dinfo.EnumerateFileSystemInfos())
                {
                    string sfileName = vv.Name;
                    DateTime dt = new DateTime(int.Parse(sfileName.Substring(0, 4)), int.Parse(sfileName.Substring(4, 2)), int.Parse(sfileName.Substring(6, 2)), int.Parse(sfileName.Substring(8, 2)), int.Parse(sfileName.Substring(10, 2)), int.Parse(sfileName.Substring(12, 2)));
                    if(!logFiles.ContainsKey(dt))
                    logFiles.Add(dt, vv.FullName);
                }
            }

            
            if(logFiles.Count>9)
            {
                foreach(var vv in logFiles.OrderBy(e=>e.Key).Take(5))
                {
                    if(System.IO.File.Exists(vv.Value))
                    {
                        try
                        {
                            if(System.IO.File.GetAttributes(vv.Value) != FileAttributes.ReadOnly)
                            System.IO.File.Delete(vv.Value);
                        }
                        catch
                        {

                        }
                    }
                }
            }

        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
