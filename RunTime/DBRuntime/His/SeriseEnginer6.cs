//==============================================================
//  Copyright (C) 2021  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2021/02/18 10:35:02.
//  Version 1.0
//  种道洋
//  较SeriseEnginer6 修改数据块指针为：固定的100000个变量的，单个变量每5分钟一个数据块指针的结构
//==============================================================
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Buffers;
using DBRuntime.His;
using System.Collections;
using System.Drawing;
using System.Runtime.InteropServices;

/*
    *  一个历史文件包括：文件头文件(*.dbm2)+数据文件文件(*.dbd2)
    * ****** DBM 文件头结构 *********
    * FileHead(98)+ DataRegionPointer
    * FileHead : DateTime(8)+LastUpdateDatetime(8)+MaxtTagCount(4)+file duration(4)+block duration(4)+Time tick duration(4)+Version(2)+DatabaseName(64)
    * DataRegionPointer:[Tag1 DataPointer1(8)+...+Tag1 DataPointerN(8)(DataRegionCount)]...[Tagn DataPointer1(8)+...+Tagn DataPointerN(8)(DataRegionCount)](MaxTagCount)
    * 
    * ****** DBD2 数据文件结构 *******
    * 多个数据块组成
    * [[Tag1 DataBlock Area1]...[Tag2 DataBlock Area2]]...[[Tag1 DataBlock AreaN]...[Tag2 DataBlock AreaN]]
    * DataBlock Area: Block Header+Block Data
    * Block Header:  NextBlockAddress(5)(同一个数据区间有多个数据块时，之间通过指针关联)+DataSize(4)+ValueType(5b)+CompressType(3b)
    * Block Data: 
*/

namespace Cdy.Tag
{
    /// <summary>
    /// 序列话引擎
    /// </summary>
    public class SeriseEnginer6 : IDataSerialize4, IHisDataManagerService, IDisposable
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private ManualResetEvent resetEvent;

        private ManualResetEvent closedEvent;

        private Thread mCompressThread;

        private Thread mDatabackThread;

        private Thread mHisFileReArrangeThread;

        private bool mIsClosed = false;

        private bool mIsBackupFinished = false;

        //private bool mIsHisFileReArrangeFinish = false;

        private Dictionary<int, CompressMemory4> mWaitForProcessMemory = new Dictionary<int, CompressMemory4>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, SeriseFileItem6> mSeriserFiles = new Dictionary<int, SeriseFileItem6>();

        /// <summary>
        /// 
        /// </summary>
        private int mLastBackupHour=-1;

        private bool mIsBusy = false;

        private StatisticsMemoryMap mStatisticsMemory;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public SeriseEnginer6()
        {
            ServiceLocator.Locator.Registor<IHisDataManagerService>(this);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 单个文件存储数据的时间长度
        /// 单位小时
        /// 最大24小时
        /// </summary>
        public int FileDuration { get; set; }

        /// <summary>
        /// 单个块持续时间
        /// 单位分钟
        /// </summary>
        public int BlockDuration { get; set; }

        /// <summary>
        /// 单个文件内变量的个数
        /// </summary>
        public int TagCountOneFile { get; set; } = 100000;

        /// <summary>
        /// 数据库名称
        /// </summary>
        public string DatabaseName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string DataSeriser { get; set; }

        /// <summary>
        /// 保持历史文件不执行 Zip 压缩的时间
        /// </summary>
        public static double KeepNoZipFileDays { get; set; } = -1;

        /// <summary>
        /// 主历史记录路径
        /// </summary>
        public static string HisDataPathPrimary { get; set; }

        /// <summary>
        /// 备份历史记录路径
        /// </summary>
        public static string HisDataPathBack { get; set; }


        /// <summary>
        /// 当前工作的历史记录路径
        /// </summary>
        public static string HisDataPath { get; set; }

        /// <summary>
        /// 历史数据在主目录里保留时间
        /// 单位天
        /// </summary>
        public static int HisDataKeepTimeInPrimaryPath { get; set; } = 30;



        #endregion ...Properties...

        #region ... Methods    ...


        /// <summary>
        /// 选择历史记录路径
        /// </summary>
        /// <returns></returns>
        private string SelectHisDataPath()
        {
            if (string.IsNullOrEmpty(HisDataPathPrimary))
            {
                return PathHelper.helper.GetDataPath(this.DatabaseName, "HisData");
            }
            else
            {
                return System.IO.Path.IsPathRooted(HisDataPathPrimary) ? HisDataPathPrimary : PathHelper.helper.GetDataPath(this.DatabaseName, HisDataPathPrimary);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private string GetBackupDataPath()
        {
            if(string.IsNullOrEmpty(HisDataPathBack))
            {
                return string.Empty;
            }
            else
            {
                return System.IO.Path.IsPathRooted(HisDataPathBack) ? HisDataPathBack : PathHelper.helper.GetDataPath(this.DatabaseName, HisDataPathBack);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            var his = ServiceLocator.Locator.Resolve<IHisEngine3>();
            var histag = his.ListAllTags().OrderBy(e => e.Id);

            mStatisticsMemory = new StatisticsMemoryMap();

            //计算数据区域个数
            var mLastDataRegionId = -1;

            SeriseFileItem6 mcurrentItem=null;

            foreach (var vv in histag)
            {
                var id = vv.Id;
                var did = id / TagCountOneFile;
                if (mLastDataRegionId != did)
                {
                    mcurrentItem = new SeriseFileItem6() { FileDuration = FileDuration, BlockDuration = BlockDuration, TagCountOneFile = TagCountOneFile, DatabaseName = DatabaseName, Id = did, StatisticsMemory = mStatisticsMemory };
                    mSeriserFiles.Add(did, mcurrentItem);
                    mLastDataRegionId = did;
                }
                mcurrentItem.MaxTagId = Math.Max(mcurrentItem.MaxTagId, id % TagCountOneFile + 1);
            }

            foreach (var vv in mSeriserFiles)
            {
                vv.Value.FileWriter = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                vv.Value.MetaFileWriter = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                vv.Value.FileWriter2 = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                vv.Value.MetaFileWriter2 = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                //vv.Value.Init();
            }

            HisDataPath = SelectHisDataPath();
        }

        /// <summary>
        /// 
        /// </summary>
        public void ReInit()
        {
            var his = ServiceLocator.Locator.Resolve<IHisEngine3>();
            var histag = his.ListAllTags().OrderBy(e => e.Id);
            //计算数据区域个数
            var mLastDataRegionId = -1;
            SeriseFileItem6 mcurrentItem;
            foreach (var vv in histag)
            {
                var id = vv.Id;
                var did = id / TagCountOneFile;
                if (mLastDataRegionId != did)
                {
                    if (mSeriserFiles.ContainsKey(did))
                    {
                        mcurrentItem=mSeriserFiles[did];
                    }
                    else
                    {
                        mcurrentItem = new SeriseFileItem6() { FileDuration = FileDuration, BlockDuration = BlockDuration, TagCountOneFile = TagCountOneFile, DatabaseName = DatabaseName, Id = did, StatisticsMemory = mStatisticsMemory };
                        mcurrentItem.FileWriter = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                        mcurrentItem.FileWriter2 = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                        mcurrentItem.MetaFileWriter= DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                        mcurrentItem.MetaFileWriter2 = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();

                        mSeriserFiles.Add(did, mcurrentItem);
                    }
                    mcurrentItem.MaxTagId = Math.Max(mcurrentItem.MaxTagId, id % TagCountOneFile + 1);
                 
                    mLastDataRegionId = did;
                }
            }

            //foreach (var vv in mSeriserFiles)
            //{
            //    vv.Value.Init();
            //}
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagCount"></param>
        public void CheckAndAddSeriseFile(IEnumerable<int> tagIds)
        {
            lock (mSeriserFiles)
            {
                foreach (var tagId in tagIds)
                {
                    var did = tagId / TagCountOneFile;
                    if (!mSeriserFiles.ContainsKey(did))
                    {
                        var sf = new SeriseFileItem6() { FileDuration = FileDuration, BlockDuration = BlockDuration, TagCountOneFile = TagCountOneFile, DatabaseName = DatabaseName, Id = did, StatisticsMemory = mStatisticsMemory };
                        sf.FileWriter = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                        sf.FileWriter2 = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                        sf.MetaFileWriter2 = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                        sf.MetaFileWriter= DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();

                        //sf.Init();
                        mSeriserFiles.Add(did, sf);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            LoggerService.Service.Info("SeriseEnginer6", "开始启动");
            mIsClosed = false;
            resetEvent = new ManualResetEvent(false);
            closedEvent = new ManualResetEvent(false);
            mCompressThread = new Thread(ThreadPro);
            mCompressThread.IsBackground = true;
            mCompressThread.Start();

            //mDatabackThread = new Thread(DatabackupThreadPro);
            //mDatabackThread.IsBackground = true;
            //mDatabackThread.Start();

            mHisFileReArrangeThread = new Thread(DataFileReArrangeThreadPro);
            mHisFileReArrangeThread.IsBackground = true;
            mHisFileReArrangeThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            LoggerService.Service.Info("SeriseEnginer6", "开始停止...");
            
            if(IsNeedSave())
            {
                LoggerService.Service.Info("SeriseEnginer6", "等待存储完成....");
                WaitForExecuteCompletely();
            }

            mIsClosed = true;
            resetEvent.Set();
            closedEvent.WaitOne();

            resetEvent.Dispose();
            resetEvent=null;

            closedEvent.Dispose();
            closedEvent = null;

            LoggerService.Service.Info("SeriseEnginer6", "等待备份线程关闭....");
            while (!mIsBackupFinished) Thread.Sleep(1);
            LoggerService.Service.Info("SeriseEnginer6", "停止完成...");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataMemory"></param>
        /// <param name="date"></param>
        public void RequestToSeriseFile(CompressMemory4 dataMemory)
        {
            lock (mWaitForProcessMemory)
            {
                if (mWaitForProcessMemory.ContainsKey(dataMemory.Id))
                {
                    mWaitForProcessMemory[dataMemory.Id] = dataMemory;
                }
                else
                {
                    mWaitForProcessMemory.Add(dataMemory.Id, dataMemory);
                }
            }
            //mCurrentTime = dataMemory.CurrentTime;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="data"></param>
        /// <param name="size"></param>
        public void ManualRequestToSeriseFile(IMemoryBlock data)
        {
            HisDataPath = SelectHisDataPath();

            int id = data.ReadInt(56);

            lock (mSeriserFiles)
            {
                foreach (var vv in mSeriserFiles)
                {
                    if (id >= vv.Value.IdStart && id < vv.Value.IdEnd)
                    {
                        vv.Value.AppendManualSeriseFile(id, data);
                        break;
                    }
                }
            }
        }

        private bool IsNeedSave()
        {
            bool re= mWaitForProcessMemory.Count > 0;
            foreach (var vv in mSeriserFiles)
            {
                re |= vv.Value.HasManualRecordData;
            }
            return re;
        }

        private void WaitForExecuteCompletely()
        {
            if (resetEvent!=null)
            {
                var vount = mExecuteCount;
                RequestToSave();
                while (mExecuteCount == vount) Thread.Sleep(100);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void RequestToSave()
        {
            lock (resetEvent)
                resetEvent.Set();
        }

        /// <summary>
        /// 执行存储次数
        /// </summary>
        private int mExecuteCount = 0;

        /// <summary>
        /// 
        /// </summary>

        private void ThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            while (!mIsClosed)
            {
                try
                {
                    resetEvent.WaitOne();
                    lock (resetEvent)
                        resetEvent.Reset();

                    if (mIsClosed)
                    {
                        if (!IsNeedSave())
                        {
                            mExecuteCount++;
                            break;
                        }
                    }

                    //HisDataArrange4.Arrange.Paused();
                    mIsBusy = true;
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    LoggerService.Service.Info("SeriseEnginer6", "********开始执行存储********", ConsoleColor.Cyan);

                    if (mWaitForProcessMemory.Count > 0)
                    {
                        SaveToFile();
                    }

                    lock (mSeriserFiles)
                    {
                        foreach (var vv in mSeriserFiles)
                        {
                            if (vv.Value.HasManualRecordData)
                                vv.Value.FreshManualDataToDisk();
                        }
                    }

                    //LogStorageManager.Instance.ReleaseManualLogs();

                    sw.Stop();
                    LoggerService.Service.Info("SeriseEnginer6", ">>>>>>>>>完成执行存储>>>>>>>  ElapsedMilliseconds:" + sw.ElapsedMilliseconds, ConsoleColor.Cyan);

                    mIsBusy = false;
                }
                catch (Exception ex)
                {
                    LoggerService.Service.Erro("SeriseEnginer6", $"{ex.Message} {ex.StackTrace}");
                }
                mExecuteCount++;


                //HisDataArrange4.Arrange.Resume();
            }
            closedEvent.Set();
        }

        private DateTime mLastDataFileReArrangeProcessTime;

        /// <summary>
        /// 
        /// </summary>
        private void DataFileReArrangeThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            while (!mIsClosed)
            {
                var wpath = SelectHisDataPath();

                if ((DateTime.Now - mLastDataFileReArrangeProcessTime).TotalSeconds > 60)
                {
                    if (System.IO.Directory.Exists(wpath))
                    {
                        try
                        {
                            if (KeepNoZipFileDays >= 0)
                                CheckAndZipHisFile(wpath);

                        }
                        catch (Exception ex)
                        {
                            LoggerService.Service.Erro("SeriseEnginer6", "DataFileReArrangeThreadPro: " + ex.Message);
                        }
                    }
                    mLastDataFileReArrangeProcessTime = DateTime.Now;
                }

                ProcessBackUp();

                Thread.Sleep(1000);
            }
            
            mIsBackupFinished = true;

            //mIsHisFileReArrangeFinish = true;
        }

        /// <summary>
        /// Zip .his file to save disk
        /// </summary>
        /// <param name="wpath"></param>
        private void CheckAndZipHisFile(string wpath)
        {
            foreach (var vv in new System.IO.DirectoryInfo(wpath).GetFiles("*.db*2"))
            {
                if (mIsClosed) break;
                while (mIsBusy) Thread.Sleep(1000);

                if (mIsClosed) break;

                string file = vv.FullName;

                if (System.IO.File.Exists(file))
                {

                    var vtime = GetTime(System.IO.Path.GetFileNameWithoutExtension(vv.Name), out bool isavaiable);
                    if (!isavaiable) continue;

                    if((DateTime.UtcNow - vtime).TotalDays>KeepNoZipFileDays)
                    {
                        ZipFile(file);
                    }

                    //System.IO.FileInfo finfo = new System.IO.FileInfo(file);

                    //if ((DateTime.Now - finfo.LastWriteTime).TotalDays > KeepNoZipFileDays)
                    //{
                    //    //保留7天的His格式的数据
                    //    ZipFile(finfo.FullName);
                    //}
                }

            }

            //清空二次压缩后，在进行查询时产生的临时文件目录的文件
            string spath = System.IO.Path.Combine(wpath, "tmp");
            if (System.IO.Directory.Exists(spath))
            {
                foreach (var vv in new System.IO.DirectoryInfo(spath).GetFiles())
                {
                    if (mIsClosed) break;
                    while (mIsBusy) Thread.Sleep(1000);

                    string file = vv.FullName;

                    if (System.IO.File.Exists(file))
                    {
                        System.IO.FileInfo finfo = new System.IO.FileInfo(file);
                        if ((DateTime.Now - finfo.LastWriteTime).TotalDays > 7)
                        {
                            try
                            {
                                finfo.Delete();
                            }
                            catch
                            {

                            }
                        }

                    }
                }
            }

        }

        /// <summary>
        /// 压缩文件
        /// </summary>
        /// <param name="sfile"></param>
        private void ZipFile(string sfile)
        {
            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                string tfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(sfile), System.IO.Path.GetFileNameWithoutExtension(sfile) + ".z" + System.IO.Path.GetExtension(sfile).Replace(".", ""));
                using (System.IO.Compression.BrotliStream bs = new System.IO.Compression.BrotliStream(System.IO.File.Create(tfile),System.IO.Compression.CompressionLevel.Fastest))
                {
                    using (var vss = System.IO.File.Open(sfile, System.IO.FileMode.Open, System.IO.FileAccess.Read,System.IO.FileShare.ReadWrite))
                    {
                        vss.CopyTo(bs);
                        vss.Flush();
                        vss.Close();
                    }
                    bs.Flush();
                    bs.Close();
                }
                System.IO.File.Delete(sfile);
                HisQueryManager.Instance.GetFileManager(DatabaseName).UpdateFile(sfile);
                HisQueryManager.Instance.GetFileManager(DatabaseName).UpdateFile(tfile);
                sw.Stop();
                LoggerService.Service.Info("SeriseEnginer6", "Zip 压缩文件 " +tfile +" 耗时:"+sw.ElapsedMilliseconds);
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("SeriseEnginer6", "ZipFile: " + ex.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        private double GetDriverUsedPercent(string path)
        {
            System.IO.DriveInfo dinfo = new System.IO.DriveInfo(System.IO.Path.GetPathRoot(path));
            return dinfo.AvailableFreeSpace * 1.0 / dinfo.TotalSize;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        private double GetDriverFreeSize(string path)
        {
            System.IO.DriveInfo dinfo = new System.IO.DriveInfo(System.IO.Path.GetPathRoot(path));
            return dinfo.AvailableFreeSpace;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="isavaiable"></param>
        /// <returns></returns>
        private DateTime GetTime(string fileName,out bool isavaiable)
        {
            if(fileName.Length<=12)
            {
                isavaiable = false;
                return DateTime.MinValue;
            }
            string stime = fileName.Substring(fileName.Length - 12, 12);
            int yy = 0, mm = 0, dd = 0;

            if (!int.TryParse(stime.Substring(0, 4), out yy))
            {
                isavaiable = false;
                return DateTime.MinValue;
            }

            if (!int.TryParse(stime.Substring(4, 2), out mm))
            {
                isavaiable = false;
                return DateTime.MinValue;
            }

            if (!int.TryParse(stime.Substring(6, 2), out dd))
            {
                isavaiable = false;
                return DateTime.MinValue;
            }
            isavaiable = true;
            return new DateTime(yy,mm,dd);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="isavaiable"></param>
        /// <returns></returns>
        private DateTime GetStadTime(string fileName, out bool isavaiable)
        {
            if (fileName.Length <= 8)
            {
                isavaiable = false;
                return DateTime.MinValue;
            }
            string stime = fileName.Substring(fileName.Length - 8, 8);
            int yy = 0, mm = 0, dd = 0;

            if (!int.TryParse(stime.Substring(0, 4), out yy))
            {
                isavaiable = false;
                return DateTime.MinValue;
            }

            if (!int.TryParse(stime.Substring(4, 2), out mm))
            {
                isavaiable = false;
                return DateTime.MinValue;
            }

            if (!int.TryParse(stime.Substring(6, 2), out dd))
            {
                isavaiable = false;
                return DateTime.MinValue;
            }
            isavaiable = true;
            return new DateTime(yy, mm, dd);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="backpath"></param>
        /// <param name="watchpath"></param>
        /// <param name="vv"></param>
        /// <param name="filetime"></param>
        private void CheckAndCopyFile(string backpath,string watchpath,System.IO.FileInfo vv,DateTime filetime)
        {
            var time = DateTime.Now;

            try
            {
                if ((time - filetime).TotalDays >= HisDataKeepTimeInPrimaryPath)
                {

                    if (!string.IsNullOrEmpty(backpath))
                    {
                        if (GetDriverFreeSize(backpath) > vv.Length)
                        {
                            string filename = System.IO.Path.Combine(backpath, vv.Name);
                            vv.CopyTo(filename,true);

                            HisQueryManager.Instance.GetFileManager(DatabaseName).UpdateFile(filename);

                            vv.Delete();

                            if (!string.IsNullOrEmpty(backpath))
                            {
                                if (GetDriverUsedPercent(backpath) < 0.2)
                                {
                                    LoggerService.Service.Warn("SeriseEnginer6", "free disk space is lower in backup path");
                                }
                            }
                        }
                        else
                        {
                            if (GetDriverUsedPercent(watchpath) < 0.05)
                            {
                                vv.Delete();
                            }
                            LoggerService.Service.Erro("SeriseEnginer6", "There is not enough space for backup! free size:" + (GetDriverFreeSize(backpath) / 1024.0 / 1024) + "M. required size:" + (vv.Length / 1024.0 / 1024) + " M");
                        }
                    }
                    else
                    {
                        vv.Delete();
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("SeriseEnginer6", ex.Message + " " + ex.StackTrace);
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        //private void DatabackupThreadPro()
        //{
        //    ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);

        //    int count = 0;

        //    while (!mIsClosed)
        //    {
        //        var wpath = SelectHisDataPath();

        //        var time = DateTime.Now;

        //        if (time.Day != mLastBackupDay)
        //        {
        //            mLastBackupDay = time.Day;

        //            try
        //            {
        //                string backpath = GetBackupDataPath();

        //                if (!string.IsNullOrEmpty(backpath))
        //                {

        //                    if(!System.IO.Directory.Exists(backpath))
        //                    {
        //                        System.IO.Directory.CreateDirectory(backpath);
        //                    }

        //                    if (GetDriverUsedPercent(backpath) < 0.2)
        //                    {
        //                        LoggerService.Service.Warn("SeriseEnginer6", "free disk space is lower in backup path");
        //                    }

        //                    if (System.IO.Directory.Exists(wpath))
        //                    {
        //                        foreach (var vv in new System.IO.DirectoryInfo(wpath).GetFiles("*.*db*2"))
        //                        {
        //                            if (mIsClosed) break;

        //                            var vtime = GetTime(System.IO.Path.GetFileNameWithoutExtension(vv.Name), out bool isavaiable);
        //                            if (!isavaiable) continue;

        //                            CheckAndCopyFile(backpath, wpath, vv, vtime);
        //                        }

        //                        foreach (var vv in new System.IO.DirectoryInfo(wpath).GetFiles("*.stad"))
        //                        {
        //                            if (mIsClosed) break;

        //                            var vtime = GetStadTime(System.IO.Path.GetFileNameWithoutExtension(vv.Name), out bool isavaiable);
        //                            if (!isavaiable) continue;

        //                            CheckAndCopyFile(backpath, wpath, vv, vtime);
        //                        }
        //                    }

        //                }

        //            }
        //            catch
        //            {

        //            }
        //        }


        //        while (count < 10 * 60 * 10 && !mIsClosed)
        //        {
        //            Thread.Sleep(100);
        //            count++;
        //        }
        //        count = 0;
        //    }

        //    mIsBackupFinished = true;
        //}

        private void ProcessBackUp()
        {
            var wpath = SelectHisDataPath();

            var time = DateTime.Now;

            if (time.Hour != mLastBackupHour)
            {
                mLastBackupHour = time.Hour;

                try
                {
                    string backpath = GetBackupDataPath();

                    if (!string.IsNullOrEmpty(backpath))
                    {

                        if (!System.IO.Directory.Exists(backpath))
                        {
                            System.IO.Directory.CreateDirectory(backpath);
                        }

                        if (GetDriverUsedPercent(backpath) < 0.2)
                        {
                            LoggerService.Service.Warn("SeriseEnginer6", "free disk space is lower in backup path");
                        }

                        if (System.IO.Directory.Exists(wpath))
                        {
                            foreach (var vv in new System.IO.DirectoryInfo(wpath).GetFiles("*.*db*2"))
                            {
                                if (mIsClosed) break;

                                var vtime = GetTime(System.IO.Path.GetFileNameWithoutExtension(vv.Name), out bool isavaiable);
                                if (!isavaiable) continue;

                                CheckAndCopyFile(backpath, wpath, vv, vtime);
                            }

                            foreach (var vv in new System.IO.DirectoryInfo(wpath).GetFiles("*.stad"))
                            {
                                if (mIsClosed) break;

                                var vtime = GetStadTime(System.IO.Path.GetFileNameWithoutExtension(vv.Name), out bool isavaiable);
                                if (!isavaiable) continue;

                                CheckAndCopyFile(backpath, wpath, vv, vtime);
                            }
                        }

                    }

                }
                catch
                {

                }
            }
        }

        /// <summary>
        /// 执行存储到磁盘
        /// </summary>
        private void SaveToFile()
        {
            /*
             1. 检查变量ID是否变动，如果变动则重新记录变量的ID列表
             2. 拷贝数据块
             3. 更新数据块指针
             */

            HisDataPath = SelectHisDataPath();
            List<CompressMemory4> mtmp;
            lock (mWaitForProcessMemory)
            {
                mtmp = mWaitForProcessMemory.Values.ToList();
                mWaitForProcessMemory.Clear();
            }

            DateTime dt = DateTime.MinValue;
            foreach (var vv in mtmp)
            {
                mSeriserFiles[vv.Id].SaveToFile(vv, vv.CurrentTime, vv.EndTime);

                if (dt == DateTime.MinValue)
                    dt = vv.CurrentTime.AddMinutes((vv.EndTime - vv.CurrentTime).TotalMinutes / 2);

                vv.Clear();
                vv.MakeMemoryNoBusy();
            }

            LogStorageManager.Instance.ReleaseSystemLog(dt);


        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            if (mSeriserFiles != null)
            {
                lock (mSeriserFiles)
                {
                    foreach (var vv in mSeriserFiles)
                    {
                        vv.Value.Dispose();
                    }
                    mSeriserFiles.Clear();
                }
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="time"></param>
        ///// <returns></returns>
        ///// <exception cref="NotImplementedException"></exception>
        //public string NewHisFile(int id, DateTime time)
        //{
        //    int idp = id / TagCountOneFile;

        //    if(mSeriserFiles.ContainsKey(idp))
        //    {
        //        return mSeriserFiles[idp].NewHisFile(time);
        //    }
        //    return string.Empty;
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public string GetHisFileName(int id,DateTime time)
        {
            int idp = id / TagCountOneFile;
            lock (mSeriserFiles)
            {
                if (mSeriserFiles.ContainsKey(idp))
                {
                    return mSeriserFiles[idp].GetHisFileName(time);
                }
                else
                {
                    var mcurrentItem = new SeriseFileItem6() { FileDuration = FileDuration, BlockDuration = BlockDuration, TagCountOneFile = TagCountOneFile, DatabaseName = DatabaseName, Id = idp, StatisticsMemory = mStatisticsMemory };
                    mcurrentItem.FileWriter = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                    mcurrentItem.FileWriter2 = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                    mcurrentItem.MetaFileWriter = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                    mcurrentItem.MetaFileWriter2 = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();

                    mcurrentItem.MaxTagId = id % TagCountOneFile;

                    mSeriserFiles.Add(idp, mcurrentItem);

                    return mcurrentItem.GetHisFileName(time);

                }
            }
            return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        public void Take(string file)
        {
            HisDataFileLocker.Locker.Take(file);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        public void Release(string file)
        {
            HisDataFileLocker.Locker.Relase(file);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="data"></param>
        /// <param name="saveType"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public bool SaveData(int id, DateTime time, MarshalMemoryBlock data, SaveType saveType,string filename)
        {
            int idp = id / TagCountOneFile;

            if (mSeriserFiles.ContainsKey(idp))
            {
                return mSeriserFiles[idp].SaveData(id%TagCountOneFile, time, data, saveType,filename);
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool ClearTmpFile()
        {
            foreach(var vv in mSeriserFiles)
            {
                vv.Value.ClearTmp();
            }
            return true;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }

    /// <summary>
    /// 
    /// </summary>
    public class SeriseFileItem6 : IDisposable
    {
        #region ... Variables  ...

        /// <summary>
        /// 变量的数据指针的相对起始地址
        /// </summary>
        private Dictionary<int, long> mIdAddrs = new Dictionary<int, long>();
        /// <summary>
        /// 
        /// </summary>
        private bool mNeedRecordDataHeader = true;

        ///// <summary>
        ///// 当前数据区首地址
        ///// </summary>
        //private long mCurrentDataRegion = 0;


        private string mCurrentFileName;
        //private string mMetaCurrentFileName;

        private DataFileSeriserbase mFileWriter;

        private DataFileSeriserbase mMetaFileWriter;

        private DataFileSeriserbase mFileWriter2;

        private DataFileSeriserbase mMetaFileWriter2;

        /// <summary>
        /// 数据文件扩展名
        /// </summary>
        public const string DataFileExtends = ".dbd2";

        public const string DataFileMetaExtends = ".dbm2";

        /// <summary>
        /// 整理后数据文件扩展名
        /// </summary>
        public const string HisDataFileExtends = ".his";

        /// <summary>
        /// 日统计文件扩展名
        /// </summary>
        public const string DayStatisticsFileExtends = ".stad";

        /// <summary>
        /// 文件头大小
        /// </summary>
        public const int FileHeadSize = 98;


        //private DateTime mCurrentTime;

        static object mFileLocker = new object();

        private Dictionary<string, Queue<IMemoryBlock>> mManualHisDataCach = new Dictionary<string, Queue<IMemoryBlock>>();

        private int mId = 0;

        private int mMaxTagId;

        private DataFileSeriserbase mStatisticsWriter;

        private MetaFileBlock mCurrentMetaBlock;

        private MetaFileBlock mManuaMetaBlock;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public StatisticsMemoryMap StatisticsMemory { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int IdStart { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int IdEnd { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Id { get { return mId; } set { mId = value; IdStart = value * TagCountOneFile;IdEnd = (value + 1) * TagCountOneFile; } }

        /// <summary>
        /// 
        /// </summary>
        public DataFileSeriserbase FileWriter { get { return mFileWriter; } set { mFileWriter = value; } }


        public DataFileSeriserbase MetaFileWriter { get { return mMetaFileWriter; } set { mMetaFileWriter = value; } }

        /// <summary>
        /// 
        /// </summary>
        public DataFileSeriserbase FileWriter2 { get { return mFileWriter2; } set { mFileWriter2 = value; } }


        public DataFileSeriserbase MetaFileWriter2 { get { return mMetaFileWriter2; } set { mMetaFileWriter2 = value; } }


        /// <summary>
        /// 
        /// </summary>
        public int FileStartHour { get; set; }

        /// <summary>
        /// 单个文件内变量的个数
        /// </summary>
        public int TagCountOneFile { get; set; }

        /// <summary>
        /// 单个块持续时间
        /// 单位分钟
        /// </summary>
        public int BlockDuration { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int FileDuration { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string DatabaseName { get; set; }

        public bool IsNeedInit { get; set; }

        /// <summary>
        /// 是否需要执行无损Zip压缩
        /// </summary>
        public bool IsEnableCompress { get; set; } = false;

        /// <summary>
        /// 
        /// </summary>
        public bool HasManualRecordData
        {
            get
            {
                return mManualHisDataCach.Count > 0;
            }
        }

        

        public int MaxTagId { get { return mMaxTagId; } set { mMaxTagId = value; mNeedRecordDataHeader = true; } }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sfile"></param>
        /// <param name="ignorlog"></param>
        private void Take(string sfile,bool enablelog=true)
        {
            if(enablelog)
            {
                LoggerService.Service.Info("HisDataFileLocker", $"加锁文件: { sfile }", ConsoleColor.Green);
            }
            ServiceLocator.Locator.Resolve<IHisDataManagerService>().Take(sfile);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sfile"></param>
        /// <param name="ignorlog"></param>
        public void Release(string sfile, bool enablelog = true)
        {
            if(enablelog)
            {
                LoggerService.Service.Info("HisDataFileLocker", $"解锁文件: { sfile }", ConsoleColor.Green);
            }
            ServiceLocator.Locator.Resolve<IHisDataManagerService>().Release(sfile);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public string GetHisFileName(DateTime time)
        {
            string sfile = GetDataPath(time, out string metafile);
            return sfile;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <returns></returns>
        //public string NewHisFile(DateTime time)
        //{
        //    string sfile = GetDataPath(time, out string metafile);
        //    Take(metafile);

        //    mFileWriter2.CreatOrOpenFile(sfile);

        //    if (mMetaFileWriter2.CreatOrOpenFile(metafile))
        //    {
        //        var mCurrentMetaBlock = NewMetaData(time, this.DatabaseName);

        //        mCurrentMetaBlock.WriteToStream(mMetaFileWriter.GetStream(), 0, mCurrentMetaBlock.AvaiableSize);

        //        //写入文件版本
        //        mFileWriter2.Write((short)0, 0);

        //        LoggerService.Service.Info("SeriseEnginer", "new meta data for a new file.");
        //    }
        //    else
        //    {
        //        if (mMetaFileWriter2.Length < FileHeadSize)
        //        {
        //            var mCurrentMetaBlock = NewMetaData(time, this.DatabaseName);
        //            mCurrentMetaBlock.IsDirty = true;
        //            mCurrentMetaBlock.UpdateDirtyDataToDisk(mMetaFileWriter2.GetStream());

        //            mFileWriter2.GoToStart();
        //            //写入文件版本
        //            mFileWriter2.Write((short)0, 0);

        //            LoggerService.Service.Info("SeriseEnginer", $"new meta data for a exist file {metafile}.");
        //        }
        //    }
        //    mFileWriter2.Close();
        //    mMetaFileWriter2.Close();

        //    Release(metafile);
        //    return sfile;
        //}

        private MetaFileBlock mTmpMetaBlock = null;
        private string mTmpMetaFile="";

        /// <summary>
        /// 
        /// </summary>
        public void ClearTmp()
        {
            if(mTmpMetaBlock != null)
            {
                mMetaFileWriter2.GoToStart();
                mTmpMetaBlock.WriteToStream(mMetaFileWriter2.GetStream(), 0, mTmpMetaBlock.AvaiableSize);

                mTmpMetaBlock.Dispose();
                mTmpMetaBlock = null;
                mTmpMetaFile = String.Empty;

                mFileWriter2.Close();
                mMetaFileWriter2.Close();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="data"></param>
        /// <param name="stype"></param>
        /// <param name="filename"></param>
        /// <returns></returns>
        public bool SaveData(int id,DateTime time,MarshalMemoryBlock data,SaveType stype,string filename)
        {
            string sfile, metafile;

            if (string.IsNullOrEmpty(filename))
            {
                sfile = GetDataPath(time, out metafile);
            }
            else
            {
                sfile = filename;
                metafile = filename.Replace(".dbd2", ".dbm2");
            }
            Take(metafile,false);
            try
            {

                if(metafile!=mTmpMetaFile)
                {
                    mTmpMetaFile = metafile;
                    if(mTmpMetaBlock!=null)
                    {
                        mMetaFileWriter2.GoToStart();
                        mTmpMetaBlock.WriteToStream(mMetaFileWriter2.GetStream(), 0, mTmpMetaBlock.AvaiableSize);

                        mTmpMetaBlock.Dispose();
                        mTmpMetaBlock = null;

                        mFileWriter2.Close();
                        mMetaFileWriter2.Close();
                    }

                    mFileWriter2.CreatOrOpenFile(sfile);

                    if (mMetaFileWriter2.CreatOrOpenFile(metafile))
                    {
                        mTmpMetaBlock = NewMetaData(time, this.DatabaseName);

                        //写入文件版本
                        mFileWriter2.Write((short)0, 0);
                        LoggerService.Service.Info("SeriseEnginer", "new meta data for a new file.");
                    }
                    else
                    {
                        if (mMetaFileWriter2.Length < FileHeadSize)
                        {
                            if (mTmpMetaBlock != null) mTmpMetaBlock.Dispose();

                            mTmpMetaBlock = NewMetaData(time, this.DatabaseName);

                            //写入文件版本
                            mFileWriter2.Write((short)0, 0);
                            LoggerService.Service.Info("SeriseEnginer", "new meta data for a exist file.");
                        }
                        else
                        {

                            //打开已有文件

                            if (mTmpMetaBlock == null)
                            {
                                mTmpMetaBlock = new MetaFileBlock((int)mMetaFileWriter2.Length) { AvaiableSize = (int)mMetaFileWriter2.Length, BlockDuration = this.BlockDuration, FileDuration = this.FileDuration };

                                mMetaFileWriter2.GoToStart();
                                //mCurrentMetaBlock.Clear();
                                mTmpMetaBlock.ReadFromStream(mMetaFileWriter2.GetStream(), (int)mMetaFileWriter2.Length);

                                LoggerService.Service.Info("SeriseEnginer", "Read meta data from exist file.");
                            }
                        }
                    }
                }

                mFileWriter2.GoToEnd();

                if (data != null)
                {
                    long dataptr = mFileWriter2.CurrentPostion;
                    data.WriteToStream(mFileWriter2.GetStream(), 0, data.Position);

                    if (stype == SaveType.Append)
                    {
                        mTmpMetaBlock.CheckAndUpdateBlockPoint(id, time, dataptr, mFileWriter2);
                    }
                    else
                    {
                        mTmpMetaBlock.UpdateBlockPoint(id, time, dataptr);
                    }
                }
                else
                {
                    //说明需要清空数据，多用于删除数据块
                    if(stype == SaveType.Replace)
                    {
                        mTmpMetaBlock.UpdateBlockPoint(id, time, 0);
                    }
                }

                

                //mFileWriter2.Close();
                //mMetaFileWriter2.Close();
            }
            catch
            {

            }
            Release(metafile,false);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagcount"></param>
        /// <returns></returns>
        private int CalMetaFileSize(int tagcount)
        {
            return 98 + (FileDuration * 60 / BlockDuration) * 8 * tagcount;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        /// <param name="time"></param>
        /// <param name="databasename"></param>
        private void FillMetaData(MetaFileBlock block,DateTime time,string databasename)
        {
            block.Clear();
            DateTime date = new DateTime(time.Year, time.Month, time.Day, ((time.Hour / FileDuration) * FileDuration), 0, 0);
            block.Write(date);
            block.WriteInt(16, MaxTagId);
            block.WriteInt(20, FileDuration);
            block.WriteInt(24, BlockDuration);
            block.WriteInt(28, HisEnginer3.MemoryTimeTick);
            block.WriteShort(32, 0);
            var vbytes = Encoding.UTF8.GetBytes(databasename);
            block.WriteShort(34, (short)vbytes.Length);
            block.WriteBytes(36, vbytes);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private MetaFileBlock NewMetaData(DateTime time, string databaseName)
        {
            //FileHead : DateTime(8)+LastUpdateDatetime(8)+MaxtTagCount(4)+file duration(4)+block duration(4)+Time tick duration(4)+Version(2)+DatabaseName(64)
            var re = new MetaFileBlock(CalMetaFileSize(TagCountOneFile));
            re.AvaiableSize = CalMetaFileSize(MaxTagId);
            re.FileDuration = FileDuration;
            re.BlockDuration = BlockDuration;
            FillMetaData(re,time,databaseName);
            return re;
        }

        

        /// <summary>
        /// 添加统计文件头部
        /// datetime(8)+databasename(64)
        /// </summary>
        private void AppendStatisticsFileHeader(DateTime time, string databaseName, DataFileSeriserbase mFileWriter)
        {
            mFileWriter.Write(time, 0);
            byte[] nameBytes = new byte[64];
            Array.Clear(nameBytes, 0, nameBytes.Length);
            var ntmp = Encoding.UTF8.GetBytes(databaseName);
            Buffer.BlockCopy(ntmp, 0, nameBytes, 0, Math.Min(64, ntmp.Length));
            mFileWriter.Write(nameBytes, 8);
        }

        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private string GetDataPath(DateTime time,out string metafile)
        {
            metafile= System.IO.Path.Combine(SeriseEnginer6.HisDataPath, GetMetaFileName(time));
            return System.IO.Path.Combine(SeriseEnginer6.HisDataPath, GetFileName(time));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        private string GetDataPath(string file)
        {
            return System.IO.Path.Combine(SeriseEnginer6.HisDataPath, file);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private string GetFileName(DateTime time)
        {
            return DatabaseName + Id.ToString("X3") + time.ToString("yyyyMMdd") + FileDuration.ToString("D2") + (time.Hour / FileDuration).ToString("D2") + DataFileExtends;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private string GetMetaFileName(DateTime time)
        {
            return DatabaseName + Id.ToString("X3") + time.ToString("yyyyMMdd") + FileDuration.ToString("D2") + (time.Hour / FileDuration).ToString("D2") + DataFileMetaExtends;
        }


        /// <summary>
        /// 获取统计文件名称
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private string GetStatisticsFileName(DateTime time)
        {
            return DatabaseName + Id.ToString("X3") + time.ToString("yyyyMMdd") + DayStatisticsFileExtends;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private string GetStatisticsDataPath(DateTime time)
        {
            return System.IO.Path.Combine(SeriseEnginer6.HisDataPath, GetStatisticsFileName(time));
        }

        private string mLastManualMetaFile = "";
        private DataFileSeriserbase mLastManualFileBase;
        private DataFileSeriserbase mLastManualMetaFileBase;

        private MetaFileBlock GetFileHeadPointBlock(string sfile,DateTime time,out DataFileSeriserbase mFileReader,out DataFileSeriserbase mMetaWriter)
        {
            if(sfile==mCurrentFileName)
            {
                mLastManualMetaFile = mCurrentMetaFile;
                Take(mLastManualMetaFile);
                mFileReader = mFileWriter;
                mMetaWriter = mMetaFileWriter;
                return mCurrentMetaBlock;
            }
            else
            {

                var vvfile = GetDataPath(sfile);

                var  metafile = vvfile.Replace(DataFileExtends, DataFileMetaExtends);

                Take(metafile);

                //和上次返回的一样，则直接返回
                if (metafile == mLastManualMetaFile && mManuaMetaBlock != null && mLastManualMetaFileBase != null && mLastManualFileBase != null)
                {
                    mMetaWriter = mLastManualMetaFileBase;
                    mFileReader = mLastManualFileBase;
                    return mManuaMetaBlock;
                }

                mLastManualMetaFile = metafile;

                

                mFileWriter2.CreatOrOpenFile(vvfile);


                if (mMetaFileWriter2.CreatOrOpenFile(metafile))
                {
                    if (mManuaMetaBlock == null)
                    {
                        mManuaMetaBlock = NewMetaData(time, this.DatabaseName);
                    }
                    else
                    {
                        FillMetaData(mManuaMetaBlock, time, this.DatabaseName);
                    }
                    mManuaMetaBlock.IsDirty = true;
                    mManuaMetaBlock.UpdateDirtyDataToDisk(mMetaFileWriter2.GetStream());

                    //写入文件版本
                    mFileWriter2.GoToStart();
                    mFileWriter2.Write((short)0, 0);

                    LoggerService.Service.Info("SeriseEnginer", $"new meta data for a new file {metafile}.");
                }
                else
                {
                    if (mMetaFileWriter2.Length < FileHeadSize)
                    {
                        if (mManuaMetaBlock == null)
                        {
                            mManuaMetaBlock = NewMetaData(time, this.DatabaseName);
                        }
                        else
                        {
                            FillMetaData(mManuaMetaBlock, time, this.DatabaseName);
                        }
                        mManuaMetaBlock.IsDirty = true;
                        mManuaMetaBlock.UpdateDirtyDataToDisk(mMetaFileWriter2.GetStream());

                        mFileWriter2.GoToStart();
                        //写入文件版本
                        mFileWriter2.Write((short)0, 0);

                        LoggerService.Service.Info("SeriseEnginer", $"new meta data for a exist file {metafile}.");
                    }
                    else
                    {

                        if(mManuaMetaBlock==null)
                        {
                            mManuaMetaBlock = NewMetaData(time, this.DatabaseName);
                        }

                        //打开已有文件
                        mMetaFileWriter2.GoToStart();
                        mManuaMetaBlock.Clear();
                        mManuaMetaBlock.ReadFromStream(mMetaFileWriter2.GetStream(), (int)mMetaFileWriter2.Length);

                        LoggerService.Service.Info("SeriseEnginer", $"read meta data for a exist file {metafile}.");

                    }
                }
                mFileReader = mFileWriter2;
                mMetaWriter = mMetaFileWriter2;

                mLastManualFileBase = mFileReader;
                mLastManualMetaFileBase = mMetaWriter;

                return mManuaMetaBlock;
            }

           
        }
      

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="datablock"></param>
        public void AppendManualSeriseFile(int id, IMemoryBlock datablock)
        {
            DateTime time = datablock.ReadDateTime(60);
            string sfile = GetFileName(time);

            lock (mManualHisDataCach)
            {
                if (mManualHisDataCach.ContainsKey(sfile))
                {
                    mManualHisDataCach[sfile].Enqueue(datablock);
                }
                else
                {
                    Queue<IMemoryBlock> blocks = new Queue<IMemoryBlock>();
                    blocks.Enqueue(datablock);
                    mManualHisDataCach.Add(sfile, blocks);
                }
            }
        }

        /// <summary>
        /// 将手动记录的数据存储到磁盘上
        /// 地址指针采用8字节，基地址(8) 
        /// </summary>
        public unsafe void FreshManualDataToDisk()
        {
            string oldFile = string.Empty;
            DataFileSeriserbase mwriter;

            //Dictionary<int, List<long>> mHeadAddress;
            Dictionary<int, List<long>> mHeadValue = new Dictionary<int, List<long>>();

            MetaFileBlock mHeadBlock;

            //MarshalMemoryBlock mb;

            while(mManualHisDataCach.Count>0)
            {
                var vv = mManualHisDataCach.First();
                lock(mManualHisDataCach)
                {
                    mManualHisDataCach.Remove(vv.Key);
                }

                Stopwatch sw = new Stopwatch();
                sw.Start();

                SortedDictionary<int,List<DateTime>> times = new SortedDictionary<int, List<DateTime>>();
                DateTime maxTime = DateTime.MinValue;
                DateTime mLastModifyTime = DateTime.MinValue;
                int datasize = 0;

                //56 是统计数据区的长度
                foreach (var vvv in vv.Value)
                {

                    int id = vvv.ReadInt(0 + 56);
                  
                    DateTime time = vvv.ReadDateTime(4 + 56);
                    DateTime realtime = vvv.ReadDateTime(12 + 56);
                    DateTime endTime = vvv.ReadDateTime(20 + 56);
                    //int size = vvv.ReadInt(28 + 56);

                    if (times.ContainsKey(id))
                    {
                        times[id].Add(realtime);
                    }
                    else
                    {
                        times.Add(id, new List<DateTime>() { realtime });
                    }
                    maxTime = realtime > maxTime ? realtime : maxTime;


                    mLastModifyTime = endTime > mLastModifyTime ? endTime : mLastModifyTime;

                    //datasize += (size - 28-56);
                }

                // mHeadAddress = GetDataRegionHeadPoint(vv.Key, times, maxTime, out mwriter);
                mHeadBlock = GetFileHeadPointBlock(vv.Key, maxTime, out mwriter, out DataFileSeriserbase metaWriter);


                long ltmp = sw.ElapsedMilliseconds;

                mHeadValue.Clear();
                mwriter.GoToEnd();
                //var blockpointer = mwriter.CurrentPostion;
                var vpointer = mwriter.CurrentPostion;


                //写入数据，同时获取数据块地址
                foreach (var vvv in vv.Value)
                {
                    int id = vvv.ReadInt(0 + 56);
                    int size = vvv.ReadInt(28 + 56);

                    if (mHeadValue.ContainsKey(id))
                    {
                        mHeadValue[id].Add(vpointer);
                    }
                    else
                    {
                        mHeadValue.Add(id, new List<long>() { vpointer });
                    }

                    vvv.WriteToStream(mwriter.GetStream(), 36 + 56, size);//直接拷贝数据块
                    vpointer += (size);
                    datasize += (size);
                }

                //更新数据块指针
                int j = 0;
                foreach(var hd in mHeadValue)
                {
                    for(j=0;j<times[hd.Key].Count;j++)
                    mHeadBlock.CheckAndUpdateBlockPoint(hd.Key%TagCountOneFile, times[hd.Key][j], hd.Value[j], mwriter);
                }


                LoggerService.Service.Info("SeriseEnginer6", "SeriseFileItem " + this.Id + " 完成存储,数据块:" + vv.Value.Count + " 数据量:" + datasize + " 耗时:" + sw.ElapsedMilliseconds);



                //更新文件的最后修改时间
                //更新meta 文件
                var dtmp = mHeadBlock.ReadDateTime(8);
                if (mLastModifyTime > dtmp)
                {
                    mHeadBlock.UpdateLastUpdateDateTime(mLastModifyTime);
                    //metaWriter.Write(mLastModifyTime, 8);
                }
                metaWriter.GoToStart();
                mHeadBlock.UpdateDirtyDataToDisk(metaWriter.GetStream());


                mwriter.Flush();
                metaWriter.Flush();

                if (mwriter != mFileWriter)
                {
                    mwriter.Close();
                    metaWriter.Close();
                }

                UpdateStaticstics(vv.Value);

                foreach (var vvv in vv.Value)
                {
                    (vvv as MarshalMemoryBlock).MakeMemoryNoBusy();
                    MarshalMemoryBlockPool.Pool.Release(vvv as MarshalMemoryBlock);
                }

                Release(mLastManualMetaFile);

                //清空缓存
                foreach(var vvt in times)
                {
                    int rid = (int)(vvt.Key / TagCountOneFile);
                    foreach (var vvv in vvt.Value)
                    {
                        LogStorageManager.Instance.DecManualRef(vvv, rid);
                        LogStorageManager.Instance.ReleaseManualLog(vvv, rid);
                    }
                }

            }

            mLastManualMetaFile = String.Empty;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="time1"></param>
        /// <param name="time2"></param>
        /// <returns></returns>
        public bool CheckInSameFile(DateTime time1)
        {
            return GetFileName(time1) == mCurrentFileName;
        }

        /// <summary>
        /// 获取统计文件值
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private DataFileSeriserbase GetStatisticsFileWriter(DateTime time)
        {
            string sfile = GetStatisticsDataPath(time);

            if (mStatisticsWriter != null && mStatisticsWriter.FileName == sfile)
            {
                return mStatisticsWriter;
            }
            else
            {
                var re = DataFileSeriserManager.manager.GetDefaultFileSersie();
                if (re.CreatOrOpenFile(sfile))
                {
                    AppendStatisticsFileHeader(time, this.DatabaseName, re);
                }
                else
                {
                    if (re.Length < MaxTagId * 8)
                    {
                        AppendStatisticsFileHeader(time, this.DatabaseName, re);
                    }
                }
               
                if (mStatisticsWriter != null) mStatisticsWriter.Dispose();
                mStatisticsWriter = re;
                return re;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mProcessMemory"></param>
        /// <param name="time"></param>
        private void UpdateStaticstics(Queue<IMemoryBlock> mProcessMemory)
        {
            
            if(mProcessMemory.Count==0)
            {
                return;
            }
            var time = mProcessMemory.First().ReadDateTime(4 + 56);
            var filewriter = GetStatisticsFileWriter(time);
            filewriter.Write(time, 0);//写入最后更新时间
            filewriter.GoTo(72);
            StatisticsMemory.StartId = this.Id * this.TagCountOneFile;
            StatisticsMemory.Load(filewriter.GetStream());
            MarshalFixedMemoryBlock mfb = new MarshalFixedMemoryBlock();

            foreach (var vv in mProcessMemory)
            {
                var id = vv.ReadInt(0);
                StatisticsMemory.GetStatisticsData(id, mfb);

                var avgcount = vv.ReadInt(8);
                var avgvalue = vv.ReadDouble(12);
                var maxtime = vv.ReadDateTime(20);
                var maxvalue = vv.ReadDouble(28);
                var mintime = vv.ReadDateTime(36);
                var minvalue = vv.ReadDouble(44);

                int offset = time.Hour * 48;

                var ncount = mfb.ReadInt( offset+ 4);
                var navgvalue = mfb.ReadDouble(offset + 8);

                mfb.WriteByte(offset + 0, 1);
                mfb.WriteInt(offset + 4, ncount + avgcount);
                if ((ncount + avgcount) > 0)
                {
                    mfb.WriteDouble(offset + 8, (avgcount * avgvalue + navgvalue * ncount) / (ncount + avgcount));
                }

                var nmaxtime = mfb.ReadDateTime(offset + 16);
                var nmaxvalue = mfb.ReadDouble(offset + 24);
                if (nmaxvalue < maxvalue || nmaxtime==DateTime.MinValue)
                {
                    mfb.WriteDatetime(offset + 16, maxtime);
                    mfb.WriteDouble(offset + 24, maxvalue);
                }

                var nmintime = mfb.ReadDateTime(offset + 32);
                var nminvalue = mfb.ReadDouble(offset + 40);
                if (nminvalue > minvalue || nmintime== DateTime.MinValue)
                {
                    mfb.WriteDatetime(offset + 32, mintime);
                    mfb.WriteDouble(offset + 40, minvalue);
                }
            }

            filewriter.GoTo(72);
            StatisticsMemory.Save(filewriter.GetStream());
           

            filewriter.Flush();
            //filewriter.Dispose();
           
        }

        /// <summary>
        /// 更新统计数据
        /// </summary>
        /// <param name="mProcessMemory"></param>
        /// <param name="dataOffset"></param>
        /// <param name="time"></param>
        private void UpdateStaticstics(MarshalMemoryBlock mProcessMemory, DateTime time)
        {
            var filewriter = GetStatisticsFileWriter(time);
            filewriter.Write(time, 0);//写入最后更新时间
            filewriter.GoTo(72);
            StatisticsMemory.StartId = this.Id * this.TagCountOneFile;
            StatisticsMemory.Load(filewriter.GetStream());
            MarshalFixedMemoryBlock mfb = new MarshalFixedMemoryBlock();
            var cm = (mProcessMemory as CompressMemory4).StaticsMemoryBlock;
            if(cm!=null)
            {
                int offset = 0;
               
                for(int i=0;i<TagCountOneFile;i++)
                {

                    offset = i * 52;

                    var id = cm.ReadInt(offset);
                    var avgcount = cm.ReadInt(offset+8);
                    var avgvalue = cm.ReadDouble();
                    var maxtime = cm.ReadLong();
                    var maxvalue = cm.ReadDouble();
                    var mintime = cm.ReadLong();
                    var minvalue = cm.ReadDouble();

                    if (id <= 0 && avgcount <= 0 && avgcount <= 0 && maxtime <= 0 && mintime <= 0) continue;
                    StatisticsMemory.GetStatisticsData(id, mfb);

                    int toffset = time.Hour * 48;

                    var ncount = mfb.ReadInt(toffset+4);
                    var navgvalue = mfb.ReadDouble(toffset + 8);

                    mfb.WriteByte(toffset + 0,1);
                    mfb.WriteInt(toffset + 4, ncount + avgcount);
                    if ((ncount + avgcount) > 0)
                    {
                        mfb.WriteDouble(toffset + 8, (avgcount * avgvalue + navgvalue * ncount) / (ncount + avgcount));
                    }

                    var nmaxtime = mfb.ReadLong(toffset + 16);
                    var nmaxvalue = mfb.ReadDouble(toffset + 24);
                    if (nmaxvalue < maxvalue || nmaxtime == 0)
                    {
                        mfb.WriteLong(toffset + 16, maxtime);
                        mfb.WriteDouble(toffset + 24, maxvalue);
                    }

                    var nmintime = mfb.ReadLong(toffset + 32);
                    var nminvalue = mfb.ReadDouble(toffset + 40);
                    if(nminvalue>minvalue || nmintime==0)
                    {
                        mfb.WriteLong(toffset + 32, mintime);
                        mfb.WriteDouble(toffset + 40, minvalue);
                    }
                }
            }

            filewriter.GoTo(72);
            StatisticsMemory.Save(filewriter.GetStream());
           
            filewriter.Flush();
            //filewriter.Dispose();

            cm.MakeMemoryNoBusy();
            cm.Clear();
        }


        private string mCurrentMetaFile;

        /// <summary>
        /// 检查文件是否存在
        /// </summary>
        /// <param name="time"></param>
        private bool CheckFile(DateTime time)
        {
            if (!CheckInSameFile(time) || mCurrentMetaBlock==null)
            {
                
                if (mFileWriter != null)
                {
                    mFileWriter.Flush();
                    mFileWriter.Close();
                }

                if (mMetaFileWriter != null)
                {
                    mMetaFileWriter.Flush();
                    mMetaFileWriter.Close();
                }

                string sfile = GetDataPath(time, out string metafile);
                Take(metafile);
                mCurrentMetaFile = metafile;

                mFileWriter.CreatOrOpenFile(sfile);

                if (mMetaFileWriter.CreatOrOpenFile(metafile))
                {
                    if (mCurrentMetaBlock == null)
                    {
                        mCurrentMetaBlock = NewMetaData(time, this.DatabaseName);
                    }
                    else
                    {
                        FillMetaData(mCurrentMetaBlock, time, this.DatabaseName);
                    }

                    mCurrentMetaBlock.WriteToStream(mMetaFileWriter.GetStream(), 0, mCurrentMetaBlock.AvaiableSize);

                    //写入文件版本
                    mFileWriter.Write((short)0, 0);

                    LoggerService.Service.Info("SeriseEnginer", "new meta data for a new file.");
                }
                else
                {
                    if (mMetaFileWriter.Length < FileHeadSize)
                    {
                        if (mCurrentMetaBlock == null)
                        {
                            mCurrentMetaBlock = NewMetaData(time, this.DatabaseName);
                        }
                        else
                        {
                            FillMetaData(mCurrentMetaBlock, time, this.DatabaseName);
                        }
                        mCurrentMetaBlock.WriteToStream(mMetaFileWriter.GetStream(), 0, mCurrentMetaBlock.AvaiableSize);

                        //写入文件版本
                        mFileWriter.Write((short)0, 0);

                        LoggerService.Service.Info("SeriseEnginer", "new meta data for a exist file.");
                    }
                    else
                    {

                        ////打开已有文件
                        
                        if(mCurrentMetaBlock == null)
                        {
                            mCurrentMetaBlock = NewMetaData(time, DatabaseName);
                        }

                        mMetaFileWriter.GoToStart();
                        mCurrentMetaBlock.Clear();
                        mCurrentMetaBlock.ReadFromStream(mMetaFileWriter.GetStream(), (int)mMetaFileWriter.Length);

                        LoggerService.Service.Info("SeriseEnginer", "Read meta data from exist file.");

                    }
                }

                if (mNeedRecordDataHeader) mNeedRecordDataHeader = false;

                mCurrentFileName = GetFileName(time);

              
                //mMetaCurrentFileName = metafile;
            }
            else
            {
                Take(mCurrentMetaFile);

                if (mNeedRecordDataHeader)
                {
                    mNeedRecordDataHeader = false;
                    var vsize = CalMetaFileSize(MaxTagId);
                    if (vsize > mCurrentMetaBlock.AvaiableSize)
                    {
                        mCurrentMetaBlock.AvaiableSize = vsize;
                    }
                }
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mProcessMemory"></param>
        /// <param name="time"></param>
        public void SaveToFile(MarshalMemoryBlock mProcessMemory, DateTime time,DateTime endTime)
        {
            SaveToFile(mProcessMemory, 0, time,endTime);
        }
                

        /// <summary>
        /// 执行存储到磁盘,
        /// 地址指针采用12字节，基地址(8) + 偏移地址(4)
        /// </summary>
        public void SaveToFile(MarshalMemoryBlock mProcessMemory, long dataOffset, DateTime time, DateTime endTime)
        {
            /*
             1. 检查变量ID是否变动，如果变动则重新记录变量的ID列表
             2. 拷贝数据块
             3. 更新数据块指针
             */
            //LoggerService.Service.Info("SeriseFileItem" + Id, "*********开始执行存储**********");
            try
            {
                lock (mFileLocker)
                {

                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    //数据大小
                    var datasize = mProcessMemory.ReadInt(dataOffset);
                    var count = mProcessMemory.ReadInt(dataOffset + 4);//变量个数
                    //mTagCount = count;
                    //mCurrentTime = time;

                    var ltmp = sw.ElapsedMilliseconds;

                    //打开文件
                    if (!CheckFile(time))
                    {
                        Release(mCurrentMetaFile);
                        sw.Stop();
                        return;
                    }

                    //更新最后写入时间

                    var dtmp = mCurrentMetaBlock.ReadDateTime(8);
                    if(endTime>dtmp)
                    {
                        mCurrentMetaBlock.UpdateLastUpdateDateTime(endTime);
                        mMetaFileWriter.Write(endTime, 8);
                    }

                    if (datasize == 0)
                    {
                        Flush();
                        sw.Stop();
                        Release(mCurrentMetaFile);
                        return;
                    }

                    var ltmp2 = sw.ElapsedMilliseconds;

                    long offset = 8 + dataOffset;
                    long start = count * 8 + offset;//计算出数据起始地址

                    var dataAddr = this.mFileWriter.GoToEnd().CurrentPostion;

                    LoggerService.Service.Info("SeriseEnginer","New write file position:"+dataAddr);

                    //写入指针头部区域
                    for (int i = 0; i < count; i++)
                    {
                        //读取ID
                        var id = mProcessMemory.ReadInt(offset) % TagCountOneFile;
                        //读取偏移地址
                        var addr = mProcessMemory.ReadInt(offset + 4);

                        offset += 8;
                        if (id > -1)
                        {
                            mCurrentMetaBlock.CheckAndUpdateBlockPoint(id,time, addr + dataAddr,mFileWriter);
                        }
                    }
                    long writedatasize = mCurrentMetaBlock.AvaiableSize;

                    this.mFileWriter.GoToEnd();
                    mProcessMemory.WriteToStream(mFileWriter.GetStream(), start, datasize);//直接拷贝数据，写入数据

                    mMetaFileWriter.GoToStart();
                    mCurrentMetaBlock.UpdateDirtyDataToDisk(mMetaFileWriter.GetStream());

                    writedatasize += datasize;

                    Flush();

                    UpdateStaticstics(mProcessMemory,time);
                    sw.Stop();

                    Release(mCurrentMetaFile);

                    LoggerService.Service.Info("SeriseFileItem" + Id, "写入数据 " + mCurrentFileName + "  数据大小：" + (writedatasize / 1024.0 / 1024) + " m" +  "存储耗时:" + (sw.ElapsedMilliseconds));
                }
            }
            catch (System.IO.IOException ex)
            {
                Release(mCurrentMetaFile);
                LoggerService.Service.Erro("SeriseEnginer" + Id, ex.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Flush()
        {
            mFileWriter?.Flush();
            mFileWriter?.CloseAndReOpen();

            mMetaFileWriter?.Flush();
            mMetaFileWriter?.CloseAndReOpen();

        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            mIdAddrs.Clear();

            mManuaMetaBlock?.Dispose();
            mCurrentMetaBlock?.Dispose();

            mFileWriter.Dispose();
            mFileWriter = null;
            mMetaFileWriter?.Dispose();

            mFileWriter2?.Dispose();
            mMetaFileWriter2.Dispose();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class MetaFileBlock: MarshalMemoryBlock
    {
        /*
            *  一个历史文件包括：文件头文件(*.dbm2)+数据文件文件(*.dbd2)
           * ****** DBM 文件头结构 *********
           * FileHead(98)+ DataRegionPointer
           * FileHead : DateTime(8)+LastUpdateDatetime(8)+MaxtTagCount(4)+file duration(4)+block duration(4)+Time tick duration(4)+Version(2)+DatabaseName(64)
           * DataRegionPointer:[Tag1 DataPointer1(8)+...+Tag1 DataPointerN(8)(DataRegionCount)]...[Tagn DataPointer1(8)+...+Tagn DataPointerN(8)(DataRegionCount)](MaxTagCount)
        */

        public bool IsDirty { get; set; }=true;

        /// <summary>
        /// 更新某个某个变量的某个时间地址
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="addr"></param>
        public void UpdateBlockPoint(int id,DateTime time,long addr)
        {
            var FileStartHour = (time.Hour / FileDuration) * FileDuration;
            int bid = ((time.Hour - FileStartHour) * 60 + time.Minute) / BlockDuration;
            int index = (id * FileDuration * 60 / BlockDuration + bid) * 8 + 98;
            if(index+8>this.AllocSize)
            {
                CheckAndResize((long)(this.AllocSize * 1.5));
            }
            WriteLong(index, addr);
            IsDirty = true;
        }

        /// <summary>
        /// 读取某个ID的所有ID块地址
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public List<long> ReadBlockPoints(int id)
        {
            List<long> blockPoints = new List<long>();
            var count = FileDuration * 60 / BlockDuration;

            long addr = id * count * 8 +98;
            
            for (int i = 0; i < count; i++)
            {
                blockPoints.Add(ReadLong(addr + i * 8));
            }
            return blockPoints;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public long ReadBlockPoint(int id,DateTime time)
        {
            var FileStartHour = (time.Hour / FileDuration) * FileDuration;
            int bid = ((time.Hour - FileStartHour) * 60 + time.Minute) / BlockDuration;
            int index = (id * FileDuration * 60 / BlockDuration + bid) * 8 + 98;
            if (this.AllocSize < index)
            {
                return 0;
            }
            else
            {
                return ReadLong(index);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="addr"></param>
        /// <param name="mDataStream"></param>
        public void CheckAndUpdateBlockPoint(int id,DateTime time,long addr,DataFileSeriserbase mDataStream)
        {
            var ad = ReadBlockPoint(id,time);
            if(ad>0)
            {
                //如果已经存在说明之前改区域已经更新过数据，该数据区间由多个数据块组成，多个数据块之间通过指针进行连接
                //Block Header: NextBlockAddress(5)(同一个数据区间有多个数据块时，之间通过指针关联)+DataSize(4)+ValueType(5b)+CompressType(3b)
                var pad = ad;
                do
                {
                    pad = ad;
                    if (mDataStream.Length < ad + 5)
                    {
                        break;
                    }

                    var vbyts = System.Buffers.ArrayPool<byte>.Shared.Rent(8);
                    var bss = vbyts.AsSpan<byte>();
                    bss.Clear();
                    mDataStream.ReadBytes(ad,vbyts,0, 5);
                    ad = pad + BitConverter.ToInt64(bss);
                    System.Buffers.ArrayPool<byte>.Shared.Return(vbyts);

                }
                while (ad != pad);
                var bvals = BitConverter.GetBytes(addr - pad).AsSpan<byte>(0, 5).ToArray();
                mDataStream.Write(bvals, pad);
            }
            else
            {
                UpdateBlockPoint(id, time, addr);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int FileDuration { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int BlockDuration { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public MetaFileBlock():base()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public MetaFileBlock(int size):base(size)
        {

        }

        /// <summary>
        /// 可用大小
        /// </summary>
        public int AvaiableSize { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        public void UpdateLastUpdateDateTime(DateTime time)
        {
            this.WriteDatetime(8,time);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        public void UpdateTagCount(int count)
        {
            this.WriteInt(16,count);
            IsDirty = true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        public void UpdateDirtyDataToDisk(System.IO.Stream stream)
        {
            if (IsDirty)
            {
                this.WriteToStream(stream, 0, this.AvaiableSize);
                IsDirty = false;
            }
        }
    }
}
