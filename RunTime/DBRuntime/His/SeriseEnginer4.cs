//==============================================================
//  Copyright (C) 2021  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2021/02/18 10:35:02.
//  Version 1.0
//  种道洋
//  较SeriseEnginer4 修改数据块指针为：固定的100000个变量的，单个变量每5分钟一个数据块指针的结构
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
    * ****DBD 文件结构****
    * 一个文件头 + 多个数据区组成 ， 一个数据区：数据区头+数据块指针区+数据块区
    * [] 表示重复的一个或多个内容
    * 
    HisData File Structor
    FileHead(84) + [HisDataRegion]

    FileHead: dataTime(8)(FileTime)+dateTime(8)(LastUpdateTime)+DataRegionCount(4)+DatabaseName(64)

    HisDataRegion Structor: RegionHead + DataBlockPoint Area + DataBlock Area

    RegionHead:          PreDataRegionPoint(8) + NextDataRegionPoint(8) + Datatime(8)+ tagcount(4)+ tagid sum(8)+file duration(4)+block duration(4)+Time tick duration(4)
    DataBlockPoint Area: [ID]+[block Point]
    [block point]:       [[tag1 block1 point,tag2 block1 point,....][tag1 block2 point(12),tag2 block2 point(12),...].....]   以时间单位对变量的数去区指针进行组织,
    [tag block point]:   offset pointer(4)+ datablock area point(8)   offset pointer: bit 32 标识data block 类型,1:标识非压缩区域，0:压缩区域,bit1~bit31 偏移地址
    DataBlock Area:      [[tag1 block1 size + compressType+ tag1 data block1][tag2 block1 size + compressType+ tag2 data block1]....][[tag1 block2 size + compressType+ tag1 data block2][tag2 block2 size + compressType+ tag2 data block2]....]....
   */

    /*
    * ****His 文件结构****
    * 一个文件头 + 多个数据区组成 ， 一个数据区：数据区头+数据块指针区+数据块区
    * [] 表示重复的一个或多个内容
    * 
    HisData File Structor
    FileHead(84) + [HisDataRegion]

    FileHead: dataTime(8)(FileTime)+dateTime(8)(LastUpdateTime)+DataRegionCount(4)+DatabaseName(64)

    HisDataRegion Structor: RegionHead + DataBlockPoint Area + DataBlock Area

    RegionHead:          PreDataRegionPoint(8) + NextDataRegionPoint(8) + Datatime(8) +file duration(4)+block duration(4)+Time tick duration(4)+ tagcount(4)
    DataBlockPoint Area: [ID]+[block Point]
    [block point]:       [[tag1 block1 point(12),tag1 block2 point(12),....][tag2 block1 point(12),tag2 block2 point(12),...].....]   以时间单位对变量的数去区指针进行组织,
    [tag block point]:   offset pointer(4)+ datablock area point(8)   offset pointer: bit 32 标识data block 类型,1:标识非压缩区域，0:压缩区域,bit1~bit31 偏移地址
    DataBlock Area:      [[tag1 block1 size + compressType + tag1 block1 data][tag1 block2 size + compressType+ tag1 block2 data]....][[tag2 block1 size + compressType+ tag2 block1 data][tag2 block2 size + compressType+ tag2 block2 data]....]....
    */

namespace Cdy.Tag
{
    /// <summary>
    /// 序列话引擎
    /// </summary>
    public class SeriseEnginer4 : IDataSerialize3, IDisposable
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

        private bool mIsHisFileReArrangeFinish = false;

        private Dictionary<int, CompressMemory3> mWaitForProcessMemory = new Dictionary<int, CompressMemory3>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, SeriseFileItem4> mSeriserFiles = new Dictionary<int, SeriseFileItem4>();

        /// <summary>
        /// 
        /// </summary>
        private int mLastBackupDay=-1;

        private bool mIsBusy = false;

        private StatisticsMemoryMap mStatisticsMemory;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public SeriseEnginer4()
        {

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
        public static int KeepNoZipFileDays { get; set; } = -1;

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

        //private long GetDriverSize(string path)
        //{
        //    System.IO.DriveInfo driverinfo = new System.IO.DriveInfo(path);

        //    return 0;
        //}

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
            foreach (var vv in histag)
            {
                var id = vv.Id;
                var did = id / TagCountOneFile;
                if (mLastDataRegionId != did)
                {
                    mSeriserFiles.Add(did, new SeriseFileItem4() { FileDuration = FileDuration, BlockDuration = BlockDuration, TagCountOneFile = TagCountOneFile, DatabaseName = DatabaseName, Id = did,StatisticsMemory= mStatisticsMemory });
                    mLastDataRegionId = did;
                }
            }

            foreach (var vv in mSeriserFiles)
            {
                vv.Value.FileWriter = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                vv.Value.FileWriter2 = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                vv.Value.Init();
            }

            HisDataPath = SelectHisDataPath();
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
                        mSeriserFiles.Add(did, new SeriseFileItem4() { FileDuration = FileDuration, BlockDuration = BlockDuration, TagCountOneFile = TagCountOneFile, DatabaseName = DatabaseName, Id = did, StatisticsMemory = mStatisticsMemory }.Init());
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            LoggerService.Service.Info("SeriseEnginer4", "开始启动");
            mIsClosed = false;
            resetEvent = new ManualResetEvent(false);
            closedEvent = new ManualResetEvent(false);
            mCompressThread = new Thread(ThreadPro);
            mCompressThread.IsBackground = true;
            mCompressThread.Start();

            mDatabackThread = new Thread(DatabackupThreadPro);
            mDatabackThread.IsBackground = true;
            mDatabackThread.Start();

            mHisFileReArrangeThread = new Thread(DataFileReArrangeThreadPro);
            mHisFileReArrangeThread.IsBackground = true;
            mHisFileReArrangeThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            LoggerService.Service.Info("SeriseEnginer4", "开始停止");
            mIsClosed = true;
            resetEvent.Set();
            closedEvent.WaitOne();
            resetEvent.Dispose();
            closedEvent.Dispose();

            lock (mSeriserFiles)
            {
                foreach (var vv in mSeriserFiles)
                {
                    vv.Value.Reset();
                }
            }

            while (!mIsBackupFinished) Thread.Sleep(1);

            while (!mIsHisFileReArrangeFinish) Thread.Sleep(1);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataMemory"></param>
        /// <param name="date"></param>
        public void RequestToSeriseFile(CompressMemory3 dataMemory)
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


        /// <summary>
        /// 
        /// </summary>
        public void RequestToSave()
        {
            lock (resetEvent)
                resetEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>

        private void ThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            while (!mIsClosed)
            {
                resetEvent.WaitOne();
                lock (resetEvent)
                    resetEvent.Reset();

                HisDataArrange4.Arrange.Paused();
               mIsBusy = true;
                //#if DEBUG 
                Stopwatch sw = new Stopwatch();
                sw.Start();
                LoggerService.Service.Info("SeriseEnginer4", "********开始执行存储********", ConsoleColor.Cyan);
                //#endif

                //mManualRequestSaveCount = 0;

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

                //#if DEBUG
                sw.Stop();
                LoggerService.Service.Info("SeriseEnginer4", ">>>>>>>>>完成执行存储>>>>>>>  ElapsedMilliseconds:" + sw.ElapsedMilliseconds, ConsoleColor.Cyan);

                mIsBusy = false;
                HisDataArrange4.Arrange.Resume();
                //#endif
            }
            closedEvent.Set();
        }

        private DateTime mLastDataFileReArrangeProcessTime;

        /// <summary>
        /// 
        /// </summary>
        private void DataFileReArrangeThreadPro()
        {
            while (!mIsClosed)
            {
                var wpath = SelectHisDataPath();

                if ((DateTime.Now - mLastDataFileReArrangeProcessTime).TotalSeconds > 30)
                {
                    if (System.IO.Directory.Exists(wpath))
                    {
                        try
                        {
                            foreach (var vv in new System.IO.DirectoryInfo(wpath).GetFiles("*.dbd"))
                            {
                                if (mIsClosed) break;
                                while (mIsBusy) Thread.Sleep(1000);

                                if (mIsClosed) break;

                                string sfile = "";

                                //if (HisDataArrange4.Arrange.CheckAndReArrangeHisFile(vv.FullName, out sfile, FileDuration, true))
                                if (HisDataArrange4.Arrange.CheckAndReArrangeHisFile(vv.FullName, out sfile, 24, true))
                                {
                                    HisQueryManager.Instance.GetFileManager(DatabaseName).UpdateFile(sfile);
                                    HisQueryManager.Instance.GetFileManager(DatabaseName).UpdateFile(vv.FullName);
                                }
                            }
                            if (KeepNoZipFileDays >= 0)
                                CheckAndZipHisFile(wpath);

                        }
                        catch (Exception ex)
                        {
                            LoggerService.Service.Erro("SeriseEnginer4", "DataFileReArrangeThreadPro: " + ex.Message);
                        }
                    }
                    mLastDataFileReArrangeProcessTime = DateTime.Now;
                }
                Thread.Sleep(1000);
            }

            mIsHisFileReArrangeFinish = true;
        }

        /// <summary>
        /// Zip .his file to save disk
        /// </summary>
        /// <param name="wpath"></param>
        private void CheckAndZipHisFile(string wpath)
        {
            foreach (var vv in new System.IO.DirectoryInfo(wpath).GetFiles("*.his"))
            {
                if (mIsClosed) break;
                while (mIsBusy) Thread.Sleep(1000);

                if (mIsClosed) break;

                string file = vv.FullName;

                if (System.IO.File.Exists(file))
                {
                    System.IO.FileInfo finfo = new System.IO.FileInfo(file);

                    if((DateTime.Now - finfo.LastWriteTime).TotalDays> KeepNoZipFileDays)
                    {
                        //保留7天的His格式的数据
                        ZipFile(finfo.FullName);
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
                using (System.IO.Compression.BrotliStream bs = new System.IO.Compression.BrotliStream(System.IO.File.Create(tfile),System.IO.Compression.CompressionLevel.Optimal))
                {
                    using (var vss = System.IO.File.Open(sfile, System.IO.FileMode.Open, System.IO.FileAccess.Read))
                    {
                        vss.CopyTo(bs);
                        vss.Close();
                    }
                    bs.Flush();
                    bs.Close();
                }
                System.IO.File.Delete(sfile);
                HisQueryManager.Instance.GetFileManager(DatabaseName).UpdateFile(sfile);
                HisQueryManager.Instance.GetFileManager(DatabaseName).UpdateFile(tfile);
                sw.Stop();
                LoggerService.Service.Info("SeriseEnginer4", "Zip 压缩文件 " +tfile +" 耗时:"+sw.ElapsedMilliseconds);
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("SeriseEnginer4", "ZipFile: " + ex.Message);
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
        private void DatabackupThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);

            int count = 0;

            while (!mIsClosed)
            {
                var wpath = SelectHisDataPath();

                var time = DateTime.Now;

                if (time.Day != mLastBackupDay)
                {
                    mLastBackupDay = time.Day;

                    try
                    {
                        string backpath = GetBackupDataPath();

                        if (!string.IsNullOrEmpty(backpath))
                        {
                            if (GetDriverUsedPercent(backpath) < 0.2)
                            {
                                LoggerService.Service.Warn("SeriseEnginer4", "free disk space is lower in backup path");
                            }
                        }

                        if (System.IO.Directory.Exists(wpath))
                        {
                            foreach (var vv in new System.IO.DirectoryInfo(wpath).GetFiles("*.his"))
                            {
                                if (mIsClosed) break;
                                try
                                {
                                    if ((time - vv.LastWriteTime).TotalDays >= HisDataKeepTimeInPrimaryPath)
                                    {

                                        if (!string.IsNullOrEmpty(backpath))
                                        {
                                            if (GetDriverFreeSize(backpath) > vv.Length)
                                            {
                                                string filename = System.IO.Path.Combine(backpath, vv.Name);
                                                vv.MoveTo(filename);
                                                HisQueryManager.Instance.GetFileManager(DatabaseName).UpdateFile(filename);

                                                if (!string.IsNullOrEmpty(backpath))
                                                {
                                                    if (GetDriverUsedPercent(backpath) < 0.2)
                                                    {
                                                        LoggerService.Service.Warn("SeriseEnginer4", "free disk space is lower in backup path");
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                if (GetDriverUsedPercent(wpath) < 0.05)
                                                {
                                                    vv.Delete();
                                                }
                                                LoggerService.Service.Erro("SeriseEnginer4", "There is not enough space for backup! free size:" + (GetDriverFreeSize(backpath) / 1024.0 / 1024) + "M. required size:" + (vv.Length / 1024.0 / 1024) + " M");
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
                                    LoggerService.Service.Erro("SeriseEnginer4", ex.Message + " " + ex.StackTrace);
                                }
                            }
                        }
                    }
                    catch
                    {

                    }
                }


                while(count>1000*60*10 && !mIsClosed)
                {
                    Thread.Sleep(1);
                    count++;
                }
                count = 0;
            }

            mIsBackupFinished = true;
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
            List<CompressMemory3> mtmp;
            lock (mWaitForProcessMemory)
            {
                mtmp = mWaitForProcessMemory.Values.ToList();
                mWaitForProcessMemory.Clear();
            }

            foreach (var vv in mtmp)
            {
                mSeriserFiles[vv.Id].SaveToFile(vv, vv.CurrentTime,vv.EndTime);
                vv.Clear();
                vv.MakeMemoryNoBusy();
            }


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

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }

    /// <summary>
    /// 
    /// </summary>
    public class SeriseFileItem4 : IDisposable
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

        /// <summary>
        /// 当前数据区首地址
        /// </summary>
        private long mCurrentDataRegion = 0;


        private string mCurrentFileName;

        private DataFileSeriserbase mFileWriter;

        private DataFileSeriserbase mFileWriter2;

        /// <summary>
        /// 数据文件扩展名
        /// </summary>
        public const string DataFileExtends = ".dbd";

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
        public const int FileHeadSize = 84;

        //private MemoryBlock mBlockPointMemory;

        //指针区域的起始地址
        private long mBlockPointOffset = 0;

        private long mDataRegionHeadSize = 0;

        //private VarintCodeMemory mTagIdMemoryCach;

        ////变量ID校验和
        //private long mTagIdSum;

        private DateTime mCurrentTime;

        //private int mTagCount = 0;

        /// <summary>
        /// 上一个数据区域首地址
        /// </summary>
        private long mPreDataRegion = 0;

        static object mFileLocker = new object();

        //private List<int> mTagIdsCach;

        //private Dictionary<DateTime, Dictionary<int, long>> mPointerCach = new Dictionary<DateTime, Dictionary<int, long>>();

        private Dictionary<string, Queue<IMemoryBlock>> mManualHisDataCach = new Dictionary<string, Queue<IMemoryBlock>>();

        //private Dictionary<int, int> mTagIndexCach = new Dictionary<int, int>();

        private int mId = 0;

        private DataFileSeriserbase mStatisticsWriter;

        private MarshalMemoryBlock mHeadMemory;

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

        /// <summary>
        /// 
        /// </summary>
        public DataFileSeriserbase FileWriter2 { get { return mFileWriter2; } set { mFileWriter2 = value; } }

        /// <summary>
        /// 
        /// </summary>
        public long CurrentDataRegion { get { return mCurrentDataRegion; } set { mCurrentDataRegion = value; } }

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


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Reset()
        {
            mNeedRecordDataHeader = true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private int GetDataRegionHeaderLength()
        {
            //头部结构：Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+file duration(4)+block duration(4)+Time tick duration(4)+ tagcount(4)
            return 8 + 8 + 8 + 4 + 4 + 4 + 4;
        }

        /// <summary>
        /// 添加文件头部
        /// </summary>
        private void AppendFileHeader(DateTime time, string databaseName,DataFileSeriserbase mFileWriter)
        {
            DateTime date = new DateTime(time.Year, time.Month, time.Day, ((time.Hour / FileDuration) * FileDuration), 0, 0);
            mFileWriter.Write(date, 0);
            byte[] nameBytes = new byte[64];
            Array.Clear(nameBytes, 0, nameBytes.Length);
            var ntmp = Encoding.UTF8.GetBytes(databaseName);
            Buffer.BlockCopy(ntmp, 0, nameBytes, 0, Math.Min(64, ntmp.Length));
            mFileWriter.Write(nameBytes, 20);
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
        /// 添加区域头部
        /// </summary>
        private void AppendDataRegionHeader(DateTime mCurrentTime, DataFileSeriserbase mFileWriter,long mPreDataRegion)
        {
            byte[] bval;
            int totalLen;
            int datalen;
            //更新上个DataRegion 的Next DataRegion Pointer 指针
            if (mPreDataRegion >= 0)
            {
                mFileWriter.Write(mCurrentDataRegion, mPreDataRegion + 8);
            }

            bval = GeneratorDataRegionHeader(mCurrentTime,out totalLen, out datalen);
            mFileWriter.Append(bval, 0, datalen);
            mFileWriter.AppendZore(totalLen - datalen);

            //更新DataRegion 的数量
            mFileWriter.Write(mFileWriter.ReadInt(16) + 1, 16);

            //LoggerService.Service.Debug("SeriseEnginer2", "AppendDataRegionHeader_数据区数量更新:" + mFileWriter.ReadInt(16));

            mPreDataRegion = mCurrentDataRegion;

            mBlockPointOffset = mCurrentDataRegion + mBlockPointOffset;

            ArrayPool<byte>.Shared.Return(bval);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mFileWriter"></param>
        private void NewDataRegionHeader(DateTime mCurrentTime, DataFileSeriserbase mFileWriter)
        {
            byte[] bval;
            int totalLen;
            int datalen;

            bval = GeneratorDataRegionHeader(mCurrentTime,out totalLen, out datalen);
            mFileWriter.Append(bval, 0, datalen);
            mFileWriter.AppendZore(totalLen - datalen);

            //将数据区域个数+1
            mFileWriter.Write(mFileWriter.ReadInt(16) + 1, 16);

            //LoggerService.Service.Debug("SeriseEnginer2", "NewDataRegionHeader_数据区数量更新:" + mFileWriter.ReadInt(16));

            ArrayPool<byte>.Shared.Return(bval);
        }

        /// <summary>
        /// 生成区域头部
        /// <paramref name="offset">偏移位置</paramref>
        /// </summary>
        private byte[] GeneratorDataRegionHeader(DateTime mCurrentTime, out int totallenght, out int datalen)
        {
            //Stopwatch sw = new Stopwatch();
            //sw.Start();
            //文件头部结构:Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+file duration(4)+ block duration(4)+Time tick duration(4)  + tagcount(4) + [data blockpoint(8)]
            int blockcount = FileDuration * 60 / BlockDuration;
            int len = GetDataRegionHeaderLength() + TagCountOneFile * (blockcount * 8);

            totallenght = len;

            byte[] bvals = ArrayPool<byte>.Shared.Rent(52);
            Array.Clear(bvals, 0, bvals.Length);

            using (System.IO.MemoryStream mHeadMemory = new System.IO.MemoryStream(bvals))
            {

                mHeadMemory.Position = 0;
                mHeadMemory.Write(BitConverter.GetBytes((long)mPreDataRegion));//更新Pre DataRegion 指针
                mHeadMemory.Write(BitConverter.GetBytes((long)0));                  //更新Next DataRegion 指针
                mHeadMemory.Write(MemoryHelper.GetBytes(mCurrentTime));            //写入时间
                mHeadMemory.Write(BitConverter.GetBytes(FileDuration));           //写入文件持续时间
                mHeadMemory.Write(BitConverter.GetBytes(BlockDuration));          //写入数据块持续时间
                mHeadMemory.Write(BitConverter.GetBytes(HisEnginer3.MemoryTimeTick)); //写入时间间隔
                mHeadMemory.Write(BitConverter.GetBytes(TagCountOneFile));

                mDataRegionHeadSize = mBlockPointOffset = mHeadMemory.Position;
                datalen = (int)mHeadMemory.Position;
                return bvals;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private string GetDataPath(DateTime time)
        {
            return System.IO.Path.Combine(SeriseEnginer4.HisDataPath, GetFileName(time));
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
            return System.IO.Path.Combine(SeriseEnginer4.HisDataPath, GetStatisticsFileName(time));
        }

        /// <summary>
        /// 搜索最后一个数据区域
        /// </summary>
        /// <returns></returns>
        private long SearchLastDataRegion()
        {
            long offset = FileHeadSize;
            while (true)
            {
                var nextaddr = mFileWriter.ReadLong(offset + 8);
                if (nextaddr <= 0)
                {
                    break;
                }
                else
                {
                    offset = nextaddr;
                }
            }
            mFileWriter.GoToEnd();
            mCurrentDataRegion = mFileWriter.CurrentPostion;
            return offset;
        }



        private Dictionary<int, List<long>> GetDataRegionHeadPoint(string sfile, SortedDictionary<int,List<DateTime>> ids, DateTime time, out DataFileSeriserbase mFileReader)
        {
            
            Dictionary<int,List<long>> re = new Dictionary<int, List<long>>();

            DataFileSeriserbase dfs;
            bool isuserhisfile = false;

            if ((time > mCurrentTime && mCurrentTime!=DateTime.MinValue)||(sfile == GetFileName(DateTime.UtcNow)))
            {
                //如果需要新建的文件，影响到自动记录存储要用到的文件，
                //则转到自动记录存储逻辑进行处理
                CheckFile(time);
                dfs = this.mFileWriter;
                //mCurrentTime = time;
            }
            else
            {
                if (sfile == mCurrentFileName)
                {
                    dfs = this.mFileWriter;
                }
                else
                {
                    dfs = mFileWriter2;
                    string hisfile = System.IO.Path.Combine(SeriseEnginer4.HisDataPath, sfile.Replace(DataFileExtends, HisDataFileExtends));

                    if (mFileWriter2.CheckExist(hisfile) && mFileWriter2.OpenFile(hisfile) && mFileWriter2.Length > FileHeadSize)
                    {
                        isuserhisfile = true;
                    }
                    else
                    {
                        var vfile = System.IO.Path.Combine(SeriseEnginer4.HisDataPath, sfile);
                        if (mFileWriter2.CreatOrOpenFile(vfile))
                        {
                            var date = new DateTime(time.Year, time.Month, time.Day, ((time.Hour / FileDuration) * FileDuration), 0, 0);
                            //新建文件
                            AppendFileHeader(time, this.DatabaseName, mFileWriter2);
                            NewDataRegionHeader(date, mFileWriter2);

                            LoggerService.Service.Info("SeriseEnginer", "new file head and data region head.");

                        }
                        else
                        {
                            //如果文件格式不正确，则新建
                            if (mFileWriter2.Length < FileHeadSize)
                            {
                                var date = new DateTime(time.Year, time.Month, time.Day, ((time.Hour / FileDuration) * FileDuration), 0, 0);
                                //新建文件
                                AppendFileHeader(time, this.DatabaseName, mFileWriter2);
                                NewDataRegionHeader(date, mFileWriter2);
                                LoggerService.Service.Info("SeriseEnginer", "new file head and data region head.");
                            }
                        }
                    }
                }
            }
            mFileReader = dfs;

            DateTime mLastRegionStartTime = DateTime.MaxValue;
            DateTime mLastRegionEndTime = DateTime.MaxValue;
            long regionOffset = 0;

            if (isuserhisfile)
            {
                //.his 格式的File文件 指针结构以变量为单位进行组织
                int blockcount = FileDuration * 60 / BlockDuration;
                foreach (var vv in ids)
                {
                    foreach (var vvv in vv.Value)
                    {
                        long ltmp = 0;
                        //计算本次更新对应的指针区域的起始地址
                        var fsh = (vvv.Hour / FileDuration) * FileDuration;
                        //int bid = ((vvv.Hour - fsh) * 60 + time.Minute) / BlockDuration;
                        int bid = ((vvv.Hour - fsh) * 60 + vvv.Minute) / BlockDuration;

                        if (mLastRegionStartTime == DateTime.MaxValue || vvv < mLastRegionStartTime || vvv > mLastRegionEndTime)
                        {
                            regionOffset = SearchDataRegionToDatetime(dfs, vvv, out mLastRegionStartTime, out mLastRegionEndTime);
                            ltmp = regionOffset;
                        }
                        else
                        {
                            ltmp = regionOffset;
                        }

                        if (ltmp < 0)
                        {
                            LoggerService.Service.Warn("SeriseEnginer4", "不允许修改本次运行之前时间的历史记录!");
                            return re;
                        }

                        //var icount = mTagIdsCach.IndexOf(vv.Key);
                        //var icount = mTagIndexCach[vv.Key];
                        var icount = vv.Key % TagCountOneFile;

                        ltmp += (mDataRegionHeadSize + blockcount * 8 * icount + bid * 8);
                        if (re.ContainsKey(vv.Key))
                        {
                            re[vv.Key].Add(ltmp);
                        }
                        else
                        {
                            re.Add(vv.Key, new List<long>() { ltmp });
                        }
                        // re.Add(vv.Key, ltmp);
                    }
                }
            }
            else
            {
                foreach (var vv in ids)
                {
                    foreach (var vvv in vv.Value)
                    {
                        long ltmp = 0;
                        //计算本次更新对应的指针区域的起始地址
                        var fsh = (vvv.Hour / FileDuration) * FileDuration;
                        //int bid = ((vvv.Hour - fsh) * 60 + time.Minute) / BlockDuration;
                        int bid = ((vvv.Hour - fsh) * 60 + vvv.Minute) / BlockDuration;

                        if (mLastRegionStartTime == DateTime.MaxValue || vvv < mLastRegionStartTime || vvv > mLastRegionEndTime)
                        {
                            regionOffset = SearchDataRegionToDatetime(dfs, vvv, out mLastRegionStartTime, out mLastRegionEndTime);
                            ltmp = regionOffset;
                        }
                        else
                        {
                            ltmp = regionOffset;
                        }

                        if (ltmp < 0)
                        {
                            LoggerService.Service.Warn("SeriseEnginer4", "不允许修改本次运行之前时间的历史记录!");
                            return re;
                        }

                        //var icount = mTagIdsCach.IndexOf(vv.Key);
                        //var icount = mTagIndexCach[vv.Key];
                        var icount = vv.Key%TagCountOneFile;

                        ltmp += mDataRegionHeadSize + TagCountOneFile * 8 * bid + icount * 8;
                        if (re.ContainsKey(vv.Key))
                        {
                            re[vv.Key].Add(ltmp);
                        }
                        else
                        {
                            re.Add(vv.Key, new List<long>() { ltmp });
                        }
                        // re.Add(vv.Key, ltmp);
                    }
                }
            }
            //LoggerService.Service.Debug("SeriseEnginer", "DataRegion Pointer:" + ltmp + ",mDataRegionHeadSize:" + mDataRegionHeadSize + ",BlockIndex:" + bid + " tag index:" + icount);

            return re;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private long SearchDataRegionToDatetime(DataFileSeriserbase mFileWriter, DateTime time,out DateTime startTime,out DateTime endTime)
        {
            long preoffset = -1, offset = FileHeadSize;
            DateTime tm;
            DateTime mStartTime=time.Date, mEndTime;
            while (true)
            {
                tm = mFileWriter.ReadDateTime(offset + 16);
                if (tm > time)
                {
                    mEndTime = tm;
                    break;
                }

                var nextaddr = mFileWriter.ReadLong(offset + 8);
                if (nextaddr <= 0)
                {
                    mStartTime = tm;
                    mEndTime = mStartTime.AddDays(1);
                    preoffset = offset;
                    break;
                }
                else
                {
                    mStartTime = tm;
                    preoffset = offset;
                    offset = nextaddr;
                }
            }
            startTime = mStartTime;
            endTime = mEndTime;
            return preoffset;
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

            Dictionary<int, List<long>> mHeadAddress;
            Dictionary<int, List<long>> mHeadValue = new Dictionary<int, List<long>>();

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
                    int id = vvv.ReadInt(0+56);
                    int size = vvv.ReadInt(20 + 56);
                    DateTime time = vvv.ReadDateTime(4 + 56);

                    DateTime endTime = vvv.ReadDateTime(12 + 56);
                   
                    if(times.ContainsKey(id))
                    {
                        times[id].Add(time);
                    }
                    else
                    {
                        times.Add(id, new List<DateTime>() { time });
                    }
                    maxTime = time > maxTime ? time : maxTime;
                    mLastModifyTime = endTime > mLastModifyTime ? endTime : mLastModifyTime;

                    //datasize += (size - 28-56);
                }

                mHeadAddress = GetDataRegionHeadPoint(vv.Key, times, maxTime, out mwriter);

                long ltmp = sw.ElapsedMilliseconds;

                mHeadValue.Clear();
                mwriter.GoToEnd();
                //var blockpointer = mwriter.CurrentPostion;
                var vpointer = mwriter.CurrentPostion;


                //写入数据，同时获取数据块地址
                foreach (var vvv in vv.Value)
                {
                    int id = vvv.ReadInt(0 + 56);
                    int size = vvv.ReadInt(20 + 56);
                    if (mHeadValue.ContainsKey(id))
                    {
                        mHeadValue[id].Add(vpointer);
                    }
                    else
                    {
                        mHeadValue.Add(id, new List<long>() { vpointer });
                    }
                    vvv.WriteToStream(mwriter.GetStream(), 28 + 56, size - 28 - 56);//直接拷贝数据块
                    vpointer += (size - 28 - 56);
                    datasize += (size - 28 - 56);
                }

                //更新数据块指针
                foreach (var hd in mHeadAddress)
                {
                    for (int i = 0; i < hd.Value.Count; i++)
                    {
                        mwriter.Write(mHeadValue[hd.Key][i], hd.Value[i]);//写入地址
                    }
                    datasize += 8;
                }
                LoggerService.Service.Info("SeriseEnginer4", "SeriseFileItem " + this.Id + " 完成存储,数据块:" + vv.Value.Count + " 数据量:" + datasize + " 耗时:" + sw.ElapsedMilliseconds);



                //更新文件的最后修改时间
                var vtmp = mwriter.ReadDateTime(8);
                if (mLastModifyTime > vtmp)
                {
                    mwriter.Write(mLastModifyTime, 8);
                }

                mwriter.Flush();

                if (mwriter != mFileWriter)
                    mwriter.Close();

                UpdateStaticstics(vv.Value);

                foreach (var vvv in vv.Value)
                {
                    (vvv as MarshalMemoryBlock).MakeMemoryNoBusy();
                    MarshalMemoryBlockPool.Pool.Release(vvv as MarshalMemoryBlock);
                }

            }
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
        /// 
        /// </summary>
        public SeriseFileItem4 Init()
        {
            mHeadMemory = new MarshalMemoryBlock(TagCountOneFile * 8, TagCountOneFile * 8);
            return this;
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
                    if (re.Length < TagCountOneFile * 8)
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
            var cm = (mProcessMemory as CompressMemory3).StaticsMemoryBlock;
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

        /// <summary>
        /// 检查文件是否存在
        /// </summary>
        /// <param name="time"></param>
        private bool CheckFile(DateTime time)
        {
            if (!CheckInSameFile(time))
            {

                if (mFileWriter != null)
                {
                    mFileWriter.Flush();
                    mFileWriter.Close();
                }

                string sfile = GetDataPath(time);
                

                if (mFileWriter.CreatOrOpenFile(sfile))
                {
                    var date = new DateTime(time.Year, time.Month, time.Day, ((time.Hour / FileDuration) * FileDuration), 0, 0);
                    AppendFileHeader(time, this.DatabaseName, mFileWriter);
                    //新建文件
                    mCurrentDataRegion = FileHeadSize;
                    mPreDataRegion = -1;
                    AppendDataRegionHeader(date,mFileWriter, -1);

                    LoggerService.Service.Info("SeriseEnginer", "new file head and data region head in CheckFile.");
                }
                else
                {
                    if (mFileWriter.Length < FileHeadSize)
                    {
                        var date = new DateTime(time.Year, time.Month, time.Day, ((time.Hour / FileDuration) * FileDuration), 0, 0);
                        AppendFileHeader(time, this.DatabaseName, mFileWriter);
                        //新建文件
                        mCurrentDataRegion = FileHeadSize;
                        mPreDataRegion = -1;
                        AppendDataRegionHeader(date, mFileWriter, -1);
                        LoggerService.Service.Info("SeriseEnginer", "new file head and data region head in CheckFile.");
                    }
                    else 
                    {
                        //打开已有文件
                        mPreDataRegion = SearchLastDataRegion();
                        AppendDataRegionHeader(mCurrentTime,mFileWriter, mPreDataRegion);

                        LoggerService.Service.Info("SeriseEnginer", "new data region head in CheckFile.");

                    }
                }

                if(mNeedRecordDataHeader) mNeedRecordDataHeader = false;

                mCurrentFileName = GetFileName(time);
            }
            else
            {
                if (mNeedRecordDataHeader)
                {
                    mPreDataRegion = SearchLastDataRegion();
                    AppendDataRegionHeader(mCurrentTime,mFileWriter, mPreDataRegion);
                    mNeedRecordDataHeader = false;
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
                    mCurrentTime = time;

                    var ltmp = sw.ElapsedMilliseconds;

                    //打开文件
                    if (!CheckFile(time))
                        return;

                    //更新最后写入时间
                    var vtmp = mFileWriter.ReadDateTime(8);
                    if (endTime > vtmp)
                    {
                        mFileWriter.Write(endTime, 8);
                    }

                    if (datasize == 0)
                    {
                        Flush();
                        sw.Stop();
                        return;
                    }

                    var ltmp2 = sw.ElapsedMilliseconds;

                    long offset = 8 + dataOffset;
                    long start = count * 8 + offset;//计算出数据起始地址

                    var dataAddr = this.mFileWriter.GoToEnd().CurrentPostion;
                   
                    //计算本次更新对应的指针区域的起始地址
                    FileStartHour = (time.Hour / FileDuration) * FileDuration;
                    int bid = ((time.Hour - FileStartHour) * 60 + time.Minute) / BlockDuration;
                    //计算出本次更新的头地址地址
                    var pointAddr = mBlockPointOffset + TagCountOneFile * 8 * bid;

                    //long ctmp = 0;

                    long writedatasize = mHeadMemory.AllocSize;

                    mHeadMemory.Clear();

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
                            //ctmp = pointAddr + id * 8;

                            mHeadMemory.WriteLong(id * 8, addr + dataAddr);
                            //mFileWriter.Write(dataAddr + addr, ctmp); //写入偏移地址
                        }
                    }
                                       
                    //CompressMemory3 cm = mProcessMemory as CompressMemory3;
                    mProcessMemory.WriteToStream(mFileWriter.GetStream(), start, datasize);//直接拷贝数据，写入数据

                    mFileWriter.GoTo(pointAddr);
                    mHeadMemory.WriteToStream(mFileWriter.GetStream(), 0, mHeadMemory.AllocSize);//写入块指针

                    writedatasize += datasize;

                    Flush();

                    UpdateStaticstics(mProcessMemory,time);
                    sw.Stop();

                    LoggerService.Service.Info("SeriseFileItem" + Id, "写入数据 " + mCurrentFileName + "  数据大小：" + (writedatasize / 1024.0 / 1024) + " m" +  "存储耗时:" + (sw.ElapsedMilliseconds));
                }
            }
            catch (System.IO.IOException ex)
            {
                LoggerService.Service.Erro("SeriseEnginer" + Id, ex.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Flush()
        {
            mFileWriter.Flush();
            mFileWriter.CloseAndReOpen();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            mIdAddrs.Clear();
            mFileWriter.Dispose();
            mFileWriter = null;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
