//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/16 13:33:36.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using System.Linq;

namespace Cdy.Tag
{

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

    /// <summary>
    /// 
    /// </summary>
    public class DataFileInfo5: IDataFile
    {

        #region ... Variables  ...

        //private SortedDictionary<DateTime, Tuple<TimeSpan, long, DateTime>> mTimeOffsets = new SortedDictionary<DateTime, Tuple<TimeSpan, long, DateTime>>();

        private bool mInited = false;

        private static object mLockObj = new object();

        private DateTime mLastTime;

        //private long mLastProcessOffset = -1;

        ///// <summary>
        ///// 
        ///// </summary>
        //private int mRegionCount = 0;

        /// <summary>
        /// 单个历史文件记录的变量个数
        /// </summary>
        public const int PageFileTagCount = 100000;

        public const int MetaFileHeadSize = 98;

        private object mLocker = new object();

        private object mLocker2 = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public DateTime LastTime
        {
            get
            {
                return mLastTime;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public string FId { get; set; }

        /// <summary>
        /// 
        /// </summary>
        private string mFileName;

        /// <summary>
        /// 
        /// </summary>
        public string FileName { get { return mFileName; } set { mFileName = value; CheckMetaFile(); } }

        private string mMetaFileName;

        /// <summary>
        /// 
        /// </summary>
        public string MetaFileName { get { return mMetaFileName; }private set { mMetaFileName = value; } }

        /// <summary>
        /// 
        /// </summary>
        public string BackFileName { get; set; }

        /// <summary>
        /// 开始时间
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime EndTime
        {
            get
            {
                return StartTime + Duration;
            }
            set
            {
                Duration = value - StartTime;
            }
        }

        /// <summary>
        /// 时间长度
        /// </summary>
        public TimeSpan Duration { get; set; }

        /// <summary>
        /// 是否为压缩文件
        /// </summary>
        public bool IsZipFile { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int MaxtTagCount { get; set; }

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
        public int TimeTick { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public short Version { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string DatabaseName { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        private void CheckMetaFile()
        {
            if(!string.IsNullOrEmpty(FileName))
            {
                string basefile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(FileName),System.IO.Path.GetFileNameWithoutExtension(FileName));
                string mm = basefile + ".dbm2";
                if(System.IO.File.Exists(mm))
                {
                    this.MetaFileName = mm;
                    return;
                }
                mm = basefile + ".zdbm2";
                if (System.IO.File.Exists(mm))
                {
                    this.MetaFileName = mm;
                }
                else
                {
                    this.MetaFileName = this.FileName.Replace("dbd2", "dbm2");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void UpdateLastDatetime()
        {

            lock (mLockObj)
            {
                //对于压缩文件，说明很老的文件，不需要更新
                if (IsZipFile) return;

                //mTimeOffsets.Clear();
                GetFileLastUpdateTime();

                lock (DataFileManager.CurrentDateTime)
                {
                    if (DataFileManager.CurrentDateTime.ContainsKey(FId))
                    {
                        if (DataFileManager.CurrentDateTime[FId] < mLastTime)
                            DataFileManager.CurrentDateTime[FId] = mLastTime;
                    }
                    else
                    {
                        DataFileManager.CurrentDateTime.Add(FId, mLastTime);
                    }
                }

                HeadPointDataCachManager.Manager.ClearMemoryCach(this.FileName);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void GetFileLastUpdateTime()
        {
            using (var ss = DataFileSeriserManager.manager.GetDefaultFileSersie())
            {
                ss.OpenForReadOnly(MetaFileName);
                //读取最后一次更新时间
                mLastTime = ss.ReadDateTime(8);
                //更新总变量数
                MaxtTagCount = ss.ReadInt(16);

                ss.Dispose();
                mInited = false;
            }
        }

        private void ReadHeadInfo()
        {
            using (var ss = DataFileSeriserManager.manager.GetDefaultFileSersie())
            {
                CheckZipFile();

                ss.OpenForReadOnly(MetaFileName);

                if (ss.Length <= 0) return;

                //读取文件时间
                DateTime fileTime = ss.ReadDateTime(0);
                //读取最后一次更新时间
                mLastTime = ss.ReadDateTime(8);

                MaxtTagCount = ss.ReadInt(16);

                FileDuration = ss.ReadInt(20);

                BlockDuration = ss.ReadInt(24);

                TimeTick = ss.ReadInt(28);

                Version = ss.ReadShort(32);

                var strlen = ss.ReadShort(34);
                DatabaseName = Encoding.UTF8.GetString(ss.ReadBytes(36, strlen));


                mInited = true;
            }
        }

       

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public void Scan()
        {
            lock (mLocker2)
            {
                if (!mInited)
                {
                    ReadHeadInfo();
                    mInited = true;
                }
            }
        }


        /// <summary>
        /// 读取某个变量的所有数据指针
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public long[] ReadAddress(int id)
        {
            Scan();
            var blockcountontag = FileDuration * 60 / BlockDuration;
            var rid = id % PageFileTagCount;

            var saddr = rid * blockcountontag * 8+ MetaFileHeadSize;
            long[] re = System.Buffers.ArrayPool<long>.Shared.Rent(blockcountontag);

            using (var ss = DataFileSeriserManager.manager.GetDefaultFileSersie())
            {
                ss.OpenForReadOnly(MetaFileName);

                if (ss.IsOpened())
                {
                    var vbyts = ss.ReadBytes(saddr, blockcountontag * 8);
                    for (int i = 0; i < blockcountontag; i++)
                    {
                        re[i] = BitConverter.ToInt64(vbyts, i * 8);
                    }
                }
                ss.Dispose();
                mInited = false;
            }
            return re;
        }

        /// <summary>
        /// 读取指定时间段覆盖的数据块
        /// </summary>
        /// <param name="id"></param>
        /// <param name="stime"></param>
        /// <param name="etime"></param>
        /// <returns></returns>
        public List<long> ReadDataBlock(int id,DateTime stime,DateTime etime)
        {
            Scan();
            List<long> re = new List<long>();
            var blocks = ReadAddress(id);
            var sindx = (int)((stime - StartTime).TotalMinutes / BlockDuration);

            if (blocks!=null && blocks.Length > 0)
            {
                DateTime ss = StartTime.AddMinutes(sindx * BlockDuration);
                do
                {
                    re.Add(blocks[sindx++]);
                    ss = ss.AddMinutes(BlockDuration);
                }
                while (ss < etime);
            }

            System.Buffers.ArrayPool<long>.Shared.Return(blocks);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="blockindex"></param>
        /// <returns></returns>
        public long ReadDataBlock(int id, int blockindex)
        {
            Scan();

            var blocks = ReadAddress(id);
            try
            {
                if (blocks != null && blocks.Length > 0)
                {

                    return blocks[blockindex];
                }
                else
                {
                    return 0;
                }
            }
            finally
            {
                System.Buffers.ArrayPool<long>.Shared.Return(blocks);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="id"></param>
        /// <param name="address"></param>
        public void UpdateDataBlock(DataFileSeriserbase file,int id,Dictionary<int,long> address)
        {
            Scan();
            var blockcountontag = FileDuration * 60 / BlockDuration;
            var rid = id % PageFileTagCount;
            var saddr = rid * blockcountontag * 8 + MetaFileHeadSize;

            foreach(var addr in address)
            {
                file.Write(addr.Value, saddr + addr.Key * 8);
            }

        }

        /// <summary>
        /// 读取时间点所在的数据块信息
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <returns>返回数据块ID，数据指针地址，数据块所包含的时间的集合</returns>
        public Dictionary<Tuple<int,long>,List<DateTime>> ReadDataBlock(int id, List<DateTime> times)
        {
            Scan();

            Dictionary<Tuple<int, long>, List<DateTime>> re = new Dictionary<Tuple<int, long>, List<DateTime>>();

            Dictionary<int, List<DateTime>> rtmp = new Dictionary<int, List<DateTime>>();

            Dictionary<long,int> indx = new Dictionary<long, int>();

            var blocks = ReadAddress(id);
            if (blocks != null && blocks.Length > 0)
            {
                foreach (var stime in times)
                {
                    //计算某个时间，所在的数据块ID
                    var sindx = (int)((stime - StartTime).TotalMinutes / BlockDuration);
                    if (rtmp.ContainsKey(sindx))
                    {
                        rtmp[sindx].Add(stime);
                    }
                    else
                    {
                        rtmp.Add(sindx, new List<DateTime>() { stime });
                    }
                }

                foreach (var vid in rtmp)
                {
                    re.Add(new Tuple<int, long>(vid.Key, blocks[vid.Key]), vid.Value);
                    //indx.Add(blocks[vid.Key],vid.Key);
                }
            }

            System.Buffers.ArrayPool<long>.Shared.Return(blocks);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <returns></returns>
        public Dictionary<Tuple<int, long>, List<DateTime>> ReadDataBlock(int id, IEnumerable<DateTime> times)
        {
            Scan();

            Dictionary<Tuple<int, long>, List<DateTime>> re = new Dictionary<Tuple<int, long>, List<DateTime>>();

            Dictionary<int, List<DateTime>> rtmp = new Dictionary<int, List<DateTime>>();

            Dictionary<long, int> indx = new Dictionary<long, int>();

            var blocks = ReadAddress(id);
            if (blocks != null && blocks.Length > 0)
            {
                foreach (var stime in times)
                {
                    var sindx = (int)((stime - StartTime).TotalMinutes / BlockDuration);
                    if (rtmp.ContainsKey(sindx))
                    {
                        rtmp[sindx].Add(stime);
                    }
                    else
                    {
                        rtmp.Add(sindx, new List<DateTime>() { stime });
                    }
                }

                foreach (var vid in rtmp)
                {
                    re.Add(new Tuple<int, long>(vid.Key, blocks[vid.Key]), vid.Value);
                    //indx.Add(blocks[vid.Key],vid.Key);
                }
            }
            System.Buffers.ArrayPool<long>.Shared.Return(blocks);
            return re;
        }

        /// <summary>
        /// 判断某个时间点是否有数据
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public bool HasValue(long[] pointers,DateTime time)
        {
            var dindex = (int)((time - StartTime).TotalMinutes / BlockDuration);
            return pointers[dindex] > 0;
        }

        public void CheckZipFile()
        {
            try
            {
                lock (mLocker)
                {
                    if (IsZipFile && (System.IO.Path.GetExtension(FileName) == DataFileManager.ZipDataFile2Extends || !System.IO.File.Exists(FileName)))
                    {
                        if (!System.IO.File.Exists(FileName) && !string.IsNullOrEmpty(BackFileName)) FileName = BackFileName;

                        //对于使用二次压缩的文件，先解压到临时目录里，然后每次查询从解压后文件进行读取
                        string spath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(FileName), "tmp");
                        if (!System.IO.Directory.Exists(spath))
                        {
                            System.IO.Directory.CreateDirectory(spath);
                        }
                        var sspath = System.IO.Path.Combine(spath, System.IO.Path.GetFileName(FileName).Replace(DataFileManager.ZipDataFile2Extends, DataFileManager.DataFile2Extends));

                        if (!System.IO.File.Exists(sspath))
                            DataFileInfo5Extend.UnZipFile(FileName, sspath);

                        //拷贝、解压Meta文件
                        string smatafile = MetaFileName;


                        if (smatafile.EndsWith(DataFileManager.ZipDataMetaFile2Extends))
                        {
                            var smpath = System.IO.Path.Combine(spath, System.IO.Path.GetFileName(smatafile).Replace(DataFileManager.ZipDataMetaFile2Extends, DataFileManager.DataMetaFile2Extends));
                            if (!System.IO.File.Exists(smpath))
                                DataFileInfo5Extend.UnZipFile(smatafile, smpath);
                        }
                        else if (smatafile.EndsWith(DataFileManager.DataMetaFile2Extends))
                        {
                            var smpath = System.IO.Path.Combine(spath, System.IO.Path.GetFileName(smatafile).Replace(DataFileManager.ZipDataMetaFile2Extends, DataFileManager.DataMetaFile2Extends));
                            if (!System.IO.File.Exists(smpath))
                                System.IO.File.Copy(smatafile, smpath, true);
                        }


                        BackFileName = FileName;
                        FileName = sspath;
                    }
                }
            }
            catch
            {

            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }




    public static class DataFileInfo5Extend
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static DataFileSeriserbase GetFileSeriser(this DataFileInfo5 file)
        {
            //判断是否为压缩文件
            file.CheckZipFile();
            var re = DataFileSeriserManager.manager.GetDefaultFileSersie();
            re.FileName = file.FileName;
            re.OpenForReadOnly(file.FileName);
            return re;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="sfile"></param>
        /// <param name="targetfile"></param>
        public static void UnZipFile(string sfile, string targetfile)
        {
            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                CheckAndFreeDisk(targetfile);
                using (System.IO.Compression.BrotliStream bs = new System.IO.Compression.BrotliStream(System.IO.File.Open(sfile, System.IO.FileMode.Open), System.IO.Compression.CompressionMode.Decompress))
                {
                    using (var vss = System.IO.File.Open(targetfile, System.IO.FileMode.OpenOrCreate, System.IO.FileAccess.ReadWrite))
                    {
                        bs.CopyTo(vss);
                        vss.Flush();
                        vss.Close();
                    }
                    bs.Close();
                }

                sw.Stop();
                LoggerService.Service.Info(" HisDataFileInfo4", "Zip 解压文件文件 " + targetfile + " 耗时:" + sw.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                LoggerService.Service.Info(" HisDataFileInfo4", "Zip 解压文件文件 " + ex.Message);
            }
        }

        private static void CheckAndFreeDisk(string file)
        {
            double lsize = GetDiskFree(file) / 1024 / 1024;
            double minisize = 4096;
            if (lsize < minisize)
            {
                var dir = new System.IO.DirectoryInfo(System.IO.Path.GetDirectoryName(file));
                if (dir.Exists)
                {
                    try
                    {
                        foreach (var vv in dir.GetFiles().OrderBy(e => e.LastWriteTime))
                        {
                            //优先删除较早的文件
                            vv.Delete();
                            lsize = GetDiskFree(file) / 1024 / 1024;
                            if (lsize > minisize) break;
                        }
                    }
                    catch (Exception ex)
                    {
                        LoggerService.Service.Info(" HisDataFileInfo4", "删除文件错误： " + ex.Message);
                    }
                }
            }
        }

        /// <summary>
        /// 获取磁盘剩余空间
        /// </summary>
        /// <returns></returns>
        private static double GetDiskFree(string file)
        {
            string dir = System.IO.Path.GetDirectoryName(file);
            if (System.IO.Directory.Exists(dir))
            {
                var vd = new System.IO.DirectoryInfo(dir);
                foreach (var vv in System.IO.DriveInfo.GetDrives())
                {
                    if (vv.RootDirectory.FullName == vd.Root.FullName)
                    {
                        return vv.AvailableFreeSpace;
                    }
                }
            }
            return 0;
        }

        #region 读取所有值


        /// <summary>
        /// 读取某时间段内的所有bool值
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue<T>(this DataFileInfo5 file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<T> result)
        {
            var blocks = file.ReadDataBlock(tid,startTime,endTime);
            using (var vss = file.GetFileSeriser())
            {
                try
                {
                    if (!vss.IsOpened()) return;

                    foreach (var block in blocks)
                    {
                        if (block > 0)
                        {
                            //从每个数据块的读取值
                            DeCompressOneTimeUnitDataBlockAllValue(vss, block, startTime, endTime, file.TimeTick, result);
                        }
                    }
                }
                catch
                {

                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="file"></param>
        /// <param name="id"></param>
        /// <param name="values"></param>
        public static void ModifyHisData<T>(this DataFileInfo5 file, int id,DateTime startime,DateTime endtime, HisQueryResult<T> values)
        {
            using (var vss = file.GetFileSeriser())
            {

                if (System.IO.File.Exists(file.MetaFileName))
                {
                    var blocks = file.ReadDataBlock(id, values.ListAllTimes());
                    foreach (var block in blocks)
                    {
                        var stime = file.StartTime.AddMinutes(file.BlockDuration * block.Key.Item1);
                        var etime = file.StartTime.AddMinutes(file.BlockDuration * (block.Key.Item1 + 1));

                        ModifyBlockHisData(file, vss, id, block.Key.Item2, file.TimeTick, stime, etime, new List<DateTime>() { startime, endtime }, values);
                    }
                }
                else
                {
                    //如果文件不存在，则新建相关配置
                    for(int i=0;i<file.FileDuration*60/file.BlockDuration;i++)
                    {
                        var stime = file.StartTime.AddMinutes(file.BlockDuration * i);
                        var etime = file.StartTime.AddMinutes(file.BlockDuration * (i + 1));

                        ModifyNewBlockHisData(file, vss, id, stime, etime, new List<DateTime>() { startime, endtime }, values);
                    }
                }
            }
            ServiceLocator.Locator.Resolve<IHisDataManagerService>()?.ClearTmpFile();
            if (file.IsZipFile)
            {
                UpdateToZipFile(file);
            }
        }

        /// <summary>
        /// 更新变动内容到压缩文件
        /// </summary>
        /// <param name="file"></param>
        public static void UpdateToZipFile(this DataFileInfo5 file)
        {
            if (file.IsZipFile && file.FileName.Contains("tmp") && !string.IsNullOrEmpty(file.BackFileName))
            {
                string stmp = file.BackFileName + "_b";
                ZipFile(file.FileName, stmp);

                if(System.IO.File.Exists(file.BackFileName))
                {
                    System.IO.File.Delete(file.BackFileName);
                }
                //System.IO.File.Copy(stmp,file.BackFileName, true);
                new System.IO.FileInfo(stmp).MoveTo(file.BackFileName);
            }
        }

        /// <summary>
        /// 压缩文件
        /// </summary>
        /// <param name="sfile"></param>
        /// <param name="targetfile"></param>
        public static  void ZipFile(string sfile,string targetfile)
        {
            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                string tfile = targetfile;
                using (System.IO.Compression.BrotliStream bs = new System.IO.Compression.BrotliStream(System.IO.File.Create(tfile), System.IO.Compression.CompressionLevel.Fastest))
                {
                    using (var vss = System.IO.File.Open(sfile, System.IO.FileMode.Open, System.IO.FileAccess.Read,System.IO.FileShare.ReadWrite))
                    {
                        vss.CopyTo(bs);
                        vss.Close();
                    }
                    bs.Flush();
                    bs.Close();
                }
                sw.Stop();
                LoggerService.Service.Info("DataFileInfo5", "Zip 压缩文件 " + tfile + " 耗时:" + sw.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("DataFileInfo5", "ZipFile: " + ex.Message);
            }
        }

        /// <summary>
        /// 清除历史数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="file"></param>
        /// <param name="id"></param>
        /// <param name="values"></param>
        public static void DeleteHisData<T>(this DataFileInfo5 file, int id, DateTime starttime,DateTime endtime)
        {
            using (var vss = file.GetFileSeriser())
            {
                var blocks = file.ReadDataBlock(id, new List<DateTime>() { starttime,endtime});
                foreach (var block in blocks)
                {
                    var stime = file.StartTime.AddMinutes(file.BlockDuration * block.Key.Item1);
                    var etime = file.StartTime.AddMinutes(file.BlockDuration * (block.Key.Item1 + 1));

                    DeleteBlockHisData<T>(file, vss, id, block.Key.Item2, file.TimeTick, stime, etime,  new List<DateTime>() { starttime,endtime});
                }
            }
            ServiceLocator.Locator.Resolve<IHisDataManagerService>()?.ClearTmpFile();
            if (file.IsZipFile)
            {
                UpdateToZipFile(file);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="file"></param>
        /// <param name="index"></param>
        /// <param name="address"></param>
        /// <param name="timeTick"></param>
        /// <param name="blockStartTime"></param>
        /// <param name="blockEndTime"></param>
        /// <param name="times"></param>
        /// <param name="values"></param>
        public static void ModifyBlockHisData<T>(DataFileInfo5 dfile,DataFileSeriserbase file,int id,long address,int timeTick,DateTime blockStartTime,DateTime blockEndTime,List<DateTime> times, HisQueryResult<T> values)
        {
            HisQueryResult<T> result = new HisQueryResult<T>(60*5*1000);

            var mtmp = ReadAllRelativeBlock(file, address);
            DateTime stime = blockStartTime;
            foreach (var block in mtmp)
            {
                DeCompressDataBlockAllValue<T>(block, stime, blockEndTime, timeTick, result);
                stime = result.LastTime.AddMilliseconds(1);
                block.Dispose();
            }
            DateTime sstime = times[0];
            DateTime estime = times[times.Count - 1];
            SortedDictionary<DateTime,Tuple<T,byte>> values2 = new SortedDictionary<DateTime, Tuple<T, byte>>();

            for (int i=0;i<result.Count;i++)
            {
                var value = result.GetValue(i, out DateTime time, out byte quality);
                if (quality == (byte)QualityConst.Null) continue;

                if(time>=sstime && time<=estime && time>=blockStartTime && time<=blockEndTime)
                {
                    //清空这段时间内的原来的历史数据
                    //对于停机标识，一定要记录，不可修改
                    if(quality == (byte)QualityConst.Close)
                    {
                        values2.Add(time,new Tuple<T, byte>(value,quality));
                    }

                }
                else
                {
                    values2.Add(time, new Tuple<T, byte>(value, quality));
                }
            }

            for(int i=0;i<values.Count;i++)
            {
                var val = values.GetValue(i,out DateTime time, out byte quality);
                if(time >= sstime && time<=estime)
                {
                    values2.Add(time, new Tuple<T, byte>(val, quality));
                }
            }

            HisQueryResult<T> result2 = new HisQueryResult<T>(60 * 5 * 1000);
            foreach (var vv in values2)
            {
               result2.Add<T>(vv.Value.Item1,vv.Key,vv.Value.Item2);
            }

            //to do write datas
            var mdata = ServiceLocator.Locator.Resolve<IDataCompressService>().CompressData(id, blockStartTime, result2);

            ServiceLocator.Locator.Resolve<IHisDataManagerService>()?.SaveData(id, blockStartTime, mdata, SaveType.Replace, dfile.FileName);

            if (mdata != null)
                ServiceLocator.Locator.Resolve<IDataCompressService>()?.Release(mdata);
            result.Dispose();
            result2.Dispose();
        }

        public static void ModifyNewBlockHisData<T>(DataFileInfo5 dfile, DataFileSeriserbase file, int id, DateTime blockStartTime, DateTime blockEndTime, List<DateTime> times, HisQueryResult<T> values)
        {
            DateTime sstime = times[0];
            DateTime estime = times[times.Count - 1];
            SortedDictionary<DateTime, Tuple<T, byte>> values2 = new SortedDictionary<DateTime, Tuple<T, byte>>();

            for (int i = 0; i < values.Count; i++)
            {
                var val = values.GetValue(i, out DateTime time, out byte quality);
                if ((time >= sstime && time <= estime)&&( time>=blockStartTime && time<=blockEndTime))
                {
                    values2.Add(time, new Tuple<T, byte>(val, quality));
                }
            }

            if (values2.Count <= 0) return;

            HisQueryResult<T> result2 = new HisQueryResult<T>(60 * 5 * 1000);
            foreach (var vv in values2)
            {
                result2.Add<T>(vv.Value.Item1, vv.Key, vv.Value.Item2);
            }

            //to do write datas
            var mdata = ServiceLocator.Locator.Resolve<IDataCompressService>().CompressData(id, blockStartTime, result2);

            ServiceLocator.Locator.Resolve<IHisDataManagerService>()?.SaveData(id, blockStartTime, mdata, SaveType.Replace,"");

            if (mdata != null)
                ServiceLocator.Locator.Resolve<IDataCompressService>()?.Release(mdata);

            result2.Dispose();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="file"></param>
        /// <param name="index"></param>
        /// <param name="address"></param>
        /// <param name="timeTick"></param>
        /// <param name="blockStartTime"></param>
        /// <param name="blockEndTime"></param>
        /// <param name="times"></param>
        /// <param name="values"></param>
        public static void DeleteBlockHisData<T>(DataFileInfo5 dfile, DataFileSeriserbase file, int id, long address, int timeTick, DateTime blockStartTime, DateTime blockEndTime, List<DateTime> timers)
        {
            HisQueryResult<T> result = new HisQueryResult<T>(60 * 5 * 1000);
            var mtmp = ReadAllRelativeBlock(file, address);
            DateTime stime = blockStartTime;
            foreach (var block in mtmp)
            {
                DeCompressDataBlockAllValue<T>(block, stime, blockEndTime, timeTick, result);
                stime = result.LastTime.AddMilliseconds(1);
                block.Dispose();
            }
            DateTime sstime = timers[0];
            DateTime estime = timers[timers.Count - 1];
            SortedDictionary<DateTime, Tuple<T, byte>> values2 = new SortedDictionary<DateTime, Tuple<T, byte>>();

            for (int i = 0; i < result.Count; i++)
            {
                var value = result.GetValue(i, out DateTime time, out byte quality);
                if (quality == (byte)QualityConst.Null) continue;

                if ((time >= sstime && time <= estime) && (time >= blockStartTime && time <= blockEndTime))
                {
                    //清空这段时间内的原来的历史数据

                    //对于停机标识，一定要记录，不可修改
                    if (quality == (byte)QualityConst.Close)
                    {
                        values2.Add(time, new Tuple<T, byte>(value, quality));
                    }

                }
                else
                {
                    values2.Add(time, new Tuple<T, byte>(value, quality));
                }
            }

            HisQueryResult<T> result2 = new HisQueryResult<T>(60 * 5 * 1000);
            foreach (var vv in values2)
            {
                result2.Add<T>(vv.Value.Item1, vv.Key, vv.Value.Item2);
            }

            //to do write datas
            var mdata = ServiceLocator.Locator.Resolve<IDataCompressService>().CompressData(id, blockStartTime, result2);
            ServiceLocator.Locator.Resolve<IHisDataManagerService>()?.SaveData(id, blockStartTime, mdata, SaveType.Replace,file.FileName);


            if (mdata != null)
                ServiceLocator.Locator.Resolve<IDataCompressService>()?.Release(mdata);
            result.Dispose();
            result2.Dispose();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="file"></param>
        /// <param name="address"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressOneTimeUnitDataBlockAllValue<T>(DataFileSeriserbase file, long address, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<T> result)
        {
            var mtmp = ReadAllRelativeBlock(file,address);
            DateTime stime = startTime;
            foreach (var block in mtmp)
            {
                DeCompressDataBlockAllValue<T>(block, stime, endTime,timeTick,result);
                stime = result.LastTime.AddMilliseconds(1);
                block.Dispose();
            }
        }

        /// <summary>
        /// 获取某个块指针对应的所有数据块
        /// </summary>
        /// <param name="file"></param>
        /// <param name="address"></param>
        /// <returns></returns>
        private static IEnumerable<MarshalMemoryBlock> ReadAllRelativeBlock(DataFileSeriserbase file,long address)
        {
            List<MarshalMemoryBlock> mtmp = new List<MarshalMemoryBlock>(2);
            MarshalMemoryBlock mblock;
            long next = address;

            while (next > 0)
            {
                file.GoTo(next + 5);
                mblock = new MarshalMemoryBlock(file.ReadInt(next + 5));
                file.GoTo(next + 9);
                mblock.ReadFromStream(file.GetStream(), (int)mblock.AllocSize);
                mtmp.Add(mblock);

                var btmp = System.Buffers.ArrayPool<byte>.Shared.Rent(8);
                var bss = btmp.AsSpan<byte>();
                bss.Clear();
                file.ReadBytes(next, btmp,0, 5);
                var re = BitConverter.ToInt64(bss);

                System.Buffers.ArrayPool<byte>.Shared.Return(btmp);

                if (re <= 0)
                {
                    break;
                }
                else
                {
                    next += re;
                }
            }
            return mtmp;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockAllValue<T>(MarshalMemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<T> result)
        {
            //MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            ctype = GetCompressType(ctype, out byte tagtype);
            var tp = CompressUnitManager2.Manager.GetCompress(ctype);
            if (tp != null)
            {
                //如果变量类型没有改变
                if (!CheckTagTypeChanged<T>(tagtype))
                {
                    tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, result);
                }
                else
                {
                    DateTime time;
                    byte qu;
                    //如果记录的类型发生了改变，则需要转换
                    TagType tpp = (TagType)ctype;
                    switch (tpp)
                    {
                        case TagType.Bool:
                            var htmp = new HisQueryResult<bool>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, htmp);
                            for (int i = 0; i < htmp.Count; i++)
                            {
                                var bval = htmp.GetTargetValue(htmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Byte:
                            var btmp = new HisQueryResult<byte>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, btmp);
                            for (int i = 0; i < btmp.Count; i++)
                            {
                                var bval = btmp.GetTargetValue(btmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Short:
                            var stmp = new HisQueryResult<short>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, stmp);
                            for (int i = 0; i < stmp.Count; i++)
                            {
                                var bval = stmp.GetTargetValue(stmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UShort:
                            var ustmp = new HisQueryResult<ushort>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, ustmp);
                            for (int i = 0; i < ustmp.Count; i++)
                            {
                                var bval = ustmp.GetTargetValue(ustmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Int:
                            var itmp = new HisQueryResult<int>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, itmp);
                            for (int i = 0; i < itmp.Count; i++)
                            {
                                var bval = itmp.GetTargetValue(itmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UInt:
                            var uitmp = new HisQueryResult<uint>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, uitmp);
                            for (int i = 0; i < uitmp.Count; i++)
                            {
                                var bval = uitmp.GetTargetValue(uitmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Long:
                            var ltmp = new HisQueryResult<long>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, ltmp);
                            for (int i = 0; i < ltmp.Count; i++)
                            {
                                var bval = ltmp.GetTargetValue(ltmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.ULong:
                            var ultmp = new HisQueryResult<ulong>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, ultmp);
                            for (int i = 0; i < ultmp.Count; i++)
                            {
                                var bval = ultmp.GetTargetValue(ultmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.DateTime:
                            var dttmp = new HisQueryResult<DateTime>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, dttmp);
                            for (int i = 0; i < dttmp.Count; i++)
                            {
                                var bval = dttmp.GetTargetValue(dttmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Float:
                            var ftmp = new HisQueryResult<float>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, ftmp);
                            for (int i = 0; i < ftmp.Count; i++)
                            {
                                var bval = ftmp.GetTargetValue(ftmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Double:
                            var dtmp = new HisQueryResult<double>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, dtmp);
                            for (int i = 0; i < dtmp.Count; i++)
                            {
                                var bval = dtmp.GetTargetValue(dtmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.String:
                            var sstmp = new HisQueryResult<string>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, sstmp);
                            for (int i = 0; i < sstmp.Count; i++)
                            {
                                var bval = sstmp.GetTargetValue(sstmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.IntPoint:
                            var iptmp = new HisQueryResult<IntPointData>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, iptmp);
                            for (int i = 0; i < iptmp.Count; i++)
                            {
                                var bval = iptmp.GetTargetValue(iptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.IntPoint3:
                            var ip3tmp = new HisQueryResult<IntPoint3Data>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, ip3tmp);
                            for (int i = 0; i < ip3tmp.Count; i++)
                            {
                                var bval = ip3tmp.GetTargetValue(ip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UIntPoint:
                            var uptmp = new HisQueryResult<UIntPointData>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, uptmp);
                            for (int i = 0; i < uptmp.Count; i++)
                            {
                                var bval = uptmp.GetTargetValue(uptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UIntPoint3:
                            var uip3tmp = new HisQueryResult<UIntPoint3Data>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, uip3tmp);
                            for (int i = 0; i < uip3tmp.Count; i++)
                            {
                                var bval = uip3tmp.GetTargetValue(uip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.LongPoint:
                            var liptmp = new HisQueryResult<LongPointData>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, liptmp);
                            for (int i = 0; i < liptmp.Count; i++)
                            {
                                var bval = liptmp.GetTargetValue(liptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.ULongPoint:
                            var uliptmp = new HisQueryResult<ULongPointData>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, uliptmp);
                            for (int i = 0; i < uliptmp.Count; i++)
                            {
                                var bval = uliptmp.GetTargetValue(uliptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.LongPoint3:
                            var lip3tmp = new HisQueryResult<LongPoint3Data>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, lip3tmp);
                            for (int i = 0; i < lip3tmp.Count; i++)
                            {
                                var bval = lip3tmp.GetTargetValue(lip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.ULongPoint3:
                            var ulip3tmp = new HisQueryResult<ULongPoint3Data>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, ulip3tmp);
                            for (int i = 0; i < ulip3tmp.Count; i++)
                            {
                                var bval = ulip3tmp.GetTargetValue(ulip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                    }
                }
            }
        }



        #endregion

        #region 读取指定时刻值



        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="time"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static object Read<T>(this DataFileInfo5 file, int tid, DateTime time, QueryValueMatchType type)
        {
            return Read<T>(file, tid, new List<DateTime> { time }, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static HisQueryResult<T> Read<T>(this DataFileInfo5 file, int tid, List<DateTime> times, QueryValueMatchType type)
        {
            HisQueryResult<T> re = new HisQueryResult<T>(times.Count);
            using (QueryContext ctx = new QueryContext())
            {
                Read<T>(file, tid, times, type, re, ctx);
            }
            return re;
        }

        private static long GetHeadPointer(DataFileInfo5 file, int id,int bindex, QueryContext context)
        {
            var vb = context.GetHeadPoint(file.FileName, bindex);
            if (vb < 0)
            {
                context.RegisorHeadPoint(file.FileName, file.ReadAddress(id));
            }
            return vb;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="file"></param>
        ///// <param name="datafile"></param>
        ///// <param name="tid"></param>
        ///// <param name="dindex"></param>
        ///// <returns></returns>
        //public static object ReadLastAvaiableValue<T>(this DataFileInfo5 file, DataFileSeriserbase datafile, int tid,int dindex, QueryContext context)
        //{
        //    object oval=null;
        //    do
        //    {
        //        dindex--;
        //        if (dindex < 0) break;

        //        oval = context.GetBlockLastValue(file.FileName, dindex);
        //        if(oval != null)
        //        {
        //            break;
        //        }

        //        var mmp = ReadAllRelativeBlock(datafile, GetHeadPointer(file, tid, dindex,context));
        //        mmp = mmp.Reverse();
        //        oval = DeCompressDataBlockRawValue<T>(mmp, 0,out bool needCancel, context,file.FileName,dindex);

        //        if (needCancel) break;
        //    }
        //    while (oval == null);
        //    return oval;
        //}


        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="file"></param>
        /// <param name="datafile"></param>
        /// <param name="tid"></param>
        /// <param name="dindex"></param>
        /// <returns></returns>
        public static object ReadLastAvaiableValue2<T>(this DataFileInfo5 file, DataFileSeriserbase datafile, int tid, int dindex, QueryContext context,out bool needCancel)
        {
            object oval = null;
            oval = context.GetBlockLastValue(file.FileName, dindex);
            if (oval != null)
            {
                needCancel = false;
                return oval;
            }

            var mmp = ReadAllRelativeBlock(datafile, GetHeadPointer(file, tid, dindex, context));
         
            if(mmp.Count()==0)
            {
                needCancel = false;
                return oval;
            }
            mmp = mmp.Reverse();
            oval = DeCompressDataBlockRawValue<T>(mmp, 0, out  needCancel, context, file.FileName, dindex);
            return oval;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="date"></param>
        ///// <param name="oldfiles"></param>
        ///// <returns></returns>
        //private static IEnumerable<string> ListOnDayFilesDesc(DateTime date, string oldfiles)
        //{
        //    var ifileservice = ServiceLocator.Locator.Resolve<IDataFileService>();
        //    string sfile = oldfiles;
        //    sfile = string.Format(oldfiles, date, date.ToString("D2"), date.Day.ToString("D2"), (5).ToString("D2"));

        //    string snam2 = "";

        //    if (sfile.EndsWith(".dbd2"))
        //    {
        //        snam2 = sfile.Replace(".dbd2", ".zdbd2");
        //    }
        //    else if (sfile.EndsWith(".zdbd2"))
        //    {
        //        snam2 = sfile.Replace(".zdbd2", ".dbd2");
        //    }

        //    if (ifileservice.CheckHisFileExist(sfile, out string realfile))
        //    {
        //        yield return realfile;
        //    }
        //    else if (ifileservice.CheckHisFileExist(snam2, out string realfile2))
        //    {
        //        yield return realfile2;
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="date"></param>
        ///// <param name="oldfiles"></param>
        ///// <returns></returns>
        //private static IEnumerable<string> ListOnDayFilesDesc2(DateTime date, string oldfiles)
        //{
        //    var ifileservice = ServiceLocator.Locator.Resolve<IDataFileService>();
        //    string sfile = oldfiles;
        //    for (int i = 5; i >= 0; i--)
        //    {
        //        sfile = string.Format(oldfiles, date, date.ToString("D2"), date.Day.ToString("D2"), i.ToString("D2"));

        //        string snam2 = "";

        //        if (sfile.EndsWith(".dbd2"))
        //        {
        //            snam2 = sfile.Replace(".dbd2", ".zdbd2");
        //        }
        //        else if (sfile.EndsWith(".zdbd2"))
        //        {
        //            snam2 = sfile.Replace(".zdbd2", ".dbd2");
        //        }

        //        if (ifileservice.CheckHisFileExist(sfile, out string realfile))
        //        {
        //            yield return realfile;
        //        }
        //        else if (ifileservice.CheckHisFileExist(snam2, out string realfile2))
        //        {
        //            yield return realfile2;
        //        }
        //    }
        //}

        //private static IEnumerable<string> ListOnDayFiles(DateTime date, string oldfiles)
        //{
        //    var ifileservice = ServiceLocator.Locator.Resolve<IDataFileService>();
        //    string sfile = oldfiles;
        //    sfile = string.Format(oldfiles, date, date.ToString("D2"), date.Day.ToString("D2"), (0).ToString("D2"));
        //    string snam2 = "";

        //    if (sfile.EndsWith(".dbd2"))
        //    {
        //        snam2 = sfile.Replace(".dbd2", ".zdbd2");
        //    }
        //    else if (sfile.EndsWith(".zdbd2"))
        //    {
        //        snam2 = sfile.Replace(".zdbd2", ".dbd2");
        //    }
        //    if (ifileservice.CheckHisFileExist(sfile, out string realfile))
        //    {
        //        yield return realfile;
        //    }
        //    else if (ifileservice.CheckHisFileExist(snam2, out realfile))
        //    {
        //        yield return realfile;
        //    }
        //}

        //private static IEnumerable<string> ListOnDayFiles2(DateTime date, string oldfiles)
        //{
        //    var ifileservice = ServiceLocator.Locator.Resolve<IDataFileService>();
        //    string sfile = oldfiles;
        //    for (int i = 0; i < 6; i++)
        //    {
        //        sfile = string.Format(oldfiles, date, date.ToString("D2"), date.Day.ToString("D2"), i.ToString("D2"));
        //        string snam2 = "";

        //        if (sfile.EndsWith(".dbd2"))
        //        {
        //            snam2 = sfile.Replace(".dbd2", ".zdbd2");
        //        }
        //        else if (sfile.EndsWith(".zdbd2"))
        //        {
        //            snam2 = sfile.Replace(".zdbd2", ".dbd2");
        //        }
        //        if (ifileservice.CheckHisFileExist(sfile, out string realfile))
        //        {
        //            yield return realfile;
        //        }
        //        else if (ifileservice.CheckHisFileExist(snam2, out realfile))
        //        {
        //            yield return realfile;
        //        }
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="time"></param>
        ///// <param name="filetemplate"></param>
        ///// <returns></returns>
        //public static IEnumerable<string> ListPreviewDataFiles(DateTime time,string filetemplate)
        //{
        //    var ifileservice = ServiceLocator.Locator.Resolve<IDataFileService>();

        //    DateTime stime = time;

        //    var ind = time.Hour / 4;

        //    if (ind > 0)
        //    {
        //        for (int i = ind - 1; i >= 0; i--)
        //        {
        //            var sfile = string.Format(filetemplate, stime.Year, stime.Month.ToString("D2"), stime.Day.ToString("D2"), i.ToString("D2"));
        //            if (ifileservice.CheckHisFileExist(sfile, out string realfile))
        //            {
        //                yield return realfile;
        //            }
        //        }
        //    }
        //    else
        //    {
        //        stime = stime.AddDays(-1);
        //        foreach (var vv in ListOnDayFilesDesc(stime, filetemplate))
        //        {
        //            if (!string.IsNullOrEmpty(vv))
        //            {
        //                yield return vv;
        //            }
        //        }
        //    }
        //    //do
        //    //{
        //    //    stime = stime.AddDays(-1);
        //    //    foreach (var vv in ListOnDayFilesDesc(stime, filetemplate))
        //    //    {
        //    //        if (!string.IsNullOrEmpty(vv))
        //    //        {
        //    //            yield return vv;
        //    //        }
        //    //    }
        //    //}
        //    //while ((time - stime).TotalDays < 30);
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="time"></param>
        ///// <param name="filetemplate"></param>
        ///// <returns></returns>
        //public static string ListDataFile2(DateTime time,string filetemplate)
        //{
        //    var ifileservice = ServiceLocator.Locator.Resolve<IDataFileService>();
        //    DateTime stime = time;
        //    var ind = time.Hour / 4;

        //    var sfile = string.Format(filetemplate, stime.Year, stime.Month.ToString("D2"), stime.Day.ToString("D2"), ind.ToString("D2"));
        //    if (ifileservice.CheckHisFileExist(sfile, out string realfile))
        //    {
        //        return realfile;
        //    }
        //    return string.Empty;
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="time"></param>
        ///// <param name="filetemplate"></param>
        ///// <returns></returns>
        //public static IEnumerable<string> ListPreviewDataFiles2(DateTime time, string filetemplate)
        //{
        //    var ifileservice = ServiceLocator.Locator.Resolve<IDataFileService>();

        //    DateTime stime = time;

        //    var ind = time.Hour / 4;

        //    for (int i = ind - 1; i >= 0; i--)
        //    {
        //        var sfile = string.Format(filetemplate, stime.Year, stime.Month.ToString("D2"), stime.Day.ToString("D2"), i.ToString("D2"));
        //        if (ifileservice.CheckHisFileExist(sfile, out string realfile))
        //        {
        //            yield return realfile;
        //        }
        //    }

        //    do
        //    {
        //        stime = stime.AddDays(-1);
        //        foreach (var vv in ListOnDayFilesDesc(stime, filetemplate))
        //        {
        //            if (!string.IsNullOrEmpty(vv))
        //            {
        //                yield return vv;
        //            }
        //        }
        //    }
        //    while ((time - stime).TotalDays < 30);
        //}

        /// <summary>
        /// 获取前一个文件
        /// </summary>
        /// <param name="currentFile"></param>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        private static string GetPreDataFile(string currentFile, int id, DateTime time)
        {
            string Database = currentFile.Substring(0, currentFile.Length - 12);
            string tmp = Database + ((int)(id / 100000)).ToString("X3") + "{0}{1}{2}04{3}.dbd2";
            string tmp2 = Database + ((int)(id / 100000)).ToString("X3") + "{0}{1}{2}04{3}.zdbd2";
            return ListPreviewDataFiles3(time, tmp,tmp2);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <param name="filetemplate"></param>
        /// <returns></returns>
        private static string ListPreviewDataFiles3(DateTime time, string filetemplate, string filetemplate2)
        {
            var ifileservice = ServiceLocator.Locator.Resolve<IDataFileService>();

            DateTime stime = time;

            var ind = time.Hour / 4;
            string sfile, realfile;
            if (ind > 0)
            {
                sfile = string.Format(filetemplate, stime.Year, stime.Month.ToString("D2"), stime.Day.ToString("D2"), (ind - 1).ToString("D2"));
                if (ifileservice.CheckHisFileExist(sfile, out realfile))
                {
                    return realfile;
                }
                else
                {
                    sfile = string.Format(filetemplate2, stime.Year, stime.Month.ToString("D2"), stime.Day.ToString("D2"), (ind - 1).ToString("D2"));
                    if (ifileservice.CheckHisFileExist(sfile, out realfile))
                    {
                        return realfile;
                    }
                }
            }
            else
            {
                stime = stime.AddDays(-1);
                sfile = string.Format(filetemplate, stime.Year, stime.Month.ToString("D2"), stime.Day.ToString("D2"), (0).ToString("D2"));
                if (ifileservice.CheckHisFileExist(sfile, out realfile))
                {
                    return realfile;
                }
                else
                {
                    sfile = string.Format(filetemplate2, stime.Year, stime.Month.ToString("D2"), stime.Day.ToString("D2"), (0).ToString("D2"));
                    if (ifileservice.CheckHisFileExist(sfile, out realfile))
                    {
                        return realfile;
                    }
                }
            }
            return string.Empty;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="time"></param>
        ///// <param name="filetemplate"></param>
        ///// <returns></returns>
        //public static IEnumerable<string> ListNextDataFiles(DateTime time, string filetemplate)
        //{
        //    var ifileservice = ServiceLocator.Locator.Resolve<IDataFileService>();

        //    DateTime stime = time;
        //    var ind = time.Hour / 4;

        //    if (ind < 6)
        //    {
        //        var sfile = string.Format(filetemplate, time.Year, time.Month.ToString("D2"), time.Day.ToString("D2"), (ind + 1).ToString("D2"));
        //        if (ifileservice.CheckHisFileExist(sfile, out string realfile))
        //        {
        //            yield return realfile;
        //        }
        //    }

        //    //for (int i = ind + 1; i < 6; i++)
        //    //{
        //    //    var sfile = string.Format(filetemplate, time.Year, time.Month.ToString("D2"), time.Day.ToString("D2"), i.ToString("D2"));
        //    //    if (ifileservice.CheckHisFileExist(sfile,out string realfile))
        //    //    {
        //    //        yield return realfile;
        //    //    }
        //    //}

        //    stime = stime.AddDays(1);
        //    foreach (var vv in ListOnDayFiles(stime, filetemplate))
        //    {
        //        if (!string.IsNullOrEmpty(vv))
        //        {
        //            yield return vv;
        //        }
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="time"></param>
        ///// <param name="filetemplate"></param>
        ///// <returns></returns>
        //public static IEnumerable<string> ListNextDataFiles2(DateTime time, string filetemplate)
        //{
        //    var ifileservice = ServiceLocator.Locator.Resolve<IDataFileService>();

        //    DateTime stime = time;
        //    var ind = time.Hour / 4;

        //    for (int i = ind + 1; i < 6; i++)
        //    {
        //        var sfile = string.Format(filetemplate, time.Year, time.Month.ToString("D2"), time.Day.ToString("D2"), i.ToString("D2"));
        //        if (ifileservice.CheckHisFileExist(sfile, out string realfile))
        //        {
        //            yield return realfile;
        //        }
        //    }

        //    do
        //    {
        //        stime = stime.AddDays(1);
        //        foreach (var vv in ListOnDayFiles(stime, filetemplate))
        //        {
        //            if (!string.IsNullOrEmpty(vv))
        //            {
        //                yield return vv;
        //            }
        //        }

        //    }
        //    while (stime <= DateTime.Now);
        //}

        /// <summary>
        /// 获取后一个文件
        /// </summary>
        /// <param name="currentFile"></param>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        private static string GetNextDataFile(string currentFile,int id,DateTime time)
        {
            string Database = currentFile.Substring(0,currentFile.Length-12);
            string tmp = Database + ((int)(id / 100000)).ToString("X3") + "{0}{1}{2}04{3}.dbd2";
            string tmp2 = Database + ((int)(id / 100000)).ToString("X3") + "{0}{1}{2}04{3}.zdbd2";
            return ListNextDataFiles3(time,tmp,tmp2);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <param name="filetemplate"></param>
        /// <returns></returns>
        private static string ListNextDataFiles3(DateTime time, string filetemplate,string filetemplate2)
        {

            var ifileservice = ServiceLocator.Locator.Resolve<IDataFileService>();

            DateTime stime = time;
            var ind = time.Hour / 4;
            string sfile, realfile;
            if (ind < 5)
            {
                sfile = string.Format(filetemplate, time.Year, time.Month.ToString("D2"), time.Day.ToString("D2"), (ind + 1).ToString("D2"));
                if (ifileservice.CheckHisFileExist(sfile, out realfile))
                {
                    return realfile;
                }
                else
                {
                    sfile = string.Format(filetemplate2, time.Year, time.Month.ToString("D2"), time.Day.ToString("D2"), (ind + 1).ToString("D2"));
                    if(ifileservice.CheckHisFileExist(sfile,out realfile))
                    {
                        return realfile;
                    }
                }
            }
            else
            {
                stime = stime.AddDays(1);
                sfile = string.Format(filetemplate, time.Year, time.Month.ToString("D2"), time.Day.ToString("D2"), 0.ToString("D2"));
                if (ifileservice.CheckHisFileExist(sfile, out realfile))
                {
                    return realfile;
                }
                else
                {
                    sfile = string.Format(filetemplate2, time.Year, time.Month.ToString("D2"), time.Day.ToString("D2"), 0.ToString("D2"));
                    if (ifileservice.CheckHisFileExist(sfile, out realfile))
                    {
                        return realfile;
                    }
                }
            }
            return string.Empty;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="file"></param>
        ///// <returns></returns>
        //private static IEnumerable<string> ListPreviewDataFiles(DataFileInfo5 file)
        //{
        //    var ifileservice = ServiceLocator.Locator.Resolve<IDataFileService>();
        //    string sname = System.IO.Path.GetFileName(file.FileName);

        //    int ind = int.Parse(sname.Substring(sname.LastIndexOf('.')-2,2));

        //    sname = sname.Substring(0,sname.LastIndexOf(".")-12)+@"{0}{1}{2}"+ "04" +"{3}"+ System.IO.Path.GetExtension(sname);



        //    //sname = sname.Replace(file.StartTime.Year.ToString(), "{0}");
        //    //sname = sname.Replace(file.StartTime.Month.ToString(), "{1}");
        //    //sname = sname.Replace(file.StartTime.Day.ToString(), "{2}");
        //    //sname = sname.Substring(0, sname.LastIndexOf("."))+"{4}"+System.IO.Path.GetExtension(sname);

        //    for(int i=ind-1;i>=0;i--)
        //    {
        //        var sfile = string.Format(sname, file.StartTime.Year, file.StartTime.Month.ToString("D2"), file.StartTime.Day.ToString("D2"), i.ToString("D2"));
        //        if (ifileservice.CheckHisFileExist(sfile,out string realfile))
        //        {
        //            yield return realfile;
        //        }
        //    }

        //    DateTime stime = file.StartTime;
        //    do
        //    {
        //        stime = stime.AddDays(-1);
        //        foreach(var vv in ListOnDayFilesDesc(stime,sname))
        //        {
        //            if(!string.IsNullOrEmpty(vv))
        //            {
        //                yield return vv;
        //            }
        //        }
        //    }
        //    while((file.StartTime- stime).TotalDays < 30);

        //}

        //private static IEnumerable<string> ListNextDataFiles(DataFileInfo5 file)
        //{
        //    var ifileservice = ServiceLocator.Locator.Resolve<IDataFileService>();
        //    string sname = System.IO.Path.GetFileName(file.FileName);

        //    int ind = int.Parse(sname.Substring(sname.LastIndexOf('.') - 2, 2));

        //    sname = sname.Substring(0, sname.LastIndexOf(".") - 12) + @"{0}{1}{2}" + "04" + "{3}" + System.IO.Path.GetExtension(sname);

        //    for (int i = ind+1; i <6; i++)
        //    {
        //        var sfile = string.Format(sname, file.StartTime.Year, file.StartTime.Month.ToString("D2"), file.StartTime.Day.ToString("D2"), i.ToString("D2"));
        //        if (ifileservice.CheckHisFileExist(sfile,out string realfile))
        //        {
        //            yield return realfile;
        //        }
        //    }

        //    DateTime stime = file.StartTime;
        //    do
        //    {
        //        stime = stime.AddDays(1);
        //        foreach (var vv in ListOnDayFiles(stime, sname))
        //        {
        //            if (!string.IsNullOrEmpty(vv))
        //            {
        //                yield return vv;
        //            }
        //        }

        //    }
        //    while (stime<=DateTime.Now);

        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="currentfile"></param>
        ///// <param name="datafile"></param>
        ///// <param name="id"></param>
        ///// <param name="index"></param>;
        ///// <returns></returns>
        //public static object ReadLastAvaiableValueCrossFile<T>(this DataFileInfo5 currentfile,DataFileSeriserbase datafile,int id,int index, QueryContext context)
        //{
        //    var obj = ReadLastAvaiableValue<T>(currentfile,datafile,id,index,context);
        //    if(obj == null)
        //    {
        //        object tobj = null;

        //        foreach(var vv in ListPreviewDataFiles(currentfile))
        //        {
        //            if (obj != null) break;
        //            DataFileInfo5 dfile = new DataFileInfo5() { FileName = vv,IsZipFile = vv.EndsWith(".zdbd2")};
        //            using (var dsf = dfile.GetFileSeriser())
        //            {
        //                for (int i = 47; i >= 0; i--)
        //                {
        //                    tobj = dfile.ReadLastAvaiableValue<T>(dsf, id, i,context);
        //                    if(tobj != null)
        //                    {
        //                        obj = tobj;
        //                        break;
        //                    }
        //                }
        //            }
        //        }
        //        //todo read pre file
        //    }
        //    return obj;
        //}

        /// <summary>
        /// 读取前一个文件的最后一个值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="currentfile"></param>
        /// <param name="datafile"></param>
        /// <param name="id"></param>
        /// <param name="index"></param>;
        /// <returns></returns>
        public static object ReadPreFileLastAvaiableValue<T>(this DataFileInfo5 currentfile, int id, QueryContext context)
        {
            object obj = null;

            string sfile = GetPreDataFile(currentfile.FileName, id, currentfile.StartTime);
            if (!string.IsNullOrEmpty(sfile))
            {
                DataFileInfo5 dfile = new DataFileInfo5() { FileName = sfile, IsZipFile = sfile.EndsWith(".zdbd2") };
                using (var dsf = dfile.GetFileSeriser())
                {
                    for (int i = 47; i > -1; i--)
                    {
                        obj = dfile.ReadLastAvaiableValue2<T>(dsf, id, i, context, out bool needcancel);
                        if (obj != null || needcancel)
                        {
                            break;
                        }
                    }
                }
            }
            return obj;
        }

        /// <summary>
        /// 读取某个文件最后一个记录的有效值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="currentfile"></param>
        /// <param name="id"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public static object ReadFileLastAvaiableValue<T>(this DataFileInfo5 currentfile, int id, QueryContext context)
        {
            using (var dsf = currentfile.GetFileSeriser())
            {
                for (int i = 47; i > -1; i--)
                {
                    var vv = currentfile.ReadLastAvaiableValue2<T>(dsf, id, i, context, out bool needcancel);
                    if (vv != null)
                    {
                        return vv;
                    }
                    else if(needcancel)
                    {
                        break;
                    }
                }
                return null;
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="currentfile"></param>
        ///// <param name="datafile"></param>
        ///// <param name="id"></param>
        ///// <param name="index"></param>
        ///// <returns></returns>
        //public static object ReadFirstAvaiableValueCrossFile<T>(this DataFileInfo5 currentfile,DataFileSeriserbase datafile,int id,int index,QueryContext context)
        //{
        //    var obj = ReadFirstAvaiableValue<T>(currentfile, datafile, id, index,context);
        //    if (obj == null)
        //    {
        //        //todo read next file
        //        object tobj = null;

        //        foreach (var vv in ListNextDataFiles(currentfile))
        //        {
        //            if (obj != null) break;
        //            DataFileInfo5 dfile = new DataFileInfo5() { FileName = vv, IsZipFile = vv.EndsWith(".zdbd2") };
        //            using (var dsf = dfile.GetFileSeriser())
        //            {
        //                for (int i = 0; i <= 47; i++)
        //                {
        //                    tobj = dfile.ReadFirstAvaiableValue<T>(dsf, id, i,context);
        //                    if (tobj != null)
        //                    {
        //                        obj = tobj;
        //                        break;
        //                    }
        //                }
        //            }

        //        }
        //    }
        //    return obj;
        //}




        /// <summary>
        /// 读取下一个文件的第一个值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="currentfile"></param>
        /// <param name="datafile"></param>
        /// <param name="id"></param>
        /// <param name="index"></param>;
        /// <returns></returns>
        public static object ReadNextFileFirstAvaiableValue<T>(this DataFileInfo5 currentfile, int id, QueryContext context)
        {
            object obj = null;

            string sfile = GetNextDataFile(currentfile.FileName, id, currentfile.StartTime);
            if (!string.IsNullOrEmpty(sfile))
            {
                DataFileInfo5 dfile = new DataFileInfo5() { FileName = sfile, IsZipFile = sfile.EndsWith(".zdbd2") };
                using (var dsf = dfile.GetFileSeriser())
                {
                    for (int i = 0; i < 48; i++)
                    {
                        obj = dfile.ReadFirstAvaiableValue2<T>(dsf, id, 0, context, out bool needcancel);
                        if (obj != null || needcancel)
                        {
                            break;
                        }
                    }
                }
            }
            return obj;
        }

        /// <summary>
        /// 读取某个文件的第一个记录的有效值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="currentFile"></param>
        /// <param name="id"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public static object ReadFileFirstAvaiableValue<T>(this DataFileInfo5 currentFile, int id, QueryContext context)
        {
            using (var dsf = currentFile.GetFileSeriser())
            {
                for (int i = 0; i < 48; i++)
                {
                    var val = currentFile.ReadFirstAvaiableValue2<T>(dsf, id, 0, context, out bool needcancel);
                    if(val!=null)
                    {
                        return val;
                    }
                    else if(needcancel)
                    {
                        break;
                    }
                }
                return null;
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="file"></param>
        ///// <param name="datafile"></param>
        ///// <param name="tid"></param>
        ///// <param name="dindex"></param>
        ///// <returns></returns>
        //public static object ReadFirstAvaiableValue<T>(this DataFileInfo5 file, DataFileSeriserbase datafile, int tid, int dindex,QueryContext context)
        //{
        //    object oval = null;
        //    do
        //    {
        //        dindex++;

        //        if (dindex > 47)
        //            break;

        //        oval = context.GetBlockFirstValue(file.FileName, dindex);
        //        if(oval != null) break;

        //        var mmp = ReadAllRelativeBlock(datafile, GetHeadPointer(file, tid, dindex, context));

        //        //if(mmp.Count() == 0) break;

        //        oval = DeCompressDataBlockRawValue<T>(mmp, 1, out bool needCancel,context,file.FileName,dindex);

        //        if (needCancel) break;

        //        //var datas = ReadTagDataBlock(datafile, tid,  dindex, out ttick);
        //        //if (datas == null) return null;
        //        //oval = DeCompressDataBlockRawValue<T>(datas, 1);
        //    }
        //    while (oval == null);
        //    return oval;
        //}

        public static object ReadFirstAvaiableValue2<T>(this DataFileInfo5 file, DataFileSeriserbase datafile, int tid, int dindex, QueryContext context, out bool needCancel)
        {
            object oval = null;
            oval = context.GetBlockFirstValue(file.FileName, dindex);
            if (oval != null)
            {
                needCancel = false;
                return oval;
            }

            var mmp = ReadAllRelativeBlock(datafile, GetHeadPointer(file, tid, dindex, context));

            if (mmp.Count() == 0)
            {
                needCancel = false;
                return oval;
            }

            oval = DeCompressDataBlockRawValue<T>(mmp, 1, out needCancel, context, file.FileName, dindex);
            return oval;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="qa"></param>
        /// <returns></returns>
        public static bool IsBadQuality(byte qa)
        {
            return (qa >= (byte)QualityConst.Bad && qa <= (byte)QualityConst.Bad + 20)||qa == (byte)QualityConst.Close;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void Read<T>(this DataFileInfo5 file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result,QueryContext context)
        {
            using (var vff = file.GetFileSeriser())
            {
                var blocks = file.ReadDataBlock(tid, times);
                DateTime dnow = DateTime.UtcNow;

                foreach(var block in blocks)
                {
                    if(block.Key.Item2 != 0)
                    {
                        //如果数据块的数据区存在
                        context.CurrentBlock = block.Key.Item1;
                        Read<T>(file,vff,  tid, block.Key.Item2, block.Key.Item1, block.Value, type, result,context);
                    }
                    else
                    {
                        //如果数据块的数据区不存在，说明此段时间异常退出了，原因是现在运行期间每个数据块必须有一条记录，2020、07、01

                        foreach (var vv in block.Value)
                        {
                            result.Add(default(T), vv, (byte)QualityConst.Null);
                        }

                        //if (type == QueryValueMatchType.Previous)
                        //{
                        //    var vobj = ReadLastAvaiableValueCrossFile<T>(file, vff, tid, block.Key.Item1,context);
                        //    if(vobj != null)
                        //    {
                        //        TagHisValue<T> hval = (TagHisValue<T>)vobj;
                        //        if (hval.Quality == (byte)QualityConst.Close)
                        //        {
                        //            foreach (var vv in block.Value)
                        //            {
                        //                if (vv <= dnow)
                        //                    result.Add(default(T), vv, (byte)QualityConst.Null);
                        //            }
                        //        }
                        //        else
                        //        {
                        //            foreach (var vv in block.Value)
                        //            {
                        //                if (vv <= dnow)
                        //                    result.Add(hval.Value, vv, hval.Quality);
                        //            }
                        //        }
                        //    }
                        //    else
                        //    {
                        //        foreach (var vv in block.Value)
                        //        {
                        //            if (vv <= dnow)
                        //                result.Add(default(T), vv, (byte)QualityConst.Null);
                        //        }
                        //    }
                        //}
                        //else if(type == QueryValueMatchType.After)
                        //{
                        //    var vobj = ReadFirstAvaiableValueCrossFile<T>(file, vff, tid, block.Key.Item1,context);
                        //    if (vobj != null)
                        //    {
                        //        TagHisValue<T> hval = (TagHisValue<T>)vobj;
                        //        foreach (var vv in block.Value)
                        //        {
                        //            if (vv <= dnow)
                        //                result.Add(hval.Value, vv, hval.Quality);
                        //        }
                        //    }
                        //    else
                        //    {
                        //        foreach (var vv in block.Value)
                        //        {
                        //            if (vv <= dnow)
                        //                result.Add(default(T), vv, (byte)QualityConst.Null);
                        //        }
                        //    }
                        //}
                        //else if(type == QueryValueMatchType.Closed)
                        //{
                        //    var pobj = ReadLastAvaiableValueCrossFile<T>(file, vff, tid, block.Key.Item1,context);
                        //    var nobj = ReadFirstAvaiableValueCrossFile<T>(file, vff, tid, block.Key.Item1,context);
                        //    if(pobj!=null && nobj!=null)
                        //    {
                        //        TagHisValue<T> hval = (TagHisValue<T>)pobj;
                        //        TagHisValue<T> nval = (TagHisValue<T>)nobj;
                        //        foreach (var vv in block.Value)
                        //        {
                        //            if(((vv -  hval.Time).TotalMinutes> (nval.Time - vv).TotalMinutes)||(hval.Quality == (byte)QualityConst.Close))
                        //            {
                        //                if (vv <= dnow)
                        //                    result.Add(nval.Value, vv, nval.Quality);
                        //            }
                        //            else
                        //            {
                        //                if (vv <= dnow)
                        //                    result.Add(hval.Value, vv, hval.Quality);
                        //            }

                        //        }

                        //    }
                        //    else if(pobj!=null)
                        //    {
                        //        TagHisValue<T> hval = (TagHisValue<T>)pobj;
                        //        if(hval.Quality!= (byte)QualityConst.Bad)
                        //        {
                        //            foreach (var vv in block.Value)
                        //            {
                        //                if (vv <= dnow)
                        //                    result.Add(hval.Value, vv, hval.Quality);
                        //            }
                        //        }
                        //        else
                        //        {
                        //            foreach (var vv in block.Value)
                        //            {
                        //                if (vv <= dnow)
                        //                    result.Add(default(T), vv, (byte)QualityConst.Null);
                        //            }
                        //        }
                        //    }
                        //    else if(nobj!=null)
                        //    {
                        //        TagHisValue<T> hval = (TagHisValue<T>)nobj;
                        //        foreach (var vv in block.Value)
                        //        {
                        //            if (vv <= dnow)
                        //                result.Add(hval.Value, vv, hval.Quality);
                        //        }
                        //    }
                        //    else
                        //    {
                        //        foreach (var vv in block.Value)
                        //        {
                        //            if (vv <= dnow)
                        //                result.Add(default(T), vv, (byte)QualityConst.Null);
                        //        }
                        //    }
                        //}
                        //else if(type == QueryValueMatchType.Linear)
                        //{
                        //    var pobj = ReadLastAvaiableValueCrossFile<T>(file, vff, tid, block.Key.Item1,context);
                        //    var nobj = ReadFirstAvaiableValueCrossFile<T>(file, vff, tid, block.Key.Item1,context);
                        //    if (pobj != null && nobj != null)
                        //    {
                        //        TagHisValue<T> hval = (TagHisValue<T>)pobj;
                        //        TagHisValue<T> nval = (TagHisValue<T>)nobj;
                        //        var tval = (nval.Time - hval.Time).TotalSeconds;

                        //        if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                        //        {
                        //            foreach (var vv in block.Value)
                        //            {
                        //                var ppval = (vv - hval.Time).TotalMilliseconds;
                        //                var ffval = (nval.Time - vv).TotalMilliseconds;

                        //                if (ppval < ffval)
                        //                {
                        //                    if (vv <= dnow)
                        //                        result.Add(hval.Value, vv, hval.Quality);
                        //                }
                        //                else
                        //                {
                        //                    if (vv <= dnow)
                        //                        result.Add(nval.Value, vv, nval.Quality);
                        //                }

                        //            }

                        //        }
                        //        else
                        //        {
                        //            if ((!IsBadQuality(hval.Quality)) && (!IsBadQuality(nval.Quality)))
                        //            {
                        //                foreach (var vv in block.Value)
                        //                {
                        //                    var pval1 = (hval.Time - vv).TotalMilliseconds;
                        //                    var tval1 = (nval.Time - vv).TotalMilliseconds;
                        //                    var sval1 = hval.Value;
                        //                    var sval2 = nval.Value;

                        //                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                        //                    string tname = typeof(T).Name;
                        //                    if (vv <= dnow)
                        //                    {
                        //                        switch (tname)
                        //                        {
                        //                            case "Byte":
                        //                                result.Add((byte)val1, vv, 0);
                        //                                break;
                        //                            case "Int16":
                        //                                result.Add((short)val1, vv, 0);
                        //                                break;
                        //                            case "UInt16":
                        //                                result.Add((ushort)val1, vv, 0);
                        //                                break;
                        //                            case "Int32":
                        //                                result.Add((int)val1, vv, 0);
                        //                                break;
                        //                            case "UInt32":
                        //                                result.Add((uint)val1, vv, 0);
                        //                                break;
                        //                            case "Int64":
                        //                                result.Add((long)val1, vv, 0);
                        //                                break;
                        //                            case "UInt64":
                        //                                result.Add((ulong)val1, vv, 0);
                        //                                break;
                        //                            case "Double":
                        //                                result.Add((double)val1, vv, 0);
                        //                                break;
                        //                            case "Single":
                        //                                result.Add((float)val1, vv, 0);
                        //                                break;
                        //                        }
                        //                    }

                        //                }
                        //            }
                        //            else if (!IsBadQuality(hval.Quality))
                        //            {
                        //                foreach (var vv in block.Value)
                        //                    if (vv <= dnow)
                        //                        result.Add(hval.Value, vv, hval.Quality);
                        //            }
                        //            else if (!IsBadQuality(nval.Quality))
                        //            {
                        //                foreach (var vv in block.Value)
                        //                    if (vv <= dnow)
                        //                        result.Add(nval.Value, vv, nval.Quality);
                        //            }
                        //            else
                        //            {
                        //                foreach (var vv in block.Value)
                        //                {
                        //                    if (vv <= dnow)
                        //                        result.Add(default(T), vv, (byte)QualityConst.Null);
                        //                }
                        //            }

                        //        }
                        //    }
                        //    else
                        //    {
                        //        foreach (var vv in block.Value)
                        //        {
                        //            if (vv <= dnow)
                        //                result.Add(default(T), vv, (byte)QualityConst.Null);
                        //        }
                        //    }
                        //}

                        //

                    }
                }
            }
        }

        /// <summary>
        /// 拟合读取历史数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTimes"></param>
        /// <param name="type"></param>
        /// <param name="res"></param>
        public static void Read<T>(this DataFileInfo5 file, DataFileSeriserbase datafile, int tid, long address, int index, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<T> res, QueryContext context)
        {
            //Dictionary<string,object> contenxt = new Dictionary<string,object>();
            context["hasnext"]= false;
            var mtmp = ReadAllRelativeBlock(datafile, address);
            context["block"] = mtmp;
            context["index"] = 0;
            List<DateTime> temp = dataTimes;

            int i = 1;
            foreach (var block in mtmp)
            {
                if (i > 1)
                {
                    temp = dataTimes.Where(e => e > res.LastTime).ToList();
                }
                context["hasnext"] = i < mtmp.Count();
                context["index"] = i-1;
                if (temp.Count > 0)
                {
                    DeCompressDataBlockValue<T>(block, temp, file.TimeTick, type, res, new Func<byte,QueryContext, object>((tp,ctx) =>
                    {

                        object oval = null;
                        int dindex = index;
                        bool needCancel = false;
                        if (tp == 0)
                        {

                            if((int)ctx["index"]>0)
                            {
                                var blocks = ctx["block"] as IEnumerable<MarshalMemoryBlock>;
                                blocks = blocks.Take((int)ctx["index"]);
                                blocks = blocks.Reverse();

                                oval = DeCompressDataBlockRawValue<T>(blocks, 0, out  needCancel,ctx,ctx.CurrentFile, dindex);
                                if(oval != null || needCancel)
                                {
                                    return oval;
                                }
                            }

                            //往前读最后一个有效值
                            dindex--;
                            if (dindex < 0)
                            {
                                //读取前一个文件的最后一个值
                                var vobj = file.ReadPreFileLastAvaiableValue<T>(tid, ctx);
                                if (vobj != null)
                                {
                                    return vobj;
                                }
                                else
                                {
                                    return TagHisValue<T>.Empty;
                                }
                            }

                            //从缓冲中读取
                            oval = ctx.GetBlockLastValue(ctx.CurrentFile, dindex);
                            if (oval != null)
                                return oval;

                            var mmp = ReadAllRelativeBlock(datafile, GetHeadPointer(file, tid, dindex, context));

                            if (mmp.Count() == 0) return oval; 
                            mmp = mmp.Reverse();

                            //var datas = ReadTagDataBlock(datafile, tid,  dindex, out ttick);
                            //if (datas == null) return null;
                            oval = DeCompressDataBlockRawValue<T>(mmp, 0, out  needCancel, ctx, ctx.CurrentFile, dindex);
                            
                        }
                        else
                        {
                            var blocks = ctx["block"] as IEnumerable<MarshalMemoryBlock>;

                            if ((int)ctx["index"] < blocks.Count()-1)
                            {

                                blocks = blocks.Skip((int)ctx["index"]+1);
                                oval = DeCompressDataBlockRawValue<T>(blocks, 0, out  needCancel,ctx,ctx.CurrentFile,ctx.CurrentBlock);
                                if (oval != null||needCancel)
                                {
                                    return oval;
                                }
                            }

                            //往后读第一个有效值
                            dindex++;

                            if (dindex > 47)
                            {
                                //读取下一个文件的第一个值
                                var vobj = file.ReadNextFileFirstAvaiableValue<T>(tid, ctx);
                                if (vobj != null)
                                {
                                    return vobj;
                                }
                                else
                                {
                                    return TagHisValue<T>.Empty;
                                }
                            }

                            oval = ctx.GetBlockFirstValue(ctx.CurrentFile, dindex);
                            if (oval != null)
                                return oval;

                            var mmp = ReadAllRelativeBlock(datafile, GetHeadPointer(file, tid, dindex, context));
                            if (mmp.Count() == 0) return oval;

                            oval = DeCompressDataBlockRawValue<T>(mmp, 1, out  needCancel, ctx, ctx.CurrentFile, dindex);
                        }
                        return oval;

                    }), context);
                }
                if(i==1)
                {
                    context.RegistorFirstKeyHisValue<T>(context.FirstValue!=null?(T)context.FirstValue:default(T), context.FirstTime, context.FirstQuality);
                }
                i++;
            }
            context.RegistorLastKeyHisValue<T>( context.LastValue!=null? (T)context.LastValue:default(T), context.LastTime, context.LastQuality);
            foreach (var vv in mtmp)
            {
                vv.Dispose();
            }
        }

        #endregion

       

        #region DeCompressData

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="memory"></param>
        ///// <param name="datatime"></param>
        ///// <param name="timeTick"></param>
        ///// <param name="type"></param>
        ///// <returns></returns>
        //private static object DeCompressDataBlockValue<T>(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type,Func<byte, QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        //{
        //    //MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
        //    //读取压缩类型
        //    var ctype = memory.ReadByte();
        //    ctype = GetCompressType(ctype, out byte tagtype);
        //    var tp = CompressUnitManager2.Manager.GetCompress(ctype);
        //    if (tp != null)
        //    {
        //        if (!CheckTagTypeChanged<T>(tagtype))
        //        {
        //            return tp.DeCompressValue<T>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction,context);
        //        }
        //        else
        //        {
        //            //如果记录的类型发生了改变，则需要转换
        //            TagType tpp = (TagType)ctype;
        //            switch (tpp)
        //            {
        //                case TagType.Bool:
        //                    return tp.DeCompressValue<bool>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.Byte:
        //                    return tp.DeCompressValue<byte>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.Short:
        //                    return tp.DeCompressValue<short>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.UShort:
        //                    return tp.DeCompressValue<ushort>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.Int:
        //                    return tp.DeCompressValue<int>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.UInt:
        //                    return tp.DeCompressValue<uint>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.Long:
        //                    return tp.DeCompressValue<long>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.ULong:
        //                    return tp.DeCompressValue<ulong>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.DateTime:
        //                    return tp.DeCompressValue<DateTime>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.Float:
        //                    return tp.DeCompressValue<float>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.Double:
        //                    return tp.DeCompressValue<double>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.String:
        //                    return tp.DeCompressValue<string>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.IntPoint:
        //                    return tp.DeCompressValue<IntPointData>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.IntPoint3:
        //                    return tp.DeCompressValue<IntPoint3Data>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.UIntPoint:
        //                    return tp.DeCompressValue<UIntPointData>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.UIntPoint3:
        //                    return tp.DeCompressValue<UIntPoint3Data>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.LongPoint:
        //                    return tp.DeCompressValue<LongPointData>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.ULongPoint:
        //                    return tp.DeCompressValue<ULongPointData>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.LongPoint3:
        //                    return tp.DeCompressValue<LongPoint3Data>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //                case TagType.ULongPoint3:
        //                    return tp.DeCompressValue<ULongPoint3Data>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, context);
        //            }
        //        }
        //    }
        //    return null;
        //}

        /// <summary>
        /// 解压读取数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockValue<T>(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<T> result, Func<byte, QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            //MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            ctype = GetCompressType(ctype, out byte tagtype);
            var tp = CompressUnitManager2.Manager.GetCompress(ctype);
            if (tp != null)
            {
                if (!CheckTagTypeChanged<T>(tagtype))
                {
                    tp.DeCompressValue<T>(memory, 1, datatime, timeTick, type, result, ReadOtherDatablockAction,context);
                }
                else
                {
                    DateTime time;
                    byte qu;
                    //如果记录的类型发生了改变，则需要转换
                    TagType tpp = (TagType)ctype;
                    switch (tpp)
                    {
                        case TagType.Bool:
                            var htmp = new HisQueryResult<bool>(600);
                            tp.DeCompressValue<bool>(memory, 1, datatime, timeTick, type, htmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < htmp.Count; i++)
                            {
                                var bval = htmp.GetTargetValue(htmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Byte:
                            var btmp = new HisQueryResult<byte>(600);
                            tp.DeCompressValue<byte>(memory, 1, datatime, timeTick, type, btmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < btmp.Count; i++)
                            {
                                var bval = btmp.GetTargetValue(btmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Short:
                            var stmp = new HisQueryResult<short>(600);
                            tp.DeCompressValue<short>(memory, 1, datatime, timeTick, type, stmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < stmp.Count; i++)
                            {
                                var bval = stmp.GetTargetValue(stmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UShort:
                            var ustmp = new HisQueryResult<ushort>(600);
                            tp.DeCompressValue<ushort>(memory, 1, datatime, timeTick, type, ustmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < ustmp.Count; i++)
                            {
                                var bval = ustmp.GetTargetValue(ustmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Int:
                            var itmp = new HisQueryResult<int>(600);
                            tp.DeCompressValue<int>(memory, 1, datatime, timeTick, type, itmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < itmp.Count; i++)
                            {
                                var bval = itmp.GetTargetValue(itmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UInt:
                            var uitmp = new HisQueryResult<uint>(600);
                            tp.DeCompressValue<uint>(memory, 1, datatime, timeTick, type, uitmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < uitmp.Count; i++)
                            {
                                var bval = uitmp.GetTargetValue(uitmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Long:
                            var ltmp = new HisQueryResult<long>(600);
                            tp.DeCompressValue<long>(memory, 1, datatime, timeTick, type, ltmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < ltmp.Count; i++)
                            {
                                var bval = ltmp.GetTargetValue(ltmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.ULong:
                            var ultmp = new HisQueryResult<ulong>(600);
                            tp.DeCompressValue<ulong>(memory, 1, datatime, timeTick, type, ultmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < ultmp.Count; i++)
                            {
                                var bval = ultmp.GetTargetValue(ultmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.DateTime:
                            var dttmp = new HisQueryResult<DateTime>(600);
                            tp.DeCompressValue<DateTime>(memory, 1, datatime, timeTick, type, dttmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < dttmp.Count; i++)
                            {
                                var bval = dttmp.GetTargetValue(dttmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Float:
                            var ftmp = new HisQueryResult<float>(600);
                            tp.DeCompressValue<float>(memory, 1, datatime, timeTick, type, ftmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < ftmp.Count; i++)
                            {
                                var bval = ftmp.GetTargetValue(ftmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Double:
                            var dtmp = new HisQueryResult<double>(600);
                            tp.DeCompressValue<double>(memory, 1, datatime, timeTick, type, dtmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < dtmp.Count; i++)
                            {
                                var bval = dtmp.GetTargetValue(dtmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.String:
                            var sstmp = new HisQueryResult<string>(600);
                            tp.DeCompressValue<string>(memory, 1, datatime, timeTick, type, sstmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < sstmp.Count; i++)
                            {
                                var bval = sstmp.GetTargetValue(sstmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.IntPoint:
                            var iptmp = new HisQueryResult<IntPointData>(600);
                            tp.DeCompressValue<IntPointData>(memory, 1, datatime, timeTick, type, iptmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < iptmp.Count; i++)
                            {
                                var bval = iptmp.GetTargetValue(iptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.IntPoint3:
                            var ip3tmp = new HisQueryResult<IntPoint3Data>(600);
                            tp.DeCompressValue<IntPoint3Data>(memory, 1, datatime, timeTick, type, ip3tmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < ip3tmp.Count; i++)
                            {
                                var bval = ip3tmp.GetTargetValue(ip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UIntPoint:
                            var uptmp = new HisQueryResult<UIntPointData>(600);
                            tp.DeCompressValue<UIntPointData>(memory, 1, datatime, timeTick, type, uptmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < uptmp.Count; i++)
                            {
                                var bval = uptmp.GetTargetValue(uptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UIntPoint3:
                            var uip3tmp = new HisQueryResult<UIntPoint3Data>(600);
                            tp.DeCompressValue<UIntPoint3Data>(memory, 1, datatime, timeTick, type, uip3tmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < uip3tmp.Count; i++)
                            {
                                var bval = uip3tmp.GetTargetValue(uip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.LongPoint:
                            var liptmp = new HisQueryResult<LongPointData>(600);
                            tp.DeCompressValue<LongPointData>(memory, 1, datatime, timeTick, type, liptmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < liptmp.Count; i++)
                            {
                                var bval = liptmp.GetTargetValue(liptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.ULongPoint:
                            var uliptmp = new HisQueryResult<ULongPointData>(600);
                            tp.DeCompressValue<ULongPointData>(memory, 1, datatime, timeTick, type, uliptmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < uliptmp.Count; i++)
                            {
                                var bval = uliptmp.GetTargetValue(uliptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.LongPoint3:
                            var lip3tmp = new HisQueryResult<LongPoint3Data>(600);
                            tp.DeCompressValue<LongPoint3Data>(memory, 1, datatime, timeTick, type, lip3tmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < lip3tmp.Count; i++)
                            {
                                var bval = lip3tmp.GetTargetValue(lip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.ULongPoint3:
                            var ulip3tmp = new HisQueryResult<ULongPoint3Data>(600);
                            tp.DeCompressValue<ULongPoint3Data>(memory, 1, datatime, timeTick, type, ulip3tmp, ReadOtherDatablockAction, context);
                            for (int i = 0; i < ulip3tmp.Count; i++)
                            {
                                var bval = ulip3tmp.GetTargetValue(ulip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                    }
                }
            }
        }

        /// <summary>
        /// 从所有数据块中读取第一\最后一个有效值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memory">数据块的集合</param>
        /// <param name="readValueType">0：最后一个，1:第一个</param>
        /// <param name="needCancel">是否遇到系统退出标志</param>
        /// <param name="context">上下文</param>
        /// <param name="file">文件名称</param>
        /// <param name="blockid">数据块ID</param>
        /// <returns></returns>
        private static object DeCompressDataBlockRawValue<T>(IEnumerable<MarshalMemoryBlock> memory, byte readValueType, out bool needCancel, QueryContext context, string file, int blockid)
        {
            int i = 0;
            QueryContext qc = new QueryContext();
            foreach (var vv in memory)
            {

                var re = DeCompressDataBlockRawValue<T>(vv, readValueType, qc);

                if (i == 0)
                {
                    if (qc.FirstValue != null)
                        context.RegistorFirstKeyHisValue<T>(file, blockid, (T)qc.FirstValue, qc.FirstTime, qc.FirstQuality);
                }

                if (re.IsMin())
                {
                    needCancel = true;
                    return null;
                }

                else if ((object)re != null && !re.IsEmpty())
                {
                    needCancel = false;
                    return re;
                }
                i++;
            }
            if (qc.LastValue != null || qc.LastQuality==(byte)QualityConst.Close)
                context.RegistorLastKeyHisValue<T>(file, blockid, (T)qc.LastValue, qc.LastTime, qc.LastQuality);
            qc.Dispose();
            needCancel = false;
            return null;
        }



        /// <summary>
        /// 从单个数据块中读取第一\最后一个有效值
        /// </summary>
        /// <param name="memory">数据块</param>
        /// <param name="readValueType">读取类型 0：最后一个，1:第一个</param>
        /// <returns></returns>
        private static TagHisValue<T> DeCompressDataBlockRawValue<T>(MarshalMemoryBlock memory,byte readValueType,QueryContext context)
        {
            var ctype = memory.ReadByte();
            ctype = GetCompressType(ctype, out byte tagtype);
            var tp = CompressUnitManager2.Manager.GetCompress(ctype);
            if (tp != null)
            {
                if (!CheckTagTypeChanged<T>(tagtype))
                {
                    return tp.DeCompressRawValue<T>(memory, 1, readValueType,context);
                }
            }
            return TagHisValue<T>.Empty;
        }

        /// <summary>
        /// 获取压缩类型
        /// </summary>
        /// <param name="val"></param>
        /// <param name="tagType"></param>
        /// <returns></returns>
        private static byte GetCompressType(byte val,out byte tagType)
        {
            tagType =  (byte)((val >> 3)-1);
            return (byte)(val & 0x03);
        }

        /// <summary>
        /// 和上次读取进行比较，数据类型是否发生了改变
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tagtype"></param>
        /// <returns></returns>
        private static bool CheckTagTypeChanged<T>(byte tagtype)
        {
            if (tagtype == 255) return false;

            string sname = typeof(T).Name.ToLower();
            TagType tp = (TagType)tagtype;
            switch (sname)
            {
                case "bool":
                    return tp != TagType.Bool;
                case "byte":
                    return tp != TagType.Byte;
                case "short":
                    return tp != TagType.Short;
                case "ushort":
                    return tp != TagType.UShort;
                case "int":
                    return tp != TagType.Int;
                case "uint":
                    return tp != TagType.UInt;
                case "long":
                    return tp != TagType.Long;
                case "ulong":
                    return tp != TagType.ULong;
                case "double":
                    return tp != TagType.Double;
                case "float":
                    return tp != TagType.Float;
                case "datetime":
                    return tp != TagType.DateTime;
                case "string":
                    return tp != TagType.String;
                case "intpoint":
                    return tp != TagType.IntPoint;
                case "intpoint3":
                    return tp != TagType.IntPoint3;
                case "uintpoint":
                    return tp != TagType.UIntPoint;
                case "uintpoint3":
                    return tp != TagType.UIntPoint3;
                case "longpoint":
                    return tp != TagType.LongPoint;
                case "longpoint3":
                    return tp != TagType.LongPoint3;
                case "ulongpoint":
                    return tp != TagType.ULongPoint;
                case "ulongpoint3":
                    return tp != TagType.ULongPoint3;
            }

            return false;
        }

        

        #endregion



        
    }
}
