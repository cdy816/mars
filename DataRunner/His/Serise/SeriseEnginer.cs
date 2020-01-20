//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Linq;

/*
 * 一个文件由多个数据区组成, 一个数据区：数据区头+数据块指针区+数据块区
 * [] 表示重复的一个或多个内容
 * 
 HisData File Structor
 [HisDataRegion] 

 HisDataRegion Structor: RegionHead + DataBlockPoint Area + DataBlock Area

 RegionHead:          PreDataRegionPoint(8) + NextDataRegionPoint(8) + Datatime(8)+ tagcount(4)+ tagid sum(8)+file duration(4)+block duration(4)+Time tick duration(4)
 DataBlockPoint Area: [ID]+[block Point]
 Block Point:         block location+block size  
 DataBlock Area:      [data block]
*/

namespace Cdy.Tag
{
    /// <summary>
    /// 序列话引擎
    /// </summary>
    public class SeriseEnginer : IDataSerialize
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private ManualResetEvent resetEvent;

        private ManualResetEvent closedEvent;

        private Thread mCompressThread;

        private bool mIsClosed = false;

        private MemoryBlock mProcessMemory;

        private DateTime mCurrentTime;

        private DataFileSeriserbase mFileWriter;

        private MemoryBlock mHeadMemory;

        private int mTagCount = 0;

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, long> mIdAddrs = new Dictionary<int, long>();

        /// <summary>
        /// 
        /// </summary>
        private bool mNeedUpdateTagHeads = false;

        /// <summary>
        /// 当前数据区首地址
        /// </summary>
        private long mCurrentDataRegion = 0;

        /// <summary>
        /// 
        /// </summary>
        private string mCurrentFileName = string.Empty;

        private int mFileStartHour = 0;

        public const string DataFileExtends = ".dbd";

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public SeriseEnginer()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataFile"></param>
        public SeriseEnginer(DataFileSeriserbase dataFile)
        {
            mFileWriter = dataFile;
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
        /// 数据库名称
        /// </summary>
        public string DatabaseName { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void Init()
        {
            mIdAddrs.Clear();
            int offset = GetDataRegionHeaderLength() + CalTagIdsSize();
            int blockcount = FileDuration * 60 / BlockDuration;
            var vv = ServiceLocator.Locator.Resolve<ITagQuery>();
            foreach(var vtag in vv.ListAllTags().OrderBy(e=>e.Id))
            {
                mIdAddrs.Add(vtag.Id, offset);
                offset += (blockcount * 12);
            }
        }

        /// <summary>
        /// 计算变量Id集合所占的大小
        /// </summary>
        /// <returns></returns>
        private int CalTagIdsSize()
        {
            var ids = ServiceLocator.Locator.Resolve<ITagQuery>().ListAllTags().OrderBy(e => e.Id);
            using (VarintCodeMemory cm = new VarintCodeMemory((int)(ids.Count() * 4 * 1.2)))
            {
                foreach (var vv in ids)
                {
                    cm.WriteInt32(vv.Id);
                }
                return cm.Position + 4;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            Init();
            resetEvent = new ManualResetEvent(false);
            closedEvent = new ManualResetEvent(false);
            mCompressThread = new Thread(ThreadPro);
            mCompressThread.IsBackground = true;
            mCompressThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            mIsClosed = false;
            resetEvent.Set();
            closedEvent.WaitOne();

            mIdAddrs.Clear();
            mFileWriter = null;

            mProcessMemory = null;
            mHeadMemory = null;

            resetEvent.Dispose();
            closedEvent.Dispose();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataMemory"></param>
        /// <param name="date"></param>
        public void RequestToSave(MemoryBlock dataMemory, DateTime date)
        {
            mProcessMemory = dataMemory;
            mCurrentTime = date;
            resetEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>

        private void ThreadPro()
        {
            while (!mIsClosed)
            {
                resetEvent.WaitOne();
                resetEvent.Reset();
                if (mIsClosed)
                    break;
                SaveToFile();
                mProcessMemory.MakeMemoryNoBusy();
            }
            closedEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private long SkipToLastHeader()
        {
            long offset = 0;
            while (true)
            {
               var preaddr = mFileWriter.ReadLong(offset);
               var nextaddr = mFileWriter.ReadLong(offset + 8);
                if(nextaddr<=0)
                {
                    break;
                }
            }
            mCurrentDataRegion = offset;

            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private string GetDataPath(DateTime time)
        {
            return System.IO.Path.Combine(PathHelper.helper.GetDataPath("HisData"),GetFileName(time));
        }

        /// <summary>
        /// 检查文件是否存在
        /// </summary>
        /// <param name="time"></param>
        private bool CheckFile(DateTime time)
        {
            if (!CheckInSameFile(time))
            {
                if (mNeedUpdateTagHeads)
                {
                    Init();
                    mNeedUpdateTagHeads = false;
                }
                mFileWriter.Flush();
                mFileWriter.Close();

                string sfile = GetDataPath(time);
                if (mFileWriter.CreatOrOpenFile(sfile))
                {
                    //新建文件
                    mCurrentDataRegion = 0;
                    AppendFileHeader();
                }
                else
                {
                    if (mFileWriter.Length < 8)
                    {
                        //新建文件
                        mCurrentDataRegion = 0;
                        AppendFileHeader();
                    }
                    else
                    {
                        //打开已有文件
                        SkipToLastHeader();
                        AppendFileHeader();
                    }
                }
            }
            else
            {
                if (mNeedUpdateTagHeads)
                {
                    Init();
                    AppendFileHeader();
                    mNeedUpdateTagHeads = false;
                }
            }
            return true;
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
        /// <param name="time"></param>
        /// <returns></returns>
        public string GetFileName(DateTime time)
        {
           
            return DatabaseName+ time.ToString("yyyyMMdd") + FileDuration.ToString("D2") + (time.Hour/ FileDuration).ToString("D2")+ DataFileExtends;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private int GetDataRegionHeaderLength()
        {
            //头部结构：Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+ tagcount(4)+ tagid sum(8)+file duration(4)+block duration(4)+Time tick duration(4)
            return 8 + 8 + 8 + 4 + 8 + 4 + 4 + 4;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        //private void WriteFileHeaderAtBegain()
        //{
        //    GeneratorFileHeader();
        //    mFileWriter.Write(mHeadMemory.Memory, 0);
        //}

        /// <summary>
        /// 
        /// </summary>
        private void AppendFileHeader()
        {
            GeneratorFileHeader();
            mFileWriter.Append(mHeadMemory.StartMemory, 0, (int)mHeadMemory.UsedSize);
            var cp = mFileWriter.CurrentPostion;
            //更新上个DataRegion 的Next DataRegion Pointer 指针
            if (mCurrentDataRegion>=0)
            {
                mFileWriter.Write(cp,mCurrentDataRegion + 8);
            }
            mCurrentDataRegion = cp;
        }

        /// <summary>
        /// 生成文件头部
        /// <paramref name="offset">偏移位置</paramref>
        /// </summary>
        private void GeneratorFileHeader()
        {
            //文件头部结构:Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+tagcount(4)+ tagid sum(8) +file duration(4)+ block duration(4)+Time tick duration(4)+ { len + tagid1+tagid2+...+tagidn }+ {tag1 block point1(8) + block size(4) + tag1 block point2(8)+ block size(4) + tag1 block point3(8)+ block size(4)+...+tag1 block pointn(8)+ block size(4) + tag2 block point1 (8)+ block size(4)+ tag2 block point2(8)+ block size(4)+....}
            int blockcount = FileDuration * 60 / BlockDuration;
            int len = GetDataRegionHeaderLength() + mTagCount * (blockcount * 12);
            long IdSum = 0;

            foreach (var vv in mIdAddrs)
            {
                IdSum += vv.Key;
            }

            using (VarintCodeMemory cm = new VarintCodeMemory((int)(mIdAddrs.Count * 4 * 1.2)))
            {
                foreach (var vv in mIdAddrs)
                {
                    cm.WriteInt32(vv.Key);
                }

                len += cm.Position + 4;

                if (mHeadMemory != null)
                {
                    if (len > mHeadMemory.Length)
                    {
                        mHeadMemory.ReAlloc(len);
                    }
                    else
                    {
                        mHeadMemory.Clear();
                    }
                }
                else
                {
                    mHeadMemory = new MemoryBlock(len);
                }

                mHeadMemory.Position = 0;
                mHeadMemory.Write((long)mCurrentDataRegion);//更新Pre DataRegion 指针
                mHeadMemory.Write((long)0);                  //更新Next DataRegion 指针
                mHeadMemory.Write(mCurrentTime);            //写入时间
                mHeadMemory.Write(mTagCount);              //写入变量个数

                mHeadMemory.Write(IdSum);                  //写入Id 校验和

                mHeadMemory.Write(FileDuration);           //写入文件持续时间
                mHeadMemory.Write(BlockDuration);          //写入数据块持续时间
                mHeadMemory.Write(HisEnginer.MemoryTimeTick); //写入时间间隔

                //写入变量编号列表
                mHeadMemory.Write(cm.Position);//写入压缩后的数组的长度
                mHeadMemory.Write(cm.Buffer, 0, cm.Position);//写入压缩数据
            }

            ////写入变量编号列表
            //foreach(var vv in mIdAddrs)
            //{
            //    mHeadMemory.Write(vv.Key);
            //}
        }

        /// <summary>
        /// 更新数据块文件指针
        /// </summary>
        /// <param name="id"></param>
        /// <param name="datapointer"></param>
        public void UpdateDataBlockPointer(int id, long datapointer, int size, DateTime dateTime)
        {
            int bindex = ((dateTime.Hour - mFileStartHour) * 60 + dateTime.Minute)/ BlockDuration;
            long ids = mCurrentDataRegion + mIdAddrs[id] + bindex * 12;
            mFileWriter.Write(BitConverter.GetBytes(datapointer), ids);
            ids += 8;
            mFileWriter.Write(BitConverter.GetBytes(size), ids);
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="dateTime"></param>
        ///// <returns></returns>
        //public long ReadDataBlockPointer(int id,DateTime dateTime)
        //{
        //    int bindex = dateTime.Minute / BlockDuration;
        //    long ids = mIdAddrs[id] + bindex * 4;
        //    return mFileWriter.Read(ids, sizeof(long)).ReadLong();
        //}

        /// <summary>
        /// 执行存储到磁盘
        /// </summary>
        private void SaveToFile()
        {
            var totalsize = mProcessMemory.ReadInt(0);
            var count = mProcessMemory.ReadInt(4);
            var time = mProcessMemory.ReadDateTime(8);
            mTagCount = count;
            mCurrentTime = time;

            int offset = 16;
            //判断变量的ID列表是否被修改了
            for (int i=0;i<count;i++)
            {
                var id = mProcessMemory.ReadInt(offset);
                if(!mIdAddrs.ContainsKey(id))
                {
                    mNeedUpdateTagHeads = true;
                    break;
                }
                offset += 8;
            }

            if (!CheckFile(time))
                return;

            offset = 16;
            int start = count * 8 + offset;
            this.mFileWriter.Append(mProcessMemory.StartMemory, start, totalsize - start);

            mFileStartHour = time.Hour / FileDuration;

            for (int i = 0; i < count; i++)
            {
                var id = mProcessMemory.ReadInt(offset);
                var addr = mProcessMemory.ReadInt(offset + 4) + mCurrentDataRegion;
                var size = mProcessMemory.ReadInt(offset + 8);
                offset += 8;
                UpdateDataBlockPointer(id, addr, size, time);
            }

            Flush();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Flush()
        {
            mFileWriter.Flush();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
