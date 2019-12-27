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

        private RecordMemory mProcessMemory;

        private DateTime mCurrentTime;

        private DataFileSeriserbase mFileWriter;

        private RecordMemory mHeadMemory;

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
        /// 当前数据区
        /// </summary>
        private long mCurrentDataRegion = 0;

        /// <summary>
        /// 
        /// </summary>
        private string mCurrentFileName = string.Empty;

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
            int offset = GetFileHeaderLength();
            int blockcount = FileDuration * 60 / BlockDuration;

            var vv = ServiceLocator.Locator.Resolve<ITagQuery>();
            foreach(var vtag in vv.ListAllTags().OrderBy(e=>e.Id))
            {
                mIdAddrs.Add(vtag.Id, offset);
                offset += mTagCount * (blockcount * 8);
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
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataMemory"></param>
        /// <param name="date"></param>
        public void RequestToSave(RecordMemory dataMemory, DateTime date)
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

                string sfile = GetFileName(time);
                if (mFileWriter.CreatOrOpenFile(sfile))
                {
                    if(mFileWriter.Length<8)
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
                else
                {
                    return false;
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
        private int GetFileHeaderLength()
        {
            //头部结构：Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+ tagcount(4)+file duration(4)+block duration(4)+Time tick duration(4)
            return 8 + 8 + 8 + 4 + 4 + 4 + 4;
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
            mFileWriter.Append(mHeadMemory.Memory, 0, mHeadMemory.Length);
            var cp = mFileWriter.CurrentPostion;

            if(mCurrentDataRegion>=0)
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
            //文件头部结构:Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+tagcount(4)+ tagid sum(8) +file duration(4)+ block duration(4)+Time tick duration(4)+ { + tagid1+tagid2+...+tagidn }+ {tag1 block point1(8) + block size(4) + tag1 block point2(8)+ block size(4) + tag1 block point3(8)+ block size(4)+...+tag1 block pointn(8)+ block size(4) + tag2 block point1 (8)+ block size(4)+ tag2 block point2(8)+ block size(4)+....}
            int blockcount = FileDuration * 60 / BlockDuration;
            int len = GetFileHeaderLength() + mTagCount * (blockcount * 12);
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
                    mHeadMemory = new RecordMemory(len);
                }

                mHeadMemory.Position = 0;
                mHeadMemory.Write((long)mCurrentDataRegion);
                mHeadMemory.Write((long)0);
                mHeadMemory.Write(mCurrentTime);
                mHeadMemory.Write(mTagCount);

                mHeadMemory.Write(IdSum);

                mHeadMemory.Write(FileDuration);
                mHeadMemory.Write(BlockDuration);
                mHeadMemory.Write(HisEnginer.MemoryTimeTick);

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
        public void UpdateDataBlockPointer(int id, long datapointer,int size, DateTime dateTime)
        {
            int bindex = dateTime.Minute / BlockDuration;
            long ids = mIdAddrs[id] + bindex * 4;
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
            var pos = this.mFileWriter.CurrentPostion;
            this.mFileWriter.Append(mProcessMemory.Memory, start, totalsize - start);
            long preaddr = 0;

            for (int i = 0; i < count; i++)
            {
                var id = mProcessMemory.ReadInt(offset);
                var addr = mProcessMemory.ReadInt(offset + 4) + pos;
                offset += 8;
                UpdateDataBlockPointer(id, addr,(int)(addr - preaddr), time);
                preaddr = addr;
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
