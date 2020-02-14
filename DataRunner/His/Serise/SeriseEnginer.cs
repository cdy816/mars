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
using System.Threading.Tasks;

/*
 * ****文件结构****
 * 一个文件头 + 多个数据区组成 ， 一个数据区：数据区头+数据块指针区+数据块区
 * [] 表示重复的一个或多个内容
 * 
 HisData File Structor
 FileHead(72) + [HisDataRegion] 

 FileHead: dataTime(8)+DatabaseName(64)
 
 HisDataRegion Structor: RegionHead + DataBlockPoint Area + DataBlock Area

 RegionHead:          PreDataRegionPoint(8) + NextDataRegionPoint(8) + Datatime(8)+ tagcount(4)+ tagid sum(8)+file duration(4)+block duration(4)+Time tick duration(4)
 DataBlockPoint Area: [ID]+[block Point]
 [block point]:       [[tag1 point,tag2 point,....][tag1 point,tag2 point,...].....]   以时间单位对变量的数去区指针进行组织
 DataBlock Area:      [block size + data block]
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

        private SeriseFileItem[] mSeriseFile;


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

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void Init()
        {
            var vv = ServiceLocator.Locator.Resolve<ITagQuery>();
            var tagCont = vv.ListAllTags().OrderBy(e => e.Id).Count();
            var scount = tagCont / TagCountOneFile;
            scount = tagCont % TagCountOneFile > 0 ? scount + 1 : scount;

            mSeriseFile = new SeriseFileItem[scount];

            for(int i=0;i<mSeriseFile.Length;i++)
            {
                mSeriseFile[i] = new SeriseFileItem() { FileDuration = FileDuration, BlockDuration = BlockDuration, TagCountOneFile = TagCountOneFile, DatabaseName = DatabaseName, Id = i };
                mSeriseFile[i].FileWriter = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                mSeriseFile[i].Init();
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

            mProcessMemory = null;
            foreach (var vv in mSeriseFile)
            {
                vv.Dispose();
            }
            mSeriseFile = null;

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
                mProcessMemory.Clear();
                mProcessMemory.MakeMemoryNoBusy();
            }
            closedEvent.Set();
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

            LoggerService.Service.Info("SeriseEnginer", "********开始执行存储********");
            Dictionary<int, long> memoryAddrs = new Dictionary<int, long>();
            var count = mProcessMemory.ReadInt(0);
            mCurrentTime = mProcessMemory.ReadDateTime(4);
            int offset = 12;
            for (int i = 0; i < count; i++)
            {
                try
                {
                    var id = mProcessMemory.ReadInt(offset);
                    var addr = mProcessMemory.ReadLong(offset + 4);
                    memoryAddrs.Add(id, addr);
                }
                catch
                {

                }
                offset += 12;
            }

            Parallel.ForEach(memoryAddrs, (keyval) => {
                mSeriseFile[keyval.Key].SaveToFile(mProcessMemory, keyval.Value,mCurrentTime);
            });

            LoggerService.Service.Info("SeriseEnginer", ">>>>>>>>>完成执行存储>>>>>>>");
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }

    /// <summary>
    /// 
    /// </summary>
    public class SeriseFileItem:IDisposable
    {
        #region ... Variables  ...

        /// <summary>
        /// 变量的数据指针的相对起始地址
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


        private string mCurrentFileName;

        private DataFileSeriserbase mFileWriter;

        /// <summary>
        /// 数据文件扩展名
        /// </summary>
        public const string DataFileExtends = ".dbd";

        /// <summary>
        /// 文件头大小
        /// </summary>
        public const int FileHeadSize = 72;

        private MemoryBlock mHeadMemory;

        private MemoryBlock mBlockPointMemory;

        private long mBlockPointOffset = 0;

        private VarintCodeMemory mTagIdMemoryCach;

        //变量ID校验和
        private long mTagIdSum;

        private DateTime mCurrentTime;

        private int mTagCount = 0;

        /// <summary>
        /// 上一个数据区域首地址
        /// </summary>
        private long mPreDataRegion = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...


        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DataFileSeriserbase FileWriter { get { return mFileWriter; } set { mFileWriter=value; } }

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


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private int GetDataRegionHeaderLength()
        {
            //头部结构：Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+ tagcount(4)+ tagid sum(8)+file duration(4)+block duration(4)+Time tick duration(4)
            return 8 + 8 + 8 + 4 + 8 + 4 + 4 + 4;
        }

        /// <summary>
        /// 
        /// </summary>
        private void AppendFileHeader(DateTime time, string databaseName)
        {
            DateTime date = new DateTime(time.Year, time.Month, time.Day, ((time.Hour / FileDuration) * FileDuration), 0, 0);
            mFileWriter.Write(date, 0);
            byte[] nameBytes = new byte[64];
            var ntmp = Encoding.UTF8.GetBytes(databaseName);
            Buffer.BlockCopy(ntmp, 0, nameBytes, 0, Math.Min(64, ntmp.Length));
            mFileWriter.Write(nameBytes, 8);
        }

        /// <summary>
        /// 
        /// </summary>
        private void AppendDataRegionHeader()
        {
            var size = GeneratorDataRegionHeader();
            mFileWriter.Append(mHeadMemory.Buffers, 0, size);

            //更新上个DataRegion 的Next DataRegion Pointer 指针
            if (mPreDataRegion >= 0)
            {
                mFileWriter.Write(mCurrentDataRegion, mPreDataRegion + 8);
            }

            mPreDataRegion = mCurrentDataRegion;

            mBlockPointOffset = mCurrentDataRegion + mBlockPointOffset;

        }

        /// <summary>
        /// 生成文件头部
        /// <paramref name="offset">偏移位置</paramref>
        /// </summary>
        private int GeneratorDataRegionHeader()
        {
            //文件头部结构:Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+tagcount(4)+ tagid sum(8) +file duration(4)+ block duration(4)+Time tick duration(4)+ { len + [tag id]}+ [data blockpoint(8)]
            int blockcount = FileDuration * 60 / BlockDuration;
            int len = GetDataRegionHeaderLength() + mTagCount * (blockcount * 8);
            len += mTagIdMemoryCach.Position + 4;

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

            if(mBlockPointMemory==null)
            {
                mBlockPointMemory = new MemoryBlock(mTagCount * 8);
            }
            else
            {
                mBlockPointMemory.Clear();
            }

            mHeadMemory.Position = 0;
            mHeadMemory.Write((long)mPreDataRegion);//更新Pre DataRegion 指针
            mHeadMemory.Write((long)0);                  //更新Next DataRegion 指针
            mHeadMemory.Write(mCurrentTime);            //写入时间
            mHeadMemory.Write(mTagCount);              //写入变量个数

            mHeadMemory.Write(mTagIdSum);                  //写入Id 校验和

            mHeadMemory.Write(FileDuration);           //写入文件持续时间
            mHeadMemory.Write(BlockDuration);          //写入数据块持续时间
            mHeadMemory.Write(HisEnginer.MemoryTimeTick); //写入时间间隔

            //写入变量编号列表
            mHeadMemory.Write(mTagIdMemoryCach.Position);//写入压缩后的数组的长度
            mHeadMemory.Write(mTagIdMemoryCach.Buffer, 0, mTagIdMemoryCach.Position);//写入压缩数据

            mBlockPointOffset = mHeadMemory.Position;
            
            return len;

        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...


        ///// <summary>
        ///// 更新数据块文件指针
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="datapointer"></param>
        //public void UpdateDataBlockPointer(int id, long datapointer,int bid)
        //{
        //    //int bindex = ((dateTime.Hour - FileStartHour) * 60 + dateTime.Minute) / BlockDuration;
        //    int icount = id / TagCountOneFile;
        //   // long ids = CurrentDataRegion + mIdAddrs[id] + bid * 8; //当前数据区域地址+数据指针的起始地址+指针偏移

        //    mBlockPointMemory.WriteLong(mIdAddrs[id] + bid * 8, datapointer);

        //   // FileWriter.Write(BitConverter.GetBytes(datapointer), ids);
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private string GetDataPath(DateTime time)
        {
            return System.IO.Path.Combine(PathHelper.helper.GetDataPath("HisData"), GetFileName(time));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private string GetFileName(DateTime time)
        {
            return DatabaseName + Id.ToString("D3") + time.ToString("yyyyMMdd") + FileDuration.ToString("D2") + (time.Hour / FileDuration).ToString("D2") + DataFileExtends;
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
            mPreDataRegion = offset;

            mFileWriter.GoToEnd();
            mCurrentDataRegion = mFileWriter.Length;

            return 0;
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
        /// 计算变量Id集合所占的大小
        /// </summary>
        /// <returns></returns>
        private int CalTagIdsSize()
        {
            if (mTagIdMemoryCach != null) mTagIdMemoryCach.Dispose();
            var aids = ServiceLocator.Locator.Resolve<ITagQuery>().ListAllTags().Where(e => e.Id >= Id * TagCountOneFile && e.Id < (Id + 1) * TagCountOneFile).OrderBy(e => e.Id).ToArray();

            mTagIdSum = 0;

            mTagIdMemoryCach = new VarintCodeMemory((int)(aids.Count() * 4 * 1.2));
            if (aids.Length > 0)
            {
                int preids = aids[0].Id;
                mTagIdSum += preids;
                mTagIdMemoryCach.WriteInt32(preids);
                for (int i = 1; i < aids.Length; i++)
                {
                    var id = aids[i].Id;
                    mTagIdMemoryCach.WriteInt32(id - preids);
                    mTagIdSum += id;
                    preids = id;
                }
            }
            return mTagIdMemoryCach.Position + 4;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            mIdAddrs.Clear();
            //long offset = GetDataRegionHeaderLength() + CalTagIdsSize();
            long offset = 0;
            int blockcount = FileDuration * 60 / BlockDuration;
            var vv = ServiceLocator.Locator.Resolve<ITagQuery>();
            var tags = vv.ListAllTags().Where(e => e.Id >= Id * TagCountOneFile && e.Id < (Id + 1) * TagCountOneFile).OrderBy(e => e.Id);
            foreach (var vtag in tags)
            {
                mIdAddrs.Add(vtag.Id, offset);
                //offset += (blockcount * 8);
                offset += 8;
            }

            CalTagIdsSize();
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
                    AppendFileHeader(time, this.DatabaseName);
                    //新建文件
                    mCurrentDataRegion = FileHeadSize;
                    mPreDataRegion = -1;
                    AppendDataRegionHeader();
                }
                else
                {
                    if (mFileWriter.Length < 72)
                    {
                        AppendFileHeader(time, this.DatabaseName);
                        //新建文件
                        mCurrentDataRegion = FileHeadSize;
                        mPreDataRegion = -1;
                        AppendDataRegionHeader();
                    }
                    else
                    {
                        //打开已有文件
                        SearchLastDataRegion();
                        AppendDataRegionHeader();
                    }
                }

                mCurrentFileName = GetFileName(time);
            }
            else
            {
                if (mNeedUpdateTagHeads)
                {
                    Init();
                    SearchLastDataRegion();
                    AppendDataRegionHeader();
                    mNeedUpdateTagHeads = false;
                }
            }
            return true;
        }


        /// <summary>
        /// 执行存储到磁盘
        /// </summary>
        public  void SaveToFile(MemoryBlock mProcessMemory,long dataOffset,DateTime time)
        {
            /*
             1. 检查变量ID是否变动，如果变动则重新记录变量的ID列表
             2. 拷贝数据块
             3. 更新数据块指针
             */

            LoggerService.Service.Info("SeriseFileItem"+Id, "*********开始执行存储**********");

            var totalsize = mProcessMemory.ReadInt(dataOffset);
            var count = mProcessMemory.ReadInt(dataOffset + 4);
            mTagCount = count;
            mCurrentTime = time;
            long offset = 8 + dataOffset;
            //判断变量的ID列表是否被修改了
            for (int i = 0; i < count; i++)
            {
                var id = mProcessMemory.ReadInt(offset);
                if (!mIdAddrs.ContainsKey(id))
                {
                    mNeedUpdateTagHeads = true;
                    break;
                }
                offset += 8;
            }

            if (!CheckFile(time))
                return;

            offset = 8 + dataOffset;
            long start = count * 8 + offset;//计算出数据起始地址

            LoggerService.Service.Info("SeriseFileItem" + Id, "写入数据 "+ (totalsize - start)/1024.0/1024 +" m");
            this.mFileWriter.Append(mProcessMemory.Buffers, (int)start, (int)(totalsize - start)); //直接拷贝数据块

            LoggerService.Service.Info("SeriseFileItem" + Id, "开始更新指针区域");

            FileStartHour = (time.Hour / FileDuration)*FileDuration;

            int bid = ((time.Hour - FileStartHour) * 60 + time.Minute) / BlockDuration;

            for (int i = 0; i < count; i++)
            {
                var id = mProcessMemory.ReadInt(offset);
                offset += 4;
                var addr = mProcessMemory.ReadInt(offset) + mCurrentDataRegion;
                offset += 4;
                mBlockPointMemory.WriteLong(mIdAddrs[id], addr);
            }

            var pointAddr = mBlockPointOffset + count * 8 * bid;

            LoggerService.Service.Info("SeriseFileItem" + Id, "开始写入指针区域到文件 offset:"+ pointAddr + " Size:"+ mBlockPointMemory.AllocSize/1024.0/1024+" m");

            

            mFileWriter.Write(mBlockPointMemory.Buffers, pointAddr, 0, (int)mBlockPointMemory.AllocSize);

            LoggerService.Service.Info("SeriseFileItem" + Id, "更新指针区域完成");
            

            Flush();

            LoggerService.Service.Info("SeriseEnginer" + Id, ">>>>>>>>>完成执行存储>>>>>>>>>");
        }

        /// <summary>
        /// 
        /// </summary>
        public void Flush()
        {
            mFileWriter.Flush();
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
    }
}
