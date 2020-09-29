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
using System.Diagnostics;
using System.Buffers;
using DotNetty.Common;
using DBRuntime.His;

/*
 * ****文件结构****
 * 一个文件头 + 多个数据区组成 ， 一个数据区：数据区头+数据块指针区+数据块区
 * [] 表示重复的一个或多个内容
 * 
 HisData File Structor
 FileHead(84) + [HisDataRegion]

 FileHead: dataTime(8)(FileTime)+dateTime(8)(LastUpdateTime)+DataRegionCount(4)+DatabaseName(64)
 
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
    public class SeriseEnginer2 : IDataSerialize2, IDisposable
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private ManualResetEvent resetEvent;

        private ManualResetEvent closedEvent;

        private Thread mCompressThread;

        private bool mIsClosed = false;

        private DateTime mCurrentTime;

        private Dictionary<int, CompressMemory2> mWaitForProcessMemory = new Dictionary<int, CompressMemory2>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, SeriseFileItem2> mSeriserFiles = new Dictionary<int, SeriseFileItem2>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public SeriseEnginer2()
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
        /// 主历史记录路径
        /// </summary>
        public string HisDataPathPrimary { get; set; }

        /// <summary>
        /// 备份历史记录路径
        /// </summary>
        public string HisDataPathBack { get; set; }

        /// <summary>
        /// 当前工作的历史记录路径
        /// </summary>
        public static string HisDataPath { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...


        /// <summary>
        /// 选择历史记录路径
        /// </summary>
        /// <returns></returns>
        private string SelectHisDataPath()
        {
            if (string.IsNullOrEmpty(HisDataPathPrimary) && string.IsNullOrEmpty(HisDataPathBack))
            {
                return PathHelper.helper.GetDataPath(this.DatabaseName, "HisData");
            }
            else
            {
                string sp = string.IsNullOrEmpty(HisDataPathPrimary) ? PathHelper.helper.GetDataPath(this.DatabaseName, "HisData") : System.IO.Path.IsPathRooted(HisDataPathPrimary) ? HisDataPathPrimary : PathHelper.helper.GetDataPath(this.DatabaseName, HisDataPathPrimary);
                string spb = string.IsNullOrEmpty(HisDataPathBack) ? PathHelper.helper.GetDataPath(this.DatabaseName, "HisData") : System.IO.Path.IsPathRooted(HisDataPathBack) ? HisDataPathBack : PathHelper.helper.GetDataPath(this.DatabaseName, HisDataPathBack);

                //to do select avaiable data path

                return sp;

            }
            //return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            var his = ServiceLocator.Locator.Resolve<IHisEngine2>();
            var histag = his.ListAllTags().OrderBy(e => e.Id);

            //计算数据区域个数
            var mLastDataRegionId = -1;
            foreach (var vv in histag)
            {
                var id = vv.Id;
                var did = id / TagCountOneFile;
                if (mLastDataRegionId != did)
                {
                    mSeriserFiles.Add(did, new SeriseFileItem2() { FileDuration = FileDuration, BlockDuration = BlockDuration, TagCountOneFile = TagCountOneFile, DatabaseName = DatabaseName, Id = did });
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
        public void Start()
        {
            LoggerService.Service.Info("SeriseEnginer", "开始启动");
            mIsClosed = false;
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
            LoggerService.Service.Info("SeriseEnginer", "开始停止");
            mIsClosed = true;
            resetEvent.Set();
            closedEvent.WaitOne();
            resetEvent.Dispose();
            closedEvent.Dispose();

            foreach(var vv in mSeriserFiles)
            {
                vv.Value.Reset();
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataMemory"></param>
        /// <param name="date"></param>
        public void RequestToSeriseFile(CompressMemory2 dataMemory)
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
            mCurrentTime = dataMemory.CurrentTime;
        }

        /// <summary>
        /// 
        /// </summary>
        public void RequestToSave()
        {
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
                resetEvent.Reset();
                if (mIsClosed)
                    break;
                SaveToFile();
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
            //#if DEBUG 
            Stopwatch sw = new Stopwatch();
            sw.Start();
            LoggerService.Service.Info("SeriseEnginer", "********开始执行存储********", ConsoleColor.Cyan);
            //#endif
            HisDataPath = SelectHisDataPath();
            List<CompressMemory2> mtmp;
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

            //#if DEBUG
            sw.Stop();
            LoggerService.Service.Info("SeriseEnginer", ">>>>>>>>>完成执行存储>>>>>>> Count:" + mtmp.Count + " ElapsedMilliseconds:" + sw.ElapsedMilliseconds, ConsoleColor.Cyan);
            //#endif
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            if (mSeriserFiles != null)
            {
                foreach (var vv in mSeriserFiles)
                {
                    vv.Value.Dispose();
                }
                mSeriserFiles.Clear();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="data"></param>
        /// <param name="size"></param>
        public void ManualRequestToSeriseFile(int id, DateTime time,DateTime endTime, MarshalMemoryBlock data, int size)
        {
            HisDataPath = SelectHisDataPath();

            foreach (var vv in mSeriserFiles)
            {
                if( id >= vv.Value.Id* TagCountOneFile && id<(vv.Value.Id+1)* TagCountOneFile)
                {
                    vv.Value.ManualRequestToSeriseFile(id, data, size, time,endTime);
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
    public class SeriseFileItem2 : IDisposable
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
        /// 文件头大小
        /// </summary>
        public const int FileHeadSize = 84;

        private MemoryBlock mBlockPointMemory;

        //指针区域的起始地址
        private long mBlockPointOffset = 0;

        private long mDataRegionHeadSize = 0;

        private VarintCodeMemory mTagIdMemoryCach;

        //变量ID校验和
        private long mTagIdSum;

        private DateTime mCurrentTime;

        private int mTagCount = 0;

        /// <summary>
        /// 上一个数据区域首地址
        /// </summary>
        private long mPreDataRegion = 0;

        static object mFileLocker = new object();

        private List<int> mTagIdsCach;


        private Dictionary<DateTime, Dictionary<int, long>> mPointerCach = new Dictionary<DateTime, Dictionary<int, long>>();

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
            //头部结构：Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+ tagcount(4)+ tagid sum(8)+file duration(4)+block duration(4)+Time tick duration(4)
            return 8 + 8 + 8 + 4 + 8 + 4 + 4 + 4;
        }

        /// <summary>
        /// 添加文件头部
        /// </summary>
        private void AppendFileHeader(DateTime time, string databaseName,DataFileSeriserbase mFileWriter)
        {
            DateTime date = new DateTime(time.Year, time.Month, time.Day, ((time.Hour / FileDuration) * FileDuration), 0, 0);
            mFileWriter.Write(date, 0);
            byte[] nameBytes = ArrayPool<byte>.Shared.Rent(64);
            var ntmp = Encoding.UTF8.GetBytes(databaseName);
            Buffer.BlockCopy(ntmp, 0, nameBytes, 0, Math.Min(64, ntmp.Length));
            mFileWriter.Write(nameBytes, 20);
            ArrayPool<byte>.Shared.Return(nameBytes);
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

            mFileWriter.Write(mFileWriter.ReadInt(16) + 1, 16);

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

            mFileWriter.Write(mFileWriter.ReadInt(16) + 1, 16);

            ArrayPool<byte>.Shared.Return(bval);
        }

        /// <summary>
        /// 生成区域头部
        /// <paramref name="offset">偏移位置</paramref>
        /// </summary>
        private byte[] GeneratorDataRegionHeader(DateTime mCurrentTime, out int totallenght, out int datalen)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            //文件头部结构:Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+tagcount(4)+ tagid sum(8) +file duration(4)+ block duration(4)+Time tick duration(4)+ { len + [tag id]}+ [data blockpoint(8)]
            int blockcount = FileDuration * 60 / BlockDuration;
            int len = GetDataRegionHeaderLength() + 4 + mTagIdMemoryCach.Position + mTagCount * (blockcount * 8);

            totallenght = len;

            byte[] bvals = ArrayPool<byte>.Shared.Rent(52 + mTagIdMemoryCach.Position);

            using (System.IO.MemoryStream mHeadMemory = new System.IO.MemoryStream(bvals))
            {

                mHeadMemory.Position = 0;
                mHeadMemory.Write(BitConverter.GetBytes((long)mPreDataRegion));//更新Pre DataRegion 指针
                mHeadMemory.Write(BitConverter.GetBytes((long)0));                  //更新Next DataRegion 指针
                mHeadMemory.Write(MemoryHelper.GetBytes(mCurrentTime));            //写入时间
                mHeadMemory.Write(BitConverter.GetBytes(mTagCount));              //写入变量个数

                mHeadMemory.Write(BitConverter.GetBytes(mTagIdSum));                  //写入Id 校验和

                mHeadMemory.Write(BitConverter.GetBytes(FileDuration));           //写入文件持续时间
                mHeadMemory.Write(BitConverter.GetBytes(BlockDuration));          //写入数据块持续时间
                mHeadMemory.Write(BitConverter.GetBytes(HisEnginer.MemoryTimeTick)); //写入时间间隔

                //写入变量编号列表
                mHeadMemory.Write(BitConverter.GetBytes(mTagIdMemoryCach.Position));// mHeadMemory.Write(mTagIdMemoryCach.Position);//写入压缩后的数组的长度
                mHeadMemory.Write(mTagIdMemoryCach.Buffer, 0, mTagIdMemoryCach.Position);//写入压缩数据

                mDataRegionHeadSize = mBlockPointOffset = mHeadMemory.Position;
                datalen = (int)mHeadMemory.Position;
                sw.Stop();

                LoggerService.Service.Info("SeriseFileItem" + Id, "GeneratorDataRegionHeader " + sw.ElapsedMilliseconds);

                return bvals;
                //return mHeadMemory.ToArray();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private string GetDataPath(DateTime time)
        {
            return System.IO.Path.Combine(SeriseEnginer2.HisDataPath, GetFileName(time));
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
            mFileWriter.GoToEnd();
            mCurrentDataRegion = mFileWriter.CurrentPostion;
            return offset;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="writer"></param>
        /// <returns></returns>
        private Dictionary<DateTime, long> GetDataRegions(DataFileSeriserbase writer)
        {
            Dictionary<DateTime, long> dd = new Dictionary<DateTime, long>();
            long offset = FileHeadSize;
            while (true)
            {
                var nextaddr = writer.ReadLong(offset + 8);
                DateTime dt = DateTime.FromBinary(writer.ReadLong(offset + 16));
                dd.Add(dt, offset);
                if (nextaddr <= 0)
                {
                    break;
                }
                else
                {
                    offset = nextaddr;
                }
            }
            return dd;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private long GetDataRegionHeadPoint(int id, DateTime time, out DataFileSeriserbase mFileReader)
        {
            string sfile = GetFileName(time);
            // DataFileSeriserbase reader = mFileWriter2;

            if (time > mCurrentTime)
            {
                //如果需要新建的文件，影响到自动记录存储要用到的文件，
                //则转到自动记录存储逻辑进行处理
                CheckFile(time);
                //mCurrentTime = time;
            }
            else
            {
                if (mFileWriter2.CreatOrOpenFile(sfile))
                {
                    var date = new DateTime(time.Year, time.Month, time.Day, ((time.Hour / FileDuration) * FileDuration), 0, 0);
                    //新建文件
                    AppendFileHeader(time, this.DatabaseName, mFileWriter2);
                    NewDataRegionHeader(date, mFileWriter2);
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
                    }
                }
            }

            long ltmp = 0;

            //计算本次更新对应的指针区域的起始地址
            var fsh = (time.Hour / FileDuration) * FileDuration;
            int bid = ((time.Hour - fsh) * 60 + time.Minute) / BlockDuration;

            var icount = mTagIdsCach.IndexOf(id);

            ltmp = SearchDataRegionToDatetime(mFileWriter2, time);

            mFileReader = mFileWriter2;

            if (ltmp < 0)
            {
                LoggerService.Service.Warn("SeriseEnginer2", "不允许修改本次运行之前时间的历史记录!");
                return -1;
            }

            ltmp += mDataRegionHeadSize + mTagCount * 8 * bid + icount * 8;

            return ltmp;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private long SearchDataRegionToDatetime(DataFileSeriserbase mFileWriter, DateTime time)
        {
            long preoffset = -1, offset = FileHeadSize;
            DateTime tm;
            while (true)
            {
                tm = mFileWriter.ReadDateTime(offset + 16);
                if (tm > time)
                {
                    break;
                }

                var nextaddr = mFileWriter.ReadLong(offset + 8);
                if (nextaddr <= 0)
                {
                    preoffset = offset;
                    break;
                }
                else
                {
                    preoffset = offset;
                    offset = nextaddr;
                }
            }
            return preoffset;
        }

        /// <summary>
        /// 通过手动更新的方式，提交历史记录
        /// </summary>
        /// <param name="id"></param>
        /// <param name="datablock"></param>
        /// <param name="size"></param>
        /// <param name="time"></param>
        /// <param name="endTime"></param>
        public void ManualRequestToSeriseFile(int id, MarshalMemoryBlock datablock, int size, DateTime time,DateTime endTime)
        {
            lock (mFileLocker)
            {
                DataFileSeriserbase mwriter;

                var heads = GetDataRegionHeadPoint(id, time, out mwriter);

                if (heads < 0) return;

                //如果更新时间，小于最后更新时间，则更新
                var vtmp = mwriter.ReadDateTime(8);
                if (endTime > vtmp)
                {
                    mwriter.Write(endTime, 8);
                }

                var vpointer = mwriter.GoToEnd().CurrentPostion;
                datablock.WriteToStream(mFileWriter.GetStream(), vpointer, size);//直接拷贝数据块
                datablock.WriteLong(heads, vpointer);
                mwriter.Flush();
                mwriter.Close();

                datablock.MakeMemoryNoBusy();
                MarshalMemoryBlockPool.Pool.Release(datablock);

                var vtime = FormateTime(time);

                //如果时间大于上次自动存储的时间，则需要将地址指针记录下来，等到下次自动存储内容更新时，将当前更新的数据的指针区同步过去
                //仿制被覆盖过去
                lock (mPointerCach)
                {
                    if (time > mCurrentTime)
                    {
                        if (mPointerCach.ContainsKey(vtime))
                        {
                            var dd = mPointerCach[vtime];
                            if (dd.ContainsKey(id))
                            {
                                dd[id] = vpointer;
                            }
                            else
                            {
                                dd.Add(id, vpointer);
                            }
                        }
                        else
                        {
                            Dictionary<int, long> dtmp = new Dictionary<int, long>();
                            dtmp.Add(id, vpointer);
                            mPointerCach.Add(vtime, dtmp);
                        }
                    }
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
        /// 计算变量Id集合所占的大小
        /// </summary>
        /// <returns></returns>
        private int CalTagIdsSize()
        {
            if (mTagIdMemoryCach != null) mTagIdMemoryCach.Dispose();
            mTagIdsCach = ServiceLocator.Locator.Resolve<IHisEngine2>().ListAllTags().Where(e => e.Id >= Id * TagCountOneFile && e.Id < (Id + 1) * TagCountOneFile).OrderBy(e => e.Id).Select(e => e.Id).ToList();

            mTagIdSum = 0;

            mTagIdMemoryCach = new VarintCodeMemory((int)(mTagIdsCach.Count * 4 * 1.2));
            if (mTagIdsCach.Count > 0)
            {
                int preids = mTagIdsCach[0];
                mTagIdSum += preids;
                mTagIdMemoryCach.WriteInt32(preids);
                for (int i = 1; i < mTagIdsCach.Count; i++)
                {
                    var id = mTagIdsCach[i];
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

            var vv = ServiceLocator.Locator.Resolve<IHisEngine2>();
            var tags = vv.ListAllTags().Where(e => e.Id >= Id * TagCountOneFile && e.Id < (Id + 1) * TagCountOneFile).OrderBy(e => e.Id);

            if (mBlockPointMemory != null) mBlockPointMemory.Dispose();

            mBlockPointMemory = new MemoryBlock(tags.Count() * 8, 4 * 1024 * 1024);
            mBlockPointMemory.Clear();

            LoggerService.Service.Info("SeriseEnginer", "Cal BlockPointMemory memory size:" + (mBlockPointMemory.AllocSize) / 1024.0 / 1024 + "M", ConsoleColor.Cyan);
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
                    }
                    else 
                    {
                        //打开已有文件
                        mPreDataRegion = SearchLastDataRegion();
                        AppendDataRegionHeader(mCurrentTime,mFileWriter, mPreDataRegion);
                        
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
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private DateTime FormateTime(DateTime time)
        {
            return new DateTime(time.Year, time.Month, time.Day, time.Hour, time.Minute, 0);
        }

        /// <summary>
        /// 执行存储到磁盘
        /// </summary>
        public void SaveToFile(MarshalMemoryBlock mProcessMemory, long dataOffset, DateTime time,DateTime endTime)
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

                    //数据库大小
                    var datasize = mProcessMemory.ReadInt(dataOffset);
                    var count = mProcessMemory.ReadInt(dataOffset + 4);//变量个数
                    mTagCount = count;
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

                    if(datasize==0)
                    {
                        Flush();
                        sw.Stop();
                        return;
                    }

                    var ltmp2 = sw.ElapsedMilliseconds;

                    long offset = 8 + dataOffset;
                    long start = count * 8 + offset;//计算出数据起始地址

                    var dataAddr = this.mFileWriter.GoToEnd().CurrentPostion;

                    mBlockPointMemory.CheckAndResize(mTagCount * 8);
                    mBlockPointMemory.Clear();

                    var vtime = FormateTime(time);
                    Dictionary<int, long> timecach;

                    lock (mPointerCach)
                    {
                        if (mPointerCach.ContainsKey(time))
                        {
                            timecach = mPointerCach[time];
                        }
                        else
                        {
                            timecach = new Dictionary<int, long>();
                        }
                    }

                    //更新数据块指针
                    for (int i = 0; i < count; i++)
                    {
                        var id = mProcessMemory.ReadInt(offset);
                        //计算新的偏移量
                        var addr = mProcessMemory.ReadInt(offset + 4) - start + dataAddr;
                        offset += 8;
                        if (id > -1)
                        {
                            //如果之前通过，手动记录已经更新了，则需要同步指针
                            if (timecach.ContainsKey(id))
                            {
                                mBlockPointMemory.WriteLong(i * 8, timecach[id]);
                            }
                            else
                            {
                                mBlockPointMemory.WriteLong(i * 8, addr);
                            }
                        }
                    }

                    lock (mPointerCach)
                    {
                        foreach (var vv in mPointerCach.Keys.ToArray())
                        {
                            if (vv <= mCurrentTime)
                            {
                                mPointerCach.Remove(vv);
                            }
                        }
                    }


                    //计算本次更新对应的指针区域的起始地址
                    FileStartHour = (time.Hour / FileDuration) * FileDuration;
                    int bid = ((time.Hour - FileStartHour) * 60 + time.Minute) / BlockDuration;

                    //计算出本次更新的头地址地址
                    var pointAddr = mBlockPointOffset + count * 8 * bid;

                    var ltmp3 = sw.ElapsedMilliseconds;

                    mFileWriter.GoToEnd();
                    //long lpp = mFileWriter.CurrentPostion;

                    mProcessMemory.WriteToStream(mFileWriter.GetStream(), start, datasize);//直接拷贝数据块
                    //写入指针头部区域
                    mFileWriter.Write(mBlockPointMemory.Buffers, pointAddr, 0, (int)mBlockPointMemory.AllocSize);
                    Flush();


                    sw.Stop();

                    LoggerService.Service.Info("SeriseFileItem" + Id, "写入数据 " + mCurrentFileName + "  数据大小：" + ((datasize) + mBlockPointMemory.AllocSize) / 1024.0 / 1024 + " m" + "其他脚本耗时:" + ltmp + "," + (ltmp2 - ltmp) + "," + (ltmp3 - ltmp2) + "存储耗时:" + (sw.ElapsedMilliseconds - ltmp3));
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
