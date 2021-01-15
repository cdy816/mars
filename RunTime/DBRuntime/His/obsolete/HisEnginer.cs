//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using DBRuntime.His;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 历史数据引擎
    /// </summary>
    public class HisEnginer : IHisEngine
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private Cdy.Tag.HisDatabase mManager;

        /// <summary>
        /// 
        /// </summary>
        private Cdy.Tag.RealEnginer mRealEnginer;

        private LogManager mLogManager;

        /// <summary>
        /// 缓存内存缓存时间,单位:s
        /// </summary>
        public int CachMemoryTime = 60;

        /// <summary>
        /// 合并内存存储时间,单位:s
        /// </summary>
        public int MergeMemoryTime = 5 * 60;

        /// <summary>
        /// 历史记录时间最短间隔
        /// 单位ms
        /// </summary>
        public const int MemoryTimeTick = 100;

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<long, HisRunTag> mHisTags = new Dictionary<long, HisRunTag>();

        /// <summary>
        /// 历史记录内存1
        /// </summary>
        private CachMemoryBlock mCachMemory1;

        /// <summary>
        /// 历史记录内存2
        /// </summary>
        private CachMemoryBlock mCachMemory2;

        /// <summary>
        /// 
        /// </summary>
        private MergeMemoryBlock mMergeMemory1;

        /// <summary>
        /// 
        /// </summary>
        private MergeMemoryBlock mMergeMemory2;

        /// <summary>
        /// 当前正在使用的内存
        /// </summary>
        private CachMemoryBlock mCurrentMemory;

        /// <summary>
        /// 
        /// </summary>
        private MergeMemoryBlock mCurrentMergeMemory;


        private CachMemoryBlock mWaitForMergeMemory;

        /// <summary>
        /// 值改变的变量列表
        /// </summary>
        private List<ValueChangedMemoryCacheProcesser> mValueChangedProcesser = new List<ValueChangedMemoryCacheProcesser>();

        /// <summary>
        /// 
        /// </summary>
        private List<TimerMemoryCacheProcesser> mRecordTimerProcesser = new List<TimerMemoryCacheProcesser>();

        /// <summary>
        /// 
        /// </summary>
        private TimerMemoryCacheProcesser mLastProcesser = new TimerMemoryCacheProcesser();

        /// <summary>
        /// 
        /// </summary>
        private ValueChangedMemoryCacheProcesser mLastValueChangedProcesser = new ValueChangedMemoryCacheProcesser() { Name = "ValueChanged0" };

        private System.Timers.Timer mRecordTimer;

        private DateTime mLastProcessTime;

        private int mTagCount = 0;

        private int mLastProcessTick = -1;

        private bool mIsBusy = false;

        private ManualResetEvent resetEvent = new ManualResetEvent(false);

        private Thread mMergeThread;

        private bool mIsClosed = false;

        private int mBlockCount = 0;

        private int mMergeCount = 0;

        private bool mNeedSnapAllTag=false;

        private DateTime mSnapAllTagTime = DateTime.Now;

        private bool mForceSubmiteToCompress = false;

        private bool mMegerProcessIsClosed = false;

        private int mStartMergeCount = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public HisEnginer()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="manager"></param>
        /// <param name="realEnginer"></param>
        public HisEnginer(Cdy.Tag.HisDatabase manager, Cdy.Tag.RealEnginer realEnginer)
        {
            mManager = manager;
            mRealEnginer = realEnginer;
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public long MegerMemorySize
        {
            get
            {
                return mMergeMemory1 != null ? mMergeMemory1.Length : 0;
            }
        }
    

        /// <summary>
        /// 当前工作的内存区域
        /// </summary>
        public CachMemoryBlock CurrentMemory
        {
            get
            {
                return mCurrentMemory;
            }
            set
            {
                mCurrentMemory = value;
                HisRunTag.HisAddr = mCurrentMemory;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public LogManager LogManager
        {
            get
            {
                return mLogManager;
            }
            set
            {
                mLogManager = value;
                if(mLogManager!=null)
                {
                    mLogManager.TimeLen = (ushort)(CachMemoryTime/60);
                }
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 初始化
        /// </summary>
        public void Init()
        {
            
            if (mRealEnginer != null)
            {
                if (mManager == null)
                    mManager = new Cdy.Tag.HisDatabaseSerise().Load();

                mLastProcesser = new TimerMemoryCacheProcesser() { Id = 0 };
                mRecordTimerProcesser.Clear();
                mRecordTimerProcesser.Add(mLastProcesser);

                mValueChangedProcesser.Clear();
                mValueChangedProcesser.Add(mLastValueChangedProcesser);

                // var count = MemoryCachTime * 1000 / MemoryTimeTick;
                var count = CachMemoryTime;
                //var realbaseaddr = mRealEnginer.Memory;
                IntPtr realHandle = mRealEnginer.MemoryHandle;
                HisRunTag mHisTag = null;

                Tagbase mRealTag;

                foreach (var vv in mManager.HisTags)
                {
                    var realaddr = (int)mRealEnginer.GetDataAddr((int)vv.Value.Id);
                    mRealTag = mRealEnginer.GetTagById(vv.Value.Id);
                    switch (vv.Value.TagType)
                    {
                        case Cdy.Tag.TagType.Bool:
                        case Cdy.Tag.TagType.Byte:
                            mHisTag = new ByteHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr,CompressType = vv.Value.CompressType,Parameters = vv.Value.Parameters };
                            break;
                        case Cdy.Tag.TagType.Short:
                        case Cdy.Tag.TagType.UShort:
                            mHisTag = new ShortHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters };
                            break;
                        case Cdy.Tag.TagType.Int:
                        case Cdy.Tag.TagType.UInt:
                            mHisTag = new IntHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters };
                            break;
                        case Cdy.Tag.TagType.Long:
                        case Cdy.Tag.TagType.ULong:
                            mHisTag = new LongHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,RealMemoryPtr=realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters };
                            break;
                        case Cdy.Tag.TagType.Float:
                            mHisTag = new FloatHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters, Precision = (mRealTag as FloatTag).Precision };
                            break;
                        case Cdy.Tag.TagType.Double:
                            mHisTag = new DoubleHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters, Precision = (mRealTag as DoubleTag).Precision };
                            break;
                        case Cdy.Tag.TagType.DateTime:
                            mHisTag = new DateTimeHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters };
                            break;
                        case Cdy.Tag.TagType.String:
                            mHisTag = new StirngHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters };
                            break;
                        case Cdy.Tag.TagType.UIntPoint:
                        case Cdy.Tag.TagType.IntPoint:
                            mHisTag = new IntPointHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters };
                            break;
                        case Cdy.Tag.TagType.UIntPoint3:
                        case Cdy.Tag.TagType.IntPoint3:
                            mHisTag = new IntPoint3HisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters };
                            break;
                        case Cdy.Tag.TagType.ULongPoint:
                        case Cdy.Tag.TagType.LongPoint:
                            mHisTag = new LongPointHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters };
                            break;
                        case Cdy.Tag.TagType.ULongPoint3:
                        case Cdy.Tag.TagType.LongPoint3:
                            mHisTag = new LongPoint3HisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters };
                            break;
                    }
                    mHisTag.MaxCount = count-1;
                    mHisTags.Add(vv.Key, mHisTag);

                    if (mHisTag.Type == Cdy.Tag.RecordType.Timer)
                    {
                        if(!mLastProcesser.AddTag(mHisTag))
                        {
                            mLastProcesser = new TimerMemoryCacheProcesser() { Id = mLastProcesser.Id + 1 };
                            mLastProcesser.AddTag(mHisTag);
                            mRecordTimerProcesser.Add(mLastProcesser);
                        }
                    }
                    else
                    {
                        if(!mLastValueChangedProcesser.AddTag(mHisTag))
                        {
                            mLastValueChangedProcesser = new ValueChangedMemoryCacheProcesser() { Name = "ValueChanged"+mTagCount };
                            mValueChangedProcesser.Add(mLastValueChangedProcesser);
                        }
                    }
                    mTagCount++;
                }
            }
            AllocMemory();
        }

        /// <summary>
        /// 计算每个变量数据块的大小
        /// </summary>
        /// <param name="tagType"></param>
        /// <returns></returns>
        private int CalMergeBlockSize(Cdy.Tag.TagType tagType,Cdy.Tag.RecordType recordType,int blockHeadSize,out int dataOffset,out int qulityOffset)
        {

            //单个数据块内容包括：时间戳(2)+数值+质量戳(1)

            qulityOffset = 0;
            int regionHeadSize = blockHeadSize;
            // int count = MemoryCachTime * 1000 / MemoryTimeTick;
            int count = MergeMemoryTime;


            ////对于值改变的记录方式,提高内存分配量,以提高值改变记录的数据个数
            //if(recordType == RecordType.ValueChanged)
            //{
            //    count = MergeMemoryTime * 1000 / MemoryTimeTick;
            //}

            //用于解码时在头尾分别记录前一个区域的值和后一个区域的值
            count += 2;

            //数据区偏移，时间戳占2个字节
            dataOffset = regionHeadSize + count * 2;
            switch (tagType)
            {
                case Cdy.Tag.TagType.Byte:
                case Cdy.Tag.TagType.Bool:
                    qulityOffset = dataOffset + count;
                    return qulityOffset + count;
                case Cdy.Tag.TagType.Short:
                case Cdy.Tag.TagType.UShort:
                    qulityOffset = dataOffset + count * 2;
                    return qulityOffset + count;
                case Cdy.Tag.TagType.Int:
                case Cdy.Tag.TagType.UInt:
                case Cdy.Tag.TagType.Float:
                    qulityOffset = dataOffset + count * 4;
                    return qulityOffset + count;
                case Cdy.Tag.TagType.Long:
                case Cdy.Tag.TagType.ULong:
                case Cdy.Tag.TagType.Double:
                case Cdy.Tag.TagType.DateTime:
                case Cdy.Tag.TagType.IntPoint:
                case Cdy.Tag.TagType.UIntPoint:
                    qulityOffset = dataOffset + count * 8;
                    return qulityOffset + count;
                case Cdy.Tag.TagType.IntPoint3:
                case Cdy.Tag.TagType.UIntPoint3:
                    qulityOffset = dataOffset + count * 12;
                    return qulityOffset + count;

                case Cdy.Tag.TagType.LongPoint:
                case Cdy.Tag.TagType.ULongPoint:
                    qulityOffset = dataOffset + count * 16;
                    return qulityOffset + count;

                case Cdy.Tag.TagType.LongPoint3:
                case Cdy.Tag.TagType.ULongPoint3:
                    qulityOffset = dataOffset + count * 24;
                    return qulityOffset + count;

                case Cdy.Tag.TagType.String:
                    qulityOffset = dataOffset + count * Const.StringSize;
                    return qulityOffset + count;
                default:
                    return 0;
            }
        }

        private int CalCachDatablockSize(Cdy.Tag.TagType tagType, Cdy.Tag.RecordType recordType, int headSize, out int dataOffset, out int qulityOffset)
        {
            //单个数据块内容包括：时间戳(2)+数值+质量戳(1)

            qulityOffset = headSize;
            int count = CachMemoryTime;

            //对于值改变的记录方式,提高内存分配量,以提高值改变记录的数据个数
            //if (recordType == RecordType.ValueChanged)
            //{
            //    count = CachMemoryTime * 1000 / MemoryTimeTick;
            //}

            //数据区偏移,时间戳占2个字节,质量戳占1个字节
            dataOffset = headSize + count * 2;
            switch (tagType)
            {
                case Cdy.Tag.TagType.Byte:
                case Cdy.Tag.TagType.Bool:
                    qulityOffset = dataOffset + count;
                    return qulityOffset + count;
                case Cdy.Tag.TagType.Short:
                case Cdy.Tag.TagType.UShort:
                    qulityOffset = dataOffset + count * 2;
                    return qulityOffset + count;
                case Cdy.Tag.TagType.Int:
                case Cdy.Tag.TagType.UInt:
                case Cdy.Tag.TagType.Float:
                    qulityOffset = dataOffset + count * 4;
                    return qulityOffset + count;
                case Cdy.Tag.TagType.Long:
                case Cdy.Tag.TagType.ULong:
                case Cdy.Tag.TagType.Double:
                case Cdy.Tag.TagType.DateTime:
                case TagType.UIntPoint:
                case TagType.IntPoint:
                    qulityOffset = dataOffset + count * 8;
                    return qulityOffset + count;
                case Cdy.Tag.TagType.IntPoint3:
                case Cdy.Tag.TagType.UIntPoint3:
                    qulityOffset = dataOffset + count * 12;
                    return qulityOffset + count;

                case Cdy.Tag.TagType.LongPoint:
                case Cdy.Tag.TagType.ULongPoint:
                    qulityOffset = dataOffset + count * 16;
                    return qulityOffset + count;

                case Cdy.Tag.TagType.LongPoint3:
                case Cdy.Tag.TagType.ULongPoint3:
                    qulityOffset = dataOffset + count * 24;
                    return qulityOffset + count;
                case Cdy.Tag.TagType.String:
                    qulityOffset = dataOffset + count * Const.StringSize;
                    return qulityOffset + count;
                default:
                    return 0;
            }
        }

        /// <summary>
        /// 分配内存
        /// </summary>
        private void AllocMemory()
        {
            /*
               数据块:数据块头+数据
               数据块头:质量戳偏移+id
               数据:[时间戳]+[值]+[质量戳]
             */
            long storeHeadSize = 0;

            long cachHeadSize = 0;
            int blockheadsize = 0;
            int qulityOffset = 0;
            int valueOffset = 0;

            Dictionary<int, Tuple<long,int, int,int>> addressoffset = new Dictionary<int, Tuple<long, int, int, int>>();

            foreach(var vv in mHisTags)
            {

                var ss = CalMergeBlockSize(vv.Value.TagType,vv.Value.Type,blockheadsize,out valueOffset, out qulityOffset);

                addressoffset.Add(vv.Value.Id, new Tuple<long, int, int,int>(storeHeadSize,valueOffset, qulityOffset,ss));
                storeHeadSize += ss;

                var css = CalCachDatablockSize(vv.Value.TagType, vv.Value.Type, blockheadsize, out valueOffset,out qulityOffset);

                vv.Value.BlockHeadStartAddr = cachHeadSize;

                vv.Value.TimerValueStartAddr = vv.Value.BlockHeadStartAddr;

                vv.Value.HisValueStartAddr = vv.Value.BlockHeadStartAddr + valueOffset;
               
                vv.Value.HisQulityStartAddr = vv.Value.BlockHeadStartAddr + qulityOffset;

                vv.Value.DataSize = css;
                cachHeadSize += css;
                //vv.Value.Init();
            }

            mMergeMemory1 = new MergeMemoryBlock(storeHeadSize) { Name = "StoreMemory1",TagAddress = addressoffset };

            mMergeMemory2 = new MergeMemoryBlock(storeHeadSize) { Name = "StoreMemory2", TagAddress = addressoffset };

            mCachMemory1 = new CachMemoryBlock(cachHeadSize) { Name = "CachMemory1" };
            mCachMemory2 = new CachMemoryBlock(cachHeadSize) { Name = "CachMemory2" };

            LoggerService.Service.Info("HisEnginer", "Cal MergeMemory memory size:" + (storeHeadSize/1024.0/1024 *2)+"M", ConsoleColor.Cyan);
            LoggerService.Service.Info("HisEnginer", "Cal CachMemoryBlock memory size:" + (cachHeadSize / 1024.0 / 1024 *2) + "M", ConsoleColor.Cyan);

            CurrentMemory = mCachMemory1;

            ClearMemoryHisData(mCachMemory1);
            ClearMemoryHisData(mCachMemory2);
            ClearMemoryHisData(mMergeMemory1);
            ClearMemoryHisData(mMergeMemory2);
            //mCachMemory1.Clear();
            //mCachMemory2.Clear();
            //mMergeMemory.Clear();

        }

        ///// <summary>
        ///// 
        ///// </summary>
        //private void PrepareForReadyMemory()
        //{
        //    //写入时间
        //    HisRunTag.StartTime = mLastProcessTime;
        //}

        private string formateDatetime(DateTime datetime)
        {
            return datetime.ToString("yyyy-MM-dd HH:mm:ss.fff");
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            mIsClosed = false;
            mMegerProcessIsClosed = false;
            if (LogManager != null)
            {
                LogManager.InitHeadData(this.mHisTags);
                LogManager.Start();
            }

            foreach (var vv in mRecordTimerProcesser)
            {
                vv.Start();
            }

            foreach(var vv in mValueChangedProcesser)
            {
                vv.Start();
            }

            LoggerService.Service.Info("Record", "历史变量个数: " + this.mHisTags.Count);

            mCachMemory1.MakeMemoryNoBusy();
            mCachMemory2.MakeMemoryNoBusy();
            mMergeMemory1.MakeMemoryNoBusy();
            mMergeMemory2.MakeMemoryNoBusy();

            mLastProcessTime = DateTime.Now;
            HisRunTag.StartTime = mLastProcessTime;
            CurrentMemory = mCachMemory1;
            CurrentMemory.CurrentDatetime = mLastProcessTime;

            mCurrentMergeMemory = mMergeMemory1;
            mCurrentMergeMemory.CurrentDatetime = CurrentMemory.CurrentDatetime;

            SnapeAllTag();
            RecordAllFirstValue();
           

            mStartMergeCount = (int)((mLastProcessTime - new DateTime(mLastProcessTime.Year, mLastProcessTime.Month, mLastProcessTime.Day, ((mLastProcessTime.Hour / mManager.Setting.FileDataDuration) * mManager.Setting.FileDataDuration), 0, 0)).TotalMinutes % (MergeMemoryTime / CachMemoryTime));
            mMergeCount = mStartMergeCount;

            mRecordTimer = new System.Timers.Timer(MemoryTimeTick);
            mRecordTimer.Elapsed += MRecordTimer_Elapsed;
            mRecordTimer.Start();

            mMergeThread = new Thread(MergerMemoryProcess);
            mMergeThread.IsBackground=true;
            mMergeThread.Start();


        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        private void CheckMemoryIsReady(MarshalFixedMemoryBlock memory)
        {
            while (memory.IsBusy())
            {
                LoggerService.Service.Info("Record",  "记录出现阻塞 " + memory.Name);
                System.Threading.Thread.Sleep(1);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        private void CheckMemoryIsReady(MarshalMemoryBlock memory)
        {
            while (memory.IsBusy())
            {
                LoggerService.Service.Info("Record", "记录出现阻塞 " + memory.Name);
                System.Threading.Thread.Sleep(1);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void MergerMemoryProcess()
        {
            int number = MergeMemoryTime / CachMemoryTime;
            int count = mStartMergeCount;
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray1);

            while (!mIsClosed)
            {
                resetEvent.WaitOne();
                resetEvent.Reset();
                //if (mIsClosed) return;

                if(mNeedSnapAllTag)
                {
                    SnapeAllTag();
                    mNeedSnapAllTag = false;
                }

                MemoryMerge(count);

                //如果合并满了，则提交给压缩流程进行压缩
                count++;
                if(count>=number || mForceSubmiteToCompress)
                {
                    
                    if (mCurrentMergeMemory != null)
                    {
                        RecordAllLastValue();
                        //mMergeMemory.Dump();

                        mCurrentMergeMemory.MakeMemoryBusy();
                        //提交到数据压缩流程
                        ServiceLocator.Locator.Resolve<IDataCompress>().RequestToCompress(mCurrentMergeMemory);
                        LoggerService.Service.Info("HisEnginer", "提交内存 " + mCurrentMergeMemory.Name + " 进行压缩",ConsoleColor.Green);

                        //等待压缩完成
                       // while(mMergeMemory1.IsBusy()) Thread.Sleep(1);

                        //切换工作合并内存
                        if(mCurrentMergeMemory == mMergeMemory1)
                        {
                            mCurrentMergeMemory = mMergeMemory2;
                        }
                        else
                        {
                            mCurrentMergeMemory = mMergeMemory1;
                        }

                        LoggerService.Service.Info("HisEnginer", "切换工作 合并内存:" + mCurrentMergeMemory.Name, ConsoleColor.Green);
                        while (mCurrentMergeMemory.IsBusy()) Thread.Sleep(1);
                        RecordAllFirstValue();
                    }
                    count = 0;
                }
            }
            LoggerService.Service.Info("HisEnginer", "合并线程退出!" , ConsoleColor.Green);

            mMegerProcessIsClosed = true;
        }

        /// <summary>
        /// 进行内存合并
        /// </summary>
        private void MemoryMerge(int count)
        {
            if (count == 0)
            {
                mCurrentMergeMemory.CurrentDatetime = mWaitForMergeMemory.CurrentDatetime;
                LoggerService.Service.Info("HisEnginer", "MergeMemory 使用新的时间起点:" + mWaitForMergeMemory.Name+" " + formateDatetime(this.mCurrentMergeMemory.CurrentDatetime), ConsoleColor.Cyan);
            }

            var mcc = mWaitForMergeMemory;
            LoggerService.Service.Info("Record", "开始内存合并" + mcc.Name);
            
            Stopwatch sw = new Stopwatch();
            sw.Start();
            //System.Threading.Tasks.Parallel.ForEach(mHisTags, (tag) => {
            foreach (var tag in mHisTags)
            {
                var vaddrs = mCurrentMergeMemory.TagAddress[tag.Value.Id];
                var addrbase = vaddrs.Item1;

                //拷贝时间
                var tcount = (tag.Value.HisValueStartAddr - tag.Value.TimerValueStartAddr);
                var vtimeaddr = addrbase + tcount * count + 2;
                // mcc.CopyTo(mMergeMemory, tag.Value.TimerValueStartAddr, vtimeaddr, tcount);

                mCurrentMergeMemory.CopyFrom(mcc, tag.Value.TimerValueStartAddr, vtimeaddr, tcount);
                //LoggerService.Service.Info("HisEnginer","拷贝时间数据："+count+"," + tag.Value.TimerValueStartAddr +","+ vtimeaddr + "," + tcount);

                //拷贝数值
                tcount = tag.Value.HisQulityStartAddr - tag.Value.HisValueStartAddr;
                vtimeaddr = addrbase + vaddrs.Item2 + tcount * count+tag.Value.SizeOfValue;
                //mcc.CopyTo(mMergeMemory, tag.Value.HisValueStartAddr, vtimeaddr, tcount);

                mCurrentMergeMemory.CopyFrom(mcc, tag.Value.HisValueStartAddr, vtimeaddr, tcount);

                //LoggerService.Service.Info("HisEnginer", "拷贝数值数据：" + count + "," + tag.Value.HisValueStartAddr + "," + vtimeaddr + "," + tcount);


                //拷贝质量戳
                tcount = tag.Value.DataSize - tag.Value.HisQulityStartAddr + tag.Value.BlockHeadStartAddr;
                vtimeaddr = addrbase + vaddrs.Item3 + tcount * count+1;
                // mcc.CopyTo(mMergeMemory, tag.Value.HisQulityStartAddr, vtimeaddr, tcount);
                mCurrentMergeMemory.CopyFrom(mcc, tag.Value.HisQulityStartAddr, vtimeaddr, tcount);

                //LoggerService.Service.Info("HisEnginer", "拷贝质量戳数据：" + count + "," + tag.Value.HisQulityStartAddr + "," + vtimeaddr + "," + tcount);

            }
            //});
          //  mMergeMemory.Dump();
            //mcc.Dump();

            mcc.MakeMemoryNoBusy();
            //ClearMemoryHisData(mcc);
           
            sw.Stop();
            LoggerService.Service.Info("Record", "合并完成 " + mcc.Name+" 次数:"+(count+1)+" 耗时:"+sw.ElapsedMilliseconds);
        }


        /// <summary>
        /// 提交内存数据到合并
        /// </summary>
        private void SubmiteMemory(DateTime dateTime)
        {
            int number = MergeMemoryTime / CachMemoryTime;

            var mcc = mCurrentMemory;

            mMergeCount++;

            mMergeCount = mMergeCount >= number ? 0 : mMergeCount;

            //HisRunTag.TimerOffset = mMergeCount * 10 * CachMemoryTime;

            if (mCurrentMemory == mCachMemory1)
            {
                CheckMemoryIsReady(mCachMemory2);
                CurrentMemory = mCachMemory2;
            }
            else
            {
                CheckMemoryIsReady(mCachMemory1);
                CurrentMemory = mCachMemory1;
            }

            CurrentMemory.CurrentDatetime = dateTime;

            if (mMergeCount==0)
            {
                mNeedSnapAllTag = true;
                LoggerService.Service.Info("HisEnginer", "使用新的时间起点:" + CurrentMemory.Name + "  " + formateDatetime(CurrentMemory.CurrentDatetime), ConsoleColor.Cyan);
                HisRunTag.StartTime = dateTime;
            }
            //PrepareForReadyMemory();
            foreach (var vv in mHisTags.Values)
            {
                vv.Reset();
            }

            if (mcc != null)
            {
                mcc.MakeMemoryBusy();
                mWaitForMergeMemory = mcc;
                //通知进行内存合并
                resetEvent.Set();

                mLogManager?.RequestToSave(mcc.CurrentDatetime,dateTime, mcc);

            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void SnapeAllTag()
        {
            mSnapAllTagTime = DateTime.Now;
            Stopwatch sw = new Stopwatch();
            sw.Start();
            foreach(var vv in mHisTags)
            {
                vv.Value.Snape();
            }
            sw.Stop();
            LoggerService.Service.Info("HisEnginer", "快照记录数值:" + formateDatetime(mSnapAllTagTime) + " 耗时:" + sw.ElapsedMilliseconds, ConsoleColor.Cyan);
        }

        /// <summary>
        /// 在数据区域头部添加数值
        /// </summary>
        /// <param name="dt"></param>
        private void RecordAllFirstValue()
        {
            
            foreach(var vv in mCurrentMergeMemory.TagAddress)
            {
                //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
                //实时数据内存结构为:实时值+时间戳+质量戳

                long timeraddr = vv.Value.Item1;
                long valueaddr = vv.Value.Item1 + vv.Value.Item2;
                long qaddr = vv.Value.Item1 + vv.Value.Item3;

                var tag = mHisTags[vv.Key];


                mCurrentMergeMemory.WriteShort(timeraddr, 0);

                //写入数值
                mCurrentMergeMemory.WriteBytesDirect(valueaddr,tag.ValueSnape);

                //更新质量戳,在现有质量戳的基础添加100，用于表示这是一个强制更新的值
                mCurrentMergeMemory.WriteByteDirect(qaddr, (byte)(tag.QulitySnape+100));
            }
        }

        /// <summary>
        /// 在数据区域尾部添加数值
        /// </summary>
        /// <param name="dt"></param>
        private void RecordAllLastValue()
        {
            DateTime time = mSnapAllTagTime;
            LoggerService.Service.Info("HisEnginer", "RecordAllLastValue:" + formateDatetime(time), ConsoleColor.Cyan);
            ushort timespan = (ushort)((time - mCurrentMergeMemory.CurrentDatetime).TotalMilliseconds / 100);
            foreach (var vv in mCurrentMergeMemory.TagAddress)
            {
                //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
                //实时数据内存结构为:实时值+时间戳+质量戳

                var tag = mHisTags[vv.Key];

                long timeraddr = vv.Value.Item1 + vv.Value.Item2-2;
                long valueaddr = vv.Value.Item1 + vv.Value.Item3-tag.SizeOfValue;
                long qaddr = vv.Value.Item1 + vv.Value.Item4-1;

                mCurrentMergeMemory.WriteUShort(timeraddr, timespan);

                //写入数值
                mCurrentMergeMemory.WriteBytesDirect(valueaddr,tag.ValueSnape);

                //更新质量戳
                mCurrentMergeMemory.WriteByteDirect(qaddr, (byte)(tag.QulitySnape+100));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void MRecordTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            //内存块的分配，严格按照绝对的时间来执行。例如5分钟一个内存缓冲，而从0点开始每隔5分钟，切换一下内存块;
            //哪怕自启动时间以来，到一个5分钟节点间不足5分钟。
            //这里以最快的固定频率触发事件处理每个内存块；对于定时记录情况，对于没有定时到的情况，不记录，对于定时到的情况，更新实际的时间戳、数值和质量戳。
            //对于值改变的情况，同样按照固定的频路定时更新质量戳（没有值），对于值改变的情况，记录实际时间戳、数值和质量戳。由此可以值改变的频路不能够大于我们最大的频率。

            if (mIsBusy)
            {
                mBlockCount++;
                LoggerService.Service.Warn("RecordTimer", "出现阻塞"+ mBlockCount);
                return;
            }
            mIsBusy = true;
            mBlockCount = 0;
            DateTime dt = DateTime.Now;
            //var mm = (dt.Hour * 24 + dt.Minute * 60 + dt.Second) / CachMemoryTime;
            var mm = dt.Minute;
            if (mm!=mLastProcessTick )
            {
                ///处理第一次运行的情况，轻质到一秒的开始部分
                if (mLastProcessTick == -1 && dt.Millisecond > 400)
                {
                    mIsBusy = false;
                    return;
                }

                LoggerService.Service.Info("Record", mm+"!="+mLastProcessTick+ "-------------------------------------------------------------------------", ConsoleColor.Green);
                LoggerService.Service.Info("Record", "准备新的内存，提交内存 "+ CurrentMemory.Name+ " 到压缩");
                //Stopwatch sw = new Stopwatch();
                //sw.Start();
                if (mLastProcessTick != -1)
                {
                    mLastProcessTime = dt;
                   
                    //将之前的Memory提交到合并流程中
                    SubmiteMemory(dt);
                }
                mLastProcessTick = mm;
                //sw.Stop();
                // LoggerService.Service.Info("Record", (CurrentMemory!=null? CurrentMemory.Name:"")+" 内存初始化:" + sw.ElapsedMilliseconds);
                LoggerService.Service.Info("Record", "*************************************************************************", ConsoleColor.Green);

            }
            foreach (var vv in mRecordTimerProcesser)
            {
                vv.Notify(dt);
            }

            //值改变的变量,也受内部定时逻辑处理。这样值改变频路高于MemoryTimeTick 的值时，无效。
            foreach (var vv in mValueChangedProcesser)
            {
                vv.Notify(dt);
            }

            mIsBusy = false;
        }


        /// <summary>
        /// 
        /// </summary>
        private void SubmitLastDataToSave()
        {
            mNeedSnapAllTag = true;
            mForceSubmiteToCompress = true;
            mIsClosed = true;
            SubmiteMemory(DateTime.Now);
            while (!mMegerProcessIsClosed) Thread.Sleep(1);
        }


        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            mRecordTimer.Stop();
            if(mRecordTimer!=null)
            {
                mRecordTimer.Elapsed -= MRecordTimer_Elapsed;
                mRecordTimer.Dispose();
                mRecordTimer = null;
            }
            foreach (var vv in mRecordTimerProcesser)
            {
                vv.Stop();
                vv.Dispose();
            }
            mRecordTimerProcesser.Clear();

            foreach(var vv in mValueChangedProcesser)
            {
                vv.Stop();
                vv.Dispose();
            }
            mValueChangedProcesser.Clear();

            if (LogManager != null) LogManager.Stop();

            SubmitLastDataToSave();

            mIsClosed = true;

            mLastValueChangedProcesser = null;
            mLastProcesser = null;

            mHisTags.Clear();

            mCachMemory1?.Dispose();
            mCachMemory2?.Dispose();
            mMergeMemory1?.Dispose();
            mMergeMemory2?.Dispose();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public void ClearMemoryHisData(MarshalFixedMemoryBlock memory)
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
        /// <param name="memory"></param>
        public void ClearMemoryHisData(MarshalMemoryBlock memory)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            memory.Clear();
            sw.Stop();
            LoggerService.Service.Info("Record", memory.Name+ "清空数据区耗时:" + sw.ElapsedMilliseconds);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public HisRunTag GetHisTag(int id)
        {
            if(mHisTags.ContainsKey(id))
            {
                return mHisTags[id];
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<HisRunTag> ListAllTags()
        {
            return mHisTags.Values;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
