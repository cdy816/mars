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
using System.Runtime;
using System.ComponentModel;
using Cdy.Tag.Driver;

namespace Cdy.Tag
{
    /// <summary>
    /// 历史数据引擎2
    /// </summary>
    [Obsolete]
    public class HisEnginer2 : IHisEngine2, ITagHisValueProduct, IDisposable, IHisTagQuery
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

        /// <summary>
        /// 
        /// </summary>
        private LogManager2 mLogManager;

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
        private HisDataMemoryBlockCollection mCachMemory1;

        /// <summary>
        /// 历史记录内存2
        /// </summary>
        private HisDataMemoryBlockCollection mCachMemory2;

        /// <summary>
        /// 
        /// </summary>
        private HisDataMemoryBlockCollection mMergeMemory1;

        /// <summary>
        /// 
        /// </summary>
        private HisDataMemoryBlockCollection mMergeMemory2;

        /// <summary>
        /// 当前正在使用的内存
        /// </summary>
        private HisDataMemoryBlockCollection mCurrentMemory;

        /// <summary>
        /// 
        /// </summary>
        private HisDataMemoryBlockCollection mCurrentMergeMemory;

        /// <summary>
        /// 
        /// </summary>
        private HisDataMemoryBlockCollection mWaitForMergeMemory;

        /// <summary>
        /// 值改变的变量列表
        /// </summary>
        private List<ValueChangedMemoryCacheProcesser2> mValueChangedProcesser = new List<ValueChangedMemoryCacheProcesser2>();

        /// <summary>
        /// 
        /// </summary>
        private List<TimerMemoryCacheProcesser2> mRecordTimerProcesser = new List<TimerMemoryCacheProcesser2>();

        /// <summary>
        /// 
        /// </summary>
        private TimerMemoryCacheProcesser2 mLastProcesser = new TimerMemoryCacheProcesser2();

        /// <summary>
        /// 
        /// </summary>
        private ValueChangedMemoryCacheProcesser2 mLastValueChangedProcesser = new ValueChangedMemoryCacheProcesser2() { Name = "ValueChanged0" };

        //private System.Timers.Timer mRecordTimer;

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

        private DateTime mSnapAllTagTime = DateTime.UtcNow;

        private bool mForceSubmiteToCompress = false;

        private bool mMegerProcessIsClosed = false;

        private int mStartMergeCount = 0;

        private Dictionary<long,SortedDictionary<DateTime, ManualHisDataMemoryBlock>> mManualHisDataCach = new Dictionary<long, SortedDictionary<DateTime, ManualHisDataMemoryBlock>>();

        private bool mIsPaused = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public HisEnginer2()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="manager"></param>
        /// <param name="realEnginer"></param>
        public HisEnginer2(Cdy.Tag.HisDatabase manager, Cdy.Tag.RealEnginer realEnginer)
        {
            mManager = manager;
            mRealEnginer = realEnginer;
        }

        #endregion ...Constructor...

        #region ... Properties ...

    

        /// <summary>
        /// 当前工作的内存区域
        /// </summary>
        public HisDataMemoryBlockCollection CurrentMemory
        {
            get
            {
                return mCurrentMemory;
            }
            set
            {
                mCurrentMemory = value;
                SwitchMemoryCach(value.Id);
                //HisRunTag.HisAddr = mCurrentMemory;
            }
        }

        /// <summary>
        /// 当前正在使用的内存
        /// </summary>
        public HisDataMemoryBlockCollection CurrentMergeMemory
        {
            get
            {
                return mCurrentMergeMemory;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public Cdy.Tag.HisDatabase HisTagManager
        {
            get
            {
                return mManager;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public LogManager2 LogManager
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
        /// 
        /// </summary>
        /// <param name="totalTagCount"></param>
        private void UpdatePerProcesserMaxTagCount(int totalTagCount)
        {
            int count = Environment.ProcessorCount / 2;
            int pcount = totalTagCount / count + count;
            TimerMemoryCacheProcesser2.MaxTagCount = ValueChangedMemoryCacheProcesser2.MaxTagCount = pcount;
        }

        /// <summary>
        /// 初始化
        /// </summary>
        public void Init()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            if (mRealEnginer != null)
            {
               
                mLastProcesser = new TimerMemoryCacheProcesser2() { Id = 0 };
                mRecordTimerProcesser.Clear();
                mRecordTimerProcesser.Add(mLastProcesser);

                mValueChangedProcesser.Clear();
                mValueChangedProcesser.Add(mLastValueChangedProcesser);

                UpdatePerProcesserMaxTagCount(mManager.HisTags.Count);

                var count = CachMemoryTime;
                var realbaseaddr = mRealEnginer.Memory;
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
                            mHisTag = new LongHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType, RealMemoryPtr=realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters };
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
                            mHisTag = new StirngHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters };
                            break;
                        case Cdy.Tag.TagType.UIntPoint:
                        case Cdy.Tag.TagType.IntPoint:
                            mHisTag = new IntPointHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters };
                            break;
                        case Cdy.Tag.TagType.UIntPoint3:
                        case Cdy.Tag.TagType.IntPoint3:
                            mHisTag = new IntPoint3HisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters };
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

                    mHisTag.MaxValueCountPerSecond = vv.Value.MaxValueCountPerSecond;
                    mHisTag.MaxCount = count-1;
                    mHisTags.Add(vv.Key, mHisTag);

                    if (mHisTag.Type == Cdy.Tag.RecordType.Timer)
                    {
                        if(!mLastProcesser.AddTag(mHisTag))
                        {
                            mLastProcesser = new TimerMemoryCacheProcesser2() { Id = mLastProcesser.Id + 1 };
                            mLastProcesser.AddTag(mHisTag);
                            mRecordTimerProcesser.Add(mLastProcesser);
                        }
                    }
                    else if(mHisTag.Type == RecordType.ValueChanged)
                    {
                        if(!mLastValueChangedProcesser.AddTag(mHisTag))
                        {
                            mLastValueChangedProcesser = new ValueChangedMemoryCacheProcesser2() { Name = "ValueChanged"+ mValueChangedProcesser.Count+1 };
                            mLastValueChangedProcesser.AddTag(mHisTag);
                            mValueChangedProcesser.Add(mLastValueChangedProcesser);
                        }
                    }
                    mTagCount++;
                }
            }
            long ltmp = sw.ElapsedMilliseconds;
            AllocMemory();

            if (LogManager != null)
            {
                LogManager.InitHeadData();
            }

            mManager.Freedatabase();

            sw.Stop();
            LoggerService.Service.Info("HisEnginer", "生成对象耗时:"+ltmp+" 内存分配耗时:"+(sw.ElapsedMilliseconds-ltmp));
        }

        /// <summary>
        /// 
        /// </summary>
        public void Pause()
        {
            mIsPaused = true;
            //mRecordTimer.Stop();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Resume()
        {
            mIsPaused = false;
            //mRecordTimer.Start();
        }

        /// <summary>
        /// 加载使能新的变量
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="mHisDatabase"></param>
        public void ReLoadTags(IEnumerable<Tag.HisTag> tags,HisDatabase mHisDatabase)
        {
            UpdatePerProcesserMaxTagCount(mManager.HisTags.Count+tags.Count());

            var realbaseaddr = this.mRealEnginer.Memory;
            IntPtr realHandle = mRealEnginer.MemoryHandle;
            HisRunTag mHisTag = null;

            Tagbase mRealTag;

            var histags = new List<HisRunTag>();

            //int tcount = 0;
            //int vcount = 0;

            foreach (var vv in tags)
            {
                var realaddr = (int)mRealEnginer.GetDataAddr((int)vv.Id);
                mRealTag = mRealEnginer.GetTagById(vv.Id);
                switch (vv.TagType)
                {
                    case Cdy.Tag.TagType.Bool:
                    case Cdy.Tag.TagType.Byte:
                        mHisTag = new ByteHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                        break;
                    case Cdy.Tag.TagType.Short:
                    case Cdy.Tag.TagType.UShort:
                        mHisTag = new ShortHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                        break;
                    case Cdy.Tag.TagType.Int:
                    case Cdy.Tag.TagType.UInt:
                        mHisTag = new IntHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                        break;
                    case Cdy.Tag.TagType.Long:
                    case Cdy.Tag.TagType.ULong:
                        mHisTag = new LongHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                        break;
                    case Cdy.Tag.TagType.Float:
                        mHisTag = new FloatHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters, Precision = (mRealTag as FloatTag).Precision };
                        break;
                    case Cdy.Tag.TagType.Double:
                        mHisTag = new DoubleHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType,RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters, Precision = (mRealTag as DoubleTag).Precision };
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        mHisTag = new DateTimeHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                        break;
                    case Cdy.Tag.TagType.String:
                        mHisTag = new StirngHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                    case Cdy.Tag.TagType.IntPoint:
                        mHisTag = new IntPointHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                    case Cdy.Tag.TagType.IntPoint3:
                        mHisTag = new IntPoint3HisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                    case Cdy.Tag.TagType.LongPoint:
                        mHisTag = new LongPointHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                    case Cdy.Tag.TagType.LongPoint3:
                        mHisTag = new LongPoint3HisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                        break;
                }
                mHisTags.Add(vv.Id, mHisTag);
                histags.Add(mHisTag);

               
                mTagCount++;
            }

            foreach(var vv in mRecordTimerProcesser)
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

            mLastProcesser = new TimerMemoryCacheProcesser2() { Id = 0 };
            mRecordTimerProcesser.Clear();
            mRecordTimerProcesser.Add(mLastProcesser);

            mLastValueChangedProcesser = new ValueChangedMemoryCacheProcesser2() { Name = "ValueChanged0" };
            mValueChangedProcesser.Clear();
            mValueChangedProcesser.Add(mLastValueChangedProcesser);

            int qulityOffset = 0;
            int valueOffset = 0;
            int blockheadsize = 0;

            foreach (var vv in mHisTags)
            {

                var ss = CalMergeBlockSize(vv.Value.TagType, blockheadsize, out valueOffset, out qulityOffset);

                var abuffer = new HisDataMemoryBlock(ss) { TimerAddress = 0, ValueAddress = valueOffset, QualityAddress = qulityOffset, Id = 1 };
                var bbuffer = new HisDataMemoryBlock(ss) { TimerAddress = 0, ValueAddress = valueOffset, QualityAddress = qulityOffset, Id = 2 };
                mMergeMemory1.AddTagAddress(vv.Value.Id, abuffer);
                mMergeMemory2.AddTagAddress(vv.Value.Id, bbuffer);

                var css = CalCachDatablockSize(vv.Value.TagType, blockheadsize, out valueOffset, out qulityOffset);
                var cbuffer = new HisDataMemoryBlock(css) { TimerAddress = 0, ValueAddress = valueOffset, QualityAddress = qulityOffset, Id = 1 };
                var dbuffer = new HisDataMemoryBlock(css) { TimerAddress = 0, ValueAddress = valueOffset, QualityAddress = qulityOffset, Id = 2 };

                vv.Value.HisValueMemory1 = cbuffer;
                vv.Value.HisValueMemory2 = dbuffer;
                mCachMemory1.AddTagAddress(vv.Value.Id, cbuffer);
                mCachMemory2.AddTagAddress(vv.Value.Id, dbuffer);

                abuffer.Clear();
                bbuffer.Clear();
                cbuffer.Clear();
                dbuffer.Clear();

                vv.Value.DataSize = css;

                if (vv.Value.Type == Cdy.Tag.RecordType.Timer)
                {
                    if (!mLastProcesser.AddTag(vv.Value))
                    {
                        mLastProcesser = new TimerMemoryCacheProcesser2() { Id = mLastProcesser.Id + 1 };
                        mLastProcesser.AddTag(vv.Value);
                        mRecordTimerProcesser.Add(mLastProcesser);
                    }
                }
                else if(vv.Value.Type == RecordType.ValueChanged)
                {
                    if (!mLastValueChangedProcesser.AddTag(vv.Value))
                    {
                        mLastValueChangedProcesser = new ValueChangedMemoryCacheProcesser2() { Name = "ValueChanged" + mValueChangedProcesser.Count + 1 };
                        mLastValueChangedProcesser.AddTag(vv.Value);
                        mValueChangedProcesser.Add(mLastValueChangedProcesser);
                    }
                }
            }

            this.mManager = mHisDatabase;

            foreach (var vv in mRecordTimerProcesser) { if (!vv.IsStarted) vv.Start(); }

            foreach (var vv in mValueChangedProcesser) { if (!vv.IsStarted) vv.Start(); }

            mLogManager.InitHeadData();

            SwitchMemoryCach(mCurrentMemory.Id);

        }

        /// <summary>
        /// 计算每个变量数据块的大小
        /// </summary>
        /// <param name="tagType"></param>
        /// <returns></returns>
        private int CalMergeBlockSize(Cdy.Tag.TagType tagType,int blockHeadSize,out int dataOffset,out int qulityOffset)
        {

            //单个数据块内容包括：时间戳(2)+数值+质量戳(1)

            qulityOffset = 0;
            int regionHeadSize = blockHeadSize;
            // int count = MemoryCachTime * 1000 / MemoryTimeTick;
            int count = MergeMemoryTime;

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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagType"></param>
        /// <param name="recordType"></param>
        /// <param name="headSize"></param>
        /// <param name="dataOffset"></param>
        /// <param name="qulityOffset"></param>
        /// <returns></returns>
        private int CalCachDatablockSize(Cdy.Tag.TagType tagType, int headSize, out int dataOffset, out int qulityOffset)
        {
            //单个数据块内容包括：时间戳(2)+数值+质量戳(1)

            qulityOffset = headSize;
            int count = CachMemoryTime;

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
        /// 
        /// </summary>
        /// <param name="tagType"></param>
        /// <param name="headSize"></param>
        /// <param name="valueCount"></param>
        /// <param name="dataOffset"></param>
        /// <param name="qulityOffset"></param>
        /// <returns></returns>
        private int CalCachDatablockSize(Cdy.Tag.TagType tagType, int headSize,int valueCount, out int dataOffset, out int qulityOffset)
        {
            //单个数据块内容包括：时间戳(2)+数值+质量戳(1)

            qulityOffset = headSize;
            int count = Math.Max(CachMemoryTime, valueCount);

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
        /// 
        /// </summary>
        /// <param name="tagType"></param>
        /// <param name="headSize"></param>
        /// <param name="valueCount"></param>
        /// <param name="dataOffset"></param>
        /// <param name="qulityOffset"></param>
        /// <returns></returns>
        private int CalCachDatablockSizeForManualRecord(Cdy.Tag.TagType tagType, int headSize, int valueCount, out int dataOffset, out int qulityOffset)
        {
            //单个数据块内容包括：时间戳(2)+数值+质量戳(1)

            qulityOffset = headSize;
            int count = Math.Max(CachMemoryTime, valueCount);

            //数据区偏移,时间戳占4个字节,质量戳占1个字节
            dataOffset = headSize + count * 4;
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

            mMergeMemory1 = new HisDataMemoryBlockCollection() { Name = "StoreMemory1", Id = 1 };

            mMergeMemory2 = new HisDataMemoryBlockCollection() { Name = "StoreMemory2", Id = 2 };

            mCachMemory1 = new HisDataMemoryBlockCollection() { Name = "CachMemory1", Id = 1 };
            mCachMemory2 = new HisDataMemoryBlockCollection() { Name = "CachMemory2", Id = 2 };

            foreach (var vv in mHisTags)
            {

                if (vv.Value.Type != RecordType.Driver)
                {
                    var ss = CalMergeBlockSize(vv.Value.TagType, blockheadsize, out valueOffset, out qulityOffset);

                    var abuffer = new HisDataMemoryBlock(ss) { TimerAddress = 0, ValueAddress = valueOffset, QualityAddress = qulityOffset, Id = 1 };
                    var bbuffer = new HisDataMemoryBlock(ss) { TimerAddress = 0, ValueAddress = valueOffset, QualityAddress = qulityOffset, Id = 2 };
                    mMergeMemory1.AddTagAddress(vv.Value.Id, abuffer);
                    mMergeMemory2.AddTagAddress(vv.Value.Id, bbuffer);

                    storeHeadSize += ss;

                    var css = CalCachDatablockSize(vv.Value.TagType, blockheadsize, out valueOffset, out qulityOffset);

                    var cbuffer = new HisDataMemoryBlock(css) { TimerAddress = 0, ValueAddress = valueOffset, QualityAddress = qulityOffset, Id = 1 };
                    var dbuffer = new HisDataMemoryBlock(css) { TimerAddress = 0, ValueAddress = valueOffset, QualityAddress = qulityOffset, Id = 2 };

                    vv.Value.HisValueMemory1 = cbuffer;
                    vv.Value.HisValueMemory2 = dbuffer;
                    mCachMemory1.AddTagAddress(vv.Value.Id, cbuffer);
                    mCachMemory2.AddTagAddress(vv.Value.Id, dbuffer);

                    vv.Value.DataSize = css;

                    cachHeadSize += css;
                }
                else
                {
                    var css = CalCachDatablockSizeForManualRecord(vv.Value.TagType, 0, MergeMemoryTime * vv.Value.MaxValueCountPerSecond + 2, out valueOffset, out qulityOffset);
                    ManualHisDataMemoryBlockPool.Pool.PreAlloc(css);
                    ManualHisDataMemoryBlockPool.Pool.PreAlloc(css);
                    cachHeadSize += css;
                }
                //else
                //{
                //    mMergeMemory1.AddTagAddress(vv.Value.Id, null);
                //    mMergeMemory2.AddTagAddress(vv.Value.Id, null);

                //    mCachMemory1.AddTagAddress(vv.Value.Id, null);
                //    mCachMemory2.AddTagAddress(vv.Value.Id, null);
                //}
            }

            

            LoggerService.Service.Info("HisEnginer", "Cal MergeMemory memory size:" + (storeHeadSize/1024.0/1024 *2)+"M", ConsoleColor.Cyan);
            LoggerService.Service.Info("HisEnginer", "Cal CachMemoryBlock memory size:" + (cachHeadSize / 1024.0 / 1024 *2) + "M", ConsoleColor.Cyan);

            CurrentMemory = mCachMemory1;
            mCurrentMergeMemory = mMergeMemory1;
            

        }

        private string FormateDatetime(DateTime datetime)
        {
            return datetime.ToString("yyyy-MM-dd HH:mm:ss.fff");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        private void SwitchMemoryCach(int id)
        {
            if(id==1)
            {
                foreach(var vv in mHisTags)
                {
                    vv.Value.HisValueMemoryStartAddr = vv.Value.HisValueMemory1;
                    vv.Value.Reset();
                }
            }
            else
            {
                foreach (var vv in mHisTags)
                {
                    vv.Value.HisValueMemoryStartAddr = vv.Value.HisValueMemory2;
                    vv.Value.Reset();
                }
            }
        }

        private Thread mScanThread;

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            LoggerService.Service.Info("HisEnginer", "开始启动");
            mIsClosed = false;
            mMegerProcessIsClosed = false;
            LoggerService.Service.Info("HisEnginer", "历史变量个数: " + this.mHisTags.Count);

            ClearMemoryHisData(mCachMemory1);
            ClearMemoryHisData(mCachMemory2);
            ClearMemoryHisData(mMergeMemory1);
            ClearMemoryHisData(mMergeMemory2);

            mCachMemory1.MakeMemoryNoBusy();
            mCachMemory2.MakeMemoryNoBusy();
            mMergeMemory1.MakeMemoryNoBusy();
            mMergeMemory2.MakeMemoryNoBusy();

            foreach (var vv in mRecordTimerProcesser)
            {
                vv.Start();
            }

            foreach(var vv in mValueChangedProcesser)
            {
                vv.Start();
            }

            mLastProcessTime = DateTime.UtcNow;
            HisRunTag.StartTime = mLastProcessTime;
            CurrentMemory = mCachMemory1;
            CurrentMemory.CurrentDatetime = mLastProcessTime;

            HisDataMemoryQueryService.Service.RegistorMemory(CurrentMemory.CurrentDatetime, mLastProcessTime.AddSeconds(CachMemoryTime), CurrentMemory);

            mCurrentMergeMemory = mMergeMemory1;
            mCurrentMergeMemory.CurrentDatetime = CurrentMemory.CurrentDatetime;

            if (LogManager != null)
            {
                LogManager.Start();
            }

            SnapeAllTag();
            RecordAllFirstValue();
           

            mStartMergeCount = (int)((mLastProcessTime - new DateTime(mLastProcessTime.Year, mLastProcessTime.Month, mLastProcessTime.Day, ((mLastProcessTime.Hour / mManager.Setting.FileDataDuration) * mManager.Setting.FileDataDuration), 0, 0)).TotalMinutes % (MergeMemoryTime / CachMemoryTime));
            mMergeCount = mStartMergeCount;

            //mRecordTimer = new System.Timers.Timer(MemoryTimeTick);
            //mRecordTimer.Elapsed += MRecordTimer_Elapsed;
            //mRecordTimer.Start();

            mScanThread = new Thread(ScanProcess);
            mScanThread.Priority = ThreadPriority.Highest;
            //mScanThread.IsBackground = true;
            mScanThread.Start();

            mMergeThread = new Thread(MergerMemoryProcess);
            mMergeThread.IsBackground=true;
            mMergeThread.Start();


        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        private void CheckMemoryIsReady(HisDataMemoryBlockCollection memory)
        {
            while (memory.IsBusy())
            {
                LoggerService.Service.Info("HisEnginer",  "记录出现阻塞 " + memory.Name);
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

                        mCurrentMergeMemory.EndDateTime = mSnapAllTagTime;

                        HisDataMemoryQueryService.Service.RegistorMemory(mCurrentMergeMemory.CurrentDatetime, mCurrentMergeMemory.EndDateTime, mCurrentMergeMemory);

                        mCurrentMergeMemory.MakeMemoryBusy();
                        //提交到数据压缩流程
                        ServiceLocator.Locator.Resolve<IDataCompress2>().RequestToCompress(mCurrentMergeMemory);
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
                LoggerService.Service.Info("HisEnginer", "MergeMemory 使用新的时间起点:" + mWaitForMergeMemory.Name+" " + FormateDatetime(this.mCurrentMergeMemory.CurrentDatetime), ConsoleColor.Green);
            }

            var mcc = mWaitForMergeMemory;
            LoggerService.Service.Info("HisEnginer", "开始内存合并" + mcc.Name, ConsoleColor.Green);
            
            Stopwatch sw = new Stopwatch();
            sw.Start();

            //foreach (var tag in mHisTags)
            foreach(var vv in mcc.TagAddress)
            {
                var tag = mHisTags[vv.Key];
                var taddrs = mCurrentMergeMemory.TagAddress[tag.Id];

                //var saddrs = mcc.TagAddress[tag.Value.Id];
                var saddrs = vv.Value;

                //
                //if (taddrs == null || saddrs == null) continue;

                //拷贝时间
                var dlen = saddrs.ValueAddress;
                var vtimeaddr = dlen * count + 2;

                saddrs.CopyTo(taddrs, 0, vtimeaddr, dlen);

                //拷贝数值
                dlen = saddrs.QualityAddress - saddrs.ValueAddress;
                vtimeaddr = taddrs.ValueAddress + dlen * count + tag.SizeOfValue;
                saddrs.CopyTo(taddrs, saddrs.ValueAddress, vtimeaddr, dlen);


                //拷贝质量戳
                dlen = tag.DataSize - saddrs.QualityAddress;

                vtimeaddr = taddrs.QualityAddress + dlen * count + 1;
                saddrs.CopyTo(taddrs, saddrs.QualityAddress, vtimeaddr, dlen);
            }

            HisDataMemoryQueryService.Service.ClearMemoryTime(mcc.CurrentDatetime);
            HisDataMemoryQueryService.Service.RegistorMemory(mCurrentMergeMemory.CurrentDatetime, mcc.EndDateTime, mCurrentMergeMemory);

            mcc.MakeMemoryNoBusy();
            sw.Stop();
            LoggerService.Service.Info("HisEnginer", "合并完成 " + mcc.Name+" 次数:"+(count+1)+" 耗时:"+sw.ElapsedMilliseconds);
        }


        /// <summary>
        /// 提交内存数据到合并
        /// </summary>
        private void SubmiteMemory(DateTime dateTime)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
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

            long ltmp = sw.ElapsedMilliseconds;

            HisDataMemoryQueryService.Service.RegistorMemory(CurrentMemory.CurrentDatetime,dateTime.AddSeconds(CachMemoryTime), CurrentMemory);
            long ltmp21 = sw.ElapsedMilliseconds;
            if (mMergeCount==0)
            {
                mNeedSnapAllTag = true;
                LoggerService.Service.Info("HisEnginer", "使用新的时间起点:" + CurrentMemory.Name + "  " + FormateDatetime(CurrentMemory.CurrentDatetime), ConsoleColor.Cyan);
                HisRunTag.StartTime = dateTime;
            }
            long ltmp22 = sw.ElapsedMilliseconds;
            ////PrepareForReadyMemory();
            //foreach (var vv in mHisTags)
            //{
            //    vv.Value.Reset();
            //}
            long ltmp2 = sw.ElapsedMilliseconds;
            if (mcc != null)
            {
                mcc.MakeMemoryBusy();
                mcc.EndDateTime = dateTime;

                mWaitForMergeMemory = mcc;
                //通知进行内存合并
                resetEvent.Set();

                mLogManager?.RequestToSave(mcc.CurrentDatetime,dateTime, mcc);

            }
            long ltmp3 = sw.ElapsedMilliseconds;
            sw.Stop();

            LoggerService.Service.Info("HisEnginer", "SubmiteMemory 耗时:" + ltmp + "," + (ltmp2-ltmp22)+","+(ltmp21-ltmp)+","+(ltmp22-ltmp21)+","+(ltmp3-ltmp2), ConsoleColor.Cyan);

        }

        /// <summary>
        /// 
        /// </summary>
        private void SnapeAllTag()
        {
            mSnapAllTagTime = DateTime.UtcNow;
            Stopwatch sw = new Stopwatch();
            sw.Start();
            foreach(var vv in mHisTags)
            {
                //if(vv.Value.Type != RecordType.Driver)
                vv.Value.Snape();
            }
            sw.Stop();
            LoggerService.Service.Info("HisEnginer", "快照记录数值:" + FormateDatetime(mSnapAllTagTime.ToLocalTime()) + " 耗时:" + sw.ElapsedMilliseconds, ConsoleColor.Cyan);
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

                //long timeraddr = vv.Value.Item1;
                //long valueaddr = vv.Value.Item1 + vv.Value.Item2;
                //long qaddr = vv.Value.Item1 + vv.Value.Item3;

                if (vv.Value == null) continue;

                var tag = mHisTags[vv.Key];

                vv.Value.WriteShort(0, 0);

                //写入数值
                vv.Value.WriteBytesDirect((int)vv.Value.ValueAddress,tag.ValueSnape);

                //更新质量戳,在现有质量戳的基础添加100，用于表示这是一个强制更新的值
                vv.Value.WriteByteDirect((int)vv.Value.QualityAddress, (byte)(tag.QulitySnape+100));
            }
        }

        /// <summary>
        /// 在数据区域尾部添加数值
        /// </summary>
        /// <param name="dt"></param>
        private void RecordAllLastValue()
        {
            DateTime time = mSnapAllTagTime;
            LoggerService.Service.Info("HisEnginer", "RecordAllLastValue:" + FormateDatetime(time), ConsoleColor.Cyan);
            ushort timespan = (ushort)((time - mCurrentMergeMemory.CurrentDatetime).TotalMilliseconds / 100);
            foreach (var vv in mCurrentMergeMemory.TagAddress)
            {
                //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
                //实时数据内存结构为:实时值+时间戳+质量戳

                if (vv.Value == null) continue;

                var tag = mHisTags[vv.Key];

                long timeraddr = vv.Value.ValueAddress-2;
                long valueaddr = vv.Value.QualityAddress-tag.SizeOfValue;
                long qaddr = vv.Value.AllocSize - 1;

                vv.Value.WriteUShort((int)timeraddr, timespan);

                //写入数值
                vv.Value.WriteBytesDirect((int)valueaddr,tag.ValueSnape);

                //更新质量戳
                vv.Value.WriteByteDirect((int)qaddr, (byte)(tag.QulitySnape+100));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private Stopwatch sw = new Stopwatch();
        /// <summary>
        /// 
        /// </summary>
        private void ScanProcess()
        {
            //内存块的分配，严格按照绝对的时间来执行。例如5分钟一个内存缓冲，而从0点开始每隔5分钟，切换一下内存块;
            //哪怕自启动时间以来，到一个5分钟节点间不足5分钟。
            //这里以最快的固定频率触发事件处理每个内存块；对于定时记录情况，对于没有定时到的情况，不记录，对于定时到的情况，更新实际的时间戳、数值和质量戳。
            //对于值改变的情况，同样按照固定的频路定时更新质量戳（没有值），对于值改变的情况，记录实际时间戳、数值和质量戳。由此可以值改变的频路不能够大于我们最大的频率。

            //LoggerService.Service.Info("HisEnginer", "Thread id:"+Thread.CurrentThread.ManagedThreadId);
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray1);

            while (!mIsClosed)
            {

                while (mIsPaused)
                {
                    if (mIsClosed) return;
                    Thread.Sleep(10);
                }
               
                sw.Start();
                mBlockCount = 0;
                DateTime dt = DateTime.UtcNow;
                var mm = dt.Minute;
                if (mm != mLastProcessTick)
                {
                    ///处理第一次运行的情况，停止到一秒的开始部分
                    if (mLastProcessTick == -1 && dt.Millisecond > 400)
                    {
                        foreach (var vv in mRecordTimerProcesser)
                        {
                            vv.UpdateLastUpdateDatetime(dt);
                        }
                        continue;
                    }

                    LoggerService.Service.Info("HisEnginer", "RecordTimer 准备新的内存，提交内存 " + CurrentMemory.Name + " 到压缩 线程ID:" + Thread.CurrentThread.ManagedThreadId + " CPU ID:" + ThreadHelper.GetCurrentProcessorNumber(), ConsoleColor.Green);
                    if (mLastProcessTick != -1)
                    {
                        mLastProcessTime = dt;

                        //将之前的Memory提交到合并流程中
                        SubmiteMemory(dt);
                    }
                    mLastProcessTick = mm;

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
                sw.Stop();
                
                if(sw.ElapsedMilliseconds>500)
                {
                    LoggerService.Service.Debug("HisEnginer", "ScanProcess 执行周期过程" + sw.ElapsedMilliseconds, ConsoleColor.Yellow);
                }

                if (sw.ElapsedMilliseconds >= 50)
                    Thread.Sleep(1);
                else
                    Thread.Sleep(50-(int)sw.ElapsedMilliseconds);

                sw.Reset();
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="sender"></param>
        ///// <param name="e"></param>
        //private void MRecordTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        //{
        //    //内存块的分配，严格按照绝对的时间来执行。例如5分钟一个内存缓冲，而从0点开始每隔5分钟，切换一下内存块;
        //    //哪怕自启动时间以来，到一个5分钟节点间不足5分钟。
        //    //这里以最快的固定频率触发事件处理每个内存块；对于定时记录情况，对于没有定时到的情况，不记录，对于定时到的情况，更新实际的时间戳、数值和质量戳。
        //    //对于值改变的情况，同样按照固定的频路定时更新质量戳（没有值），对于值改变的情况，记录实际时间戳、数值和质量戳。由此可以值改变的频路不能够大于我们最大的频率。

        //    //LoggerService.Service.Info("HisEnginer", "Thread id:"+Thread.CurrentThread.ManagedThreadId);

        //    var tick = (DateTime.Now - mdatetime).Milliseconds;

        //    if (mIsBusy)
        //    {
        //        mBlockCount++;
        //        LoggerService.Service.Warn("HisEnginer", "RecordTimer 出现阻塞" + mBlockCount);
        //        return;
        //    }

        //    Stopwatch sw = new Stopwatch();
        //    sw.Start();

        //    mIsBusy = true;
        //    mBlockCount = 0;
        //    DateTime dt = DateTime.Now;
        //    //var mm = (dt.Hour * 24 + dt.Minute * 60 + dt.Second) / CachMemoryTime;
        //    var mm = dt.Minute;
        //    if (mm!=mLastProcessTick )
        //    {
        //        ///处理第一次运行的情况，停止到一秒的开始部分
        //        if (mLastProcessTick == -1 && dt.Millisecond > 400)
        //        {
        //            mIsBusy = false;
        //            return;
        //        }

        //        LoggerService.Service.Info("HisEnginer", "RecordTimer 准备新的内存，提交内存 " + CurrentMemory.Name+ " 到压缩 线程ID:"+ Thread.CurrentThread.ManagedThreadId+" CPU ID:"+ ThreadHelper.GetCurrentProcessorNumber(), ConsoleColor.Green);
        //        if (mLastProcessTick != -1)
        //        {
        //            mLastProcessTime = dt;
                   
        //            //将之前的Memory提交到合并流程中
        //            SubmiteMemory(dt);
        //        }
        //        mLastProcessTick = mm;

        //    }

        //    long ltmp1 = sw.ElapsedMilliseconds;

        //    foreach (var vv in mRecordTimerProcesser)
        //    {
        //        vv.Notify(dt);
        //    }

        //    long ltmp2 = sw.ElapsedMilliseconds;

        //    //值改变的变量,也受内部定时逻辑处理。这样值改变频路高于MemoryTimeTick 的值时，无效。
        //    foreach (var vv in mValueChangedProcesser)
        //    {
        //        vv.Notify(dt);
        //    }

        //    long ltmp3 = sw.ElapsedMilliseconds;

        //    sw.Stop();

        //    if (ltmp3>0)
        //    LoggerService.Service.Debug("HisEnginer", "MRecordTimer_Elapsed:" + ltmp1 + "," + ltmp2 + "," + ltmp3);

        //    var dtmp = (DateTime.Now - mdatetime).TotalMilliseconds;
        //    if (dtmp>150)
        //    {
        //        LoggerService.Service.Debug("HisEnginer", "MRecordTimer_Elapsed 定时器超时！，定时间隔："+dtmp +" 定时精度： "+ tick, ConsoleColor.Red);
        //    }

        //    mdatetime = DateTime.Now;

        //    mIsBusy = false;
        //}


        /// <summary>
        /// 
        /// </summary>
        private void SubmitLastDataToSave()
        {
            mNeedSnapAllTag = true;
            mForceSubmiteToCompress = true;
            mIsClosed = true;

            SubmiteMemory(DateTime.UtcNow);
            while (!mMegerProcessIsClosed) Thread.Sleep(1);

            SaveManualCachData();
        }

        /// <summary>
        /// 保存手动提交历史记录缓存部分
        /// </summary>
        private void SaveManualCachData()
        {
            foreach(var vv in mManualHisDataCach)
            {
                foreach (var vvv in vv.Value)
                {
                    ServiceLocator.Locator.Resolve<IDataCompress2>().RequestManualToCompress(vvv.Value);
                }
                ServiceLocator.Locator.Resolve<IDataCompress2>().SubmitManualToCompress();
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            LoggerService.Service.Info("HisEnginer", "开始停止");
            //if(mRecordTimer!=null)
            //{
            //    mRecordTimer.Stop();
            //    mRecordTimer.Elapsed -= MRecordTimer_Elapsed;
            //    mRecordTimer.Dispose();
            //    mRecordTimer = null;
            //}

            foreach (var vv in mRecordTimerProcesser)
            {
                vv.Stop();
            }

            foreach (var vv in mValueChangedProcesser)
            {
                vv.Stop();
            }
            
            SubmitLastDataToSave();

            if (LogManager != null) LogManager.Stop();
            
            mLastProcessTick = -1;
            mForceSubmiteToCompress = false;
            mNeedSnapAllTag = false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public void ClearMemoryHisData(HisDataMemoryBlockCollection memory)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            //while (memory.IsBusy()) ;
            memory.Clear();           
            sw.Stop();
            LoggerService.Service.Info("HisEnginer", memory.Name + "清空数据区耗时:" + sw.ElapsedMilliseconds);
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
            LoggerService.Service.Info("HisEnginer", memory.Name+ "清空数据区耗时:" + sw.ElapsedMilliseconds);
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
        public List<HisRunTag> ListAllTags()
        {
            return mHisTags.Values.ToList();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            foreach (var vv in mRecordTimerProcesser)
            {
                vv.Dispose();
            }
            mRecordTimerProcesser.Clear();

            foreach (var vv in mValueChangedProcesser)
            {
                vv.Dispose();
            }
            mValueChangedProcesser.Clear();

            mLastValueChangedProcesser = null;
            mLastProcesser = null;

            mHisTags.Clear();

            mCachMemory1?.Dispose();
            mCachMemory2?.Dispose();
            mMergeMemory1?.Dispose();
            mMergeMemory2?.Dispose();
        }

        /// <summary>
        /// 手动插入历史数据,
        /// 数据会累计到整个数据块大小时再提交到后面进行压缩、存储处理
        /// </summary>
        /// <param name="id"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        private bool ManualRecordHisValues(long id, IEnumerable<Cdy.Tag.TagValue> values)
        {
            if (mIsClosed) return false;

            int valueOffset, qulityOffset = 0;

            DateTime mLastTime = DateTime.MinValue;

            SortedDictionary<DateTime, ManualHisDataMemoryBlock> datacach;

            if (mHisTags.ContainsKey(id) && mHisTags[id].Type == RecordType.Driver)
            {
                var tag = mHisTags[id];

                lock (mManualHisDataCach)
                {
                    if (mManualHisDataCach.ContainsKey(id))
                    {
                        datacach = mManualHisDataCach[id];
                    }
                    else
                    {
                        datacach = new SortedDictionary<DateTime, ManualHisDataMemoryBlock>();
                        mManualHisDataCach.Add(id, datacach);
                    }
                }
               
                ManualHisDataMemoryBlock hb = null;
                DateTime utcnow = DateTime.UtcNow;

                foreach (var vv in values)
                {
                    if (vv.Time < tag.LastUpdateTime || vv.Time>utcnow)
                    {
                        continue;
                    }
                    tag.LastUpdateTime = vv.Time;

                    var vdata = vv.Time.Date;
                    var mms = (int)(vv.Time.Subtract(vdata).TotalSeconds / MergeMemoryTime);
                    var time = vdata.AddSeconds(mms * MergeMemoryTime);
                    if (datacach.ContainsKey(time))
                    {
                        hb = datacach[time];
                    }
                    else
                    {
                        if(hb!=null)
                            HisDataMemoryQueryService.Service.RegistorManual(id, hb.Time, hb.EndTime, hb);
                        var css = CalCachDatablockSizeForManualRecord(tag.TagType, 0, MergeMemoryTime * tag.MaxValueCountPerSecond + 2, out valueOffset, out qulityOffset);
                        hb = ManualHisDataMemoryBlockPool.Pool.Get(css);
                        hb.Time = time;
                        hb.MaxCount = MergeMemoryTime * tag.MaxValueCountPerSecond + 2;
                        hb.TimeUnit = 1;
                        hb.TimeLen = 4;
                        hb.TimerAddress = 0;
                        hb.ValueAddress = valueOffset;
                        hb.QualityAddress = qulityOffset;
                        hb.Id = (int)id;
                        HisDataMemoryQueryService.Service.RegistorManual(id, hb.Time, time, hb);
                        datacach.Add(time, hb);

                    }
                    mLastTime = time;

                    if (hb.CurrentCount < hb.MaxCount && vv.Time > hb.EndTime)
                    {
                        hb.Lock();
                        var vtime = (int)((vv.Time - hb.Time).TotalMilliseconds / 1);
                        //写入时间戳
                        hb.WriteInt(hb.TimerAddress + hb.CurrentCount * 4, vtime);
                        switch (tag.TagType)
                        {
                            case TagType.Bool:
                                hb.WriteByteDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToByte(Convert.ToBoolean(vv.Value)));
                                break;
                            case TagType.Byte:
                                hb.WriteByteDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToByte(vv.Value));
                                break;
                            case TagType.Short:
                                hb.WriteShortDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt16(vv.Value));
                                break;
                            case TagType.UShort:
                                hb.WriteUShortDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt16(vv.Value));
                                break;
                            case TagType.Int:
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt32(vv.Value));
                                break;
                            case TagType.UInt:
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt32(vv.Value));
                                break;
                            case TagType.Long:
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt64(vv.Value));
                                break;
                            case TagType.ULong:
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt64(vv.Value));
                                break;
                            case TagType.Float:
                                hb.WriteFloatDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToSingle(vv.Value));
                                break;
                            case TagType.Double:
                                hb.WriteDoubleDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToDouble(vv.Value));
                                break;
                            case TagType.String:
                                hb.WriteStringDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToString(vv.Value), Encoding.Unicode);
                                break;
                            case TagType.DateTime:
                                hb.WriteDatetime(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToDateTime(vv.Value));
                                break;
                            case TagType.UIntPoint:
                                UIntPointData data = (UIntPointData)vv.Value;
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, data.X);
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, data.Y);
                                break;
                            case TagType.IntPoint:
                                IntPointData idata = (IntPointData)vv.Value;
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, idata.X);
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, idata.Y);
                                break;
                            case TagType.UIntPoint3:
                                UIntPoint3Data udata3 = (UIntPoint3Data)vv.Value;
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, udata3.X);
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, udata3.Y);
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, udata3.Z);
                                break;
                            case TagType.IntPoint3:
                                IntPoint3Data idata3 = (IntPoint3Data)vv.Value;
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, idata3.X);
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, idata3.Y);
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, idata3.Z);
                                break;

                            case TagType.ULongPoint:
                                ULongPointData udata = (ULongPointData)vv.Value;
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, udata.X);
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, udata.Y);
                                break;
                            case TagType.LongPoint:
                                LongPointData lidata = (LongPointData)vv.Value;
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, lidata.X);
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, lidata.Y);
                                break;
                            case TagType.ULongPoint3:
                                ULongPoint3Data ludata3 = (ULongPoint3Data)vv.Value;
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, ludata3.X);
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, ludata3.Y);
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 24, ludata3.Z);
                                break;
                            case TagType.LongPoint3:
                                LongPoint3Data lidata3 = (LongPoint3Data)vv.Value;
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, lidata3.X);
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, lidata3.Y);
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 24, lidata3.Z);
                                break;
                        }
                        hb.WriteByte(hb.QualityAddress + hb.CurrentCount, vv.Quality);
                        hb.EndTime = vv.Time;
                        hb.CurrentCount++;
                        hb.Relase();
                    }
                }
                if (hb != null)
                    HisDataMemoryQueryService.Service.RegistorManual(id, hb.Time, hb.EndTime, hb);

                bool isNeedSubmite = false;

                if (values.Count() > 0)
                {
                    foreach (var vv in datacach.ToArray())
                    {
                        if (vv.Key < mLastTime)
                        {
                            ServiceLocator.Locator.Resolve<IDataCompress2>().RequestManualToCompress(vv.Value);
                            datacach.Remove(vv.Key);
                            isNeedSubmite = true;
                        }
                    }
                    if (isNeedSubmite)
                        ServiceLocator.Locator.Resolve<IDataCompress2>().SubmitManualToCompress();
                }
               
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        private bool ManualRecordHisValues(long id, Cdy.Tag.TagValue value, out bool isNeedSubmite)
        {
            return ManualRecordHisValues(id, value.Time, value.Value, value.Quality,out isNeedSubmite);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="datetime"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        private bool ManualRecordHisValues<T>(long id,DateTime datetime ,T value,byte quality,out bool isNeedSubmite)
        {
            if (mIsClosed)
            {
                isNeedSubmite = false;
                return false;
            }
            isNeedSubmite = false;
            int valueOffset, qulityOffset = 0;

            SortedDictionary<DateTime, ManualHisDataMemoryBlock> datacach;
           
            if (mHisTags.ContainsKey(id) && mHisTags[id].Type == RecordType.Driver)
            {
                var tag = mHisTags[id];
                DateTime utcnow = DateTime.UtcNow;
                if (datetime<tag.LastUpdateTime || datetime>utcnow)
                {
                    return false;
                }
                tag.LastUpdateTime = datetime;
                ManualHisDataMemoryBlock hb = null;

                lock (mManualHisDataCach)
                {
                    if (mManualHisDataCach.ContainsKey(id))
                    {
                        datacach = mManualHisDataCach[id];
                    }
                    else
                    {
                        datacach = new SortedDictionary<DateTime, ManualHisDataMemoryBlock>();
                        mManualHisDataCach.Add(id, datacach);
                    }
                }

                var vdata = datetime.Date;
                var mms = (int)(datetime.Subtract(vdata).TotalSeconds / MergeMemoryTime);
                var time = vdata.AddSeconds(mms * MergeMemoryTime);
                if (datacach.ContainsKey(time))
                {
                    hb = datacach[time];
                }
                else
                {
                    var css = CalCachDatablockSizeForManualRecord(tag.TagType, 0, MergeMemoryTime * tag.MaxValueCountPerSecond + 2, out valueOffset, out qulityOffset);
                    hb = ManualHisDataMemoryBlockPool.Pool.Get(css);
                    hb.Time = time;
                    hb.MaxCount = MergeMemoryTime * tag.MaxValueCountPerSecond + 2;
                    hb.TimeUnit = 1;
                    hb.TimeLen = 4;
                    hb.TimerAddress = 0;
                    hb.ValueAddress = valueOffset;
                    hb.QualityAddress = qulityOffset;
                    hb.Id = (int)id;
                    hb.CurrentCount = 0;

                    while(datacach.Count>0)
                    {
                        isNeedSubmite = true;
                        var vdd = datacach.First();
                        ServiceLocator.Locator.Resolve<IDataCompress2>().RequestManualToCompress(vdd.Value);
                        datacach.Remove(vdd.Key);
                    }
                    datacach.Add(time, hb);
                    //LoggerService.Service.Info("HisEnginer", "new cal datablock");
                }

                if (hb.CurrentCount < hb.MaxCount && datetime > hb.EndTime)
                {
                    hb.Lock();
                    var vtime = (int)((datetime - hb.Time).TotalMilliseconds / 1);
                    //写入时间戳
                    hb.WriteInt(hb.TimerAddress + hb.CurrentCount * 4, vtime);
                    switch (tag.TagType)
                    {
                        case TagType.Bool:
                            hb.WriteByteDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToByte(Convert.ToBoolean(value)));
                            break;
                        case TagType.Byte:
                            hb.WriteByteDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToByte(value));
                            break;
                        case TagType.Short:
                            hb.WriteShortDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt16(value));
                            break;
                        case TagType.UShort:
                            hb.WriteUShortDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt16(value));
                            break;
                        case TagType.Int:
                            hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt32(value));
                            break;
                        case TagType.UInt:
                            hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt32(value));
                            break;
                        case TagType.Long:
                            hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt64(value));
                            break;
                        case TagType.ULong:
                            hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt64(value));
                            break;
                        case TagType.Float:
                            hb.WriteFloatDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToSingle(value));
                            break;
                        case TagType.Double:
                            hb.WriteDoubleDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToDouble(value));
                            break;
                        case TagType.String:
                            hb.WriteStringDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToString(value), Encoding.Unicode);
                            break;
                        case TagType.DateTime:
                            hb.WriteDatetime(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToDateTime(value));
                            break;
                        case TagType.UIntPoint:
                            UIntPointData data = (UIntPointData)(object)value;
                            hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, data.X);
                            hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, data.Y);
                            break;
                        case TagType.IntPoint:
                            IntPointData idata = (IntPointData)(object)value;
                            hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, idata.X);
                            hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, idata.Y);
                            break;
                        case TagType.UIntPoint3:
                            UIntPoint3Data udata3 = (UIntPoint3Data)(object)value;
                            hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, udata3.X);
                            hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, udata3.Y);
                            hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, udata3.Z);
                            break;
                        case TagType.IntPoint3:
                            IntPoint3Data idata3 = (IntPoint3Data)(object)value;
                            hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, idata3.X);
                            hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, idata3.Y);
                            hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, idata3.Z);
                            break;

                        case TagType.ULongPoint:
                            ULongPointData udata = (ULongPointData)(object)value;
                            hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, udata.X);
                            hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, udata.Y);
                            break;
                        case TagType.LongPoint:
                            LongPointData lidata = (LongPointData)(object)value;
                            hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, lidata.X);
                            hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, lidata.Y);
                            break;
                        case TagType.ULongPoint3:
                            ULongPoint3Data ludata3 = (ULongPoint3Data)(object)value;
                            hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, ludata3.X);
                            hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, ludata3.Y);
                            hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 24, ludata3.Z);
                            break;
                        case TagType.LongPoint3:
                            LongPoint3Data lidata3 = (LongPoint3Data)(object)value;
                            hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, lidata3.X);
                            hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, lidata3.Y);
                            hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 24, lidata3.Z);
                            break;
                    }
                    hb.WriteByte(hb.QualityAddress + hb.CurrentCount, quality);
                    hb.EndTime = datetime;
                    hb.CurrentCount++;
                    hb.Relase();
                    HisDataMemoryQueryService.Service.RegistorManual(id, hb.Time, hb.EndTime, hb);
                }
                else
                {
                    LoggerService.Service.Warn("HisEnginer", "his value count("+hb.CurrentCount+") > maxcount("+hb.MaxCount+")");
                }

                return true;
            }
            else
            {
                return false;
            }
        }

        private bool ManualRecordHisValues<T>(long id, DateTime datetime, byte quality, out bool isNeedSubmite,params T[]  value)
        {
            if (mIsClosed)
            {
                isNeedSubmite = false;
                return false;
            }
            isNeedSubmite = false;
            

            SortedDictionary<DateTime, ManualHisDataMemoryBlock> datacach;

            if (mHisTags.ContainsKey(id) && mHisTags[id].Type == RecordType.Driver)
            {
                var tag = mHisTags[id];
                DateTime utcnow = DateTime.UtcNow;
                if (datetime < tag.LastUpdateTime || datetime > utcnow)
                {
                    return false;
                }
                tag.LastUpdateTime = datetime;
                ManualHisDataMemoryBlock hb = null;

                lock (mManualHisDataCach)
                {
                    if (mManualHisDataCach.ContainsKey(id))
                    {
                        datacach = mManualHisDataCach[id];
                    }
                    else
                    {
                        datacach = new SortedDictionary<DateTime, ManualHisDataMemoryBlock>();
                        mManualHisDataCach.Add(id, datacach);
                    }
                }

                var vdata = datetime.Date;
                var mms = (int)(datetime.Subtract(vdata).TotalSeconds / MergeMemoryTime);
                var time = vdata.AddSeconds(mms * MergeMemoryTime);
                if (datacach.ContainsKey(time))
                {
                    hb = datacach[time];
                }
                else
                {
                    int valueOffset, qulityOffset = 0;
                    var css = CalCachDatablockSizeForManualRecord(tag.TagType, 0, MergeMemoryTime * tag.MaxValueCountPerSecond + 2, out valueOffset, out qulityOffset);
                    hb = ManualHisDataMemoryBlockPool.Pool.Get(css);
                    hb.Time = time;
                    hb.MaxCount = MergeMemoryTime * tag.MaxValueCountPerSecond + 2;
                    hb.TimeUnit = 1;
                    hb.TimeLen = 4;
                    hb.TimerAddress = 0;
                    hb.ValueAddress = valueOffset;
                    hb.QualityAddress = qulityOffset;
                    hb.Id = (int)id;
                    hb.CurrentCount = 0;

                    while (datacach.Count > 0)
                    {
                        isNeedSubmite = true;
                        var vdd = datacach.First();
                        ServiceLocator.Locator.Resolve<IDataCompress2>().RequestManualToCompress(vdd.Value);
                        datacach.Remove(vdd.Key);
                    }
                    datacach.Add(time, hb);

                    LoggerService.Service.Info("HisEnginer","new cal datablock");

                }

                if (hb.CurrentCount < hb.MaxCount && datetime > hb.EndTime)
                {
                    hb.Lock();
                    var vtime = (int)((datetime - hb.Time).TotalMilliseconds / 1);
                    //写入时间戳
                    hb.WriteInt(hb.TimerAddress + hb.CurrentCount * 4, vtime);
                    switch (tag.TagType)
                    {
                        
                        case TagType.UIntPoint:
                            hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt32(value[0]));
                            hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, Convert.ToUInt32(value[1]));
                            break;
                        case TagType.IntPoint:
                            hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt32(value[0]));
                            hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, Convert.ToInt32(value[1]));
                            break;
                        case TagType.UIntPoint3:
                            hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt32(value[0]));
                            hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, Convert.ToUInt32(value[1]));
                            hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, Convert.ToUInt32(value[2]));
                            break;
                        case TagType.IntPoint3:
                            hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt32(value[0]));
                            hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, Convert.ToInt32(value[1]));
                            hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, Convert.ToInt32(value[2]));
                            break;

                        case TagType.ULongPoint:
                           
                            hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt64(value[0]));
                            hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, Convert.ToUInt64(value[1]));
                            break;
                        case TagType.LongPoint:
                           
                            hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt64(value[0]));
                            hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, Convert.ToInt64(value[1]));
                            break;
                        case TagType.ULongPoint3:
                            hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt64(value[0]));
                            hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, Convert.ToUInt64(value[1]));
                            hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 24, Convert.ToUInt64(value[2]));
                            break;
                        case TagType.LongPoint3:
                            hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt64(value[0]));
                            hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, Convert.ToInt64(value[1]));
                            hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 24, Convert.ToInt64(value[2]));
                            break;
                    }
                    hb.WriteByte(hb.QualityAddress + hb.CurrentCount, quality);
                    hb.EndTime = datetime;
                    hb.CurrentCount++;
                    hb.Relase();
                    HisDataMemoryQueryService.Service.RegistorManual(id, hb.Time, hb.EndTime, hb);
                }

                return true;
            }
            else
            {
                return false;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeUnit"></param>
        /// <returns></returns>
        public bool SetTagHisValue(Dictionary<int,TagValue> values)
        {
            bool needsubmite = false,ntmp;
            foreach(var vv in values)
            {
                ManualRecordHisValues(vv.Key, vv.Value,out ntmp);
                needsubmite |= ntmp;
            }
            if (needsubmite)
                ServiceLocator.Locator.Resolve<IDataCompress2>().SubmitManualToCompress();
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="timeUnit"></param>
        /// <returns></returns>
        public bool SetTagHisValue(int id, TagValue value)
        {
            bool needsubmite = false;
            try
            {
                return ManualRecordHisValues(id, value, out needsubmite);
            }
            finally
            {
                if (needsubmite)
                    ServiceLocator.Locator.Resolve<IDataCompress2>().SubmitManualToCompress();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="timeUnit"></param>
        /// <returns></returns>
        public bool SetTagHisValue<T>(int id, DateTime time, T value, byte quality)
        {
            bool needsubmite = false;
            try
            {
                return ManualRecordHisValues(id, time, value, quality, out needsubmite);
            }
            finally
            {
                if (needsubmite)
                    ServiceLocator.Locator.Resolve<IDataCompress2>().SubmitManualToCompress();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool SetTagHisValue<T>(int id, DateTime time, byte quality,params T[] value)
        {
            bool needsubmite = false;
            try
            {
                return ManualRecordHisValues(id, time,  quality, out needsubmite,value);
            }
            finally
            {
                if (needsubmite)
                    ServiceLocator.Locator.Resolve<IDataCompress2>().SubmitManualToCompress();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="timeUnit"></param>
        /// <returns></returns>
        public bool SetTagHisValue<T>(int id, T value)
        {
            return SetTagHisValue(id, DateTime.UtcNow, value, 0);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool SetTagHisValue(int id, List<TagValue> values)
        {
           return ManualRecordHisValues(id, values);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool SetTagHisValues(Dictionary<int, List<TagValue>> values)
        {
            foreach(var vv in values)
            {
                ManualRecordHisValues(vv.Key, vv.Value);
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeUnit"></param>
        /// <returns></returns>
        public bool SetTagHisValues(Dictionary<int, TagValue> values)
        {
            bool needsubmite = false, ntmp;
            foreach (var vv in values)
            {
                ManualRecordHisValues(vv.Key, vv.Value,out ntmp);
                needsubmite |= ntmp;
            }
            if (needsubmite)
                ServiceLocator.Locator.Resolve<IDataCompress2>().SubmitManualToCompress();
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<int> GetManualRecordTagId()
        {
            return this.mHisTags.Where(e => e.Value.Type == RecordType.Driver).Select(e=>e.Value.Id).ToList();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public Dictionary<int,RecordType> GetTagRecordType(List<int> id)
        {
            Dictionary<int, RecordType> re = new Dictionary<int, RecordType>();
            foreach(var vv in id)
            {
                if(mHisTags.ContainsKey(vv))
                {
                    re.Add(vv, mHisTags[vv].Type);
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        IEnumerable<HisTag> IHisTagQuery.ListAllTags()
        {
            return this.mHisTags.Values;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<HisTag> ListAllDriverRecordTags()
        {
            return mHisTags.Values.Where(e => e.Type == RecordType.Driver);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public HisTag GetHisTagById(int id)
        {
            return mHisTags.ContainsKey(id) ? mHisTags[id] : null;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
