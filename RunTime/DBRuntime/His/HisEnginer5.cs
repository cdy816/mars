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
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Timers;
using System.Collections;

namespace Cdy.Tag
{
    /// <summary>
    /// 历史数据引擎5
    /// </summary>
    public class HisEnginer5 : IHisEngine3, ITagHisValueProduct, IDisposable, IHisTagQuery
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

        ///// <summary>
        ///// 
        ///// </summary>
        //private LogManager3 mLogManager;

        /// <summary>
        /// 缓存内存缓存时间,单位:s
        /// </summary>
        public int CachMemoryTime = 60*5;

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

        ///// <summary>
        ///// 历史记录内存1
        ///// </summary>
        //private HisDataMemoryBlockCollection3 mCachMemory1;

        ///// <summary>
        ///// 历史记录内存2
        ///// </summary>
        //private HisDataMemoryBlockCollection3 mCachMemory2;

        /// <summary>
        /// 
        /// </summary>
        private HisDataMemoryBlockCollection3 mMergeMemory1;

        /// <summary>
        /// 
        /// </summary>
        private HisDataMemoryBlockCollection3 mMergeMemory2;

        /// <summary>
        /// 当前正在使用的内存
        /// </summary>
        private HisDataMemoryBlockCollection3 mCurrentMemory;

        ///// <summary>
        ///// 
        ///// </summary>
        //private HisDataMemoryBlockCollection3 mCurrentMergeMemory;

        ///// <summary>
        ///// 
        ///// </summary>
        //private HisDataMemoryBlockCollection3 mWaitForMergeMemory;

        /// <summary>
        /// 值改变的变量列表
        /// </summary>
        private List<ValueChangedMemoryCacheProcesser3> mValueChangedProcesser = new List<ValueChangedMemoryCacheProcesser3>();

        /// <summary>
        /// 
        /// </summary>
        private List<TimerMemoryCacheProcesser3> mRecordTimerProcesser = new List<TimerMemoryCacheProcesser3>();

        /// <summary>
        /// 
        /// </summary>
        private TimerMemoryCacheProcesser3 mLastProcesser = new TimerMemoryCacheProcesser3() { Id=0};

        /// <summary>
        /// 
        /// </summary>
        private ValueChangedMemoryCacheProcesser3 mLastValueChangedProcesser = new ValueChangedMemoryCacheProcesser3() { Name = "ValueChanged0",Id=1 };

        private DateTime mLastProcessTime;

        private int mTagCount = 0;

        private int mLastProcessTick = -1;

        private ManualResetEvent resetEvent = new ManualResetEvent(false);

        private bool mIsClosed = false;

        private DateTime mSnapAllTagTime = DateTime.UtcNow;

        private Dictionary<long,SortedDictionary<DateTime, ManualHisDataMemoryBlock>> mManualHisDataCach = new Dictionary<long, SortedDictionary<DateTime, ManualHisDataMemoryBlock>>();

        private bool mIsPaused = false;


        public const int TagCountPerFile = 100000;

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, ManualHisDataMemoryBlockAreaCollection> mAreaBlockCachs = new Dictionary<string, ManualHisDataMemoryBlockAreaCollection>();


        private Dictionary<string, TagArea> mTagAreas = new Dictionary<string, TagArea>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public HisEnginer5()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="manager"></param>
        /// <param name="realEnginer"></param>
        public HisEnginer5(Cdy.Tag.HisDatabase manager, Cdy.Tag.RealEnginer realEnginer)
        {
            mManager = manager;
            mRealEnginer = realEnginer;
        }

        #endregion ...Constructor...

        #region ... Properties ...

    

        /// <summary>
        /// 当前工作的内存区域
        /// </summary>
        public HisDataMemoryBlockCollection3 CurrentMemory
        {
            get
            {
                return mCurrentMemory;
            }
            set
            {
                mCurrentMemory = value;
                HisRunTag.CurrentDataMemory = mCurrentMemory;
                SwitchMemoryCach(value.Id);
                
                //HisRunTag.HisAddr = mCurrentMemory;
            }
        }

        /// <summary>
        /// 当前正在使用的内存
        /// </summary>
        public HisDataMemoryBlockCollection3 CurrentMergeMemory
        {
            get
            {
                return mCurrentMemory;
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


        ///// <summary>
        ///// 
        ///// </summary>
        //public LogManager3 LogManager
        //{
        //    get
        //    {
        //        return mLogManager;
        //    }
        //    set
        //    {
        //        mLogManager = value;
        //        if(mLogManager!=null)
        //        {
        //            mLogManager.TimeLen = (ushort)(CachMemoryTime/60);
        //        }
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<long, HisRunTag> Tags
        {
            get
            {
                return mHisTags;
            }
        }

        /// <summary>
        /// 工作模式
        /// </summary>
        public HisWorkMode WorkMode { get; set; } = HisWorkMode.Passive;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="totalTagCount"></param>
        private void UpdatePerProcesserMaxTagCount(int totalTagCount)
        {
            //int count = Environment.ProcessorCount / 2;
            int count = Environment.ProcessorCount;
            int pcount = Math.Max(totalTagCount / count + 1,1000);
            TimerMemoryCacheProcesser3.MaxTagCount = ValueChangedMemoryCacheProcesser3.MaxTagCount = pcount;
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
               
                mLastProcesser = new TimerMemoryCacheProcesser3() { Id = 0 };
                mRecordTimerProcesser.Clear();
                mRecordTimerProcesser.Add(mLastProcesser);

                mValueChangedProcesser.Clear();
                mValueChangedProcesser.Add(mLastValueChangedProcesser);

                UpdatePerProcesserMaxTagCount(mManager.HisTags.Count);

                var count = CachMemoryTime;
                //var realbaseaddr = mRealEnginer.Memory;
                IntPtr realHandle = mRealEnginer.MemoryHandle;
                HisRunTag mHisTag = null;

                Tagbase mRealTag;

                int i = 0;

                foreach (var vv in mManager.HisTags)
                {
                    var realaddr = (int)mRealEnginer.GetDataAddr((int)vv.Value.Id);
                    mRealTag = mRealEnginer.GetTagById(vv.Value.Id);
                    
                    if (mRealTag == null||mRealTag is ComplexTag) continue;

                    switch (vv.Value.TagType)
                    {
                        case Cdy.Tag.TagType.Bool:
                        case Cdy.Tag.TagType.Byte:
                            mHisTag = new ByteHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr,CompressType = vv.Value.CompressType,Parameters = vv.Value.Parameters };
                            break;
                        case Cdy.Tag.TagType.Short:
                        case Cdy.Tag.TagType.UShort:
                            mHisTag = new ShortHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters };
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
                            mHisTag = new FloatHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters, Precision = (mRealTag as FloatingTagBase).Precision };
                            break;
                        case Cdy.Tag.TagType.Double:
                            mHisTag = new DoubleHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.Value.CompressType, Parameters = vv.Value.Parameters, Precision = (mRealTag as FloatingTagBase).Precision };
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
                    mHisTag.Area = mRealTag.Area;
                    mHisTags.Add(vv.Key, mHisTag);

                    if (mHisTag.Type == Cdy.Tag.RecordType.Timer)
                    {
                        if(!mLastProcesser.AddTag(mHisTag))
                        {
                            i++;
                            mLastProcesser = new TimerMemoryCacheProcesser3() { Id = i };
                            mLastProcesser.AddTag(mHisTag);
                            mRecordTimerProcesser.Add(mLastProcesser);
                        }
                    }
                    else if(mHisTag.Type == RecordType.ValueChanged)
                    {
                        //对于值改变，最快允许100毫秒一个值
                        mHisTag.MaxCount = count * 2 - 1;
                        if(!mLastValueChangedProcesser.AddTag(mHisTag))
                        {
                            i++;
                            mLastValueChangedProcesser = new ValueChangedMemoryCacheProcesser3() { Name = "ValueChanged"+ mValueChangedProcesser.Count+1,Id= i };
                            mLastValueChangedProcesser.AddTag(mHisTag);
                            mValueChangedProcesser.Add(mLastValueChangedProcesser);
                        }
                    }
                    mTagCount++;

                    if(!string.IsNullOrEmpty(mHisTag.Area))
                    {
                        TagArea ta = null;
                        string skey = ManualHisDataMemoryBlockArea.GetKey(mHisTag.Area, mHisTag.Id);
                        if (mTagAreas.ContainsKey(skey))
                        {
                            ta = mTagAreas[skey];
                        }
                        else
                        {
                            ta = new TagArea();
                            mTagAreas.Add(skey, ta);
                        }
                        ta.Tags.Add(mHisTag.Id);
                    }

                }

                if(mLastProcesser.Count==0 && mRecordTimerProcesser.Contains(mLastProcesser))
                {
                    mRecordTimerProcesser.Remove(mLastProcesser);
                }

                if(mLastValueChangedProcesser.Count==0 && mValueChangedProcesser.Contains(mLastValueChangedProcesser))
                {
                    mValueChangedProcesser.Remove(mLastValueChangedProcesser);
                }

            }
            long ltmp = sw.ElapsedMilliseconds;
            AllocMemory();

            //if (LogManager != null)
            //{
            //    LogManager.InitHeadData();
            //}

            mManager.Freedatabase();

            MarshalMemoryBlockPool.MaxSize = mHisTags.Count;

            sw.Stop();
            LoggerService.Service.Info("HisEnginer", "生成对象耗时:"+ltmp+" 内存分配耗时:"+(sw.ElapsedMilliseconds-ltmp));
        }

        private bool HasSelfScan()
        {
            return mRecordTimerProcesser.Count>0 || mValueChangedProcesser.Count > 0;
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

        private HisRunTag GetHisRunTag(HisTag vv)
        {
            HisRunTag mHisTag=null;
            IntPtr realHandle = mRealEnginer.MemoryHandle;
            var realaddr = (int)mRealEnginer.GetDataAddr((int)vv.Id);
            var mRealTag = mRealEnginer.GetTagById(vv.Id);
            switch (vv.TagType)
            {
                case Cdy.Tag.TagType.Bool:
                case Cdy.Tag.TagType.Byte:
                    mHisTag = new ByteHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                    break;
                case Cdy.Tag.TagType.Short:
                case Cdy.Tag.TagType.UShort:
                    mHisTag = new ShortHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                    break;
                case Cdy.Tag.TagType.Int:
                case Cdy.Tag.TagType.UInt:
                    mHisTag = new IntHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                    break;
                case Cdy.Tag.TagType.Long:
                case Cdy.Tag.TagType.ULong:
                    mHisTag = new LongHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                    break;
                case Cdy.Tag.TagType.Float:
                    mHisTag = new FloatHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters, Precision = (mRealTag as FloatingTagBase).Precision };
                    break;
                case Cdy.Tag.TagType.Double:
                    mHisTag = new DoubleHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters, Precision = (mRealTag as FloatingTagBase).Precision };
                    break;
                case Cdy.Tag.TagType.DateTime:
                    mHisTag = new DateTimeHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                    break;
                case Cdy.Tag.TagType.String:
                    mHisTag = new StirngHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                case Cdy.Tag.TagType.IntPoint:
                    mHisTag = new IntPointHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                case Cdy.Tag.TagType.IntPoint3:
                    mHisTag = new IntPoint3HisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                case Cdy.Tag.TagType.LongPoint:
                    mHisTag = new LongPointHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                case Cdy.Tag.TagType.LongPoint3:
                    mHisTag = new LongPoint3HisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType, RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters };
                    break;
            }
            mHisTag.Area = mRealTag.Area;
            return mHisTag;
        }

        /// <summary>
        /// 加载使能新的变量
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="mHisDatabase"></param>
        public void AddTags(IEnumerable<Tag.HisTag> tags)
        {
            UpdatePerProcesserMaxTagCount(this.mHisTags.Count+tags.Count());

            IntPtr realHandle = mRealEnginer.MemoryHandle;
            HisRunTag mHisTag = null;
           
            Tagbase mRealTag;

            var histags = new List<HisRunTag>();

            foreach (var vv in tags)
            {
                mRealTag = mRealEnginer.GetTagById(vv.Id);
                if (mRealTag == null) continue;
                var realaddr = (int)mRealEnginer.GetDataAddr((int)vv.Id);

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
                        mHisTag = new FloatHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters, Precision = (mRealTag as FloatingTagBase).Precision };
                        break;
                    case Cdy.Tag.TagType.Double:
                        mHisTag = new DoubleHisRunTag() { Id = vv.Id, Circle = vv.Circle, Type = vv.Type, TagType = vv.TagType,  RealMemoryPtr = realHandle, RealValueAddr = realaddr, CompressType = vv.CompressType, Parameters = vv.Parameters, Precision = (mRealTag as FloatingTagBase).Precision };
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

                mHisTag.MaxValueCountPerSecond = vv.MaxValueCountPerSecond;
                mHisTag.Area = mRealTag.Area;
                mHisTag.MaxCount = CachMemoryTime - 1;
                if(mHisTag.Type == RecordType.ValueChanged)
                {
                    mHisTag.MaxCount = CachMemoryTime * 2 - 1;
                }

                if (!string.IsNullOrEmpty(mHisTag.Area))
                {
                    TagArea ta = null;
                    string skey = ManualHisDataMemoryBlockArea.GetKey(mHisTag.Area, mHisTag.Id);
                    if (mTagAreas.ContainsKey(skey))
                    {
                        ta = mTagAreas[skey];
                    }
                    else
                    {
                        ta = new TagArea();
                        mTagAreas.Add(skey, ta);
                    }
                    ta.Tags.Add(mHisTag.Id);
                }

                this.mManager.AddOrUpdate(vv);
                 mTagCount++;
            }

            int qulityOffset = 0;
            int valueOffset = 0;
            int blockheadsize = 0;

            int isize = 0;
            int msize = 0;

            int i=0, c = 0;

            foreach (var vv in histags)
            {
                if (vv.Type != RecordType.Driver)
                {
                    var ss = CalMergeBlockSize(vv.Type,vv.TagType, blockheadsize, out valueOffset, out qulityOffset);

                    vv.DataMemoryPointer1 = mMergeMemory1.AddTagAddress(vv.Id, 0, valueOffset, qulityOffset, ss, 2);
                    vv.DataMemoryPointer2 = mMergeMemory2.AddTagAddress(vv.Id, 0, valueOffset, qulityOffset, ss, 2);
                    vv.DataSize = ss;
                    vv.TimerValueStartAddr = 0;
                    vv.HisValueStartAddr = valueOffset;
                    vv.HisQulityStartAddr = qulityOffset;
                    msize += ss;

                    //var css = CalCachDatablockSize(vv.Type,vv.TagType, blockheadsize, out valueOffset, out qulityOffset);

                    //vv.DataMemoryPointer1 = mCachMemory1.AddTagAddress(vv.Id, 0, valueOffset, qulityOffset, css, 2);
                    //vv.DataMemoryPointer2 = mCachMemory2.AddTagAddress(vv.Id, 0, valueOffset, qulityOffset, css, 2);
                    //vv.DataSize = css;
                    //vv.TimerValueStartAddr = 0;
                    //vv.HisValueStartAddr = valueOffset;
                    //vv.HisQulityStartAddr = qulityOffset;
                    //msize += css;
                    i++;
                }
                else
                {
                    var css = CalCachDatablockSizeForManualRecord(vv.TagType, 0, MergeMemoryTime * vv.MaxValueCountPerSecond + 2, out valueOffset, out qulityOffset);
                    ManualHisDataMemoryBlockPool.Pool.PreAlloc(css);
                    ManualHisDataMemoryBlockPool.Pool.PreAlloc(css);
                    ManualHisDataMemoryBlockPool.Pool.MaxCachCount += 2;
                    isize += css;
                    c++;
                }
            }

            mLastProcesser = mRecordTimerProcesser.Count > 0 ? mRecordTimerProcesser.Last() : null;
            mLastValueChangedProcesser = mValueChangedProcesser.Count > 0 ? mValueChangedProcesser.Last() : null;

            int count = 2;
            if(mRecordTimerProcesser!=null && mRecordTimerProcesser.Count>0)
            {
                count = Math.Max(count, mRecordTimerProcesser.Select(e => e.Id).Max());
            }

            if(mValueChangedProcesser != null && mValueChangedProcesser.Count>0)
            {
                count = Math.Max(count, mValueChangedProcesser.Select(e => e.Id).Max());
            }

            foreach (var vv in histags)
            {
                if (mCurrentMemory.Id == 1)
                {
                    vv.CurrentMemoryPointer = vv.DataMemoryPointer1;
                }
                else
                {
                    vv.CurrentMemoryPointer = vv.DataMemoryPointer2;
                }

                bool isadd = false;
                if (vv.Type == Cdy.Tag.RecordType.Timer)
                {
                    foreach(var vvp in mRecordTimerProcesser)
                    {
                        if(vvp.AddTag(vv))
                        {
                            isadd = true;
                            break;
                        }
                    }

                    if (!isadd)
                    {
                        count++;
                        mLastProcesser = new TimerMemoryCacheProcesser3() { Id = count };
                        mLastProcesser.AddTag(vv);
                        mRecordTimerProcesser.Add(mLastProcesser);
                    }
                }
                else if (vv.Type == RecordType.ValueChanged)
                {
                    foreach(var vvp in mValueChangedProcesser)
                    {
                        if (vvp.AddTag(vv))
                        {
                            isadd = true;
                            break;
                        }
                    }

                    if (!isadd)
                    {
                        count++;
                        mLastValueChangedProcesser = new ValueChangedMemoryCacheProcesser3() { Name = "ValueChanged" + mValueChangedProcesser.Count + 1,Id=count };
                        mLastValueChangedProcesser.AddTag(vv);
                        mValueChangedProcesser.Add(mLastValueChangedProcesser);
                    }
                }
            }

            //计算缓冲队列的大小
            int pcount = i / TimerMemoryCacheProcesser3.MaxTagCount;
            if (i % TimerMemoryCacheProcesser3.MaxTagCount > 0)
            {
                pcount++;
            }

            pcount = pcount == 0 ? 1 : pcount;
            ServiceLocator.Locator.Registor("CachMemorySize", Convert.ToInt64(ServiceLocator.Locator.Resolve("CachMemorySize")) + (long)Math.Floor((double)msize / pcount));

            pcount = c / TagCountPerFile;
            if (c % TagCountPerFile > 0)
            {
                pcount++;
            }
            pcount = pcount == 0 ? 1 : pcount;
           
            ServiceLocator.Locator.Registor("ManualTagCachMemorySize", Convert.ToInt64(ServiceLocator.Locator.Resolve("ManualTagCachMemorySize")) + (long)Math.Floor((double)isize / pcount));
        }

        /// <summary>
        /// 
        /// </summary>
        public void CheckStartProcess()
        {
            if (WorkMode == HisWorkMode.Initiative)
            {
                foreach (var vv in mRecordTimerProcesser) { if (!vv.IsStarted) vv.Start(); }

                foreach (var vv in mValueChangedProcesser) { if (!vv.IsStarted) vv.Start(); }
            }
        }

        /// <summary>
        /// 修改变量
        /// </summary>
        /// <param name="tags"></param>
        public void UpdateTags(IEnumerable<HisTag> tags)
        {
            foreach(var vv in tags)
            {
                UpdateTag(vv);
            }

            MarshalMemoryBlockPool.MaxSize = mHisTags.Count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        public void RemoveTags(IEnumerable<long> ids)
        {
            foreach(var vv in ids)
            {
                if(Tags.ContainsKey(vv))
                {
                    RemoveTag(Tags[vv]);
                }
            }
        }

        /// <summary>
        /// 删除变量
        /// </summary>
        /// <param name="tags"></param>
        public void RemoveTags(IEnumerable<HisTag> tags)
        {
            foreach(var vv in tags)
            {
                RemoveTag(vv);
            }
        }

        /// <summary>
        /// 删除变量
        /// </summary>
        /// <param name="tag"></param>
        private void RemoveTag(HisTag tag)
        {
            //var oldtag = this.mManager.GetHisTagById(tag.Id);
            try
            {
                var oldruntag = this.mHisTags.ContainsKey(tag.Id) ? this.mHisTags[tag.Id] : null;

                if (oldruntag.Type == RecordType.Timer)
                {
                    foreach (var vvt in mRecordTimerProcesser)
                    {
                        vvt.Remove(oldruntag);
                    }
                }
                else if (oldruntag.Type == RecordType.ValueChanged)
                {
                    foreach (var vvt in mValueChangedProcesser)
                    {
                        vvt.Remove(oldruntag);
                    }
                }
                else if (oldruntag.Type == RecordType.Driver)
                {
                    if (mManualHisDataCach.ContainsKey(oldruntag.Id))
                    {
                        var vvm = mManualHisDataCach[oldruntag.Id];
                        mManualHisDataCach.Remove(oldruntag.Id);
                        if (vvm != null)
                        {
                            foreach (var vvv in vvm)
                            {
                                ManualHisDataMemoryBlockPool.Pool.Release(vvv.Value);
                            }
                            vvm.Clear();
                        }
                    }
                }

                this.mHisTags.Remove(tag.Id);
                mManager.RemoveHisTag(tag.Id);

                //mCachMemory1.RemoveTagAdress(tag.Id);
                //mCachMemory2.RemoveTagAdress(tag.Id);
                mMergeMemory1.RemoveTagAdress(tag.Id);
                mMergeMemory2.RemoveTagAdress(tag.Id);

                if (!string.IsNullOrEmpty(oldruntag.Area))
                {
                    string skey = ManualHisDataMemoryBlockArea.GetKey(oldruntag.Area, tag.Id);
                    if (mTagAreas.ContainsKey(skey))
                    {
                        var ta = mTagAreas[skey];
                        ta.Tags.Remove(tag.Id);
                    }
                }

                oldruntag.Dispose();
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("HisEnginer3", $"RemoveTag {ex.Message} {ex.StackTrace}");
            }


        }

        /// <summary>
        /// 修改变量
        /// </summary>
        /// <param name="tag"></param>
        public void UpdateTag(Tag.HisTag tag)
        {
            //var oldtag = this.mManager.GetHisTagById(tag.Id);
            var oldruntag = this.mHisTags.ContainsKey(tag.Id) ? this.mHisTags[tag.Id] : null;

            if(oldruntag!=null &&!string.IsNullOrEmpty(oldruntag.Area))
            {
                string skey = ManualHisDataMemoryBlockArea.GetKey(oldruntag.Area, tag.Id);
                if (mTagAreas.ContainsKey(skey))
                {
                    mTagAreas[skey].Tags.Remove(tag.Id);
                }
            }

            var targettag = oldruntag;

            int isize = 0;
            int msize = 0;

            if(oldruntag != null)
            {
                if (!string.IsNullOrEmpty(oldruntag.Area))
                {
                    string skey = ManualHisDataMemoryBlockArea.GetKey(oldruntag.Area, tag.Id);
                    if (mTagAreas.ContainsKey(skey))
                    {
                        var ta = mTagAreas[skey];
                        ta.Tags.Remove(oldruntag.Id);
                    }
                }

                if (tag.TagType != oldruntag.TagType)
                {
                    //整个变量的类型都发生了改变
                    int qulityOffset = 0;
                    int valueOffset = 0;
                    int blockheadsize = 0;
                    HisRunTag vv = GetHisRunTag(tag);

                    var mtag = mRealEnginer.GetTagById(tag.Id);

                    vv.Area = mtag.Area;

                    if (oldruntag.DataMemoryPointer1 == 0)
                    {
                        //说明之前是驱动类型的记录
                        msize = CalSignleTagMemory(vv);
                    }
                    else if (tag.Type != RecordType.Driver)
                    {

                        if(tag.Type == RecordType.ValueChanged)
                        {
                            vv.MaxCount = CachMemoryTime * 2 - 1;
                        }

                        var ss = CalMergeBlockSize(tag.Type,tag.TagType, blockheadsize, out valueOffset, out qulityOffset);
                        vv.DataMemoryPointer1 = mMergeMemory1.ReAllocTagAddress(tag.Id, 0, valueOffset, qulityOffset, ss, 2);
                        vv.DataMemoryPointer2 = mMergeMemory2.ReAllocTagAddress(tag.Id, 0, valueOffset, qulityOffset, ss, 2);
                        vv.DataSize = ss;
                        msize = ss;

                        //var css = CalCachDatablockSize(tag.Type,tag.TagType, blockheadsize, out valueOffset, out qulityOffset);
                        //vv.DataMemoryPointer1 = mCachMemory1.ReAllocTagAddress(vv.Id, 0, valueOffset, qulityOffset, css, 2);
                        //vv.DataMemoryPointer2 = mCachMemory2.ReAllocTagAddress(vv.Id, 0, valueOffset, qulityOffset, css, 2);
                        //vv.DataSize = css;

                        //msize = css;

                        if (mCurrentMemory.Id == 1)
                        {
                            vv.CurrentMemoryPointer = vv.DataMemoryPointer1;
                        }
                        else
                        {
                            vv.CurrentMemoryPointer = vv.DataMemoryPointer2;
                        }
                    }
                    
                    targettag = vv;

                    if (mHisTags.ContainsKey(tag.Id))
                    {
                        mHisTags[tag.Id] = vv;
                    }
                    else
                    {
                        mHisTags.Add(tag.Id, vv);
                    }

                    if(oldruntag.Type == RecordType.Timer)
                    {
                        foreach(var vvt in mRecordTimerProcesser)
                        {
                            vvt.Remove(oldruntag);
                        }
                    }
                    else if(oldruntag.Type == RecordType.ValueChanged)
                    {
                        foreach(var vvt in mValueChangedProcesser)
                        {
                            vvt.Remove(oldruntag);
                        }
                    }
                    else if(oldruntag.Type == RecordType.Driver)
                    {
                        if(mManualHisDataCach.ContainsKey(oldruntag.Id))
                        {
                            var vvm = mManualHisDataCach[oldruntag.Id];
                            mManualHisDataCach.Remove(oldruntag.Id);
                            if(vvm!=null)
                            {
                                foreach(var vvv in vvm)
                                {
                                    ManualHisDataMemoryBlockPool.Pool.Release(vvv.Value);
                                }
                                vvm.Clear();
                            }
                        }
                    }    

                    if(tag.Type == RecordType.Timer)
                    {
                        foreach(var vvt in mRecordTimerProcesser)
                        {
                            if(vvt.AddTag(vv))
                            {
                                break;
                            }
                        }

                    }
                    else if(tag.Type == RecordType.ValueChanged)
                    {
                        foreach (var vvt in mValueChangedProcesser)
                        {
                            if (vvt.AddTag(vv))
                            {
                                break;
                            }
                        }
                    }
                    
                }
                else
                {
                   
                    if (tag.Type != oldruntag.Type)
                    {
                        
                        //释放旧的缓冲数据
                        if (oldruntag.Type == RecordType.Timer)
                        {
                            foreach (var vvt in mRecordTimerProcesser)
                            {
                                vvt.Remove(oldruntag);
                            }
                        }
                        else if(oldruntag.Type == RecordType.ValueChanged)
                        {
                            foreach (var vvt in mValueChangedProcesser)
                            {
                                vvt.Remove(oldruntag);
                            }
                        }
                        else if(oldruntag.Type == RecordType.Driver)
                        {
                            //释放驱动记录的使用的内存
                            if (mManualHisDataCach.ContainsKey(oldruntag.Id))
                            {
                                var vvm = mManualHisDataCach[oldruntag.Id];
                                mManualHisDataCach.Remove(oldruntag.Id);
                                if (vvm != null)
                                {
                                    foreach (var vvv in vvm)
                                    {
                                        ManualHisDataMemoryBlockPool.Pool.Release(vvv.Value);
                                    }
                                    vvm.Clear();
                                }

                            }
                        }

                        if (tag.Type == RecordType.ValueChanged)
                        {
                            oldruntag.MaxCount = CachMemoryTime * 2 - 1;
                        }

                        var mtag = mRealEnginer.GetTagById(tag.Id);

                        oldruntag.Type = tag.Type;
                        oldruntag.Circle = tag.Circle;
                        oldruntag.Area = mtag.Area;

                        //分配新的内存
                        if (tag.Type == RecordType.Timer)
                        {
                            if (oldruntag.DataMemoryPointer2 == 0)
                            {
                                msize = CalSignleTagMemory(oldruntag);
                            }

                            foreach (var vvt in mRecordTimerProcesser)
                            {
                                if (vvt.AddTag(oldruntag))
                                {
                                    break;
                                }
                            }
                            
                        }
                        else if (tag.Type == RecordType.ValueChanged)
                        {
                            if (oldruntag.DataMemoryPointer2 == 0)
                            {
                                msize = CalSignleTagMemory(oldruntag);
                            }

                            foreach (var vvt in mValueChangedProcesser)
                            {
                                if (vvt.AddTag(oldruntag))
                                {
                                    break;
                                }
                            }
                        }
                        else
                        {
                            mMergeMemory1.RemoveTagAdress(oldruntag.Id);
                            mMergeMemory2.RemoveTagAdress(oldruntag.Id);
                            //mCachMemory1.RemoveTagAdress(oldruntag.Id);
                            //mCachMemory2.RemoveTagAdress(oldruntag.Id);
                            oldruntag.DataMemoryPointer1 = oldruntag.DataMemoryPointer2 = 0;
                            oldruntag.MaxValueCountPerSecond = tag.MaxValueCountPerSecond;

                        }
                    }
                    else if(tag.Circle != oldruntag.Circle)
                    {
                        //定时记录时，修改定时周期
                        var oldcircle = oldruntag.Circle;
                        oldruntag.Circle = tag.Circle;
                        if (oldruntag.Type == RecordType.Timer)
                        {
                            foreach(var vvt in mRecordTimerProcesser)
                            {
                                vvt.UpdateCircle(oldruntag, oldcircle);
                            }
                        }
                      
                    }
                    else if(tag.MaxValueCountPerSecond!= oldruntag.MaxValueCountPerSecond)
                    {
                        //更新驱动记录时内存最大缓冲
                        if(tag.MaxValueCountPerSecond> oldruntag.MaxValueCountPerSecond && mManualHisDataCach.ContainsKey(tag.Id) && mManualHisDataCach[tag.Id].Count>0)
                        {
                            var hb = mManualHisDataCach[tag.Id].Last().Value;
                            
                            var css = CalCachDatablockSizeForManualRecord(tag.TagType, 0, MergeMemoryTime * tag.MaxValueCountPerSecond + 2, out int valueOffset, out int qulityOffset);

                            hb.CheckAndResize(css);

                            isize = css;

                        }
                    }
                }

                this.mManager.AddOrUpdate(tag);

                if (targettag != null)
                {
                    targettag.CompressType = tag.CompressType;
                    targettag.Parameters = tag.Parameters;
                }

                var mRealTag = mRealEnginer.GetTagById(tag.Id);

                if (mRealTag!=null && !string.IsNullOrEmpty(mRealTag.Area))
                {
                    string skey = ManualHisDataMemoryBlockArea.GetKey(mRealTag.Area, tag.Id);
                    TagArea ta = null;
                    if (mTagAreas.ContainsKey(skey))
                    {
                        ta = mTagAreas[skey];
                    }
                    else
                    {
                        ta = new TagArea();
                        mTagAreas.Add(skey, ta);
                    }
                    ta.Tags.Add(tag.Id);
                }

                if (msize>0)
                ServiceLocator.Locator.Registor("CachMemorySize", Convert.ToInt64(ServiceLocator.Locator.Resolve("CachMemorySize")) + msize);
                if(isize>0)
                ServiceLocator.Locator.Registor("ManualTagCachMemorySize", Convert.ToInt64(ServiceLocator.Locator.Resolve("ManualTagCachMemorySize")) + isize);
            }


            if (targettag!=null && !string.IsNullOrEmpty(targettag.Area))
            {
                string skey = ManualHisDataMemoryBlockArea.GetKey(targettag.Area, tag.Id);
                if (mTagAreas.ContainsKey(skey))
                {
                    var ta = mTagAreas[skey];
                    if(!ta.Tags.Contains(targettag.Id))
                    ta.Tags.Add(targettag.Id);
                }
                else
                {
                    var ta = new TagArea();
                    mTagAreas.Add(skey, ta);
                    ta.Tags.Add(targettag.Id);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="vv"></param>
        private int CalSignleTagMemory(HisRunTag vv)
        {
            int blockheadsize = 0;

            var ss = CalMergeBlockSize(vv.Type,vv.TagType, blockheadsize, out int valueOffset, out int qulityOffset);

            vv.DataMemoryPointer1 = mMergeMemory1.AddTagAddress(vv.Id, 0, valueOffset, qulityOffset, ss, 2);
            vv.DataMemoryPointer2 = mMergeMemory2.AddTagAddress(vv.Id, 0, valueOffset, qulityOffset, ss, 2);
            vv.DataSize = ss;
            //var css = CalCachDatablockSize(vv.Type,vv.TagType, blockheadsize, out valueOffset, out qulityOffset);

            //vv.DataMemoryPointer1 = mCachMemory1.AddTagAddress(vv.Id, 0, valueOffset, qulityOffset, css, 2);
            //vv.DataMemoryPointer2 = mCachMemory2.AddTagAddress(vv.Id, 0, valueOffset, qulityOffset, css, 2);
            //vv.DataSize = css;

            vv.TimerValueStartAddr = 0;
            vv.HisValueStartAddr = valueOffset;
            vv.HisQulityStartAddr = qulityOffset;

            if (mCurrentMemory.Id == 1)
            {
                vv.CurrentMemoryPointer = vv.DataMemoryPointer1;
            }
            else
            {
                vv.CurrentMemoryPointer = vv.DataMemoryPointer2;
            }
            return ss;
        }

        /// <summary>
        /// 计算每个变量数据块的大小
        /// </summary>
        /// <param name="rtype"记录类型></param>
        /// <param name="tagType"></param>
        /// <param name="blockHeadSize"></param>
        /// <param name="dataOffset"></param>
        /// <param name="qulityOffset"></param>
        /// <returns></returns>
        private int CalMergeBlockSize(RecordType rtype, Cdy.Tag.TagType tagType,int blockHeadSize,out int dataOffset,out int qulityOffset)
        {

            //单个数据块内容包括：时间戳(2)+数值+质量戳(1)

            qulityOffset = 0;
            int regionHeadSize = blockHeadSize;
            // int count = MemoryCachTime * 1000 / MemoryTimeTick;
            int count = MergeMemoryTime;

            if(rtype == RecordType.ValueChanged)
            {
                //对于值改变,时间最小单位是100ms
                count = count * 2;
            }

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
        /// <param name="rtype"></param>
        /// <param name="tagType"></param>
        /// <param name="headSize"></param>
        /// <param name="dataOffset"></param>
        /// <param name="qulityOffset"></param>
        /// <returns></returns>
        private int CalCachDatablockSize(RecordType rtype,Cdy.Tag.TagType tagType, int headSize, out int dataOffset, out int qulityOffset)
        {
            //单个数据块内容包括：时间戳(2)+数值+质量戳(1)

            qulityOffset = headSize;
            int count = CachMemoryTime;

            if(rtype == RecordType.ValueChanged)
            {
                //值改变的时间最小单位是100ms
                count = CachMemoryTime * 2;
            }

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


            mMergeMemory1 = new HisDataMemoryBlockCollection3(mHisTags.Count) { Name = "StoreMemory1", Id = 1 };

            mMergeMemory2 = new HisDataMemoryBlockCollection3(mHisTags.Count) { Name = "StoreMemory2", Id = 2 };

            //mCachMemory1 = new HisDataMemoryBlockCollection3(mHisTags.Count) { Name = "CachMemory1", Id = 1 };

            //mCachMemory2 = new HisDataMemoryBlockCollection3(mHisTags.Count) { Name = "CachMemory2", Id = 2 };

            long isize = 0;
            long msize = 0;

            int i = 0;
            int c = 0;
            foreach (var vv in mHisTags)
            {

                if (vv.Value.Type != RecordType.Driver)
                {
                    var ss = CalMergeBlockSize(vv.Value.Type,vv.Value.TagType, blockheadsize, out valueOffset, out qulityOffset);
                    vv.Value.DataMemoryPointer1 = mMergeMemory1.AddTagAddress(vv.Value.Id, 0, valueOffset, qulityOffset, ss, 2);
                    vv.Value.DataMemoryPointer2 = mMergeMemory2.AddTagAddress(vv.Value.Id, 0, valueOffset, qulityOffset, ss, 2);
                    vv.Value.DataSize = ss;
                    vv.Value.TimerValueStartAddr = 0;
                    vv.Value.HisValueStartAddr = valueOffset;
                    vv.Value.HisQulityStartAddr = qulityOffset;
                    storeHeadSize += ss;
                    msize += ss*2;
                    i++;
                }
                else
                {
                    //在内存池中，预先分配一些内存，以提高运行时分配内存的速度。
                    var css = CalCachDatablockSizeForManualRecord(vv.Value.TagType, 0, MergeMemoryTime * vv.Value.MaxValueCountPerSecond + 2, out valueOffset, out qulityOffset);
                    ManualHisDataMemoryBlockPool.Pool.PreAlloc(css);
                    ManualHisDataMemoryBlockPool.Pool.PreAlloc(css);
                    ManualHisDataMemoryBlockPool.Pool.MaxCachCount += 2;
                    isize += css*2;
                    cachHeadSize += css;
                    c++;
                }
            }

            //计算缓冲队列的大小
            int pcount = i / TimerMemoryCacheProcesser3.MaxTagCount;
            if(i% TimerMemoryCacheProcesser3.MaxTagCount>0)
            {
                pcount++;
            }

            pcount = pcount == 0 ? 1 : pcount;

            ServiceLocator.Locator.Registor("CachMemorySize", (long)Math.Floor((double)msize/pcount*1.2));

            pcount = c / TagCountPerFile;
            if (c % TagCountPerFile > 0)
            {
                pcount++;
            }
            pcount = pcount == 0 ? 1 : pcount;

            ServiceLocator.Locator.Registor("ManualTagCachMemorySize", (long)Math.Floor((double)isize/pcount*1.2));

            LoggerService.Service.Info("HisEnginer", "分配缓存大小:" + (storeHeadSize/1024.0/1024 *2).ToString("f1") + " M");

            CurrentMemory = mMergeMemory1;
            

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datetime"></param>
        /// <returns></returns>
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
                    vv.Value.CurrentMemoryPointer = vv.Value.DataMemoryPointer1;
                    vv.Value.Reset();
                }
            }
            else
            {
                foreach (var vv in mHisTags)
                {
                    vv.Value.CurrentMemoryPointer = vv.Value.DataMemoryPointer2;
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

            LogStorageManager.Instance.Start();
            mIsClosed = false;
            //mIsMergeClosed = false;
            //mNeedRecordCloseValue = false;
            //mMegerProcessIsClosed = false;
            mScanClosed = false;
            LoggerService.Service.Info("HisEnginer", "历史变量个数: " + this.mHisTags.Count);

            //ClearMemoryHisData(mCachMemory1);
            //ClearMemoryHisData(mCachMemory2);
            ClearMemoryHisData(mMergeMemory1);
            ClearMemoryHisData(mMergeMemory2);

            //mCachMemory1.MakeMemoryNoBusy();
            //mCachMemory2.MakeMemoryNoBusy();
            mMergeMemory1.MakeMemoryNoBusy();
            mMergeMemory2.MakeMemoryNoBusy();
            
            if (WorkMode == HisWorkMode.Initiative)
            {
                foreach (var vv in mRecordTimerProcesser)
                {
                    vv.Start();
                }

                foreach (var vv in mValueChangedProcesser)
                {
                    vv.Start();
                }
            }

            mLastProcessTime = DateTime.UtcNow;
            HisRunTag.StartTime = mLastProcessTime;
            CurrentMemory = mMergeMemory1;
            CurrentMemory.CurrentDatetime = mLastProcessTime;
            CurrentMemory.BaseTime = HisRunTag.StartTime;

            var vtt = new DateTime(mLastProcessTime.Year, mLastProcessTime.Month, mLastProcessTime.Day, mLastProcessTime.Hour, mLastProcessTime.Minute, 0).AddMinutes(1);

            HisDataMemoryQueryService3.Service.RegistorMemory(CurrentMemory.CurrentDatetime, vtt, CurrentMemory);

            //mCurrentMergeMemory = mMergeMemory1;
            mMergeMemory2.CurrentDatetime =  CurrentMemory.CurrentDatetime;
            mMergeMemory2.BaseTime = CurrentMemory.BaseTime;



            if (WorkMode == HisWorkMode.Initiative && HasSelfScan())
            {
                SnapeAllTag();
                RecordAllFirstValue();
            }

            if (WorkMode == HisWorkMode.Initiative && HasSelfScan())
            {
                mScanThread = new Thread(ScanProcess);
                mScanThread.Priority = ThreadPriority.Highest;
                mScanThread.Start();
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        private void CheckMemoryIsReady(HisDataMemoryBlockCollection3 memory)
        {
            while (memory.IsBusy())
            {
                LoggerService.Service.Info("HisEnginer",  "记录出现阻塞 " + memory.Name);
                System.Threading.Thread.Sleep(1);
            }
        }

        private void RequestToCompress(HisDataMemoryBlockCollection3 memory)
        {
            var dur = MergeMemoryTime / 60;
            var mSnapAllTagTime = memory.CurrentDatetime.Date.AddHours(memory.CurrentDatetime.Hour).AddMinutes((int)(memory.CurrentDatetime.Minute / dur) * (dur) + dur);
            memory.EndDateTime = mSnapAllTagTime;
            memory.MakeMemoryBusy();
            //提交到数据压缩流程
            ServiceLocator.Locator.Resolve<IDataCompress3>().RequestToCompress(memory);
            LoggerService.Service.Info("HisEnginer", "提交内存 " + memory.Name + " 进行压缩", ConsoleColor.Green);
        }

        ///// <summary>
        ///// 提交内存数据到合并
        ///// </summary>
        //private void SubmiteMemory2(DateTime dateTime)
        //{
        //    Stopwatch sw = new Stopwatch();
        //    sw.Start();
        //    int number = MergeMemoryTime / CachMemoryTime;

        //    var basetime = HisRunTag.StartTime;
        //    var mcc = mCurrentMemory;

        //    mMergeCount++;

        //    mMergeCount = mMergeCount >= number ? 0 : mMergeCount;

        //    //HisRunTag.TimerOffset = mMergeCount * 10 * CachMemoryTime;

        //    if (mCurrentMemory == mCachMemory1)
        //    {
        //        CheckMemoryIsReady(mCachMemory2);
        //        CurrentMemory = mCachMemory2;
        //    }
        //    else
        //    {
        //        CheckMemoryIsReady(mCachMemory1);
        //        CurrentMemory = mCachMemory1;
        //    }

        //    CurrentMemory.CurrentDatetime = dateTime;

        //    ////使能日志
        //    //for(int i = 0; i < mTagCount/per; i++)
        //    //LogStorageManager.Instance.IncSystemRef(dateTime);

        //    //long ltmp = sw.ElapsedMilliseconds;

        //    var vtt = new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, 0).AddMinutes(1);

        //    HisDataMemoryQueryService3.Service.RegistorMemory(CurrentMemory.CurrentDatetime, vtt, CurrentMemory);
        //    // HisDataMemoryQueryService3.Service.RegistorMemory(CurrentMemory.CurrentDatetime,dateTime.AddSeconds(CachMemoryTime), CurrentMemory);
        //    //long ltmp21 = sw.ElapsedMilliseconds;
        //    if (mMergeCount==0)
        //    {
        //        mNeedSnapAllTag = true;
        //        LoggerService.Service.Debug("HisEnginer", "使用新的时间起点:" + CurrentMemory.Name + "  " + FormateDatetime(CurrentMemory.CurrentDatetime));
        //        HisRunTag.StartTime = dateTime;
        //    }

        //    CurrentMemory.BaseTime = HisRunTag.StartTime;

        //    //long ltmp22 = sw.ElapsedMilliseconds;
        //    ////PrepareForReadyMemory();
        //    //foreach (var vv in mHisTags)
        //    //{
        //    //    vv.Value.Reset();
        //    //}
        //    //long ltmp2 = sw.ElapsedMilliseconds;
        //    if (mcc != null)
        //    {
        //        mcc.MakeMemoryBusy();
        //        mcc.EndDateTime = dateTime;

        //        mWaitForMergeMemory = mcc;
        //        //通知进行内存合并
        //        resetEvent.Set();

        //        //mLogManager?.RequestToSave(mcc.CurrentDatetime,dateTime, basetime, mcc);

        //    }
        //    //long ltmp3 = sw.ElapsedMilliseconds;
        //    sw.Stop();

        //    LoggerService.Service.Info("HisEnginer", "SubmiteMemory 耗时:" + sw.ElapsedMilliseconds);

        //}

        private void SubmiteMemory(DateTime dateTime)
        {
            LoggerService.Service.Info("HisEnginer", "RecordTimer 准备新的内存，提交内存 " + CurrentMemory.Name + " 到压缩 线程ID:" + Thread.CurrentThread.ManagedThreadId + " CPU ID:" + ThreadHelper.GetCurrentProcessorNumber()+"  时间："+dateTime.ToLongTimeString(), ConsoleColor.Green);

            Stopwatch sw = new Stopwatch();
            sw.Start();

            ////生成值快照
            //SnapeAllTag();
            //最数据段尾部添加数据
            CheckRecordAllValue();
            //提交压缩
            RequestToCompress(mCurrentMemory);

            //切换当前工作内存
            if (mCurrentMemory == mMergeMemory1)
            {
                CheckMemoryIsReady(mMergeMemory2);
                CurrentMemory = mMergeMemory2;
            }
            else
            {
                CheckMemoryIsReady(mMergeMemory1);
                CurrentMemory = mMergeMemory1;
            }

            CurrentMemory.BaseTime = HisRunTag.StartTime = CurrentMemory.CurrentDatetime = dateTime;
            ////在数据段开始位置添加数据
            //RecordAllFirstValue();

            //注册内存历史查询
            var vtt = new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, 0).AddMinutes(5);
            HisDataMemoryQueryService3.Service.RegistorMemory(CurrentMemory.CurrentDatetime, vtt, CurrentMemory);

            sw.Stop();

            LoggerService.Service.Info("HisEnginer", "SubmiteMemory 耗时:" + sw.ElapsedMilliseconds);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dateTime"></param>
        private void SubmiteMemoryAndWait(DateTime dateTime)
        {
            //生成值快照
            SnapeAllTag();
            //最数据段尾部添加数据
            RecordCloseValue();
            //提交压缩
            RequestToCompress(mCurrentMemory);
        }

        /// <summary>
        /// 
        /// </summary>
        private void SnapeAllTag()
        {
           var  mSnapAllTagTime = DateTime.UtcNow;
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Parallel.ForEach(mHisTags, (vv) => {
                vv.Value.Snape(mSnapAllTagTime);
            });

            //foreach(var vv in mHisTags)
            //{
            //    //if(vv.Value.Type != RecordType.Driver)
            //    vv.Value.Snape(mSnapAllTagTime);
            //}
            sw.Stop();
            LoggerService.Service.Info("HisEnginer", "快照记录数值:" + FormateDatetime(mSnapAllTagTime.ToLocalTime()) + " 耗时:" + sw.ElapsedMilliseconds, ConsoleColor.Cyan);
        }

        /// <summary>
        /// 在数据区域头部添加数值
        /// </summary>
        /// <param name="dt"></param>
        private void RecordAllFirstValue()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            int i = 0;
            Parallel.ForEach(mCurrentMemory.TagAddress, (vv) =>
            //foreach (var vv in mCurrentMemory.TagAddress)
            {
                //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
                //实时数据内存结构为:实时值+时间戳+质量戳
                var baseaddress = mCurrentMemory.ReadDataBaseAddressByIndex(vv.Value);
                if (mHisTags.ContainsKey(vv.Key))
                {
                    var tag = mHisTags[vv.Key];

                    if (tag.LastQuality == (byte)QualityConst.Null)
                    {
                        //vv.Value.WriteShort(0, 0);
                        mCurrentMemory.WriteShortDirect(baseaddress, 0, 0);

                        //写入数值
                        //vv.Value.WriteBytesDirect((int)vv.Value.ValueAddress,tag.ValueSnape);

                        mCurrentMemory.WriteBytesDirect(baseaddress, mCurrentMemory.ReadValueOffsetAddressByIndex(vv.Value), tag.ValueSnape);

                        //更新质量戳,在现有质量戳的基础添加100，用于表示这是一个强制更新的值
                        //vv.Value.WriteByteDirect((int)vv.Value.QualityAddress, (byte)(tag.QulitySnape+100));
                        mCurrentMemory.WriteByteDirect(baseaddress, mCurrentMemory.ReadQualityOffsetAddressByIndex(vv.Value), (byte)(QualityConst.Start));
                    }
                    else
                    {
                        //vv.Value.WriteShort(0, 0);
                        if (tag.SnapeTime < mCurrentMemory.CurrentDatetime)
                        {
                            mCurrentMemory.WriteShortDirect(baseaddress, 0, 0);
                        }
                        else
                        {
                            var timespan = (tag.SnapeTime - mCurrentMemory.CurrentDatetime).TotalMilliseconds;
                            mCurrentMemory.WriteShortDirect(baseaddress, 0, (short)(timespan / 100));
                        }

                        //写入数值
                        //vv.Value.WriteBytesDirect((int)vv.Value.ValueAddress,tag.ValueSnape);

                        mCurrentMemory.WriteBytesDirect(baseaddress, mCurrentMemory.ReadValueOffsetAddressByIndex(vv.Value), tag.ValueSnape);

                        //更新质量戳,在现有质量戳的基础添加100，用于表示这是一个强制更新的值
                        //vv.Value.WriteByteDirect((int)vv.Value.QualityAddress, (byte)(tag.QulitySnape+100));
                        mCurrentMemory.WriteByteDirect(baseaddress, mCurrentMemory.ReadQualityOffsetAddressByIndex(vv.Value), (byte)(QualityConst.Start));
                    }
                }
                i++;
            });

            sw.Stop();
            LoggerService.Service.Info("HisEnginer", "RecordAllFirstValue  耗时:" + sw.ElapsedMilliseconds, ConsoleColor.Cyan);

        }

        ///// <summary>
        ///// 在数据区域尾部添加数值
        ///// </summary>
        ///// <param name="dt"></param>
        //private void RecordAllLastValue()
        //{
        //    //DateTime time = mSnapAllTagTime;
        //    DateTime time = DateTime.Now;
        //    Stopwatch sw = new Stopwatch();
        //    sw.Start();

        //    //LoggerService.Service.Info("HisEnginer", "RecordAllLastValue:" + FormateDatetime(time));
        //    ushort timespan = (ushort)((time - mCurrentMemory.CurrentDatetime).TotalMilliseconds / 100);

        //    Parallel.ForEach(mCurrentMemory.TagAddress, (vv) =>
        //    {

        //        //foreach (var vv in mCurrentMemory.TagAddress)
        //        {
        //        //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
        //        //实时数据内存结构为:实时值+时间戳+质量戳


        //        var baseaddress = mCurrentMemory.ReadDataBaseAddressByIndex(vv.Value);
        //        //if (mHisTags.ContainsKey(vv.Key))
        //        {
        //            mHisTags.TryGetValue(vv.Key, out HisRunTag tag);
        //                //var tag = mHisTags[vv.Key];

        //                if (tag != null && tag.Count <= 0)
        //                //if (tag != null)
        //                {

        //                //如果单位时间内没有记录值，则需要记录值
        //                tag.Snape(time);

        //                long timeraddr = mCurrentMemory.ReadValueOffsetAddressByIndex(vv.Value) - 2;
        //                long valueaddr = mCurrentMemory.ReadQualityOffsetAddressByIndex(vv.Value) - tag.SizeOfValue;
        //                long qaddr = mCurrentMemory.ReadDataSizeByIndex(vv.Value) - 1;

        //                //
        //                mCurrentMemory.WriteUShortDirect(baseaddress, (int)timeraddr, timespan);

        //                //写入数值
        //                mCurrentMemory.WriteBytesDirect(baseaddress, (int)valueaddr, tag.ValueSnape);

        //                //更新质量戳
        //                mCurrentMemory.WriteByteDirect(baseaddress, (int)qaddr, (byte)(tag.QulitySnape + 100));
        //            }
        //        }
        //    }
        //    });

        //    sw.Stop();
        //    LoggerService.Service.Info("HisEnginer", "RecordAllLastValue  耗时:" + sw.ElapsedMilliseconds, ConsoleColor.Cyan);
        //}

        private void RecordCloseValue()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            ushort timespan = 0;
            foreach (var vv in mCurrentMemory.TagAddress)
            {
                //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
                //实时数据内存结构为:实时值+时间戳+质量戳

                //if (vv.Value == null) continue;

                var baseaddress = mCurrentMemory.ReadDataBaseAddressByIndex(vv.Value);
                if (mHisTags.ContainsKey(vv.Key))
                {
                    var tag = mHisTags[vv.Key];

                    var vcount = tag.Count < tag.MaxCount ? tag.Count  : tag.MaxCount;

                    long timeraddr = vcount * 2;
                    long valueaddr = mCurrentMemory.ReadValueOffsetAddressByIndex(vv.Value) + tag.SizeOfValue*vcount;
                    long qaddr = mCurrentMemory.ReadQualityOffsetAddressByIndex(vv.Value) +vcount;

                    //

                    timespan = (ushort)((tag.SnapeTime - mCurrentMemory.CurrentDatetime).TotalMilliseconds / 100);

                    mCurrentMemory.WriteUShortDirect(baseaddress, (int)timeraddr, timespan);

                    //写入数值
                    mCurrentMemory.WriteBytesDirect(baseaddress, (int)valueaddr, tag.ValueSnape);

                    //更新质量戳
                    mCurrentMemory.WriteByteDirect(baseaddress, (int)qaddr, (byte)(QualityConst.Close));
                }
            }
             sw.Stop();
            LoggerService.Service.Info("HisEnginer", "RecordAllLastValue  耗时:" + sw.ElapsedMilliseconds+$" stop time {mCurrentMemory.CurrentDatetime} time tick: {timespan} ", ConsoleColor.Cyan);
        }

        /// <summary>
        /// 如果一个区域内没有数据，则记录一个数据,将上次记录的值延续记录下来
        /// </summary>
        /// <param name="dt"></param>
        private void CheckRecordAllValue()
        {
            //DateTime time = mSnapAllTagTime;
            DateTime time = DateTime.Now;
            Stopwatch sw = new Stopwatch();
            sw.Start();

            //LoggerService.Service.Info("HisEnginer", "RecordAllLastValue:" + FormateDatetime(time));
            ushort timespan = (ushort)((time - mCurrentMemory.CurrentDatetime).TotalMilliseconds / 100);

            Parallel.ForEach(mCurrentMemory.TagAddress, (vv) =>
            {

                //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
                //实时数据内存结构为:实时值+时间戳+质量戳
                var baseaddress = mCurrentMemory.ReadDataBaseAddressByIndex(vv.Value);
                {
                    mHisTags.TryGetValue(vv.Key, out HisRunTag tag);

                    if (tag != null && tag.Count <= 0)
                    {
                        byte qu = mCurrentMemory.ReadByte(baseaddress, mCurrentMemory.ReadQualityOffsetAddressByIndex(vv.Value));
                        if (qu == 0)
                        {
                            if (tag.LastQuality == (byte)QualityConst.Null)
                            {
                                //如果始终没有记录，则取系统运行之后第一次记录的值，同时质量戳取Start
                                //写入时间戳

                                var dur = (mCurrentMemory.CurrentDatetime-tag.SnapeTime).TotalMinutes;
                                ushort min = (ushort)(dur / 5);
                                ushort sec = (ushort)((dur % 5)*60 * 10);
                                //进行时间换算，转换成2个ushort ,第一个是5分钟的倍数，第二个是余数，换算成100ms的倍数

                                mCurrentMemory.WriteUShortDirect(baseaddress, 0, min);
                                mCurrentMemory.WriteUShortDirect(baseaddress, 2, sec);

                                //写入数值
                                mCurrentMemory.WriteBytesDirect(baseaddress, mCurrentMemory.ReadValueOffsetAddressByIndex(vv.Value), tag.ValueSnape);

                                //更新质量戳,在现有质量戳的基础添加100，用于表示这是一个强制更新的值
                                mCurrentMemory.WriteByteDirect(baseaddress, mCurrentMemory.ReadQualityOffsetAddressByIndex(vv.Value), (byte)(QualityConst.Start + 100));
                            }
                            else
                            {
                                //将最近一次记录的值写入,
                                var dur = (mCurrentMemory.CurrentDatetime-tag.LastTime).TotalMinutes;
                                ushort min = (ushort)(dur / 5);
                                ushort sec = (ushort)((dur % 5) * 60 * 10);
                                //进行时间换算，转换成2个ushort ,第一个是5分钟的倍数，第二个是余数，换算成100ms的倍数

                                mCurrentMemory.WriteUShortDirect(baseaddress, 0, min);
                                mCurrentMemory.WriteUShortDirect(baseaddress, 2, sec);

                                ////写入时间戳
                                //mCurrentMemory.WriteLongDirect(baseaddress, 0, tag.LastTime.Ticks);

                                //写入数值
                                mCurrentMemory.WriteBytesDirect(baseaddress, mCurrentMemory.ReadValueOffsetAddressByIndex(vv.Value), tag.LastValue);

                                //更新质量戳,在现有质量戳的基础添加100，用于表示这是一个强制更新的值
                                mCurrentMemory.WriteByteDirect(baseaddress, mCurrentMemory.ReadQualityOffsetAddressByIndex(vv.Value), (byte)(tag.LastQuality + 100));
                            }
                        }
                    }
                }
            });

            sw.Stop();
            LoggerService.Service.Info("HisEnginer", "RecordAllLastValue  耗时:" + sw.ElapsedMilliseconds, ConsoleColor.Cyan);
        }

        /// <summary>
        /// 
        /// </summary>
        private Stopwatch sw = new Stopwatch();

        private bool mScanClosed = false;

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
                //mBlockCount = 0;
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
                   
                    if (mLastProcessTick != -1 && (mm % 5 == 0))
                    {
                        mLastProcessTime = dt.AddSeconds(-dt.Second).AddMilliseconds(-dt.Millisecond);

                        //将之前的Memory进行压缩，切换工作内存
                        SubmiteMemory(mLastProcessTime);
                    }
                    mLastProcessTick = mm;
                }

                dt = DateTime.UtcNow;

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
            mScanClosed = true;
            LoggerService.Service.Info("HisEnginer", "扫描线程退出!", ConsoleColor.Yellow);
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
        private void SubmitLastDataToSave(bool needSaveCloseValue)
        {
            mIsClosed = true;

            if (WorkMode == HisWorkMode.Initiative && mScanThread!=null && mScanThread.IsAlive)
            {
                while (!mScanClosed)
                    Thread.Sleep(1);
                SubmiteMemoryAndWait(DateTime.UtcNow);
                resetEvent.Set();
            }
            SaveManualCachData(needSaveCloseValue);
        }

        /// <summary>
        /// 保存手动提交历史记录缓存部分
        /// </summary>
        private void SaveManualCachData(bool needSaveCloseValue)
        {
            if(needSaveCloseValue)
            {
                LoggerService.Service.Info("HisEnginer", "写入停止标识位!");
            }
            foreach(var vv in mManualHisDataCach)
            {
                if(vv.Value.Count==0 && needSaveCloseValue)
                {
                    LoggerService.Service.Info("HisEnginer", $"写入退出标识失败{vv.Key}", ConsoleColor.Yellow);
                }

                foreach (var vvv in vv.Value)
                {
                    if (needSaveCloseValue)
                    {
                        //添加一个退出质量戳，表示系统退出
                        AppendCloseValue(vvv.Value, (int)vv.Key, vvv.Value.LastValue, vvv.Value.EndTime, (byte)QualityConst.Close);
                    }
                    ServiceLocator.Locator.Resolve<IDataCompress3>().RequestManualToCompress(vvv.Value);
                }
                ServiceLocator.Locator.Resolve<IDataCompress3>().SubmitManualToCompress();
            }

            foreach(var vv in mManualHisDataCach)
            {
                vv.Value.Clear();
            }

            foreach(var vv in mAreaBlockCachs)
            {
                foreach(var vvv in vv.Value)
                {
                    foreach(var vvb in vvv.Value.ListMemorys())
                    {
                        if (needSaveCloseValue)
                        {
                            //添加一个退出质量戳，表示系统退出
                            AppendCloseValue(vvb, (int)vvb.Id, vvb.LastValue, vvb.EndTime, (byte)QualityConst.Close);
                        }
                    }
                    vvv.Value.SubmiteToCompress();
                }
                vv.Value.Clear();
            }
            mAreaBlockCachs.Clear();
        }


        /// <summary>
        /// 
        /// </summary>
        public void Stop(bool needSaveCloseValue)
        {
            LoggerService.Service.Info("HisEnginer", "开始停止");

            if (WorkMode == HisWorkMode.Initiative)
            {
                foreach (var vv in mRecordTimerProcesser)
                {
                    vv.Stop();
                }

                foreach (var vv in mValueChangedProcesser)
                {
                    vv.Stop();
                }
            }
            
            SubmitLastDataToSave(needSaveCloseValue);
            
            mLastProcessTick = -1;
            CacheTagLastUpdateTime();
            LogStorageManager.Instance.Stop();
            LogStorageManager.Instance.Clear();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public void ClearMemoryHisData(HisDataMemoryBlockCollection3 memory)
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

            //mCachMemory1?.Dispose();
            //mCachMemory2?.Dispose();
            mMergeMemory1?.Dispose();
            mMergeMemory2?.Dispose();
        }

        private void InsertManualRecord(HisRunTag tag, DateTime stime,DateTime etime, object value,DateTime time,byte quality)
        {
            DateTime sstim = stime.AddSeconds(MergeMemoryTime);
            //InsertManualRecord(tag, sstim, value, time, quality);
            while (sstim < etime)
            {
                InsertManualRecord(tag,sstim, value, time, quality);
                sstim = sstim.AddSeconds(MergeMemoryTime);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="time"></param>
        /// <param name="value"></param>
        /// <param name="valuetime"></param>
        /// <param name="quality"></param>
        private void InsertManualRecord(HisRunTag tag, DateTime time,object value,DateTime valuetime,byte quality)
        {
            ManualHisDataMemoryBlock hb = null;

            var css = CalCachDatablockSizeForManualRecord(tag.TagType, 0, 4, out int valueOffset, out int qulityOffset);
            hb = ManualHisDataMemoryBlockPool.Pool.Get(css);
            hb.Time = time;
            hb.RealTime = time;
            hb.MaxCount = 4;
            hb.TimeUnit = 1;
            hb.TimeLen = 4;
            hb.TimerAddress = 0;
            hb.ValueAddress = valueOffset;
            hb.QualityAddress = qulityOffset;
            hb.Id = (int)tag.Id;

            hb.LastValue = value;
            hb.LastQuality = quality;
            hb.EndTime = valuetime;

            if (value is object[])
            {
                AppendPointHisValue(hb, tag, valuetime, quality, value as object[]);
            }
            else if (value != null)
            {
                AppendHisValue(hb, tag, value, valuetime, quality);
            }

            if(mManualHisDataCach.ContainsKey(tag.Id))
            {
                mManualHisDataCach[tag.Id].Add(hb.Time, hb);
            }
            else
            {
                var dd = new SortedDictionary<DateTime, ManualHisDataMemoryBlock>();
                dd.Add(hb.Time, hb);
                mManualHisDataCach.Add(tag.Id, dd);
            }
        }

        private Dictionary<long,bool> mManualRecordBusyCache= new Dictionary<long,bool>();

        private void LockManualTag(long id)
        {
            lock (mManualRecordBusyCache)
            {
                if (mManualRecordBusyCache.ContainsKey(id))
                {
                    while (mManualRecordBusyCache[id]) Thread.Sleep(1);

                    mManualRecordBusyCache[id] = true;
                }
                else
                {
                    mManualRecordBusyCache.Add(id, true);
                }
            }
        }

        private void UnLockManualTag(long id)
        {
            lock (mManualRecordBusyCache)
            {
                if (mManualRecordBusyCache.ContainsKey(id))
                {
                    mManualRecordBusyCache[id] = false;
                }
            }
        }

        /// <summary>
        /// 手动插入历史数据,
        /// 数据会累计到整个数据块大小时再提交到后面进行压缩、存储处理
        /// </summary>
        /// <param name="id"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        private bool ManualRecordHisValues(long id, IEnumerable<Cdy.Tag.TagValue> values,out bool isNeedSubmite)
        {
            if (mIsClosed)
            {
                isNeedSubmite = false;
                return false;
            }

            LockManualTag(id);

            int valueOffset, qulityOffset = 0;

            DateTime mLastTime = DateTime.MinValue;

            SortedDictionary<DateTime, ManualHisDataMemoryBlock> datacach;
            try
            {
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

                    int rid = (int)(id / TagCountPerFile);

                    foreach (var vv in values)
                    {
                        //if (WorkMode == HisWorkMode.Initiative)
                        //{
                        //    //主动工作模式不允许时间超出当前时间
                        //    if (vv.Time < tag.LastUpdateTime || vv.Time > utcnow)
                        //    {
                        //        continue;
                        //    }
                        //}
                        //else
                        //{
                        if (vv.Time <= tag.LastUpdateTime)
                        {
                            continue;
                        }
                        //}
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
                            if (hb != null)
                            {

                                //if ((time - hb.Time).TotalMinutes > 5)
                                //{
                                //    InsertManualRecord(tag, hb.Time, time, hb.LastValue, hb.EndTime, hb.LastQuality < 100 ? (byte)(hb.LastQuality + 100) : hb.LastQuality);
                                //}

                                HisDataMemoryQueryService3.Service.RegistorManual(id, hb.Time, hb.EndTime, hb);
                            }
                            LogStorageManager.Instance.IncManualRef(time, rid);

                            var css = CalCachDatablockSizeForManualRecord(tag.TagType, 0, MergeMemoryTime * tag.MaxValueCountPerSecond + 4, out valueOffset, out qulityOffset);
                            hb = ManualHisDataMemoryBlockPool.Pool.Get(css);
                            hb.Time = time;
                            hb.RealTime = vv.Time;

                            hb.MaxCount = MergeMemoryTime * tag.MaxValueCountPerSecond + 2;
                            hb.TimeUnit = 1;
                            hb.TimeLen = 4;
                            hb.TimerAddress = 0;
                            hb.ValueAddress = valueOffset;
                            hb.QualityAddress = qulityOffset;
                            hb.Id = (int)id;

                            HisDataMemoryQueryService3.Service.RegistorManual(id, hb.Time, time, hb);
                            datacach.Add(time, hb);

                        }
                        mLastTime = time;

                        var log = LogStorageManager.Instance.GetManualLog(time, rid);

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
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, vv.Value);
                                    break;
                                case TagType.Byte:
                                    hb.WriteByteDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToByte(vv.Value));
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, vv.Value);
                                    break;
                                case TagType.Short:
                                    hb.WriteShortDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt16(vv.Value));
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, vv.Value);
                                    break;
                                case TagType.UShort:
                                    hb.WriteUShortDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt16(vv.Value));
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, vv.Value);
                                    break;
                                case TagType.Int:
                                    hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt32(vv.Value));
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, vv.Value);
                                    break;
                                case TagType.UInt:
                                    hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt32(vv.Value));
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, vv.Value);
                                    break;
                                case TagType.Long:
                                    hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt64(vv.Value));
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, vv.Value);
                                    break;
                                case TagType.ULong:
                                    hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt64(vv.Value));
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, vv.Value);
                                    break;
                                case TagType.Float:
                                    hb.WriteFloatDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, (float)Math.Round(Convert.ToSingle(vv.Value), tag.Precision));
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, vv.Value);
                                    break;
                                case TagType.Double:
                                    hb.WriteDoubleDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Math.Round(Convert.ToDouble(vv.Value), tag.Precision));
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, vv.Value);
                                    break;
                                case TagType.String:
                                    hb.WriteStringDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToString(vv.Value), Encoding.Unicode);
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, vv.Value);
                                    break;
                                case TagType.DateTime:
                                    hb.WriteDatetime(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToDateTime(vv.Value));
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, vv.Value);
                                    break;
                                case TagType.UIntPoint:
                                    UIntPointData data = (UIntPointData)vv.Value;
                                    hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, data.X);
                                    hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, data.Y);
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, data.X, data.Y);
                                    break;
                                case TagType.IntPoint:
                                    IntPointData idata = (IntPointData)vv.Value;
                                    hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, idata.X);
                                    hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, idata.Y);
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, idata.X, idata.Y);
                                    break;
                                case TagType.UIntPoint3:
                                    UIntPoint3Data udata3 = (UIntPoint3Data)vv.Value;
                                    hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, udata3.X);
                                    hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, udata3.Y);
                                    hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, udata3.Z);
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, udata3.X, udata3.Y, udata3.Z);
                                    break;
                                case TagType.IntPoint3:
                                    IntPoint3Data idata3 = (IntPoint3Data)vv.Value;
                                    hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, idata3.X);
                                    hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, idata3.Y);
                                    hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, idata3.Z);
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, idata3.X, idata3.Y, idata3.Z);
                                    break;

                                case TagType.ULongPoint:
                                    ULongPointData udata = (ULongPointData)vv.Value;
                                    hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, udata.X);
                                    hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, udata.Y);
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, udata.X, udata.Y);
                                    break;
                                case TagType.LongPoint:
                                    LongPointData lidata = (LongPointData)vv.Value;
                                    hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, lidata.X);
                                    hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, lidata.Y);
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, lidata.X, lidata.Y);
                                    break;
                                case TagType.ULongPoint3:
                                    ULongPoint3Data ludata3 = (ULongPoint3Data)vv.Value;
                                    hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, ludata3.X);
                                    hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, ludata3.Y);
                                    hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, ludata3.Z);
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, ludata3.X, ludata3.Y, ludata3.Z);
                                    break;
                                case TagType.LongPoint3:
                                    LongPoint3Data lidata3 = (LongPoint3Data)vv.Value;
                                    hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, lidata3.X);
                                    hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, lidata3.Y);
                                    hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, lidata3.Z);
                                    log?.Append(tag.TagType, tag.Id, vv.Time, vv.Quality, lidata3.X, lidata3.Y, lidata3.Z);
                                    break;
                            }
                            hb.WriteByte(hb.QualityAddress + hb.CurrentCount, vv.Quality);

                            hb.EndTime = vv.Time;
                            hb.LastValue = vv.Value;
                            hb.LastQuality = vv.Quality;

                            hb.CurrentCount++;
                            hb.Relase();
                        }
                    }
                    if (hb != null)
                        HisDataMemoryQueryService3.Service.RegistorManual(id, hb.Time, hb.EndTime, hb);

                    isNeedSubmite = false;

                    if (values.Count() > 0)
                    {
                        foreach (var vv in datacach.ToArray())
                        {
                            if (vv.Key < mLastTime)
                            {
                                ServiceLocator.Locator.Resolve<IDataCompress3>().RequestManualToCompress(vv.Value);
                                datacach.Remove(vv.Key);
                                //LogStorageManager.Instance.DecManualRef(vv.Key, rid);
                                isNeedSubmite = true;
                            }
                        }
                        //if (isNeedSubmite && !mIsSuspend)
                        //    ServiceLocator.Locator.Resolve<IDataCompress3>().SubmitManualToCompress();
                    }

                    return true;
                }
                else
                {
                    isNeedSubmite = false;
                    return false;
                }
            }
            finally
            {
                UnLockManualTag(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="hb"></param>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        private void AppendCloseValue(ManualHisDataMemoryBlock hb,int id,object value,DateTime time,byte quality)
        {
            var tag = mHisTags[id];
            if(hb.LastValue != null && hb.LastValue is object[])
            {
                AppendPointHisValue(hb,tag,time,quality,hb.LastValue as object[]);
            }
            else if(hb.LastValue != null)
            {
                AppendHisValue(hb, tag, value,time, quality);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="hb"></param>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        private void AppendHisValue(ManualHisDataMemoryBlock hb,HisRunTag tag,object value,DateTime time,byte quality)
        {
            if (hb.CurrentCount < hb.MaxCount+2)
            {
                hb.Lock();
                var vtime = (int)((time - hb.Time).TotalMilliseconds / 1);
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
                        hb.WriteFloatDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, (float)Math.Round(Convert.ToSingle(value), tag.Precision));
                        break;
                    case TagType.Double:
                        hb.WriteDoubleDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Math.Round(Convert.ToDouble(value), tag.Precision));
                        break;
                    case TagType.String:
                        hb.WriteStringDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToString(value), Encoding.Unicode);
                        break;
                    case TagType.DateTime:
                        hb.WriteDatetime(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToDateTime(value));
                        break;
                    case TagType.UIntPoint:
                        UIntPointData data = (UIntPointData)value;
                        hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, data.X);
                        hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, data.Y);
                        break;
                    case TagType.IntPoint:
                        IntPointData idata = (IntPointData)value;
                        hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, idata.X);
                        hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, idata.Y);
                        break;
                    case TagType.UIntPoint3:
                        UIntPoint3Data udata3 = (UIntPoint3Data)value;
                        hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, udata3.X);
                        hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, udata3.Y);
                        hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, udata3.Z);
                        break;
                    case TagType.IntPoint3:
                        IntPoint3Data idata3 = (IntPoint3Data)value;
                        hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, idata3.X);
                        hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, idata3.Y);
                        hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, idata3.Z);
                        break;

                    case TagType.ULongPoint:
                        ULongPointData udata = (ULongPointData)value;
                        hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, udata.X);
                        hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, udata.Y);
                        break;
                    case TagType.LongPoint:
                        LongPointData lidata = (LongPointData)value;
                        hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, lidata.X);
                        hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, lidata.Y);
                        break;
                    case TagType.ULongPoint3:
                        ULongPoint3Data ludata3 = (ULongPoint3Data)value;
                        hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, ludata3.X);
                        hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, ludata3.Y);
                        hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, ludata3.Z);
                        break;
                    case TagType.LongPoint3:
                        LongPoint3Data lidata3 = (LongPoint3Data)value;
                        hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, lidata3.X);
                        hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, lidata3.Y);
                        hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, lidata3.Z);
                        break;
                }
                hb.WriteByte(hb.QualityAddress + hb.CurrentCount, quality);
                //hb.EndTime = time;
                hb.CurrentCount++;
                hb.Relase();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="hb"></param>
        /// <param name="tag"></param>
        /// <param name="datetime"></param>
        /// <param name="quality"></param>
        /// <param name="value"></param>
        private void AppendPointHisValue(ManualHisDataMemoryBlock hb, HisRunTag tag,DateTime datetime, byte quality,params object[]  value)
        {
            if (hb.CurrentCount < hb.MaxCount+2)
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
                        hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, Convert.ToUInt64(value[2]));
                        break;
                    case TagType.LongPoint3:
                        hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt64(value[0]));
                        hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, Convert.ToInt64(value[1]));
                        hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, Convert.ToInt64(value[2]));
                        break;
                }
                hb.WriteByte(hb.QualityAddress + hb.CurrentCount, quality);
                hb.EndTime = datetime;
                hb.CurrentCount++;
                hb.Relase();
                HisDataMemoryQueryService3.Service.RegistorManual(tag.Id, hb.Time, hb.EndTime, hb);
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
        /// 将历史变量的最后更新时间缓存到磁盘上
        /// </summary>
        public void CacheTagLastUpdateTime()
        {
            try
            {
                string sfile = PathHelper.helper.GetDataPath(this.mManager.Name, "histime.ch");
                using (var vv = System.IO.File.Open(sfile, System.IO.FileMode.Create))
                {
                    foreach(var vtag in mHisTags)
                    {
                        vv.Write(BitConverter.GetBytes(vtag.Key));
                        vv.Write(BitConverter.GetBytes(vtag.Value.LastUpdateTime.Ticks));
                    }
                    vv.Flush();
                }
            }
            catch(Exception ex)
            {
                LoggerService.Service.Warn("HisEnginer", $"CacheTagLastUpdateTime {ex.Message} {ex.StackTrace}");
            }
        }

        /// <summary>
        /// 加载变量最后记录时间
        /// </summary>
        /// <param name="isloadFromHis"></param>
        public void LoadTagLastUpdateTime(bool isloadFromHis)
        {
            if(isloadFromHis)
            {
                LoadLastUpdateTimeFromHisData();
                LoadTagLastUpdateTime();
            }
            else
            {
                if (!LoadTagLastUpdateTime())
                {
                    LoadLastUpdateTimeFromHisData();
                }
            }
        }

        /// <summary>
        /// 从历史文件中，读取变量最后更新时间
        /// </summary>
        private void LoadLastUpdateTimeFromHisData()
        {
            var vis = ServiceLocator.Locator.Resolve<IHisQuery>();
            foreach(var vv in mHisTags)
            {
                if(vv.Value.LastUpdateTime == DateTime.MinValue && vv.Value.Type == RecordType.Driver)
                {
                    try
                    {
                        switch (vv.Value.TagType)
                        {
                            case Cdy.Tag.TagType.Bool:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<bool>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.Byte:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<byte>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.DateTime:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<DateTime>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.Double:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<double>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.Float:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<float>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.Int:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<int>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.Long:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<long>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.Short:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<short>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.String:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<string>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.UInt:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<uint>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.ULong:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<ulong>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.UShort:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<ushort>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.IntPoint:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<IntPointData>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.UIntPoint:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<UIntPointData>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.IntPoint3:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<IntPoint3Data>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.UIntPoint3:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<UIntPoint3Data>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.LongPoint:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<LongPointData>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.ULongPoint:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<ULongPointData>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.LongPoint3:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<LongPoint3Data>(vv.Value.Id);
                                break;
                            case Cdy.Tag.TagType.ULongPoint3:
                                vv.Value.LastUpdateTime = vis.ReadTagLastAvaiableValueTime<ULongPoint3Data>(vv.Value.Id);
                                break;
                        }
                    }
                    catch(Exception ex)
                    {
                        LoggerService.Service.Warn("HisEnginer", $"LoadLastUpdateTimeFromHisData {ex.Message} {ex.StackTrace}");
                    }
                }
            }
        }

        /// <summary>
        /// 加载变量的最后更新时间
        /// </summary>
        public bool LoadTagLastUpdateTime()
        {
            try
            {
                string sfile = PathHelper.helper.GetDataPath(this.mManager.Name, "histime.ch");
                if(System.IO.File.Exists(sfile))
                {
                    using (var vv = System.IO.File.Open(sfile, System.IO.FileMode.Open,System.IO.FileAccess.ReadWrite,System.IO.FileShare.ReadWrite))
                    {
                        byte[] bb = new byte[8];
                        while(vv.Position<vv.Length)
                        {
                            vv.Read(bb, 0, bb.Length);
                            var vid = BitConverter.ToInt64(bb);
                            vv.Read(bb, 0, bb.Length);
                            var vtime = DateTime.FromBinary(BitConverter.ToInt64(bb));
                            if(mHisTags.ContainsKey(vid))
                            {
                                if(vtime> mHisTags[vid].LastUpdateTime)
                                mHisTags[vid].LastUpdateTime = vtime;
                            }
                        }
                    }
                    return true;
                }
            }
            catch (Exception ex)
            {
                LoggerService.Service.Warn("HisEnginer", $"CacheTagLastUpdateTime {ex.Message} {ex.StackTrace}");
            }
            return false;
        }

        /// <summary>
        /// 手动记录简单数据类型的值
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

            //Stopwatch sw = Stopwatch.StartNew();
            //sw.Start();

            //long ltmp1 = sw.ElapsedMilliseconds;
            //long ltmp2 = 0, ltmp3 = 0, ltmp4 = 0;

            try
            {
               

                LockManualTag(id);

                isNeedSubmite = false;
                int valueOffset, qulityOffset = 0;

                int rid = (int)(id / TagCountPerFile);

                SortedDictionary<DateTime, ManualHisDataMemoryBlock> datacach;

                if (mHisTags.ContainsKey(id) && mHisTags[id].Type == RecordType.Driver)
                {
                    var tag = mHisTags[id];
                    DateTime utcnow = DateTime.UtcNow;

                    if (datetime <= tag.LastUpdateTime)
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

                    //ltmp2 = sw.ElapsedMilliseconds;

                    var vdata = datetime.Date;
                    var mms = (int)(datetime.Subtract(vdata).TotalSeconds / MergeMemoryTime);
                    var time = vdata.AddSeconds(mms * MergeMemoryTime);
                    if (datacach.ContainsKey(time))
                    {
                        hb = datacach[time];
                    }
                    else
                    {
                        if (datacach.Count > 0)
                        {
                            var prevb = datacach.Last().Value;

                            //if((time - prevb.Time ).TotalMinutes>5)
                            //{
                            //    InsertManualRecord(tag, prevb.Time, time, prevb.LastValue, prevb.EndTime, prevb.LastQuality < 100 ? (byte)(prevb.LastQuality + 100) : prevb.LastQuality);
                            //}

                            HisDataMemoryQueryService3.Service.RegistorManual(id, prevb.Time, prevb.EndTime, prevb);
                        }


                        LogStorageManager.Instance.IncManualRef(time, rid);

                        var css = CalCachDatablockSizeForManualRecord(tag.TagType, 0, MergeMemoryTime * tag.MaxValueCountPerSecond + 4, out valueOffset, out qulityOffset);
                        hb = ManualHisDataMemoryBlockPool.Pool.Get(css);
                        hb.Time = time;
                        hb.RealTime = datetime;
                        hb.MaxCount = MergeMemoryTime * tag.MaxValueCountPerSecond + 2;
                        hb.TimeUnit = 1;
                        hb.TimeLen = 4;
                        hb.TimerAddress = 0;
                        hb.ValueAddress = valueOffset;
                        hb.QualityAddress = qulityOffset;
                        hb.Id = (int)id;
                        hb.CurrentCount = 0;

                        //if (prevb != null)
                        //{
                        //    //将上一个区域的最后一个值，写入到下一个区域的第一个位置上

                        //    //hb.PreQuality = prevb.LastQuality;
                        //    //hb.PreTime = prevb.EndTime;
                        //    //hb.PreValue = prevb.LastValue;

                        //    //AppendHisValue(hb, tag, prevb.LastValue, prevb.EndTime, (byte)(prevb.LastQuality + 100));
                        //}

                        //提交存储
                        lock (mManualHisDataCach)
                        {
                            while (datacach.Count > 0)
                            {
                                isNeedSubmite = true;
                                var vdd = datacach.First();
                                ServiceLocator.Locator.Resolve<IDataCompress3>().RequestManualToCompress(vdd.Value);

                                //LogStorageManager.Instance.DecManualRef(vdd.Key, rid);

                                datacach.Remove(vdd.Key);
                            }
                            datacach.Add(time, hb);
                        }
                        //LoggerService.Service.Info("HisEnginer", "new cal datablock");
                    }

                    //ltmp3 = sw.ElapsedMilliseconds;

                    var log = LogStorageManager.Instance.GetManualLog(time, rid);

                    //ltmp4= sw.ElapsedMilliseconds;

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
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.Byte:
                                hb.WriteByteDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToByte(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.Short:
                                hb.WriteShortDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt16(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.UShort:
                                hb.WriteUShortDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt16(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.Int:
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt32(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.UInt:
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt32(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.Long:
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt64(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.ULong:
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt64(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.Float:
                                hb.WriteFloatDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, (float)Math.Round(Convert.ToSingle(value), tag.Precision));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.Double:
                                hb.WriteDoubleDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Math.Round(Convert.ToDouble(value), tag.Precision));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.String:
                                hb.WriteStringDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToString(value), Encoding.Unicode);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.DateTime:
                                hb.WriteDatetime(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToDateTime(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.UIntPoint:
                                UIntPointData data = (UIntPointData)(object)value;
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, data.X);
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, data.Y);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, data.X, data.Y);
                                break;
                            case TagType.IntPoint:
                                IntPointData idata = (IntPointData)(object)value;
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, idata.X);
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, idata.Y);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, idata.X, idata.Y);
                                break;
                            case TagType.UIntPoint3:
                                UIntPoint3Data udata3 = (UIntPoint3Data)(object)value;
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, udata3.X);
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, udata3.Y);
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, udata3.Z);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, udata3.X, udata3.Y, udata3.Z);
                                break;
                            case TagType.IntPoint3:
                                IntPoint3Data idata3 = (IntPoint3Data)(object)value;
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, idata3.X);
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, idata3.Y);
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, idata3.Z);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, idata3.X, idata3.Y, idata3.Z);
                                break;

                            case TagType.ULongPoint:
                                ULongPointData udata = (ULongPointData)(object)value;
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, udata.X);
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, udata.Y);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, udata.X, udata.Y);
                                break;
                            case TagType.LongPoint:
                                LongPointData lidata = (LongPointData)(object)value;
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, lidata.X);
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, lidata.Y);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, lidata.X, lidata.Y);
                                break;
                            case TagType.ULongPoint3:
                                ULongPoint3Data ludata3 = (ULongPoint3Data)(object)value;
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, ludata3.X);
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, ludata3.Y);
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, ludata3.Z);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, ludata3.X, ludata3.Y, ludata3.Z);
                                break;
                            case TagType.LongPoint3:
                                LongPoint3Data lidata3 = (LongPoint3Data)(object)value;
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, lidata3.X);
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, lidata3.Y);
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, lidata3.Z);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, lidata3.X, lidata3.Y, lidata3.Z);
                                break;
                        }
                        hb.WriteByte(hb.QualityAddress + hb.CurrentCount, quality);
                        hb.EndTime = datetime;
                        hb.CurrentCount++;
                        hb.LastQuality = quality;
                        hb.LastValue = value;



                        hb.Relase();
                        HisDataMemoryQueryService3.Service.RegistorManual(id, hb.Time, hb.EndTime, hb);
                        //LoggerService.Service.Info("ManualRecordHisValues", $" {hb.Time} {vtime}");
                    }
                    else
                    {
                        LoggerService.Service.Warn("HisEnginer", "his value count(" + hb.CurrentCount + ") > maxcount(" + hb.MaxCount + ") or" + datetime + "<=" + hb.EndTime);
                    }
                    return true;
                }
                else
                {
                  
                    return false;
                }
            }
            finally
            {
                UnLockManualTag(id);
                //sw.Stop();
                //if(sw.ElapsedMilliseconds>200)
                //{
                //    LoggerService.Service.Warn("HisEnginer", $"系统被阻塞{ltmp1}  {ltmp2}  {ltmp3}  {ltmp4} {sw.ElapsedMilliseconds}");
                //}
            
            }
        }

        /// <summary>
        /// 手动记录复杂类型的值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="datetime"></param>
        /// <param name="quality"></param>
        /// <param name="isNeedSubmite"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        private bool ManualRecordHisValues<T>(long id, DateTime datetime, byte quality, out bool isNeedSubmite,params T[]  value)
        {
            if (mIsClosed)
            {
                isNeedSubmite = false;
                return false;
            }
            isNeedSubmite = false;
            int rid = (int)(id / TagCountPerFile);
            
            LockManualTag(id);
            try
            {
                SortedDictionary<DateTime, ManualHisDataMemoryBlock> datacach;

                if (mHisTags.ContainsKey(id) && mHisTags[id].Type == RecordType.Driver)
                {
                    var tag = mHisTags[id];
                    DateTime utcnow = DateTime.UtcNow;
                    if (WorkMode == HisWorkMode.Initiative)
                    {
                        if (datetime < tag.LastUpdateTime || datetime > utcnow)
                        {
                            return false;
                        }
                    }
                    else
                    {
                        if (datetime < tag.LastUpdateTime)
                        {
                            return false;
                        }
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
                        if (datacach.Count > 0)
                        {
                            var prevb = datacach.Last().Value;

                            //if ((time - prevb.Time).TotalMinutes > 5)
                            //{
                            //    InsertManualRecord(tag, prevb.Time, time, prevb.LastValue, prevb.EndTime, prevb.LastQuality<100? (byte)(prevb.LastQuality + 100): prevb.LastQuality);
                            //}

                            HisDataMemoryQueryService3.Service.RegistorManual(id, prevb.Time, prevb.EndTime, prevb);
                        }

                        LogStorageManager.Instance.IncManualRef(time, rid);

                        int valueOffset, qulityOffset = 0;
                        var css = CalCachDatablockSizeForManualRecord(tag.TagType, 0, MergeMemoryTime * tag.MaxValueCountPerSecond + 4, out valueOffset, out qulityOffset);
                        hb = ManualHisDataMemoryBlockPool.Pool.Get(css);
                        hb.Time = time;
                        hb.RealTime = datetime;
                        hb.MaxCount = MergeMemoryTime * tag.MaxValueCountPerSecond + 2;
                        hb.TimeUnit = 1;
                        hb.TimeLen = 4;
                        hb.TimerAddress = 0;
                        hb.ValueAddress = valueOffset;
                        hb.QualityAddress = qulityOffset;
                        hb.Id = (int)id;
                        hb.CurrentCount = 0;

                        //if (prevb != null)
                        //{
                        //    //将上一个区域的最后一个值，写入到下一个区域的第一个位置上
                        //    //hb.PreQuality = prevb.LastQuality;
                        //    //hb.PreTime = prevb.EndTime;
                        //    //hb.PreValue = prevb.LastValue;
                        //    //AppendPointHisValue(hb, tag, prevb.EndTime, (byte)(prevb.LastQuality + 100), prevb.LastValue as object[]);
                        //}
                        lock (mManualHisDataCach)
                        {
                            while (datacach.Count > 0)
                            {
                                isNeedSubmite = true;
                                var vdd = datacach.First();
                                ServiceLocator.Locator.Resolve<IDataCompress3>().RequestManualToCompress(vdd.Value);
                                //LogStorageManager.Instance.DecManualRef(vdd.Key,rid);
                                datacach.Remove(vdd.Key);
                            }
                            datacach.Add(time, hb);
                        }

                        LoggerService.Service.Info("HisEnginer", "new cal datablock");

                    }

                    var log = LogStorageManager.Instance.GetManualLog(time, rid);

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
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, Convert.ToUInt64(value[2]));
                                break;
                            case TagType.LongPoint3:
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt64(value[0]));
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, Convert.ToInt64(value[1]));
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, Convert.ToInt64(value[2]));
                                break;
                        }
                        hb.WriteByte(hb.QualityAddress + hb.CurrentCount, quality);

                        log?.Append(tag.TagType, tag.Id, datetime, quality, value);

                        hb.EndTime = datetime;
                        hb.CurrentCount++;
                        hb.LastQuality = quality;
                        hb.LastValue = value;
                        hb.Relase();
                        HisDataMemoryQueryService3.Service.RegistorManual(id, hb.Time, hb.EndTime, hb);
                    }
                    return true;
                }
                else
                {
                    return false;
                }
            }
            finally
            {
                UnLockManualTag(id);
            }
        }

       

        private ManualHisDataMemoryBlockArea GetAreaBlocks(HisRunTag tag,DateTime time,out bool isnew,out ManualHisDataMemoryBlockAreaCollection mLast)
        {
            lock(mAreaBlockCachs)
            {
                string skey = ManualHisDataMemoryBlockArea.GetKey(tag.Area,tag.Id);
                if(string.IsNullOrEmpty(skey)||!mTagAreas.ContainsKey(skey))
                {
                    isnew=false;
                    mLast = null;
                    return null;
                }
                if (mAreaBlockCachs.ContainsKey(skey))
                {
                    var vvs = mAreaBlockCachs[skey];
                    mLast = vvs;
                    if (vvs.ContainsKey(time))
                    {
                        isnew = false;
                        return vvs[time];
                    }
                    else
                    {
                        var vv = ManualHisDataMemoryBlockAreaPool.Instance.Get();
                        vv.Area = mTagAreas[skey];
                        vv.Name = tag.Area;
                        vv.Time = time;
                        vvs.Add(time, vv);
                        vvs.Current = vv;
                        isnew = true;
                        return vv;
                    }
                }
                else
                {
                    var vv = ManualHisDataMemoryBlockAreaPool.Instance.Get();
                    vv.Area = mTagAreas[skey];
                    vv.Name = tag.Area;
                    vv.Time = time;
                    //var vv = new ManualHisDataMemoryBlockArea() { Area = mTagAreas[skey], Name = tag.Area, Time = time };
                    ManualHisDataMemoryBlockAreaCollection dtmp = new ManualHisDataMemoryBlockAreaCollection();
                    dtmp.Add(time, vv);
                    dtmp.Current = vv;
                    mAreaBlockCachs.Add(skey, dtmp);
                    isnew = true;
                    mLast = dtmp;
                    return vv;
                }
            }
        }

        /// <summary>
        /// 手动记录某个区域内变量的值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="datetime"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="isNeedSubmite"></param>
        /// <returns></returns>
        private bool ManualRecordAreaHisValues(long id, DateTime datetime, object value, byte quality, out bool isNeedSubmite)
        {
            if (mIsClosed)
            {
                isNeedSubmite = false;
                return false;
            }
            try
            {


                LockManualTag(id);

                isNeedSubmite = false;

                int rid = (int)(id / TagCountPerFile);

                if (mHisTags.ContainsKey(id) && mHisTags[id].Type == RecordType.Driver)
                {
                    var tag = mHisTags[id];
                    DateTime utcnow = DateTime.UtcNow;

                    if (datetime <= tag.LastUpdateTime)
                    {

                        return false;
                    }

                    tag.LastUpdateTime = datetime;
                    ManualHisDataMemoryBlock hb = null;

                    var vdata = datetime.Date;
                    var mms = (int)(datetime.Subtract(vdata).TotalSeconds / MergeMemoryTime);
                    var time = vdata.AddSeconds(mms * MergeMemoryTime);

                    var gb = GetAreaBlocks(tag, time,out bool isnew,out ManualHisDataMemoryBlockAreaCollection mLast);
                    if(gb == null)
                    {
                        return false;
                    }
                    //如果新建一个，则获取上一个进行压缩处理
                    if (mLast != null && mLast.Count>1)
                    {
                       isNeedSubmite = mLast.CheckTag(tag);
                    }
                    
                    if (isnew)
                    {
                        LogStorageManager.Instance.IncManualRef(time, rid);
                    }

                    hb = gb.GetBlock(tag, time, datetime);

                    var log = LogStorageManager.Instance.GetManualLog(time, rid);

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
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.Byte:
                                hb.WriteByteDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToByte(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.Short:
                                hb.WriteShortDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt16(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.UShort:
                                hb.WriteUShortDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt16(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.Int:
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt32(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.UInt:
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt32(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.Long:
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt64(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.ULong:
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToUInt64(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.Float:
                                hb.WriteFloatDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, (float)Math.Round(Convert.ToSingle(value), tag.Precision));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.Double:
                                hb.WriteDoubleDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Math.Round(Convert.ToDouble(value), tag.Precision));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.String:
                                hb.WriteStringDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToString(value), Encoding.Unicode);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.DateTime:
                                hb.WriteDatetime(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToDateTime(value));
                                log?.Append(tag.TagType, tag.Id, datetime, quality, value);
                                break;
                            case TagType.UIntPoint:
                                UIntPointData data = (UIntPointData)(object)value;
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, data.X);
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, data.Y);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, data.X, data.Y);
                                break;
                            case TagType.IntPoint:
                                IntPointData idata = (IntPointData)(object)value;
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, idata.X);
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, idata.Y);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, idata.X, idata.Y);
                                break;
                            case TagType.UIntPoint3:
                                UIntPoint3Data udata3 = (UIntPoint3Data)(object)value;
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, udata3.X);
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, udata3.Y);
                                hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, udata3.Z);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, udata3.X, udata3.Y, udata3.Z);
                                break;
                            case TagType.IntPoint3:
                                IntPoint3Data idata3 = (IntPoint3Data)(object)value;
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, idata3.X);
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 4, idata3.Y);
                                hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, idata3.Z);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, idata3.X, idata3.Y, idata3.Z);
                                break;

                            case TagType.ULongPoint:
                                ULongPointData udata = (ULongPointData)(object)value;
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, udata.X);
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, udata.Y);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, udata.X, udata.Y);
                                break;
                            case TagType.LongPoint:
                                LongPointData lidata = (LongPointData)(object)value;
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, lidata.X);
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, lidata.Y);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, lidata.X, lidata.Y);
                                break;
                            case TagType.ULongPoint3:
                                ULongPoint3Data ludata3 = (ULongPoint3Data)(object)value;
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, ludata3.X);
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, ludata3.Y);
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, ludata3.Z);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, ludata3.X, ludata3.Y, ludata3.Z);
                                break;
                            case TagType.LongPoint3:
                                LongPoint3Data lidata3 = (LongPoint3Data)(object)value;
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, lidata3.X);
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, lidata3.Y);
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, lidata3.Z);
                                log?.Append(tag.TagType, tag.Id, datetime, quality, lidata3.X, lidata3.Y, lidata3.Z);
                                break;
                        }
                        hb.WriteByte(hb.QualityAddress + hb.CurrentCount, quality);
                        hb.EndTime = datetime;
                        hb.CurrentCount++;
                        hb.LastQuality = quality;
                        hb.LastValue = value;
                        hb.Relase();
                        HisDataMemoryQueryService3.Service.RegistorManual(id, hb.Time, hb.EndTime, hb);
                    }
                    else
                    {
                        LoggerService.Service.Warn("HisEnginer", "his value count(" + hb.CurrentCount + ") > maxcount(" + hb.MaxCount + ") or" + datetime + "<=" + hb.EndTime);
                    }
                    return true;
                }
                else
                {

                    return false;
                }
            }
            finally
            {
                UnLockManualTag(id);
            }
        }

        /// <summary>
        /// 手动记录复杂类型的值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="datetime"></param>
        /// <param name="quality"></param>
        /// <param name="isNeedSubmite"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        private bool ManualRecordAreaHisValues<T>(long id, DateTime datetime, byte quality, out bool isNeedSubmite, params T[] value)
        {
            if (mIsClosed)
            {
                isNeedSubmite = false;
                return false;
            }
            isNeedSubmite = false;
            int rid = (int)(id / TagCountPerFile);

            LockManualTag(id);
            try
            {
                if (mHisTags.ContainsKey(id) && mHisTags[id].Type == RecordType.Driver)
                {
                    var tag = mHisTags[id];
                    DateTime utcnow = DateTime.UtcNow;
                    if (WorkMode == HisWorkMode.Initiative)
                    {
                        if (datetime < tag.LastUpdateTime || datetime > utcnow)
                        {
                            return false;
                        }
                    }
                    else
                    {
                        if (datetime < tag.LastUpdateTime)
                        {
                            return false;
                        }
                    }

                    tag.LastUpdateTime = datetime;
                    ManualHisDataMemoryBlock hb = null;
                    var vdata = datetime.Date;
                    var mms = (int)(datetime.Subtract(vdata).TotalSeconds / MergeMemoryTime);
                    var time = vdata.AddSeconds(mms * MergeMemoryTime);


                    var gb = GetAreaBlocks(tag, time, out bool isnew, out ManualHisDataMemoryBlockAreaCollection mLast);
                    if (gb == null)
                    {
                        return false;
                    }

                    //如果新建一个，则获取上一个进行压缩处理
                    if (mLast != null)
                    {
                        isNeedSubmite = mLast.CheckTag(tag);
                    }

                    if (isnew)
                    {
                        LogStorageManager.Instance.IncManualRef(time, rid);
                    }

                    hb = gb.GetBlock(tag, time, datetime);

                    var log = LogStorageManager.Instance.GetManualLog(time, rid);

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
                                hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, Convert.ToUInt64(value[2]));
                                break;
                            case TagType.LongPoint3:
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue, Convert.ToInt64(value[0]));
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 8, Convert.ToInt64(value[1]));
                                hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * tag.SizeOfValue + 16, Convert.ToInt64(value[2]));
                                break;
                        }
                        hb.WriteByte(hb.QualityAddress + hb.CurrentCount, quality);

                        log?.Append(tag.TagType, tag.Id, datetime, quality, value);

                        hb.EndTime = datetime;
                        hb.CurrentCount++;
                        hb.LastQuality = quality;
                        hb.LastValue = value;
                        hb.Relase();
                        HisDataMemoryQueryService3.Service.RegistorManual(id, hb.Time, hb.EndTime, hb);
                    }
                    return true;
                }
                else
                {
                    return false;
                }
            }
            finally
            {
                UnLockManualTag(id);
            }
        }

        /// <summary>
        /// 设置区域相关的一组点的历史值
        /// </summary>
        /// <param name="time"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool SetAreaTagHisValue(DateTime time,Dictionary<int, Tuple<object, byte>> values)
        {
            bool needsubmite = false, ntmp;
            foreach (var vv in values)
            {
                if(vv.Value.Item1 is object[])
                {
                    ManualRecordAreaHisValues(vv.Key, time, vv.Value.Item2, out ntmp, vv.Value.Item1 as object[]);
                }
                else
                {
                    ManualRecordAreaHisValues(vv.Key, time, vv.Value.Item1, vv.Value.Item2, out ntmp);
                }
              
                needsubmite |= ntmp;
            }
            if (needsubmite)
                ServiceLocator.Locator.Resolve<IDataCompress3>().SubmitManualToCompress();
            return false;
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
                ServiceLocator.Locator.Resolve<IDataCompress3>().SubmitManualToCompress();
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
                //if (needsubmite && !mIsSuspend)
                //    ServiceLocator.Locator.Resolve<IDataCompress3>().SubmitManualToCompress();
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
                //if (needsubmite && !mIsSuspend)
                //    ServiceLocator.Locator.Resolve<IDataCompress3>().SubmitManualToCompress();
            }
        }

        //private bool mIsSuspend = false;

        ///// <summary>
        ///// 暂停提交历史数据
        ///// </summary>
        //public void SuspendSubmit()
        //{
        //    mIsSuspend = true;
        //}

        ///// <summary>
        ///// 恢复提交历史数据
        ///// </summary>
        //public void ResumeSubmit()
        //{
        //    mIsSuspend = false;
        //    ServiceLocator.Locator.Resolve<IDataCompress3>().SubmitManualToCompress();
        //}

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
                //if (needsubmite && !mIsSuspend)
                //    ServiceLocator.Locator.Resolve<IDataCompress3>().SubmitManualToCompress();
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
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        public bool SetTagHisValue<T>(int id, T value, byte quality)
        {
            return SetTagHisValue(id, DateTime.UtcNow, value, quality);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool SetTagHisValue(int id, List<TagValue> values)
        {
           return ManualRecordHisValues(id, values,out bool isNeedSave);
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        ///// <returns></returns>
        //public bool SetTagHisValues(Dictionary<int, List<TagValue>> values)
        //{
        //    bool isNeedSave = false;
        //    foreach(var vv in values)
        //    {
        //        ManualRecordHisValues(vv.Key, vv.Value,out isNeedSave);
        //    }
        //    if(isNeedSave)
        //    {
        //        ServiceLocator.Locator.Resolve<IDataCompress3>().SubmitManualToCompress();
        //    }
        //    return true;
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        ///// <param name="timeUnit"></param>
        ///// <returns></returns>
        //public bool SetTagHisValues(Dictionary<int, TagValue> values)
        //{
        //    bool needsubmite = false, ntmp;
        //    foreach (var vv in values)
        //    {
        //        ManualRecordHisValues(vv.Key, vv.Value,out ntmp);
        //        needsubmite |= ntmp;
        //    }
        //    if (needsubmite)
        //        ServiceLocator.Locator.Resolve<IDataCompress3>().SubmitManualToCompress();
        //    return true;
        //}

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

        /// <summary>
        /// 
        /// </summary>
        public void SubmitCach()
        {
            ServiceLocator.Locator.Resolve<IDataCompress3>().SubmitManualToCompress();
        }



        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public class TagArea
    {
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<int> Tags { get; set; } = new List<int>();

        /// <summary>
        /// 
        /// </summary>
        public int TagCount
        {
            get
            {
                return Tags != null ? Tags.Count : 0;
            }
        }

    }


    /// <summary>
    /// 
    /// </summary>
    public class ManualHisDataMemoryBlockArea:IDisposable
    {

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, ManualHisDataMemoryBlock> mMemorys = new Dictionary<int, ManualHisDataMemoryBlock>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, bool> mIsReadys = new Dictionary<int, bool>();

        /// <summary>
        /// 缓存内存缓存时间,单位:s
        /// </summary>
        public static int CachMemoryTime = 60 * 5;

        /// <summary>
        /// 合并内存存储时间,单位:s
        /// </summary>
        public static int MergeMemoryTime = 5 * 60;

        /// <summary>
        /// 
        /// </summary>
        public static int TagCountOneFile { get; set; } = 100000;

        /// <summary>
        /// 
        /// </summary>
        public DateTime Time { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public short Id { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Key { get { return Name + Time+Id; } }


        /// <summary>
        /// 
        /// </summary>
        public TagArea Area { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsReadToCompress { get; set; }

        public static string GetKey(string name,int id)
        {
            return name + (id / TagCountOneFile);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<ManualHisDataMemoryBlock> ListMemorys()
        {
            return mMemorys.Values;
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
        /// 更新内存缓冲历史，供历史查询使用
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public ManualHisDataMemoryBlock RegistorManual(int id)
        {
            lock (mMemorys)
            {
                if (mMemorys.ContainsKey(id))
                {
                    var mm = mMemorys[id];
                    HisDataMemoryQueryService3.Service.RegistorManual(id, mm.Time, mm.EndTime, mm);
                }
                return null;
            }
        }

        /// <summary>
        /// 获取内存块
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="time"></param>
        /// <param name="realtime"></param>
        /// <returns></returns>
        public ManualHisDataMemoryBlock GetBlock(HisRunTag tag,DateTime time,DateTime realtime)
        {
            lock (mMemorys)
            {
                if (mMemorys.ContainsKey(tag.Id))
                {
                    return mMemorys[tag.Id];
                }
                else
                {
                    var css = CalCachDatablockSizeForManualRecord(tag.TagType, 0, MergeMemoryTime * tag.MaxValueCountPerSecond + 4, out int valueOffset, out int qulityOffset);
                    var hb = ManualHisDataMemoryBlockPool.Pool.Get(css);
                    hb.Time = time;
                    hb.RealTime = realtime;
                    hb.MaxCount = MergeMemoryTime * tag.MaxValueCountPerSecond + 2;
                    hb.TimeUnit = 1;
                    hb.TimeLen = 4;
                    hb.TimerAddress = 0;
                    hb.ValueAddress = valueOffset;
                    hb.QualityAddress = qulityOffset;
                    hb.Id = (int)tag.Id;
                    hb.CurrentCount = 0;
                    mMemorys.Add(tag.Id, hb);
                    return hb;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void NotifyIsReadyToCompresss(int id)
        {
            if(mIsReadys.ContainsKey(id))
            {
                mIsReadys[id] = true;
            }
            else
            {
                mIsReadys.Add(id, true);
            }
            if(mIsReadys.Count == Area.TagCount)
            {
                IsReadToCompress = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool IsReady(int id)
        {
            return mIsReadys.ContainsKey(id) && mIsReadys[id];
        }



        /// <summary>
        /// 检查并提交到压缩
        /// </summary>
        public void SubmiteToCompress()
        {
            ServiceLocator.Locator.Resolve<IDataCompress3>().RequestManualToCompress(this);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            IsReadToCompress = false;
            Name = string.Empty;

            mMemorys.Clear();
            mIsReadys.Clear();
            Area = null;
        }
    }

    public class ManualHisDataMemoryBlockAreaCollection : SortedDictionary<DateTime, ManualHisDataMemoryBlockArea>
    {
        /// <summary>
        /// 
        /// </summary>
        public ManualHisDataMemoryBlockArea Current { get; set; }

        private Queue<DateTime> mRemoves = new Queue<DateTime>();

        public bool CheckTag(HisTag tag)
        {
            bool re = false;
            lock (mRemoves)
            {
                if (this.Count > 1)
                {
                    int i = 0;
                    mRemoves.Clear();

                    foreach (var vv in this)
                    {
                        if (i >= this.Count - 1)
                        {
                            break;
                        }
                        if (!vv.Value.IsReady(tag.Id))
                        {
                            vv.Value.RegistorManual(tag.Id);
                            vv.Value.NotifyIsReadyToCompresss(tag.Id);
                            if (vv.Value.IsReadToCompress)
                            {
                                vv.Value.SubmiteToCompress();
                                mRemoves.Enqueue(vv.Key);
                                re = true;
                            }
                        }
                        i++;
                    }

                    while (mRemoves.Count > 0)
                    {
                        this.Remove(mRemoves.Dequeue());
                    }
                }
                return re;
            }
        }
    }

}
