﻿//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 历史数据引擎
    /// </summary>
    public class HisEnginer
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
        /// 每个变量在内存中保留的历史记录历史的长度
        /// 单位s
        /// </summary>
        public int MemoryCachTime = 60;

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
        private MemoryBlock mMemory1;

        /// <summary>
        /// 历史记录内存2
        /// </summary>
        private MemoryBlock mMemory2;

        /// <summary>
        /// 当前正在使用的内存
        /// </summary>
        private MemoryBlock mCurrentMemory;
        

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
        private ValueChangedMemoryCacheProcesser mLastValueChangedProcesser = new ValueChangedMemoryCacheProcesser();

        private System.Timers.Timer mRecordTimer;

        private DateTime mLastProcessTime;

        private int mTagCount = 0;

        private int mLastProcessTick = -1;

        private bool mIsBusy = false;

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
        /// 当前工作的内存区域
        /// </summary>
        public MemoryBlock CurrentMemory
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
                    mManager = new Cdy.Tag.HisDatabaseManager().Load();

                mLastProcesser = new TimerMemoryCacheProcesser();
                mRecordTimerProcesser.Clear();
                mRecordTimerProcesser.Add(mLastProcesser);

                mValueChangedProcesser.Clear();
                mValueChangedProcesser.Add(mLastValueChangedProcesser);

                // var count = MemoryCachTime * 1000 / MemoryTimeTick;
                var count = MemoryCachTime;
                var realbaseaddr = mRealEnginer.Memory;
                HisRunTag mHisTag = null;
                foreach (var vv in mManager.HisTags)
                {
                    var realaddr = (int)mRealEnginer.GetDataAddr((int)vv.Value.Id);
                    switch (vv.Value.TagType)
                    {
                        case Cdy.Tag.TagType.Bool:
                        case Cdy.Tag.TagType.Byte:
                            mHisTag = new ByteHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType, RealMemoryAddr = realbaseaddr, RealValueAddr = realaddr };
                            break;
                        case Cdy.Tag.TagType.Short:
                        case Cdy.Tag.TagType.UShort:
                            mHisTag = new ShortHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType, RealMemoryAddr = realbaseaddr, RealValueAddr = realaddr };
                            break;
                        case Cdy.Tag.TagType.Int:
                        case Cdy.Tag.TagType.UInt:
                            mHisTag = new IntHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType, RealMemoryAddr = realbaseaddr, RealValueAddr = realaddr };
                            break;
                        case Cdy.Tag.TagType.Long:
                        case Cdy.Tag.TagType.ULong:
                            mHisTag = new LongHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType, RealMemoryAddr = realbaseaddr, RealValueAddr = realaddr };
                            break;
                        case Cdy.Tag.TagType.Float:
                            mHisTag = new FloatHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType, RealMemoryAddr = realbaseaddr, RealValueAddr = realaddr };
                            break;
                        case Cdy.Tag.TagType.Double:
                            mHisTag = new DoubleHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType, RealMemoryAddr = realbaseaddr, RealValueAddr = realaddr };
                            break;
                        case Cdy.Tag.TagType.DateTime:
                            mHisTag = new DateTimeHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType, RealMemoryAddr = realbaseaddr, RealValueAddr = realaddr };
                            break;
                        case Cdy.Tag.TagType.String:
                            mHisTag = new StirngHisRunTag() { Id = vv.Value.Id, Circle = vv.Value.Circle, Type = vv.Value.Type, TagType = vv.Value.TagType, RealMemoryAddr = realbaseaddr, RealValueAddr = realaddr };
                            break;
                    }
                    mHisTag.MaxCount = count;
                    mHisTags.Add(vv.Key, mHisTag);

                    if (mHisTag.Type == Cdy.Tag.RecordType.Timer)
                    {
                        if(!mLastProcesser.AddTag(mHisTag))
                        {
                            mLastProcesser = new TimerMemoryCacheProcesser();
                            mRecordTimerProcesser.Add(mLastProcesser);
                        }
                    }
                    else
                    {
                        if(!mLastValueChangedProcesser.AddTag(mHisTag))
                        {
                            mLastValueChangedProcesser = new ValueChangedMemoryCacheProcesser();
                            mValueChangedProcesser.Add(mLastValueChangedProcesser);
                        }
                    }
                    mTagCount++;
                }
            }
            AllocMemory();
        }

        /// <summary>
        /// 块标题大小
        /// </summary>
        /// <returns></returns>
        private long CalHeadSize()
        {
            //Flag + DateTime + Data Count+MemoryCachTime+MemoryTimeTick + 变量ID,数据偏移地址,数据大小
            //Flag 表示 0：空闲，1：忙
            return 1 + 8 + 4 + 4 + 4 + mTagCount * 12;
        }

        /// <summary>
        /// 内存块的头部大小
        /// </summary>
        /// <returns></returns>
        private int CalBlockHeadSize()
        {
            //qulity address offset + Type + TagType + CompressType + compressParamter1+ compressParamter2+ compressParamter3
            return 4 + 1 + 1 + 1 + 4 + 4 + 4;
        }

        /// <summary>
        /// 计算每个变量数据块的大小
        /// </summary>
        /// <param name="tagType"></param>
        /// <returns></returns>
        private int CalBlockSize(Cdy.Tag.TagType tagType,int blockSize,out int dataOffset,out int qulityOffset)
        {

            //单个数据块内容包括：时间戳(2)+数值+质量戳(1)

            qulityOffset = 0;
            int regionHeadSize = blockSize;
            // int count = MemoryCachTime * 1000 / MemoryTimeTick;
            int count = MemoryCachTime;//
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
                    qulityOffset = dataOffset + count * 8;
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
               内存结构:内存头+[数据块指针]+[数据块(一个变量一个数据块)]
               内存头:标记 + 记录时间 + 数据个数+MemoryCachTime+MemoryTimeTick
               数据块指针:变量ID,数据偏移地址,数据大小
               数据块:数据块头+数据
               数据块头:数据区大小+记录类型(byte)+变量类型(byte)+压缩类型(byte)+压缩参数1(float)+压缩参数2(float)+压缩参数3(float)
               数据:[时间戳]+[值]+[质量戳]
             */
            long headSize = CalHeadSize();
            int blockheadsize = CalBlockHeadSize();
            int qulityOffset = 0;
            int valueOffset = 0;
            foreach(var vv in mHisTags)
            {
                var ss = CalBlockSize(vv.Value.TagType,blockheadsize,out valueOffset, out qulityOffset);

                vv.Value.BlockHeadStartAddr = (int)headSize;

                vv.Value.TimerValueStartAddr = vv.Value.BlockHeadStartAddr + blockheadsize;

                vv.Value.HisValueStartAddr = vv.Value.BlockHeadStartAddr + valueOffset;
               
                vv.Value.HisQulityStartAddr = vv.Value.BlockHeadStartAddr + qulityOffset;

                vv.Value.DataSize = ss;
                headSize += ss;
            }
            mMemory1 = new MemoryBlock(headSize);
            mMemory2 = new MemoryBlock(headSize);

            CurrentMemory = mMemory1;
            
        }

        /// <summary>
        /// 清空内存
        /// </summary>
        private void InitMemory()
        {
            mCurrentMemory.Clear();
            //写入时间
            mCurrentMemory.WriteDatetime(1, mLastProcessTime);
            //写入变量个数
            mCurrentMemory.WriteInt(9, mTagCount);
            //写入时间内存保存数据的时间
            mCurrentMemory.WriteInt(13, MemoryCachTime);
            //写入最小时间间隔
            mCurrentMemory.WriteInt(17, MemoryTimeTick);
            int offset = 21;
            foreach (var vv in mHisTags)
            {
                mCurrentMemory.WriteInt(offset, vv.Value.Id); //Tag id int(4)
                mCurrentMemory.WriteInt(offset + 4, vv.Value.BlockHeadStartAddr); //历史数据偏移地址 int(4)
                mCurrentMemory.WriteInt(offset + 8, vv.Value.DataSize); //历史数据大小 int(4)
                offset += 12;
            }
            HisRunTag.StartTime = mLastProcessTime;
            mMemory1.MakeMemoryBusy();
            mMemory2.MakeMemoryNoBusy();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            foreach(var vv in mRecordTimerProcesser)
            {
                vv.Start();
            }
            mRecordTimer = new System.Timers.Timer(MemoryTimeTick);
            mRecordTimer.Elapsed += MRecordTimer_Elapsed;
            mRecordTimer.Start();
            mLastProcessTime = DateTime.Now;
            InitMemory();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        private void CheckMemoryIsReady(MemoryBlock memory)
        {
            while (memory.IsBusy)
            {
                LoggerService.Service.Info("Record", "记录出现阻塞");
                System.Threading.Thread.Sleep(1);
            }
        }

        /// <summary>
        /// 切换内存
        /// </summary>
        private void SwitchMemory()
        {
            var mcc = mCurrentMemory;
            if (mCurrentMemory == mMemory1)
            {
                CheckMemoryIsReady(mMemory2);
                CurrentMemory = mMemory2;
            }
            else
            {
                CheckMemoryIsReady(mMemory1);
                CurrentMemory = mMemory1;
            }

            //提交到数据压缩流程
            ServiceLocator.Locator.Resolve<IDataCompress>().RequestToCompress(mcc);

            InitMemory();

            foreach(var vv in mHisTags.Values)
            {
                vv.Reset();
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
                LoggerService.Service.Info("RecordTimer", "出现阻塞");
                return;
            }
            mIsBusy = true;

            DateTime dt = DateTime.Now;
            var mm = (dt.Hour * 24 + dt.Minute * 60 + dt.Second) / MemoryCachTime;
            if (mm!=mLastProcessTick )
            {
                
                if(mLastProcessTick != -1)
                {
                    //在内存尾部一次填充所有值
                    if (mCurrentMemory != null)
                    {
                        foreach (var vv in mRecordTimerProcesser)
                        {
                            vv.RecordAllValue(dt);
                        }

                        foreach (var vv in mValueChangedProcesser)
                        {
                            vv.RecordAllValue(dt);
                        }
                    }

                    mLastProcessTime = dt;
                    //将之前的Memory提交到历史存储流程中
                    SwitchMemory();
                }
                mLastProcessTick = mm;

                //在内存头部一次填充所有值
                foreach (var vv in mRecordTimerProcesser)
                {
                    vv.WriteHeader();
                    vv.RecordAllValue(dt);
                }

                foreach (var vv in mValueChangedProcesser)
                {
                    vv.WriteHeader();
                    vv.RecordAllValue(dt);
                }
            }
            else
            {
                foreach (var vv in mRecordTimerProcesser)
                {
                    vv.Notify(dt);
                }

                //值改变的变量,也受内部定时逻辑处理。这样值改变频路高于MemoryTimeTick 的值时，无效。
                foreach (var vv in mValueChangedProcesser)
                {
                    vv.Notify(dt);
                }
            }

            mIsBusy = false;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            mRecordTimer.Stop();
            if(mRecordTimer!=null)
            {
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

            mLastValueChangedProcesser = null;
            mLastProcesser = null;

            mHisTags.Clear();

        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
