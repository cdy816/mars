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

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class CompressEnginer : IDataCompress
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private ManualResetEvent resetEvent;

        private ManualResetEvent closedEvent;

        private Thread mCompressThread;

        private bool mIsClosed = false;

        private MemoryBlock mSourceMemory;

        private MemoryBlock mTargetMemory;

        private MemoryBlock mMemory1;

        private MemoryBlock mMemory2;

        private DateTime mCurrentTime;

        private int mMemoryCachTime = 0;

        private int mMemoryTimeTick = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public CompressEnginer(long size)
        {
            CalMemory(size);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 单个文件内变量的个数
        /// </summary>
        public int TagCountOneFile { get; set; } = 100000;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void CalMemory(long size)
        {
            /* 内存结构:Head+[DataRegion]
             * Head:数据区域个数(4)+时间(8)+[区域ID(4)+区域地址(8)]
             * DataRegion:region head+数据区指针+数据区
               region head:数据大小(4)+变量数量(4)
               数据区指针:[ID(4) + address(4)]
               数据区:[data block]
               data block:size+compressType+data
             */
            mMemory1 = new MemoryBlock(size) { Name = "CompressMemory1" };
            mMemory2 = new MemoryBlock(size) { Name = "CompressMemory2" };
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
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

            mSourceMemory = null;
            mTargetMemory = null;

            this.mMemory1 = null;
            this.mMemory2 = null;
            resetEvent.Dispose();
            closedEvent.Dispose();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataMemory"></param>
        public void RequestToCompress(MemoryBlock dataMemory)
        {
            mSourceMemory = dataMemory;
            mCurrentTime = dataMemory.ReadDateTime(1);
            resetEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private MemoryBlock SelectMemory()
        {
            if(!mMemory1.IsBusy)
            {
                return mMemory1;
            }
            else if(!mMemory2.IsBusy)
            {
                return mMemory2;
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>

        private void ThreadPro()
        {
            while(!mIsClosed)
            {
                resetEvent.WaitOne();
                resetEvent.Reset();
                if (mIsClosed)
                    break;
                LoggerService.Service.Info("Compress", "********开始执行压缩********");

                var sm = mSourceMemory;

                mTargetMemory = SelectMemory();
                while (mTargetMemory == null)
                {
                    LoggerService.Service.Info("Compress", "压缩出现阻塞");
                    Thread.Sleep(10);
                    mTargetMemory = SelectMemory();
                }
                mTargetMemory.MakeMemoryBusy();
                Compress();

                //sm.Clear();
                ServiceLocator.Locator.Resolve<IHisEngine>().ClearMemoryHisData(sm);
                sm.MakeMemoryNoBusy();
                LoggerService.Service.Info("Compress", ">>>>>>>>>压缩完成>>>>>>>>>" + mTargetMemory.UsedSize);

                //System.Threading.Tasks.Task.Run(new Action(() => {
                //    ServiceLocator.Locator.Resolve<IDataSerialize>().RequestToSave(mTargetMemory, mCurrentTime);
                //}));

                //new System.Threading.Tasks.TaskFactory().StartNew(new Action(() => {
                    ServiceLocator.Locator.Resolve<IDataSerialize>().RequestToSave(mTargetMemory, mCurrentTime);
                //}));
            }
            closedEvent.Set();
        }

        

        /// <summary>
        /// 执行压缩
        /// </summary>
        private void Compress()
        {

            /* 内存结构:Head+[DataRegion]
             * Head:数据区域个数(4)+时间(8)+[区域ID(4)+区域地址(8)]
             * DataRegion:region head+数据区指针+数据区
               region head:数据大小(4)+变量数量(4)
               数据区指针:[ID(4) + address(4)]
               数据区:[data block]
               data block:size+compressType+data
             */

            //读取变量个数
            int count = mSourceMemory.ReadInt(9);

            //读取内存保存时间
            mMemoryCachTime = mSourceMemory.ReadInt(13);
            //读取时间最小间隔
            mMemoryTimeTick = mSourceMemory.ReadInt(17);

            //源内存数据块头部信息偏移地址
            int offset = 21;

            int mLastDataRegionId = -1;
            Dictionary<int,long> Drids = new Dictionary<int, long>();
            //记录每个数据区域内ID的个数
            Dictionary<int, int> mDRcount = new Dictionary<int, int>();
            //计算数据区域个数
            for(int i=0;i<count;i++)
            {
                var id = mSourceMemory.ReadInt(offset);
                var did = id / TagCountOneFile;
                if (mLastDataRegionId != did)
                {
                    Drids.Add(did, 0);
                    mDRcount.Add(did, 1);
                    mLastDataRegionId = did;
                }
                else
                {
                    mDRcount[did]++;
                }
                offset += 12;
            }

            offset = 21;

            //数据区头部指针偏移
            int headoffset = 0;
            //数据区起始地址
            int mHeadAddress = -1;

            //数据区地址
            int mDataPosition = 12 + Drids.Count * 12;

            mLastDataRegionId = -1;

            for (int i=0;i<count;i++)
            {
                var id = mSourceMemory.ReadInt(offset);
                var qaddr = mSourceMemory.ReadInt(offset + 4);
                var len = mSourceMemory.ReadInt(offset + 8);

                int rid = id / TagCountOneFile;
                int size = 0;
                if (rid != mLastDataRegionId)
                {
                    //开启一个新的数据区域

                    if (mHeadAddress >= 0)
                    {
                        mTargetMemory.WriteInt(mHeadAddress, mDataPosition);//写入上一个区域数据块的大小
                        mTargetMemory.WriteInt(mHeadAddress + 4, mDRcount[mLastDataRegionId]);//写入变量个数
                    }

                    Drids[rid] = mHeadAddress  = mDataPosition;
                    headoffset = mHeadAddress + 8;
                    mDataPosition = headoffset + mDRcount[rid] * 8 + 8;
                    mLastDataRegionId = rid;
                }

                //压缩数据
                size = CompressBlockMemory(qaddr, mDataPosition, len);

                //更新头部指针区域数据
                //写入变量ID
                mTargetMemory.WriteInt(headoffset, id);
                //写入数据区地址
                mTargetMemory.WriteInt(headoffset + 4, mDataPosition);

                headoffset += 8;
                offset += 12;
                mDataPosition += size;
            }
            if (count > 0)
            {
                mTargetMemory.WriteInt(mHeadAddress, mDataPosition);//写入上一个区域数据块的大小
                mTargetMemory.WriteInt(mHeadAddress + 4, mDRcount[mLastDataRegionId]);//写入变量个数
            }

            mTargetMemory.WriteInt(0, Drids.Count);
            mTargetMemory.WriteDatetime(4, mCurrentTime);
            //更新数据区域地址
            offset = 12;
            foreach (var vid in Drids)
            {
                mTargetMemory.WriteInt(offset, vid.Key);
                mTargetMemory.WriteLong(offset+4, vid.Value);
                offset += 12;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="targetPosition"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        private int CompressBlockMemory(int addr,int targetPosition,int len)
        {
            var qulityoffset = mSourceMemory.ReadInt(addr);
            byte rtype = mSourceMemory[addr + 4];//记录类型
            byte tagtype = mSourceMemory[addr + 5];//变量类型
            byte comtype = mSourceMemory[addr + 6];//压缩类型
            float cp1 = mSourceMemory.ReadFloat(addr + 7);//压缩附属参数
            float cp2 = mSourceMemory.ReadFloat(addr + 11);//压缩附属参数
            float cp3 = mSourceMemory.ReadFloat(addr + 15);//压缩附属参数


            //写入压缩类型
            mTargetMemory.WriteByte(targetPosition + 4, comtype);

            var tp = CompressUnitManager.Manager.GetCompress(comtype);
            if (tp != null)
            {
                tp.QulityOffset = qulityoffset;
                tp.TagType = tagtype;
                tp.RecordType = (RecordType)rtype;
                tp.StartTime = mCurrentTime;
                tp.CompressParameter1 = cp1;
                tp.CompressParameter2 = cp2;
                tp.CompressParameter3 = cp3;
                var size = tp.Compress(mSourceMemory, addr + 19, mTargetMemory, targetPosition + 5, len - 19) + 1;
                mTargetMemory.WriteInt(targetPosition,size);
                return size + 4;
            }
            return 0;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
