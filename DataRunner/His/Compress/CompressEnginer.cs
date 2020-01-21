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

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void CalMemory(long size)
        {
            /* 内存结构:head+数据区指针+数据区
               head:数据起始地址(4)+变量数量(4)+起始时间(8)
               数据区指针:[ID(4) + address(4) + datasize(4)]
               数据区:[data block]
               data block:size+compressType+data
             */
            mMemory1 = new MemoryBlock(size);
            mMemory2 = new MemoryBlock(size);
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

                mTargetMemory = SelectMemory();
                while (mTargetMemory == null)
                {
                    LoggerService.Service.Info("Comperss", "压缩出现阻塞");
                    Thread.Sleep(10);
                    mTargetMemory = SelectMemory();
                }
                mTargetMemory.MakeMemoryBusy();
                Compress();
                mSourceMemory.MakeMemoryNoBusy();
                ServiceLocator.Locator.Resolve<IDataSerialize>().RequestToSave(mTargetMemory, mCurrentTime);
            }
            closedEvent.Set();
        }

        

        /// <summary>
        /// 执行压缩
        /// </summary>
        private void Compress()
        {
            //读取变量个数
            int count = mSourceMemory.ReadInt(9);

            //读取内存保存时间
            mMemoryCachTime = mSourceMemory.ReadInt(13);
            //读取时间最小间隔
            mMemoryTimeTick = mSourceMemory.ReadInt(17);

            //源内存数据块头部信息偏移地址
            int offset = 21;

            //压缩后内存头大小
            int headoffset = 16;

            //数据区地址
            int mTargetPosition = count * 12 + headoffset;

            for (int i=0;i<count;i++)
            {
                var id = mSourceMemory.ReadInt(offset);
                var qaddr = mSourceMemory.ReadInt(offset + 4);
                var len = mSourceMemory.ReadInt(offset + 8);

                //压缩数据
                var size = CompressBlockMemory(qaddr, mTargetPosition,len);
                
                //更新头部指针区域数据
                //写入变量ID
                mTargetMemory.WriteInt(headoffset,id);
                //写入数据区地址
                mTargetMemory.WriteInt(headoffset + 4, mTargetPosition);

                //写入数据区大小
                mTargetMemory.WriteInt(headoffset + 8, size);

                offset += 12;

                headoffset += 12;
                mTargetPosition += size;
            }
            mTargetMemory.WriteInt(0, mTargetPosition);//写入数据起始地址
            mTargetMemory.WriteInt(4, count);//写入变量数量
            mTargetMemory.WriteDatetime(8, mCurrentTime);//写入时间
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
