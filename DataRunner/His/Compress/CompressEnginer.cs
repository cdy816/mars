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
            int count = mSourceMemory.ReadInt(9);
            mMemoryCachTime = mSourceMemory.ReadInt(13);
            mMemoryTimeTick = mSourceMemory.ReadInt(17);

            int offset = 21;
            int headoffset = 16;
            int mTargetPosition = count * 8 + headoffset;

            for (int i=0;i<count;i++)
            {
                var id = mSourceMemory.ReadInt(offset);
                var qaddr = mSourceMemory.ReadInt(offset + 4);
                var len = mSourceMemory.ReadInt(offset + 8);
                var size = CompressBlockMemory(qaddr, mTargetPosition,len);
                
                mTargetMemory.WriteInt(headoffset,id);
                mTargetMemory.WriteInt(headoffset + 4, mTargetPosition);

                offset += 12;

                headoffset += 8;
                mTargetPosition += size;
            }
            mTargetMemory.WriteInt(0, mTargetPosition);//写入数据起始地址
            mTargetMemory.WriteInt(4, count);//写入数量
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
            byte rtype = mSourceMemory[addr + 4];
            byte tagtype = mSourceMemory[addr + 5];
            byte comtype = mSourceMemory[addr + 6];
            float cp1 = mSourceMemory.ReadFloat(addr + 7);
            float cp2 = mSourceMemory.ReadFloat(addr + 11);
            float cp3 = mSourceMemory.ReadFloat(addr + 15);

            //写入压缩类型
            mTargetMemory.WriteByte(targetPosition, comtype);

            var tp = CompressUnitManager.Manager.GetCompress(comtype);
            if(tp!=null)
            {
                tp.QulityOffset = qulityoffset;
                tp.TagType = tagtype;
                tp.RecordType = (RecordType)rtype;
                tp.StartTime = mCurrentTime;
                tp.CompressParameter1 = cp1;
                tp.CompressParameter2 = cp2;
                tp.CompressParameter3 = cp3;
                return tp.Compress(mSourceMemory, addr + 19, mTargetMemory, targetPosition + 1, len-19) + 1;
            }
            return 0;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
