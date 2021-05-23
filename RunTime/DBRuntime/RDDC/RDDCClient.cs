//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/8/17 17:03:59.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using Cheetah;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace DBRuntime.RDDC
{
    /// <summary>
    /// 
    /// </summary>
    public class RDDCClient:SocketClient2
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        public const byte WorkStateFun = 1;

        /// <summary>
        /// 
        /// </summary>
        public const byte RealDataSyncFun = 2;



        public const byte AysncReturn = byte.MaxValue;


        /// <summary>
        /// 获取启动时间
        /// </summary>
        public const byte GetStartTimeFun = 0;

        /// <summary>
        /// 切换到从机
        /// </summary>
        public const byte ChangeToStandbyFun = 1;

        /// <summary>
        /// 切换同主机
        /// </summary>
        public const byte ChangeToPrimaryFun = 2;

        /// <summary>
        /// 获取工作状态
        /// </summary>
        public const byte GetStateFun = 3;


        private ManualResetEvent mWorkStateEvent = new ManualResetEvent(false);

        private ByteBuffer mWorkStateData;

        private ManualResetEvent mRealDataSyncEvent = new ManualResetEvent(false);

        private ByteBuffer mRealDataSyncData;

        private object mWorkStateLockObj = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="datas"></param>
        protected override void ProcessData(byte fun, ByteBuffer datas)
        {
            datas.IncRef();
            //收到异步请求回调数据
            switch (fun)
            {
                case WorkStateFun:
                    mWorkStateData = datas;
                    mWorkStateEvent.Set();
                    break;
                case RealDataSyncFun:
                    mRealDataSyncData = datas;
                    this.mRealDataSyncEvent.Set();
                    break;
                case byte.MaxValue:
                    break;
            }

            base.ProcessData(fun, datas);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public ByteBuffer SyncRealData(int timeout=5000)
        {
            var mb = GetBuffer(RealDataSyncFun, 0);
            mRealDataSyncEvent.Reset();
            SendData(mb);
            try
            {
                if (mRealDataSyncEvent.WaitOne(timeout))
                {
                    return mRealDataSyncData;
                }
            }
            finally
            {

            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public DateTime? GetStartTime(int timeout = 5000)
        {
            lock (mWorkStateLockObj)
            {
                var mb = GetBuffer(WorkStateFun, 1);
                mb.WriteByte(GetStartTimeFun);
                mWorkStateEvent.Reset();
                SendData(mb);
                try
                {
                    if (mWorkStateEvent.WaitOne(timeout))
                    {
                        return DateTime.FromBinary(mWorkStateData.ReadLong());
                    }
                }
                finally
                {

                }
            }

            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public WorkState? GetWorkState(int timeout = 5000)
        {
            lock (mWorkStateLockObj)
            {
                var mb = GetBuffer(WorkStateFun, 1);
                mb.WriteByte(GetStateFun);
                mWorkStateEvent.Reset();
                SendData(mb);
                try
                {
                    if (mWorkStateEvent.WaitOne(timeout))
                    {
                        return (WorkState)mWorkStateData.ReadByte();
                    }
                }
                finally
                {

                }
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool? SwitchToPrimary(int timeout = 5000)
        {
            lock (mWorkStateLockObj)
            {
                var mb = GetBuffer(WorkStateFun, 1);
                mb.WriteByte(ChangeToPrimaryFun);
                mWorkStateEvent.Reset();
                SendData(mb);
                try
                {
                    if (mWorkStateEvent.WaitOne(timeout))
                    {
                        return mWorkStateData.ReadByte() > 0;
                    }
                }
                finally
                {

                }
            }
            return false;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool? SwitchToStandby(int timeout = 5000)
        {
            lock (mWorkStateLockObj)
            {
                var mb = GetBuffer(WorkStateFun, 1);
                mb.WriteByte(ChangeToStandbyFun);
                mWorkStateEvent.Reset();
                SendData(mb);
                try
                {
                    if (mWorkStateEvent.WaitOne(timeout))
                    {
                        return mWorkStateData.ReadByte() > 0;
                    }
                }
                finally
                {

                }
            }
            return false;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
