//==============================================================
//  Copyright (C) 2020 Chongdaoyang Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/8/17 20:23:20 .
//  Version 1.0
//  CDYWORK
//==============================================================

using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace DBRuntime.RDDC
{
    /// <summary>
    /// 
    /// </summary>
    public class DataSync
    {

        #region ... Variables  ...

        private Thread mScanThread;

        private RDDCClient mClient;

        private bool mIsStoped = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public RDDCClient Client
        {
            get
            {
                return mClient;
            }
            set
            {
                mClient = value;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public bool Enable { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            mScanThread = new Thread(ThreadPro);
            mScanThread.IsBackground = true;
            mScanThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        private void ThreadPro()
        {
            while(!mIsStoped)
            {
                if (mClient.IsConnected && Enable)
                {
                    SyncData();
                    Thread.Sleep(100);
                }
                else
                {
                    Thread.Sleep(500);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void SyncData()
        {
            try
            {
                var realenginer = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                if (realenginer != null)
                {
                    var block = mClient.SyncRealData();
                    if (block != null)
                    {
                        var size = block.ReadInt();
                        Buffer.BlockCopy(block.Array, block.ArrayOffset + block.ReaderIndex, (realenginer as RealEnginer).Memory, 0, size);
                    }
                }
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("DataSync", ex.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            mIsStoped = true;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
