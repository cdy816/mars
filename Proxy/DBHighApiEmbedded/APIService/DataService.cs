//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/14 10:22:00.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using Cheetah;
//using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace DBHighApi.Api
{
    public class ApiFunConst
    {
        /// <summary>
        /// 
        /// </summary>
        public const byte TagInfoRequest = 1;

        /// <summary>
        /// 
        /// </summary>
        public const byte RealDataRequestFun = 10;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagValueCallBack = 30;

        /// <summary>
        /// 
        /// </summary>
        public const byte RealDataPushFun = 12;


        /// <summary>
        /// 
        /// </summary>
        public const byte HisDataRequestFun = 20;




        public const byte AysncReturn = byte.MaxValue;
    }

    /// <summary>
    /// 
    /// </summary>
    public class DataService: SocketServer2
    {

        #region ... Variables  ...

        private ByteBuffer mAsyncCalldata;

        private Dictionary<byte, ServerProcessBase> mProcess = new Dictionary<byte, ServerProcessBase>();

        private HisDataServerProcess mHisProcess;
        private RealDataServerProcess mRealProcess;
        private TagInfoServerProcess mInfoProcess;

        /// <summary>
        /// 
        /// </summary>
        public static DataService Service = new DataService();

        private bool mIsRunning = false;

        private int mPort = -1;

        /// <summary>
        /// 
        /// </summary>
        private List<string> mClientIds = new List<string>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public DataService()
        {
            RegistorInit();
        }

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public override string Name => "ConsumeDataService";

        /// <summary>
        /// 
        /// </summary>
        public bool IsPause { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...


        public override void Start(int port)
        {
            if (mPort != port)
            {
                if (mPort != -1)
                    Stop();


                mHisProcess = new HisDataServerProcess() { Parent = this };
                mRealProcess = new RealDataServerProcess() { Parent = this };
                mInfoProcess = new TagInfoServerProcess() { Parent = this };
                mHisProcess.Start();
                mRealProcess.Start();
                mInfoProcess.Start();
                base.Start(port);
            }
            Pause(false);
            mPort = port;
            mIsRunning = true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Pause(bool value)
        {
            mHisProcess.IsPause = value;
            mRealProcess.IsPause = value;
            mInfoProcess.IsPause = value;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="port"></param>
        //protected override void StartInner(int port)
        //{
        //    mHisProcess = new HisDataServerProcess() { Parent = this };
        //    mRealProcess = new RealDataServerProcess() { Parent = this };
        //    mInfoProcess = new TagInfoServerProcess() { Parent = this };
        //    mHisProcess.Start();
        //    mRealProcess.Start();
        //    mInfoProcess.Start();
        //    base.StartInner(port);
        //    mIsRunning = true;
        //}

        /// <summary>
        /// 
        /// </summary>
        public override void Stop()
        {
            if (mIsRunning)
            {
                base.Stop();
                if (mHisProcess != null)
                {
                    mHisProcess.Stop();
                    mHisProcess.Dispose();
                    mHisProcess = null;
                }
                if (mRealProcess != null)
                {
                    mRealProcess.Stop();
                    mRealProcess.Dispose();
                    mRealProcess = null;
                }
                if (mInfoProcess != null)
                {
                    mInfoProcess.Stop();
                    mInfoProcess.Dispose();
                    mInfoProcess = null;
                }
            }

            mPort = -1;
        }

        private ByteBuffer GetAsyncData()
        {
            mAsyncCalldata = Allocate(ApiFunConst.AysncReturn, 4);
            mAsyncCalldata.Write(0);
            return mAsyncCalldata;
        }

        /// <summary>
        /// 
        /// </summary>
        private void RegistorInit()
        {
            
            this.RegistorFunCallBack(ApiFunConst.TagInfoRequest, TagInfoRequest);
            this.RegistorFunCallBack(ApiFunConst.RealDataRequestFun, ReadDataRequest);
            this.RegistorFunCallBack(ApiFunConst.HisDataRequestFun, HisDataRequest);
        }

        private object mLocker = new object();

        /// <summary>
        /// 
        /// </summary>
        public void PushRealDatatoClient(string clientId,byte[] value)
        {
            lock (mLocker)
            {
                if (value != null)
                    this.SendData(clientId, Api.ApiFunConst.RealDataPushFun, value, value.Length);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="value"></param>
        public void PushRealDatatoClient(string clientId, ByteBuffer value)
        {
            lock (mLocker)
            {
                if (value != null)
                    this.SendDataToClient(clientId, value);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="fun"></param>
        /// <param name="value"></param>
        public void AsyncCallback(string clientId,byte fun, byte[] value,int len)
        {
            lock (mLocker)
            {
                if (value != null)
                    this.SendData(clientId, fun, value, len);
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        public void AsyncCallback(string clientId, ByteBuffer data)
        {
            lock (mLocker)
            {
                if (data != null)
                    this.SendDataToClient(clientId, data);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="fun"></param>
        /// <param name="value"></param>
        /// <param name="len"></param>
        public void AsyncCallback(string clientId, byte fun, IntPtr value, int len)
        {
            lock (mLocker)
            {
                if (value != IntPtr.Zero)
                    this.SendData(clientId, fun, value, len);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private ByteBuffer TagInfoRequest(string clientId, ByteBuffer memory)
        {
            mInfoProcess.ProcessData(clientId,memory);
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="memory"></param>
        /// <returns></returns>
        private ByteBuffer ReadDataRequest(string clientId, ByteBuffer memory)
        {
            this.mRealProcess.ProcessData(clientId, memory);
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="memory"></param>
        /// <returns></returns>
        private ByteBuffer HisDataRequest(string clientId, ByteBuffer memory)
        {
            this.mHisProcess.ProcessData(clientId, memory);
            return null;
        }

        public override void OnClientConnected(string id, bool isConnected)
        {
            if(!isConnected)
            {
                mRealProcess.OnClientDisconnected(id);
                mHisProcess.OnClientDisconnected(id);
                mInfoProcess.OnClientDisconnected(id);
                ServiceLocator.Locator.Resolve<IRuntimeSecurity>().LogoutByClientId(id);
                lock(mClientIds)
                {
                    if(mClientIds.Contains(id))
                    {
                        mClientIds.Remove(id);
                    }
                }
                LoggerService.Service.Info("TagInfo", $"客户端 {id} 退出连接... 客户端在线总数:{mClientIds.Count}");
            }
            else
            {
                lock (mClientIds)
                {
                    if (!mClientIds.Contains(id))
                    {
                        mClientIds.Add(id);
                    }
                }
                LoggerService.Service.Info("TagInfo", $"客户端 {id} 建立连接... 客户端在线总数:{mClientIds.Count}");
            }
            base.OnClientConnected(id, isConnected);
        }

        //protected override void OnClientDisConnected(string id)
        //{
        //    mRealProcess.OnClientDisconnected(id);
        //    mHisProcess.OnClientDisconnected(id);
        //    mInfoProcess.OnClientDisconnected(id);
        //    ServiceLocator.Locator.Resolve<IRuntimeSecurity>().LogoutByClientId(id);
        //    base.OnClientDisConnected(id);
        //}

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
