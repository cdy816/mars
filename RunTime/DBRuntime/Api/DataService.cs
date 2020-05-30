//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/14 10:22:00.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace DBRuntime.Api
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
    public class DataService: SocketServer
    {

        #region ... Variables  ...

        private IByteBuffer mAsyncCalldata;

        private Dictionary<byte, ServerProcessBase> mProcess = new Dictionary<byte, ServerProcessBase>();

        private HisDataServerProcess mHisProcess;
        private RealDataServerProcess mRealProcess;
        private TagInfoServerProcess mInfoProcess;

        /// <summary>
        /// 
        /// </summary>
        public static DataService Service = new DataService();

        /// <summary>
        /// 
        /// </summary>
        public override string Name => "ConsumeDataService";

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

        #endregion ...Properties...

        #region ... Methods    ...


        /// <summary>
        /// 
        /// </summary>
        /// <param name="port"></param>
        protected override void StartInner(int port)
        {
            mHisProcess = new HisDataServerProcess() { Parent = this };
            mRealProcess = new RealDataServerProcess() { Parent = this };
            mInfoProcess = new TagInfoServerProcess() { Parent = this };
            mHisProcess.Start();
            mRealProcess.Start();
            mInfoProcess.Start();
            base.StartInner(port);

        }

        /// <summary>
        /// 
        /// </summary>
        public override void Stop()
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

        private IByteBuffer GetAsyncData()
        {
            mAsyncCalldata = BufferManager.Manager.Allocate(ApiFunConst.AysncReturn, 4);
            mAsyncCalldata.WriteInt(0);
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

        /// <summary>
        /// 
        /// </summary>
        public void PushRealDatatoClient(string clientId,byte[] value)
        {
            this.SendData(clientId, Api.ApiFunConst.RealDataPushFun, value,value.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="value"></param>
        public void PushRealDatatoClient(string clientId, IByteBuffer value)
        {
            this.SendData(clientId, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="fun"></param>
        /// <param name="value"></param>
        public void AsyncCallback(string clientId,byte fun, byte[] value,int len)
        {
            this.SendData(clientId, fun, value, len);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        public void AsyncCallback(string clientId, IByteBuffer data)
        {
            this.SendData(clientId, data);
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
            this.SendData(clientId, fun, value, len);
        }

        /// <summary>
        /// 
        /// </summary>
        private IByteBuffer TagInfoRequest(string clientId, IByteBuffer memory)
        {
            mInfoProcess.ProcessData(clientId,memory);
            return GetAsyncData();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="memory"></param>
        /// <returns></returns>
        private IByteBuffer ReadDataRequest(string clientId, IByteBuffer memory)
        {
            this.mRealProcess.ProcessData(clientId, memory);
            return GetAsyncData();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="memory"></param>
        /// <returns></returns>
        private IByteBuffer HisDataRequest(string clientId, IByteBuffer memory)
        {
            this.mHisProcess.ProcessData(clientId, memory);
            return GetAsyncData();
        }


        protected override void OnClientDisConnected(string id)
        {
            mRealProcess.OnClientDisconnected(id);
            mHisProcess.OnClientDisconnected(id);
            mInfoProcess.OnClientDisconnected(id);
            ServiceLocator.Locator.Resolve<IRuntimeSecurity>().LogoutByClientId(id);
            base.OnClientDisConnected(id);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
