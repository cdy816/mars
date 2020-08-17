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

namespace DBRuntime.RDDC
{
    public class ApiFunConst
    {

        /// <summary>
        /// 
        /// </summary>
        public const byte WorkStateFun = 1;

        /// <summary>
        /// 
        /// </summary>
        public const byte RealDataSyncFun = 2;



        public const byte AysncReturn = byte.MaxValue;
    }

    /// <summary>
    /// 
    /// </summary>
    public class RDDCDataService: SocketServer
    {

        #region ... Variables  ...

        private IByteBuffer mAsyncCalldata;

        private Dictionary<byte, RDDCServerProcessBase> mProcess = new Dictionary<byte, RDDCServerProcessBase>();

        private WorStateServerProcess mWorkStateProcess;
        private RealDataSyncServerProcess mRealDataSyncProcess;

        /// <summary>
        /// 
        /// </summary>
        public static RDDCDataService Service = new RDDCDataService();

        /// <summary>
        /// 
        /// </summary>
        public override string Name => "RDDCDataService";

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public RDDCDataService()
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
            mWorkStateProcess = new WorStateServerProcess() { Parent = this };
            mRealDataSyncProcess = new RealDataSyncServerProcess() { Parent = this };
            mWorkStateProcess.Start();
            mRealDataSyncProcess.Start();
            base.StartInner(port);
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Stop()
        {
            base.Stop();
            if (mWorkStateProcess != null)
            {
                mWorkStateProcess.Stop();
                mWorkStateProcess.Dispose();
                mWorkStateProcess = null;
            }
            if (mRealDataSyncProcess != null)
            {
                mRealDataSyncProcess.Stop();
                mRealDataSyncProcess.Dispose();
                mRealDataSyncProcess = null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
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
            this.RegistorFunCallBack(ApiFunConst.WorkStateFun, WorkStateProcess);
            this.RegistorFunCallBack(ApiFunConst.RealDataSyncFun, RealDataSyncProcess);
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
        /// <param name="clientId"></param>
        /// <param name="memory"></param>
        /// <returns></returns>
        private IByteBuffer WorkStateProcess(string clientId, IByteBuffer memory)
        {
            this.mWorkStateProcess.ProcessData(clientId, memory);
            return GetAsyncData();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="memory"></param>
        /// <returns></returns>
        private IByteBuffer RealDataSyncProcess(string clientId, IByteBuffer memory)
        {
            this.mRealDataSyncProcess.ProcessData(clientId, memory);
            return GetAsyncData();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        protected override void OnClientDisConnected(string id)
        {
            mRealDataSyncProcess.OnClientDisconnected(id);
            this.mWorkStateProcess.OnClientDisconnected(id);
            ServiceLocator.Locator.Resolve<IRuntimeSecurity>().LogoutByClientId(id);
            base.OnClientDisConnected(id);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
