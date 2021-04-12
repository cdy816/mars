//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/27 9:02:43.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace SpiderDriver
{
    /// <summary>
    /// 
    /// </summary>
    public class DataService: SocketServer
    {

        #region ... Variables  ...
        private TagInfoServerProcess mInfoProcess;
        private RealDataServerProcess mRealProcess;
        private HisDataServerProcess mHisProcess;

        private bool mIsStarted = false;

        //private IByteBuffer mAsyncCalldata;

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
        public override string Name => "SpiderDriveDataService";
        #endregion ...Properties...

        #region ... Methods    ...
        /// <summary>
        /// 
        /// </summary>
        private void RegistorInit()
        {
            this.RegistorFunCallBack(APIConst.TagInfoRequestFun, TagInfoRequest);
            this.RegistorFunCallBack(APIConst.RealValueFun, RealDataRequest);
            this.RegistorFunCallBack(APIConst.HisValueFun, HisDataRequest);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        protected override void OnClientConnected(string id)
        {
            mRealProcess?.OnClientConnected(id);
            mInfoProcess?.OnClientConnected(id);
            mHisProcess?.OnClientConnected(id);
            base.OnClientConnected(id);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        protected override void OnClientDisConnected(string id)
        {
            mRealProcess?.OnClientDisconnected(id);
            mInfoProcess?.OnClientDisconnected(id);
            mHisProcess?.OnClientDisconnected(id);
            base.OnClientDisConnected(id);
        }


        /// <summary>
        /// 
        /// </summary>
        public void PushRealDatatoClient(string clientId, byte[] value)
        {
            this.SendData(clientId, APIConst.PushDataChangedFun, value, value.Length);
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
        public void AsyncCallback(string clientId, byte fun, byte[] value, int len)
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

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <returns></returns>
        //private IByteBuffer GetAsyncData()
        //{
        //    mAsyncCalldata = BufferManager.Manager.Allocate(APIConst.AysncReturn, 4);
        //    mAsyncCalldata.WriteInt(0);
        //    return mAsyncCalldata;
        //}

        /// <summary>
        /// 
        /// </summary>
        private IByteBuffer TagInfoRequest(string clientId, IByteBuffer memory)
        {
            if (mIsStarted&& mInfoProcess!=null)
            {
                mInfoProcess.ProcessData(clientId, memory);
            }
            else
            {
                LoggerService.Service.Info("Spider Driver", "Spider driver is not started!");
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="memory"></param>
        /// <returns></returns>
        private IByteBuffer RealDataRequest(string clientId, IByteBuffer memory)
        {
            if (mIsStarted && mRealProcess != null)
            {
                this.mRealProcess.ProcessData(clientId, memory);
            }
            else
            {
                LoggerService.Service.Info("Spider Driver", "Spider driver is not started!");
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="memory"></param>
        /// <returns></returns>
        private IByteBuffer HisDataRequest(string clientid,IByteBuffer memory)
        {
            if (mIsStarted && mHisProcess != null)
            {
                this.mHisProcess.ProcessData(clientid, memory);
            }
            else
            {
                LoggerService.Service.Info("Spider Driver", "Spider driver is not started!");
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="isRealChanged"></param>
        /// <param name="isHisChanged"></param>
        public void NotifyDatabaseChangd(bool isRealChanged,bool isHisChanged)
        {
            this.mInfoProcess.NotifyDatabaseChanged(isRealChanged, isHisChanged);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="port"></param>
        protected override void StartInner(int port)
        {
            try
            {
               
                mRealProcess = new RealDataServerProcess() { Parent = this };
                mRealProcess.Init();
                mInfoProcess = new TagInfoServerProcess() { Parent = this };
                mHisProcess = new HisDataServerProcess() { Parent = this };
                mRealProcess.Start();
                mInfoProcess.Start();
                mHisProcess.Start();
                mIsStarted = true;
                LoggerService.Service.Info("Spider Driver", "Start Sucessfull!");

            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("Spider Driver", ex.Message);
            }
            base.StartInner(port);

        }

        /// <summary>
        /// 
        /// </summary>
        public override void Stop()
        {
            base.Stop();
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

            if (mHisProcess != null)
            {
                mHisProcess.Stop();
                mHisProcess.Dispose();
                mHisProcess = null;
            }
        }
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
