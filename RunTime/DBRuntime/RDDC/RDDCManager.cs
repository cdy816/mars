﻿//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/8/17 16:34:14.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using DBRuntime.RDDC;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DBRuntime
{
    public class RDDCManager
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static RDDCManager Manager = new RDDCManager();

        private RDDCDataService mServer;

        private RDDCClient mClient;

        private DataSync mSync;

        private bool mIsInited = false;

        private object mLockObj = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        public RDDCManager()
        {

        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public bool EnableRDDC { get; set; }

        /// <summary>
        /// 当前状态
        /// </summary>
        public WorkState CurrentState { get; set; } = WorkState.Unknow;

        /// <summary>
        /// 
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// 服务端口
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// 备机IP
        /// </summary>
        public string RemoteIp { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Func<WorkState,bool> SwitchWorkStateAction { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="databaseName"></param>
        public void Load(string databaseName)
        {
            string spath = PathHelper.helper.GetDataPath(databaseName, "RDDC.cfg");
            if(System.IO.File.Exists(spath))
            {
                XElement xx = XElement.Load(spath);
                EnableRDDC = bool.Parse(xx.Attribute("Enable")?.Value);
                this.Port = int.Parse(xx.Attribute("Port")?.Value);
                this.RemoteIp = xx.Attribute("RemoteIp")?.Value;
            }
           
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            mIsInited = false;
            if (EnableRDDC)
            {
                mServer = new RDDCDataService();
                mServer.Start(Port);

                mClient = new RDDCClient();
                mClient.Connect(RemoteIp, Port);
                mClient.PropertyChanged += MClient_PropertyChanged;

                mSync = new DataSync() { Client = mClient };
                mSync.Start();
                CheckWorkState();
            }
            else
            {
                CurrentState = WorkState.Primary;
            }
            mIsInited = true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void MClient_PropertyChanged(object sender, System.ComponentModel.PropertyChangedEventArgs e)
        {
            if(mIsInited)
            {
                Task.Run(ProcessClientConnectChanged);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            mSync.Stop();
            mServer.Stop();
            mClient.PropertyChanged -= MClient_PropertyChanged;
            mClient.Close();
        }

        /// <summary>
        /// 
        /// </summary>
        private void ProcessClientConnectChanged()
        {
            if (!mClient.IsConnected)
            {
                if(CurrentState != WorkState.Primary)
                SwitchTo(WorkState.Primary);
            }
            else
            {
                var state = mClient.GetWorkState();
                if ((state == WorkState.Primary && this.CurrentState == WorkState.Primary) || (state == WorkState.Standby && this.CurrentState == WorkState.Standby))
                {
                    var time = mClient.GetStartTime();
                    var ss = time > this.StartTime ? WorkState.Primary : WorkState.Standby;
                    SwitchTo(ss);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void CheckWorkState()
        {
            int count = 0;
            while(!mClient.IsConnected)
            {
                Thread.Sleep(100);
                count++;
                if (count > 30) break;
            }

            if(!mClient.IsConnected)
            {
                CurrentState = WorkState.Primary;
            }
            else
            {
                var time = mClient.GetStartTime();
                if(time!=null)
                {
                    CurrentState = time > this.StartTime ? WorkState.Primary : WorkState.Standby;
                }
                else
                {
                    CurrentState = WorkState.Standby;
                }
            }
        }

        /// <summary>
        /// 手动切换工作状态
        /// </summary>
        /// <param name="state"></param>
        public bool ManualSwitchTo(WorkState state)
        {
            if(mClient.IsConnected)
            {
                if (state == WorkState.Standby)
                {
                    if(mClient.SwitchToPrimary().Value)
                    {
                       return SwitchTo(WorkState.Standby);
                    }
                    else
                    {
                        return false;
                    }
                }
                else
                {
                    if(mClient.SwitchToStandby().Value)
                    {
                        return SwitchTo(WorkState.Primary);
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            else
            {
                if(state == WorkState.Standby)
                {
                    LoggerService.Service.Warn("RDDCManager", "remote is offline,local machine must be primary!");
                    return false;
                }
                else
                {
                    return SwitchTo(state);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="state"></param>
        /// <returns></returns>
        public bool SwitchTo(WorkState state)
        {
            lock (mLockObj)
            {
                var olds = CurrentState;
                CurrentState = WorkState.Switching;
                if (SwitchWorkStateAction != null)
                {
                    if (SwitchWorkStateAction(state))
                    {
                        CurrentState = state;
                        return true;
                    }
                    else
                    {
                        CurrentState = olds;
                        return false;
                    }
                }
                CurrentState = state;
                return true;
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}