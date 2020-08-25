﻿//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/8/17 15:50:59.
//  Version 1.0
//  种道洋
//==============================================================

using DBRuntime.Api;
using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace DBRuntime.RDDC
{
    public class WorStateServerProcess : RDDCServerProcessBase
    {

        #region ... Variables  ...
        /// <summary>
        /// 获取启动时间
        /// </summary>
        public const byte GetStartTime = 0;

        /// <summary>
        /// 切换到从机
        /// </summary>
        public const byte ChangeToStandby = 1;

        /// <summary>
        /// 切换同主机
        /// </summary>
        public const byte ChangeToPrimary = 2;

        /// <summary>
        /// 获取工作状态
        /// </summary>
        public const byte GetState = 3;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public override byte FunId => 1;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        protected override void ProcessSingleData(string client, IByteBuffer data)
        {
            byte cmd = data.ReadByte();
            switch (cmd)
            {
                case GetStartTime:
                    ProcessGetStartTime(client);
                    break;
                case ChangeToPrimary:
                    var re = ProcessSwichToPrimary();
                    Parent.AsyncCallback(client, ToByteBuffer(GetStartTime, re ? 1 : 0));
                    break;
                case ChangeToStandby:
                    re = ProcessSwichToStandby();
                    Parent.AsyncCallback(client, ToByteBuffer(GetStartTime, re ? 1 : 0));
                    break;
                case GetState:
                    var state = (byte)RDDCManager.Manager.CurrentState;
                    Parent.AsyncCallback(client, ToByteBuffer(GetStartTime, state));
                    break;
            }

            base.ProcessSingleData(client, data);
        }

        /// <summary>
        /// 
        /// </summary>
        private void ProcessGetStartTime(string client)
        {
            Parent.AsyncCallback(client, ToByteBuffer(GetStartTime, RDDCManager.Manager.StartTime.ToBinary()));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private bool ProcessSwichToPrimary()
        {
            return RDDCManager.Manager.SwitchTo(Cdy.Tag.WorkState.Primary);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private bool ProcessSwichToStandby()
        {
            return RDDCManager.Manager.SwitchTo(Cdy.Tag.WorkState.Standby);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}