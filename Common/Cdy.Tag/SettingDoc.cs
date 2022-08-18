//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/2/16 10:52:44.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class SettingDoc
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public int RealDataServerPort { get; set; } = 14330;

        /// <summary>
        /// 
        /// </summary>
        public bool EnableWebApi { get; set; } = true;

        /// <summary>
        /// 
        /// </summary>
        public bool EnableGrpcApi { get; set; } = false;

        /// <summary>
        /// 
        /// </summary>
        public bool EnableHighApi { get; set; } = false;

        /// <summary>
        /// 
        /// </summary>
        public bool EnableOpcServer { get; set; } = false;


        /// <summary>
        /// 历史数据工作模式
        /// </summary>
        public HisWorkMode HisWorkMode { get; set; } = HisWorkMode.Initiative;

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
    /// <summary>
    /// 工作模式
    /// </summary>
    public enum HisWorkMode
    {
        /// <summary>
        /// 主动
        /// </summary>
        Initiative,
        /// <summary>
        /// 被动
        /// </summary>
        Passive
    }
}
