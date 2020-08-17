//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/8/17 13:24:21.
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
    public enum WorkState
    {
        /// <summary>
        /// 未知
        /// </summary>
        Unknow,
        /// <summary>
        /// 工作机
        /// </summary>
        Primary,
        /// <summary>
        /// 备份机
        /// </summary>
        Standby,
        /// <summary>
        /// 切换中
        /// </summary>
        Switching
    }
}
