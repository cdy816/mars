//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 质量戳常量
    /// </summary>
    public enum QualityConst
    {
        /// <summary>
        /// 好
        /// </summary>
        Good= 0,
        /// <summary>
        /// 不可靠值,20~40 范围内都是坏值
        /// 其他都是好值
        /// </summary>
        Bad= 20,
        /// <summary>
        /// 超出范围
        /// </summary>
        OutOfRang = 30,
        /// <summary>
        /// 
        /// </summary>
        BelowLowerLimit = 31,
        /// <summary>
        /// 
        /// </summary>
        AboveUpperLimit = 32,
        /// <summary>
        /// 离线
        /// </summary>
        Offline=33,
        /// <summary>
        ///  初始状态
        /// </summary>
        Init = 63,
        
        /// <summary>
        /// 系统关闭
        /// </summary>
        Close=64,

        /// <summary>
        /// 起始值,用于系统第一次运行时，软件强制记录的值的标识
        /// </summary>
        Start=65,

        /// <summary>
        /// 无数据
        /// </summary>
        Tick =253,
        /// <summary>
        /// 保持和前置一样
        /// </summary>
        KeepPreview= 254,
        /// <summary>
        /// 空值
        /// </summary>
        Null= 255,
    }
}
