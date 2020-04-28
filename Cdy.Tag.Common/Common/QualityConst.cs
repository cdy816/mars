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
        /// 不可靠值
        /// </summary>
        Bad= 20,
        /// <summary>
        ///  初始状态
        /// </summary>
        Init = 63,
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
        Null= 255
    }
}
