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
    /// 值的拟合方式
    /// 0:取前一个值,1:取后一个值,2:取较近的值,3:线性插值
    /// </summary>
    public enum QueryValueMatchType
    {
        /// <summary>
        /// 取前一个值
        /// </summary>
        Previous,
        /// <summary>
        /// 取后一个值
        /// </summary>
        After,
        /// <summary>
        /// 取较近的值
        /// </summary>
        Closed,
        /// <summary>
        /// 线性插值
        /// </summary>
        Linear
    }

    /// <summary>
    /// 
    /// </summary>
    public enum NumberStatisticsType
    {
        /// <summary>
        /// 大于某个值的值
        /// </summary>
        GreatValue,
        /// <summary>
        /// 小于某个值的值
        /// </summary>
        LowValue,

        /// <summary>
        /// 是否有等于某个值的值
        /// </summary>
        EqualsValue,
        /// <summary>
        /// 最大值
        /// </summary>
        Max,
        /// <summary>
        /// 最小值
        /// </summary>
        Min,
        /// <summary>
        /// 平均值
        /// </summary>
        Avg,
        

    }


}
