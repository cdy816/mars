//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/2/13 14:07:00.
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
    public interface IHisEngine
    {
        /// <summary>
        /// 清空内存数据区域
        /// </summary>
        public void ClearMemoryHisData(MemoryBlock memory);
    }
}
