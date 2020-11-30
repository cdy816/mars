//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/2/13 14:07:00.
//  Version 1.0
//  种道洋
//==============================================================
using DBRuntime.His;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public interface IHisEngine2
    {
        /// <summary>
        /// 清空内存数据区域
        /// </summary>
        public void ClearMemoryHisData(HisDataMemoryBlockCollection memory);

        /// <summary>
        /// 
        /// </summary>
        public HisDataMemoryBlockCollection CurrentMemory { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public HisDataMemoryBlockCollection CurrentMergeMemory { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public HisRunTag GetHisTag(int id);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<HisRunTag> ListAllTags();

    }
}
