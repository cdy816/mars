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
    public interface IHisEngine3
    {
        /// <summary>
        /// 清空内存数据区域
        /// </summary>
        public void ClearMemoryHisData(HisDataMemoryBlockCollection3 memory);

        /// <summary>
        /// 
        /// </summary>
        public HisDataMemoryBlockCollection3 CurrentMemory { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public HisDataMemoryBlockCollection3 CurrentMergeMemory { get; }

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
