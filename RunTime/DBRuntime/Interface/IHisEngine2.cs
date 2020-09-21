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
        /// <param name="id"></param>
        /// <returns></returns>
        public HisRunTag GetHisTag(int id);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<HisRunTag> ListAllTags();

        /// <summary>
        /// 手动插入历史数据
        /// </summary>
        /// <param name="id"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool InsertHisValues(long id,SortedDictionary<DateTime, object> values);
    }
}
