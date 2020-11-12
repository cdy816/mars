//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/10/20 16:42:26.
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
    public interface IHisTagQuery
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        IEnumerable<HisTag> ListAllTags();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        IEnumerable<HisTag> ListAllDriverRecordTags();
        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        HisTag GetHisTagById(int id);

    }
}
