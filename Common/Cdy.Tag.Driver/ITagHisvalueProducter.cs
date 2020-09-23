//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/9/23 14:43:31.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag.Driver
{
    /// <summary>
    /// 历史值驱动接口
    /// </summary>
    public interface ITagHisValueProduct
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 设置变量的历史值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="values"></param>
        /// <param name="timeUnit"></param>
        /// <returns></returns>
        bool SetTagHisValue(int id, List<TagValue> values, int timeUnit);

        /// <summary>
        /// 设置变量的历史值
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeUnit"></param>
        /// <returns></returns>
        bool SetTagHisValues(Dictionary<int, List<TagValue>> values, int timeUnit);

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
