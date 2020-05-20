//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/8 14:59:28.
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
    public interface IRealTagComsumer
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
        /// 获取变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        object GetTagValue(int id, out byte quality, out DateTime time,out byte valueType);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        object GetTagValue(string name, out byte quality, out DateTime time, out byte valueType);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValueForConsumer(int id, object value);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValueForConsumer(string name, object value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        List<int?> GetTagIdByName(List<string> name);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        unsafe void* GetDataRawAddr(int id);

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
