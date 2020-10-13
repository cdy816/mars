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
        /// 获取手工记录的变量的Id
        /// </summary>
        /// <returns></returns>
        List<int> GetManualRecordTagId();

        /// <summary>
        /// 获取变量的记录类型
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        Dictionary<int, RecordType> GetTagRecordType(List<int> id);

        /// <summary>
        /// 设置变量的历史值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="values"></param>
        /// <param name="timeUnit"></param>
        /// <returns></returns>
        bool SetTagHisValue(int id, List<TagValue> values, int timeUnit);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="timeUnit"></param>
        /// <returns></returns>
        bool SetTagHisValue(int id, TagValue value, int timeUnit);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="timeUnit"></param>
        /// <returns></returns>
        bool SetTagHisValue(int id, DateTime time, object value, byte quality, int timeUnit);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="timeUnit"></param>
        /// <returns></returns>
        bool SetTagHisValue(int id, object value, int timeUnit);

        /// <summary>
        /// 设置变量的历史值
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeUnit"></param>
        /// <returns></returns>
        bool SetTagHisValues(Dictionary<int, List<TagValue>> values, int timeUnit);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeUnit"></param>
        /// <returns></returns>
        bool SetTagHisValues(Dictionary<int, TagValue> values, int timeUnit);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeUnit"></param>
        /// <returns></returns>
        bool SetTagHisValue(Dictionary<int, TagValue> values, int timeUnit);

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
