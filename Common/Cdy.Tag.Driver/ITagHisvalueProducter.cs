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
        /// 设置变量的一组历史值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        bool SetTagHisValue(int id, List<TagValue> values);


        /// <summary>
        /// 设置变量的历史值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagHisValue(int id, TagValue value);

        /// <summary>
        /// 设置变量的历史值
        /// </summary>
        /// <param name="id">Id</param>
        /// <param name="time">时间</param>
        /// <param name="value">值</param>
        /// <param name="quality">质量</param>
        /// <returns></returns>
        bool SetTagHisValue<T>(int id, DateTime time, T value, byte quality);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagHisValue<T>(int id, DateTime time, byte quality, params T[] value);

        /// <summary>
        /// 设置变量的历史值
        /// </summary>
        /// <param name="id">Id</param>
        /// <param name="value">值</param>
        /// <returns></returns>
        bool SetTagHisValue<T>(int id, T value);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagHisValue<T>(int id, T value,byte quality);

        /// <summary>
        /// 设置一组变量的一组历史值
        /// </summary>
        /// <param name="values">值</param>
        /// <returns></returns>
        bool SetTagHisValues(Dictionary<int, List<TagValue>> values);


        /// <summary>
        /// 设置一组变量的历史值
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        bool SetTagHisValues(Dictionary<int, TagValue> values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        ///// <param name="timeUnit"></param>
        ///// <returns></returns>
        //bool SetTagHisValue(Dictionary<int, TagValue> values);

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
