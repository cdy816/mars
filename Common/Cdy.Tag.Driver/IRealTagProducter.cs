//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/3/28 21:02:59.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag.Driver
{
    /// <summary>
    /// 
    /// </summary>
    public interface IRealTagProduct
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
        /// 通过变量ID获取变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        object GetTagValueForProductor(int id);

        /// <summary>
        /// 通过变量组设置值
        /// </summary>
        /// <param name="group"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        bool SetTagByGroup(string group, params object[] values);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id">Id</param>
        /// <param name="value">值</param>
        /// <returns></returns>
        bool SetTagValue(int id, object value);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id">Id</param>
        /// <param name="value">值</param>
        /// <param name="time">时间</param>
        /// <param name="quality">质量</param>
        /// <returns></returns>
        bool SetTagValue(int id, object value,DateTime time,byte quality);


        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id">Id</param>
        /// <param name="value">值</param>
        /// <param name="quality">质量</param>
        /// <returns></returns>
        bool SetTagValue(int id, object value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="tag">变量实例</param>
        /// <param name="value">值</param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag, object value);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="tag">变量实例</param>
        /// <param name="value">值</param>
        /// <param name="time">时间</param>
        /// <param name="quality">质量</param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag, object value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="tag">变量实例</param>
        /// <param name="value">值</param>
        /// <param name="quality">质量</param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag, object value, byte quality);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, object value);

        /// <summary>
        /// 将一组变量设置成一个值
        /// </summary>
        /// <param name="ids">变量Id集合</param>
        /// <param name="value">值</param>
        /// <returns></returns>
        bool SetTagValue(List<int> ids, object value);



        /// <summary>
        /// 设置Point类型的变量的值
        /// </summary>
        /// <param name="id">Id</param>
        /// <param name="values">值列表</param>
        /// <returns></returns>
        bool SetPointValue(int id, params object[] values);

        /// <summary>
        /// 设置Point类型的变量的值
        /// </summary>
        /// <param name="tag">变量实例</param>
        /// <param name="values">值列表</param>
        /// <returns></returns>
        bool SetPointValue(Tagbase tag, params object[] values);


        /// <summary>
        /// 设置一组Point类型的变量的值为同一个值
        /// </summary>
        /// <param name="ids">Id集合</param>
        /// <param name="values">值列表</param>
        /// <returns></returns>

        bool SetPointValue(List<int> ids, params object[] values);

        /// <summary>
        /// 设置一组Point类型的变量的值为同一个值
        /// </summary>
        /// <param name="tags">变量实例集合</param>
        /// <param name="values">值列表</param>
        /// <returns></returns>
        bool SetPointValue(List<Tagbase> tags, params object[] values);

        /// <summary>
        /// 通过变量的连接地址获取变量实例
        /// </summary>
        /// <param name="address">连接地址</param>
        /// <returns></returns>
        List<Tagbase> GetTagByLinkAddress(string address);

        /// <summary>
        /// 通过变量的连接地址获取变量Id
        /// </summary>
        /// <param name="address">连接地址</param>
        /// <returns></returns>
        List<int> GetTagIdsByLinkAddress(string address);

        /// <summary>
        /// 通过变量ID获取变量实例
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        Tagbase GetTagById(int id);

        /// <summary>
        /// 通过连接地址，获取变量ID
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        Dictionary<string, List<int>> GetTagsIdByLinkAddress(List<string> address);

        /// <summary>
        /// 通过连接地址,获取变量
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        Dictionary<string, List<Tagbase>> GetTagsByLinkAddress(List<string> address);

        /// <summary>
        /// 订购值改变通知
        /// </summary>
        /// <param name="name"></param>
        /// <param name="valueChanged"></param>
        /// <param name="tagRegistor"></param>
        void SubscribeValueChangedForProducter(string name, ProducterValueChangedNotifyProcesser.ValueChangedDelagete valueChanged, Func<List<int>> tagRegistor);

        /// <summary>
        /// 取消订购值改变通知
        /// </summary>
        /// <param name="name"></param>
        void UnSubscribeValueChangedForProducter(string name);

        /// <summary>
        /// 提交值
        /// </summary>
        void SubmiteNotifyChanged();
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
