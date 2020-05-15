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
    public interface IRealTagProducter
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
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        object GetTagValueForProductor(int id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        bool SetTagByGroup(string group, params object[] values);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, object value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag, object value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, object value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(List<int> ids, object value);



        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        bool SetPointValue(int id, params object[] values);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        bool SetPointValue(Tagbase tag, params object[] values);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="values"></param>
        /// <returns></returns>

        bool SetPointValue(List<int> ids, params object[] values);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        bool SetPointValue(List<Tagbase> tags, params object[] values);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        List<Tagbase> GetTagByLinkAddress(string address);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        List<int> GetTagIdsByLinkAddress(string address);

        /// <summary>
        /// 
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
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="valueChanged"></param>
        /// <param name="tagRegistor"></param>
        void SubscribeProducter(string name, ProducterValueChangedNotifyProcesser.ValueChangedDelagete valueChanged, Func<List<int>> tagRegistor);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        void UnSubscribeProducter(string name);


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
