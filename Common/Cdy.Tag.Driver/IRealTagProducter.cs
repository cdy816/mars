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


        #region Set by Id

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref bool value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id,ref byte value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref short value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref ushort value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref int value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref uint value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref long value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref ulong value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref float value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref double value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, string value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref DateTime value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref IntPointData value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref UIntPointData value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref IntPoint3Data value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref UIntPoint3Data value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref LongPointData value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref ULongPointData value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref LongPoint3Data value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, ref ULongPoint3Data value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id">Id</param>
        /// <param name="value">值</param>
        /// <param name="time">时间</param>
        /// <param name="quality">质量</param>
        /// <returns></returns>
        bool SetTagValue<T>(int id,ref T value,DateTime time,byte quality);

        /// <summary>
        /// 将一组变量设置成一个值
        /// </summary>
        /// <param name="ids">变量Id集合</param>
        /// <param name="value">值</param>
        /// <returns></returns>
        bool SetTagValue<T>(List<int> ids, T value, byte quality);

        /// <summary>
        /// 设置变量的质量戳
        /// </summary>
        /// <param name="id">Id</param>
        /// <param name="quality">质量戳</param>
        /// <param name="time">时间</param>
        void SetTagQuality(int id, byte quality, DateTime time);

        /// <summary>
        /// 获取变量的时间戳
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        DateTime GetTagUpdateTime(int id);

        #endregion

        #region Set by Tag instance

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="tag">变量实例</param>
        /// <param name="value">值</param>
        /// <param name="time">时间</param>
        /// <param name="quality">质量</param>
        /// <returns></returns>
        bool SetTagValue<T>(Tagbase tag,ref T value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="tag">变量实例</param>
        /// <param name="value">值</param>
        /// <param name="quality">质量</param>
        /// <returns></returns>
        bool SetTagValue<T>(Tagbase tag,ref T value, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref bool value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref byte value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref short value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref ushort value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref int value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref uint value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref long value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref ulong value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref float value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref double value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag, string value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref DateTime value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref IntPointData value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref UIntPointData value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref IntPoint3Data value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref UIntPoint3Data value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref LongPointData value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref ULongPointData value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref LongPoint3Data value, DateTime time, byte quality);

        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(Tagbase tag,ref ULongPoint3Data value, DateTime time, byte quality);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref double value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref float value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref bool value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref byte value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref short value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref ushort value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref int value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref uint value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref long value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref ulong value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref DateTime value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, string value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref IntPointData value, byte quality);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref UIntPointData value, byte quality);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref UIntPoint3Data value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref IntPoint3Data value, byte quality);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref LongPointData value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref ULongPointData value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref LongPoint3Data value, byte quality);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool SetTagValue(List<Tagbase> tag, ref ULongPoint3Data value, byte quality);



        /// <summary>
        /// 设置变量的质量戳
        /// </summary>
        /// <param name="tag">变量实例</param>
        /// <param name="quality">质量戳</param>
        /// <param name="time">时间</param>
        void SetTagQuality(Tagbase tag, byte quality, DateTime time);
        #endregion

        /// <summary>
        /// 通过变量的连接地址匹配指定字符串获取变量实例
        /// </summary>
        /// <param name="address">连接地址</param>
        /// <returns></returns>
        List<Tagbase> GetTagByLinkAddressStartHeadString(string address);

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
        /// 根据变量的名称获取ID
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        List<int?> GetTagIdByName(List<string> name);

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
