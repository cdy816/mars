//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 实时数据操作接口
    /// </summary>
    public interface IRealData
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

        //#region 数据下发接口

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        //void SetValue(int id, byte value);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, byte> values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        ///// <param name="qulity"></param>
        ///// <param name="time"></param>
        //void SetValue(int id, byte value, byte qulity, DateTime time);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, Tuple<byte, byte, DateTime>> values);


        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        //void SetValue(int id, short value);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, short> values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        ///// <param name="qulity"></param>
        ///// <param name="time"></param>
        //void SetValue(int id, short value, byte qulity, DateTime time);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, Tuple<short, byte, DateTime>> values);


        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        //void SetValue(int id, ushort value);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, ushort> values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        ///// <param name="qulity"></param>
        ///// <param name="time"></param>
        //void SetValue(int id, ushort value, byte qulity, DateTime time);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, Tuple<ushort, byte, DateTime>> values);



        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        //void SetValue(int id, int value);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, int> values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        ///// <param name="qulity"></param>
        ///// <param name="time"></param>
        //void SetValue(int id, int value, byte qulity, DateTime time);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, Tuple<int, byte, DateTime>> values);



        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        //void SetValue(int id, uint value);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, uint> values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        ///// <param name="qulity"></param>
        ///// <param name="time"></param>
        //void SetValue(int id, uint value, byte qulity, DateTime time);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, Tuple<uint, byte, DateTime>> values);


        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        //void SetValue(int id, long value);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, long> values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        ///// <param name="qulity"></param>
        ///// <param name="time"></param>
        //void SetValue(int id, long value, byte qulity, DateTime time);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, Tuple<long, byte, DateTime>> values);



        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        //void SetValue(int id, ulong value);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, ulong> values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        ///// <param name="qulity"></param>
        ///// <param name="time"></param>
        //void SetValue(int id, ulong value, byte qulity, DateTime time);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, Tuple<ulong, byte, DateTime>> values);


        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        //void SetValue(int id, float value);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, float> values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        ///// <param name="qulity"></param>
        ///// <param name="time"></param>
        //void SetValue(int id, float value, byte qulity, DateTime time);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, Tuple<float, byte, DateTime>> values);



        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        //void SetValue(int id, double value);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, double> values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        ///// <param name="qulity"></param>
        ///// <param name="time"></param>
        //void SetValue(int id, double value, byte qulity, DateTime time);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, Tuple<double, byte, DateTime>> values);


        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        //void SetValue(int id, DateTime value);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, DateTime> values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        ///// <param name="qulity"></param>
        ///// <param name="time"></param>
        //void SetValue(int id, DateTime value, byte qulity, DateTime time);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, Tuple<DateTime, byte, DateTime>> values);



        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        //void SetValue(int id, string value);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, string> values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="value"></param>
        ///// <param name="qulity"></param>
        ///// <param name="time"></param>
        //void SetValue(int id, string value, byte qulity, DateTime time);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        //void SetValue(Dictionary<int, Tuple<string, byte, DateTime>> values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="quality"></param>
        ///// <param name="time"></param>
        ///// <param name="values"></param>
        ///// <returns></returns>
        //bool SetPointValue(int id, byte quality, DateTime time, params object[] values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="quality"></param>
        ///// <param name="values"></param>
        ///// <returns></returns>
        //bool SetPointValue(int id, byte quality, params object[] values);

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="values"></param>
        ///// <returns></returns>
        //bool SetPointValue(int id, params object[] values);

        //#endregion

        #region 数据读取接口

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        byte? ReadByteValue(int id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        byte? ReadByteValue(int id, out byte qulity, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        short? ReadShortValue(int id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        short? ReadShortValue(int id, out byte qulity, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        int? ReadIntValue(int id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        int? ReadIntValue(int id, out byte qulity, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        long? ReadInt64Value(int id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        long? ReadInt64Value(int id, out byte qulity, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        double? ReadDoubleValue(int id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        double? ReadDoubleValue(int id, out byte qulity, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        float? ReadFloatValue(int id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        float? ReadFloatValue(int id, out byte qulity, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        DateTime? ReadDatetimeValue(int id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        DateTime? ReadDatetimeValue(int id, out byte qulity, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="encoding"></param>
        /// <returns></returns>
        string ReadStringValue(int id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="encoding"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        string ReadStringValue(int id, out byte qulity, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        IntPointData ReadIntPointValue(int id, out byte quality, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        UIntPointData ReadUIntPointValue(int id, out byte quality, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        IntPoint3Data ReadIntPoint3Value(int id, out byte quality, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        UIntPoint3Data ReadUIntPoint3Value(int id, out byte quality, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>

        LongPointData ReadLongPointValue(int id, out byte quality, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        ULongPointData ReadULongPointValue(int id, out byte quality, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        LongPoint3Data ReadLongPoint3Value(int id, out byte quality, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        ULongPoint3Data ReadULongPoint3Value(int id, out byte quality, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="z"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        bool ReadULongPoint3Value(int id, out ulong x, out ulong y, out ulong z, out byte quality, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="z"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        bool ReadLongPoint3Value(int id, out long x, out long y, out long z, out byte quality, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        bool ReadULongPointValue(int id, out ulong x, out ulong y, out byte quality, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        bool ReadLongPointValue(int id, out long x, out long y, out byte quality, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="z"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        bool ReadUIntPoint3Value(int id, out uint x, out uint y, out uint z, out byte quality, out DateTime time);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="z"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>

        bool ReadIntPoint3Value(int id, out int x, out int y, out int z, out byte quality, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>

        bool ReadUIntPointValue(int id, out uint x, out uint y, out byte quality, out DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>

        bool ReadIntPointValue(int id, out int x, out int y, out byte quality, out DateTime time);

        #endregion

        #region Common Read/Write
        /// <summary>
        /// 读取变量的值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        object Read(int id,out byte quality ,out DateTime time);

        /// <summary>
        /// 写入值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        bool Write(int id,object value,DateTime time,byte quality);

        #endregion
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
