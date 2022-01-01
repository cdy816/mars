//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/28 15:21:27.
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
    public interface IHisQuery
    {

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValue<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValueByUTCTime<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result);



        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        HisQueryResult<T> ReadValue<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        HisQueryResult<T> ReadValueByUTCTime<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValue<T>(int id, DateTime startTime, DateTime endTime, HisQueryResult<T> result);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime, HisQueryResult<T> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        HisQueryResult<T>  ReadAllValue<T>(int id, DateTime startTime, DateTime endTime);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        HisQueryResult<T> ReadAllValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime);

        /// <summary>
        /// 查找数字型变量的的等于指定值的时间
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">变量Id </param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="para">比较的参数</param>
        /// <param name="type">类型</param>
        /// <returns></returns>
       Tuple< DateTime,object> FindNumberTagValue<T>(int id, DateTime startTime, DateTime endTime, double para,double para2, NumberStatisticsType type);

        /// <summary>
        /// 查找数字型变量的的等于指定值的时间集合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">变量Id </param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="para"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        Dictionary<DateTime,object> FindNumberTagValues<T>(int id, DateTime startTime, DateTime endTime, double para, double para2, NumberStatisticsType type);

        /// <summary>
        /// 查找数字型变量的值等于指定值得保持时间
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">变量Id </param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="para"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        double FindNumberTagValueDuration<T>(int id, DateTime startTime, DateTime endTime, double para,double para2, NumberStatisticsType type);

        /// <summary>
        /// 查找数字型变量的值的最大值、最小值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">变量Id </param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="type"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        double FindNumberTagMaxMinValue<T>(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type, out IEnumerable<DateTime> time);

        /// <summary>
        /// 获取数字型变量的值在一段时间内的平均值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">变量Id</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <returns></returns>
        double FindNumberTagAvgValue<T>(int id, DateTime startTime, DateTime endTime);

        /// <summary>
        /// 查找数字型变量的的等于指定值的时间
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">变量Id </param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="para">比较的参数</param>
        /// <param name="para2">相等时，区间</param>
        /// <param name="type">类型</param>
        /// <returns></returns>
        Tuple<DateTime, object> FindNumberTagValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime, double para, double para2, NumberStatisticsType type);

        /// <summary>
        /// 查找数字型变量的的等于指定值的时间集合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">变量Id </param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="para">比较的值</param>
        /// <param name="para2">比较的值区间</param>
        /// <param name="type"></param>
        /// <returns></returns>
        Dictionary<DateTime,object> FindNumberTagValuesByUTCTime<T>(int id, DateTime startTime, DateTime endTime, double para, double para2, NumberStatisticsType type);

        /// <summary>
        /// 查找数字型变量的值等于指定值得保持时间
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">变量Id </param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="para"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        double FindNumberTagValueDurationByUTCTime<T>(int id, DateTime startTime, DateTime endTime, double para, double para2, NumberStatisticsType type);

        /// <summary>
        /// 查找数字型变量的值的最大值、最小值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">变量Id </param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="type"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        double FindNumberTagMaxMinValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type, out IEnumerable<DateTime> time);

        /// <summary>
        /// 获取数字型变量的值在一段时间内的平均值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">变量Id</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <returns></returns>
        double FindNumberTagAvgValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime);

        /// <summary>
        /// 查找指定的值所持续的时间
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">变量Id </param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="para">比较的值</param>
        /// <returns></returns>
        double FindNoNumberTagValueDuration<T>(int id, DateTime startTime, DateTime endTime, object para);

        /// <summary>
        /// 查找指定的值
        /// 多个值时，返回第一个
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para">比较的值</param>
        /// <returns>时间</returns>
        DateTime FindNoNumberTagValue<T>(int id, DateTime startTime, DateTime endTime, object para);

        /// <summary>
        /// 查找指定的值
        /// 返回所有的值的时间
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para">比较的值</param>
        /// <returns></returns>
        List<DateTime> FindNoNumberTagValues<T>(int id, DateTime startTime, DateTime endTime, object para);


        /// <summary>
        /// 查找指定的值所持续的时间
        /// 通过UTC时间进行查询
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para">比较的值</param>
        /// <returns></returns>
        double FindNoNumberTagValueDurationByUTCTime<T>(int id, DateTime startTime, DateTime endTime, object para);

        /// <summary>
        /// 查找指定的值
        /// 多个值时，返回第一个
        /// 通过UTC时间进行查询
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para">比较的值</param>
        /// <returns>时间</returns>
        DateTime FindNoNumberTagValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime, object para);

        /// <summary>
        /// 查找指定的值
        /// 返回所有的值的时间
        /// 通过UTC时间进行查询
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para">比较的值</param>
        /// <returns></returns>
        List<DateTime> FindNoNumberTagValuesByUTCTime<T>(int id, DateTime startTime, DateTime endTime, object para);



        /// <summary>
        /// 读取某个时间段内，值类型变量的统计信息
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        NumberStatisticsQueryResult ReadNumberStatistics(int id, DateTime startTime, DateTime endTime);


        /// <summary>
        /// 读取某个时间段（UTC时间）内，值类型变量的统计信息
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        NumberStatisticsQueryResult ReadNumberStatisticsByUTCTime(int id, DateTime startTime, DateTime endTime);

        /// <summary>
        /// 读取指定时间点的，值类型变量的统计信息
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="result"></param>
        NumberStatisticsQueryResult ReadNumberStatistics(int id, IEnumerable<DateTime> times);

        /// <summary>
        /// 读取指定时间点（UTC时间）的，值类型变量的统计信息
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="result"></param>
        NumberStatisticsQueryResult ReadNumberStatisticsByUTCTime(int id, IEnumerable<DateTime> times);
    }
}
