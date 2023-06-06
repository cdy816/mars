//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/10/12 9:12:46.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 从内存中查询数据
    /// </summary>
    public interface IHisQueryFromMemory
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        bool CheckTime(long id, DateTime time,out bool isgreat);

        /// <summary>
        /// 
        /// </summary>
        void LockMemoryFile();

        /// <summary>
        /// 
        /// </summary>
        void UnLockMemoryFile();
        /// <summary>
        /// 内存历史时间范围
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        Tuple<DateTime, DateTime> GetMemoryTimer(long id);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        DateTime GetStartMemoryTime(long id);

        /// <summary>
        /// 内存区的第一个值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        object GetStartValue<T>(long id, out DateTime time, out byte quality);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValue<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result,QueryContext context,out DateTime timelimit);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValue<T>(int id, DateTime startTime, DateTime endTime, HisQueryResult<T> result);
    }


}
