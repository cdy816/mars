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
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<bool> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<byte> result);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<short> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<ushort> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<int> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<uint> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<ulong> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<long> result);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<float> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<double> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<DateTime> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<string> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<bool> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<byte> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<short> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<ushort> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<int> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<uint> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<long> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<ulong> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<float> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<double> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<DateTime> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<string> result);
    }
}
