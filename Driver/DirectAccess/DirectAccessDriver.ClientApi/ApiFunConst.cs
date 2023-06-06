//==============================================================
//  Copyright (C) 2022  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/1/2 11:00:57.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace DirectAccessDriver.ClientApi
{
    public class ApiFunConst
    {
        /// <summary>
        /// 
        /// </summary>
        public const byte LoginFun = 1;

        /// <summary>
        /// 
        /// </summary>
        public const byte Login2Fun = 11;

        /// <summary>
        /// 
        /// </summary>
        public const byte RealValueFun = 1;

        /// <summary>
        /// 
        /// </summary>
        public const byte AysncValueBack = 81;

        /// <summary>
        /// 
        /// </summary>
        public const byte HisValueFun = 6;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagHisValue = 61;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagHisValue2 = 62;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetAreaTagHisValue = 63;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetAreaTagHisValue2 = 64;


        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagValueFun = 11;


        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagValueAndQualityFun = 12;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagValueAndQualityFunAsync = 82;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagRealAndHisValueFun = 13;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagRealAndHisValueFunAsync = 83;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagValueTimeAndQualityFun = 14;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagValueTimeAndQualityFunAsync = 84;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagRealAndHisValueWithTimeFun = 15;


        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagRealAndHisValueWithTimeFunAsync = 85;


        /// <summary>
        /// 通过变量名设置实时历史值
        /// </summary>
        public const byte SetTagRealAndHisValueWithTagNameFun = 86;

        /// <summary>
        /// 通过变量名设置实时历史值,并带时间戳
        /// </summary>
        public const byte SetTagRealAndHisValueTimerWithTagNameFun = 87;

        /// <summary>
        /// 
        /// </summary>
        public const byte PushDataChangedFun = 3;

        /// <summary>
        /// 
        /// </summary>
        public const byte TagInfoRequestFun = 4;

        /// <summary>
        /// 
        /// </summary>
        public const byte DatabaseChangedNotify = 5;

        /// <summary>
        /// 
        /// </summary>
        public const byte RegistorTag = 41;

        /// <summary>
        /// 
        /// </summary>
        public const byte ClearRegistorTag = 42;

        /// <summary>
        /// 
        /// </summary>
        public const byte UnRegistorTag = 43;

        /// <summary>
        /// 
        /// </summary>
        public const byte QueryAllTagFun = 45;



        public const byte GetTagIdByNameFun = 0;

        public const byte QueryAllTagNameAndIds = 2;

        public const byte GetTagIdByFilterRegistor = 3;


        public const byte GetDriverRecordTypeTagIds = 5;

        public const byte GetDriverRecordTypeTagIds2 = 51;

        /// <summary>
        /// 
        /// </summary>
        public const byte AysncReturn = byte.MaxValue;

        public const byte RealServerBusy = 31;
        public const byte RealServerNoBusy = 32;

        public const byte HisServerBusy = 33;
        public const byte HisServerNoBusy = 34;

        public const byte TagInfoServerBusy = 35;
        public const byte TagINfoServerNoBusy = 36;

        public const byte Hart = 255;

        //数据中带有账户信息
        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagValueWithUserFun = 111;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagValueAndQualityWithUserFun = 112;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagRealAndHisValueWithUserFun = 113;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagRealAndHisValueTimerWithUserFun = 114;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagHisValueWithUser = 161;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagHisValueWithUser2 = 162;

        /// <summary>
        /// 读取历史数据
        /// </summary>
        public const byte ReadTagAllHisValue = 151;

        /// <summary>
        /// 根据时间间隔读取历史数据
        /// </summary>
        public const byte ReadHisDataByTimeSpan = 152;

        /// <summary>
        /// 根据时间点读取历史数据
        /// </summary>
        public const byte ReadHisDatasByTimePoint = 153;

        /// <summary>
        /// 根据时间间隔读取历史数据,忽略系统退出的影响
        /// </summary>
        public const byte ReadHisDataByTimeSpanByIgnorClosedQuality = 155;

        /// <summary>
        /// 根据时间点读取历史数据,忽略系统退出的影响
        /// </summary>
        public const byte ReadHisDatasByTimePointByIgnorClosedQuality = 156;


        /// <summary>
        /// 读取实时值
        /// </summary>
        public const byte ReadTagRealValue = 91;

        /// <summary>
        /// 历史数据查询返回
        /// </summary>
        public const byte ReadTagHisValueReturn = 159;

        /// <summary>
        /// 获取变量名称
        /// </summary>
        public const byte GetDatabaseName = 4;

        /// <summary>
        /// 通过SQL查询历史
        /// </summary>
        public const byte ReadHisValueBySQL = 140;
    }
}
