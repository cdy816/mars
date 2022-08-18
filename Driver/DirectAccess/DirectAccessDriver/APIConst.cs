﻿//==============================================================
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

namespace DirectAccessDriver
{
    public class APIConst
    {
        /// <summary>
        /// 
        /// </summary>
        public const byte LoginFun = 0;

        /// <summary>
        /// 
        /// </summary>
        public const byte RealValueFun = 1;

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
        public const byte SetTagValueFun = 11;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagValueAndQualityFun = 12;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagRealAndHisValueFun = 13;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagValueTimeAndQualityFun = 14;

        /// <summary>
        /// 
        /// </summary>
        public const byte SetTagRealAndHisValueWithTimeFun = 15;

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
        public const byte RemoveRegistorTag = 43;

        /// <summary>
        /// 
        /// </summary>
        public const byte QueryAllTagFun = 45;

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

    }
}
