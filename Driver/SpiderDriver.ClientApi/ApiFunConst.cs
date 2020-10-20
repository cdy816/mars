//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/30 18:00:57.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace SpiderDriver.ClientApi
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
        public const byte PushDataChangedFun = 3;

        /// <summary>
        /// 
        /// </summary>
        public const byte TagInfoRequestFun = 4;

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


        public const byte GetDriverRecordTypeTagIds = 5;

        /// <summary>
        /// 
        /// </summary>
        public const byte AysncReturn = byte.MaxValue;
    }
}
