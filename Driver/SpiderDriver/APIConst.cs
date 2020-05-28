//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/27 15:54:33.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace SpiderDriver
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
        public const byte SetTagValueFun = 11;

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
        public const byte RemoveRegistorTag = 43;

        /// <summary>
        /// 
        /// </summary>
        public const byte QueryAllTagFun = 45;

        /// <summary>
        /// 
        /// </summary>
        public const byte AysncReturn = byte.MaxValue;

    }
}
