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
    public class ULongTag : NumberTagBase
    {
        /// <summary>
        /// 
        /// </summary>
        public override TagType Type => TagType.ULong;

        public override int ValueSize => 17;

        /// <summary>
        /// 
        /// </summary>
        public override double AllowMaxValue => ulong.MaxValue;

        /// <summary>
        /// 
        /// </summary>
        public override double AllowMinValue => ulong.MinValue;
    }
}
