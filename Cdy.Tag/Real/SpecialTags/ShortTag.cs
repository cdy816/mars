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
    public class ShortTag : NumberTagBase
    {
        /// <summary>
        /// 
        /// </summary>
        public override TagType Type => TagType.Short;


        /// <summary>
        /// 
        /// </summary>
        public override double AllowMaxValue => short.MaxValue;

        /// <summary>
        /// 
        /// </summary>
        public override double AllowMinValue => short.MinValue;
    }
}
