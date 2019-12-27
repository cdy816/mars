//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class StirngHisRunTag:HisRunTag
    {
        /// <summary>
        /// 
        /// </summary>
        public override byte SizeOfValue => (byte)(RealEnginer.StringSize);

        

    }
}
