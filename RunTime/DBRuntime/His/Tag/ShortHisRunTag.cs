﻿//==============================================================
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
    /// <summary>
    /// 
    /// </summary>
    public class ShortHisRunTag:HisRunTag
    {
        private short mLastValue = short.MinValue;

        /// <summary>
        /// 
        /// </summary>
        public override byte SizeOfValue => 2;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startMemory"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        public override unsafe bool CheckValueChangeToLastRecordValue(void* startMemory, long offset)
        {
            var val = MemoryHelper.ReadShort(startMemory, offset);
            if (val != mLastValue || mLastValue == short.MinValue)
            {
                mLastValue = val;
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
