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
    /// <summary>
    /// 
    /// </summary>
    public class IntHisRunTag:HisRunTag
    {
        private int mLastValue = int.MinValue;
        /// <summary>
        /// 
        /// </summary>
        public override byte SizeOfValue => 4;

        public override unsafe bool CheckValueChangeToLastRecordValue(void* startMemory, long offset)
        {
            var val = MemoryHelper.ReadInt32(startMemory, offset);
            if (val != mLastValue || mLastValue == int.MinValue)
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
