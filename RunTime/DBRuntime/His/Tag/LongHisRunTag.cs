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
    public class LongHisRunTag:HisRunTag
    {
        private long mLastValue = long.MinValue;
        private long mTmpValue = 0;

        public override byte SizeOfValue => 8;

        public override unsafe bool CheckValueChangeToLastRecordValue(void* startMemory, long offset)
        {
            mTmpValue = MemoryHelper.ReadInt64(startMemory, offset);
            if (mTmpValue != mLastValue || mLastValue == long.MinValue)
            {
                mLastValue = mTmpValue;
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
