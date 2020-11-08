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
    public class ShortHisRunTag:HisRunTag
    {
        private short mLastValue = short.MinValue;
        private short mTmpValue = 0;

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
            mTmpValue = MemoryHelper.ReadShort(startMemory, offset);
            if (mTmpValue != mLastValue || mLastValue == short.MinValue)
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
