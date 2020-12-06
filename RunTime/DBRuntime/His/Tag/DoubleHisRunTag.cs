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
using System.Runtime.CompilerServices;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class DoubleHisRunTag:HisRunTag
    {
        /// <summary>
        /// 
        /// </summary>
        private double mLastValue = double.MinValue;


        /// <summary>
        /// 
        /// </summary>
        public override byte SizeOfValue => 8;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startMemory"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        public override unsafe bool CheckValueChangeToLastRecordValue(void* startMemory, long offset)
        {
            double mTmpValue = MemoryHelper.ReadDouble(startMemory, offset);
            if (mTmpValue != mLastValue || mLastValue == double.MinValue)
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
