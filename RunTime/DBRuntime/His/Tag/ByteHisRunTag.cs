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
    public class ByteHisRunTag:HisRunTag
    {
        private byte mLastValue = byte.MinValue;
        private byte mTmpValue = 0;

        /// <summary>
        /// 
        /// </summary>
        public override byte SizeOfValue => 1;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startMemory"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        public override unsafe bool CheckValueChangeToLastRecordValue(void* startMemory, long offset)
        {
            mTmpValue = MemoryHelper.ReadByte(startMemory, offset);
            if (mTmpValue != mLastValue || mLastValue == byte.MinValue)
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
