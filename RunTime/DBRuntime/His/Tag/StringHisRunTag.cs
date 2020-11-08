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
        private string mLastValue = string.Empty;
        private string mTmpValue = "";

        /// <summary>
        /// 
        /// </summary>
        public override byte SizeOfValue => (byte)(Const.StringSize);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startMemory"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        public override unsafe bool CheckValueChangeToLastRecordValue(void* startMemory, long offset)
        {
            var len = MemoryHelper.ReadByte(startMemory, offset + 1);
            mTmpValue = new string((char*)startMemory, (int)(offset + 1), len);
            if (mTmpValue != mLastValue)
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
