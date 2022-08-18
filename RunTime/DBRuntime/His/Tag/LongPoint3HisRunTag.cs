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
    public class LongPoint3HisRunTag:HisRunTag
    {
        private long x = long.MinValue, y = long.MinValue, z = long.MinValue;
        public override byte SizeOfValue => 24;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startMemory"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        public override unsafe bool CheckValueChangeToLastRecordValue(void* startMemory, long offset, int tim)
        {
            var xx = MemoryHelper.ReadInt64(startMemory, offset); 
            var yy = MemoryHelper.ReadInt64(startMemory, offset + 8); 
            var zz = MemoryHelper.ReadInt64(startMemory, offset + 16);
            if ((xx != x || yy != y || zz != z || xx == long.MinValue) && Math.Abs(tim - mLastTime) > 0.1)
            {
                x = xx;
                y = yy;
                z = zz;
                mLastTime = tim;
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
