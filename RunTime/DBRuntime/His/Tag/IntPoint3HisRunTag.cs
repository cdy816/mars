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
    public class IntPoint3HisRunTag:HisRunTag
    {
        private int x = int.MinValue, y = int.MinValue, z = int.MinValue;
        public override byte SizeOfValue => 12;

        public override unsafe bool CheckValueChangeToLastRecordValue(void* startMemory, long offset)
        {
            int xx = MemoryHelper.ReadInt32(startMemory,offset) , yy = MemoryHelper.ReadInt32(startMemory, offset+4), zz = MemoryHelper.ReadInt32(startMemory, offset+8);
            if(xx!=x||yy!=y||zz!=z || xx == int.MinValue)
            {
                x = xx;
                y = yy;
                z = zz;
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
