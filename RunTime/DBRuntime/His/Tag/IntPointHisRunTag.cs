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
    public class IntPointHisRunTag:HisRunTag
    {
        private int x = int.MinValue, y = int.MinValue;
        //private int xx = 0, yy;
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
            var xx = MemoryHelper.ReadInt32(startMemory, offset); 
            var yy = MemoryHelper.ReadInt32(startMemory, offset + 4);
            if (xx != x || yy != y || xx == int.MinValue)
            {
                x = xx;
                y = yy;
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
