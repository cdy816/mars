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
        public override byte SizeOfValue => 8;

        //private double mLastValue = -1;
        //private DateTime mHisTime;

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="count"></param>
        ///// <param name="tim"></param>
        //public unsafe override void UpdateValue2(int count, int tim)
        //{
        //    base.UpdateValue2(count, tim);
        //    if (this.Id == 1)
        //    {
        //        double dtmp = MemoryHelper.ReadDouble((void*)(RealMemoryPtr), RealValueAddr);
        //        var mQuality = RealMemoryAddr[RealValueAddr + SizeOfValue + 8];
        //        if (mLastValue != dtmp)
        //        {
        //            mLastValue = dtmp;
        //            LoggerService.Service.Info("DoubleHisTag", mLastValue + "last time:" + mHisTime.ToString("yyyy-MM-dd HH:mm:ss.fff"), ConsoleColor.Green);
        //        }
        //        else
        //        {
        //            var dt = DateTime.Now;
        //            LoggerService.Service.Info("DoubleHisTag", dtmp + " = " + mLastValue + "last time:" + mHisTime.ToString("yyyy-MM-dd HH:mm:ss.fff"), ConsoleColor.Red);
        //        }

        //        mHisTime = DateTime.Now;
        //    }
        //}

    }
}
