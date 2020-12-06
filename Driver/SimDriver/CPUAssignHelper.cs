//==============================================================
//  Copyright (C) 2020 Chongdaoyang Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/7/20 20:07:14 .
//  Version 1.0
//  CDYWORK
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class CPUAssignHelper
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static CPUAssignHelper Helper = new CPUAssignHelper();


        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public int[] CPUArray1 { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int[] CPUArray2 { get; set; }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            int count = ThreadHelper.GetProcessNumbers();
            if(count==1)
            {
                CPUArray1 = new int[] { 0 };
                CPUArray2 = new int[] { 0 };
            }
            else if(count==2)
            {
                CPUArray1 = new int[] { 0 };
                CPUArray2 = new int[] { 1 };
            }
            else
            {
                List<int> ltmp = new List<int>();
                for(int i=0;i<count/2;i++)
                {
                    ltmp.Add(i);
                }
                CPUArray1 = ltmp.ToArray();
                ltmp.Clear();
                for(int i=count/2;i<count;i++)
                {
                    ltmp.Add(i);
                }
                CPUArray2 = ltmp.ToArray();
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
