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
    public class BoolTag : Tagbase
    {
        /// <summary>
        /// 
        /// </summary>
        public override TagType Type => TagType.Bool;

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="value"></param>
        ///// <returns></returns>
        //public override object ConvertValue<T>(object value)
        //{
        //    if (this.Conveter != null)
        //    {
        //       return this.Conveter.ConvertTo(value);
        //    }
        //    else
        //    {
        //        return Convert.ToBoolean(value);
        //    }
        //}
    }
}
