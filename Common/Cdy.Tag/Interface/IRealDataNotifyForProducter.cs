//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/8 18:02:39.
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
    public interface IRealDataNotifyForProducter
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="valueChanged"></param>
        /// <param name="tagRegistor"></param>
        public void SubscribeValueChangedForProducter(string name, ProducterValueChangedNotifyProcesser.ValueChangedDelagete valueChanged, Func<List<int>> tagRegistor);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void UnSubscribeValueChangedForProducter(string name);

    }
}
