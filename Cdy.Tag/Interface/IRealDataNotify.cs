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
    /// 值改变通知
    /// </summary>
    public interface IRealDataNotify
    {
        ///// <summary>
        ///// 订购
        ///// </summary>
        ///// <param name="name"></param>
        ///// <returns></returns>
        //ValueChangedNotifyProcesser SubscribeComsumer(string name);

        /// <summary>
        /// 订购
        /// </summary>
        /// <param name="name"></param>
        /// <param name="valueChanged"></param>
        /// <param name="tagRegistor"></param>
        void SubscribeConsumer(string name, ValueChangedNotifyProcesser.ValueChangedDelagete valueChanged, Func<List<int>> tagRegistor);

        /// <summary>
        /// 取消订购
        /// </summary>
        /// <param name="name"></param>
        void UnSubscribeConsumer(string name);
    }
}
