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
        /// <summary>
        /// 订购
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        ValueChangedNotifyProcesser Subscribe(string name);

        /// <summary>
        /// 订购
        /// </summary>
        /// <param name="name"></param>
        /// <param name="valueChanged"></param>
        /// <param name="tagRegistor"></param>
        void Subscribe(string name, ValueChangedNotifyProcesser.ValueChangedDelagete valueChanged, Func<List<int>> tagRegistor);

        /// <summary>
        /// 取消订购
        /// </summary>
        /// <param name="name"></param>
        void UnSubscribe(string name);
    }
}
