using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 历史记录
    /// </summary>
    public class HisTag
    {
        /// <summary>
        /// 
        /// </summary>
        public Tagbase Tag { get; set; }

        /// <summary>
        /// 记录类型
        /// </summary>
        public RecordType Type { get; set; }

        /// <summary>
        /// 定时记录周期,ms
        /// </summary>
        public long Circle { get; set; }
    }
}
