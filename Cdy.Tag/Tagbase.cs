using System;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class Tagbase
    {
        /// <summary>
        /// 编号
        /// </summary>
        public long Id { get; set; }

        /// <summary>
        /// 类型
        /// </summary>
        public abstract TagType Type { get; }

        /// <summary>
        /// 名称
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 描述
        /// </summary>
        public string Desc { get; set; }

        /// <summary>
        /// 值地址
        /// </summary>
        public long ValueAddress { get; set; }

    }
}
