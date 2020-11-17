using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public struct TagHisValue<T>
    {

        #region ... Variables  ...
        
        /// <summary>
        /// 
        /// </summary>
        public static TagHisValue<T> Empty = new TagHisValue<T>() { Quality = byte.MaxValue,Time=DateTime.MinValue };
        
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 时间
        /// </summary>
        public DateTime Time { get; set; }
        /// <summary>
        /// 值
        /// </summary>
        public T Value { get; set; }

        /// <summary>
        /// 质量
        /// </summary>
        public byte Quality { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        public bool IsEmpty()
        {
            return this.Time == DateTime.MinValue && this.Quality == byte.MaxValue;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
