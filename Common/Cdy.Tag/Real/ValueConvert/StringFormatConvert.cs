using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    public class StringFormatConvert : IValueConvert
    {
        /// <summary>
        /// 
        /// </summary>
        public string Name => "StringFormat";

        /// <summary>
        /// 
        /// </summary>
        public string Format { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IValueConvert Clone()
        {
            return new StringFormatConvert() { Format = Format };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public object ConvertBackTo(object value)
        {
            return value;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public object ConvertTo(object value)
        {
            return string.Format(Format, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public IValueConvert LoadFromString(string value)
        {
            if(string.IsNullOrEmpty(value)) return new StringFormatConvert();
            return new StringFormatConvert() { Format = value };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string SaveToString()
        {
            return Format;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        public bool SupportTag(Tagbase tag)
        {
            if(tag is StringTag) return true;
            return false;
        }
    }
}
