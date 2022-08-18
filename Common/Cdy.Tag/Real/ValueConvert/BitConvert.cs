using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    public class BitConvert : IValueConvert
    {
        public string Name => "BitConvert";

        /// <summary>
        /// 
        /// </summary>
        public byte Index { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IValueConvert Clone()
        {
            return new BitConvert() { Index = this.Index };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public object ConvertBackTo(object value)
        {
            var val = Convert.ToByte(value);
            return val << Index;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public object ConvertTo(object value)
        {
            return (Convert.ToInt64(value) >> Index & 0x01) >0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public IValueConvert LoadFromString(string value)
        {
            if (string.IsNullOrEmpty(value)) return new BitConvert();
            return new BitConvert() { Index = byte.Parse(value)};
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string SaveToString()
        {
            return this.Index.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        public bool SupportTag(Tagbase tag)
        {
            if ((tag is BoolTag)) return true;
            return false;
        }
    }
}
