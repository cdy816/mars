using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 历史记录
    /// </summary>
    public class HisTag
    {
        /// <summary>
        /// Tag Id
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// 记录类型
        /// </summary>
        public RecordType Type { get; set; }

        /// <summary>
        /// 变量类型
        /// </summary>
        public TagType TagType { get; set; }

        /// <summary>
        /// 压缩算法
        /// </summary>
        public int CompressType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public float CompressParameter1 { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public float CompressParameter2 { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public float CompressParameter3 { get; set; }

        /// <summary>
        /// 定时记录周期,ms
        /// </summary>
        public long Circle { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public static class HisTagExtends
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        public static XElement SaveToXML(this HisTag tag)
        {
            XElement xe = new XElement("HisTag");
            xe.SetAttributeValue("Id", tag.Id);
            xe.SetAttributeValue("Type", (int)tag.Type);
            xe.SetAttributeValue("TagType", (int)tag.TagType);
            xe.SetAttributeValue("Circle", tag.Circle);
            xe.SetAttributeValue("CompressType", tag.CompressType);

            xe.SetAttributeValue("CompressParameter1", tag.CompressParameter1);
            xe.SetAttributeValue("CompressParameter2", tag.CompressParameter2);
            xe.SetAttributeValue("CompressParameter3", tag.CompressParameter3);
            return xe;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="xe"></param>
        /// <returns></returns>
        public static HisTag LoadHisTagFromXML(this XElement xe)
        {
            HisTag hisTag = new HisTag();
            hisTag.Id = int.Parse(xe.Attribute("Id").Value);
            hisTag.Circle = long.Parse(xe.Attribute("Circle").Value);
            hisTag.Type = (RecordType)(int.Parse(xe.Attribute("Type").Value));
            hisTag.TagType = (TagType)(int.Parse(xe.Attribute("TagType").Value));
            hisTag.CompressType = int.Parse(xe.Attribute("CompressType").Value);

            hisTag.CompressParameter1 = float.Parse(xe.Attribute("CompressParameter1").Value);
            hisTag.CompressParameter2 = float.Parse(xe.Attribute("CompressParameter2").Value);
            hisTag.CompressParameter3 = float.Parse(xe.Attribute("CompressParameter3").Value);
            return hisTag;
        }
    }


}
