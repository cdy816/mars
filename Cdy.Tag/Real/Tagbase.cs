//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Xml.Linq;

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
        public int Id { get; set; }

        /// <summary>
        /// 类型
        /// </summary>
        public abstract TagType Type { get; }

        /// <summary>
        /// 名称
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 组
        /// </summary>
        public string Group { get; set; }

        /// <summary>
        /// 描述
        /// </summary>
        public string Desc { get; set; }

        /// <summary>
        /// 外部管理IO的地址
        /// </summary>
        public string LinkAddress { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public long ValueAddress { get; set; }

    }

    /// <summary>
    /// 
    /// </summary>
    public static class TagExtends
    {
        public static XElement SaveToXML(this Tagbase tag)
        {
            XElement xe = new XElement("Tag");
            xe.SetAttributeValue("Id", tag.Id);
            xe.SetAttributeValue("Name", tag.Name);
            xe.SetAttributeValue("Type", (int)tag.Type);
            xe.SetAttributeValue("Group", tag.Group);
            xe.SetAttributeValue("Desc", tag.Desc);
            xe.SetAttributeValue("LinkAddress", tag.LinkAddress);
            return xe;
        }

        public static Tagbase LoadTagFromXML(this XElement xe)
        {
            TagType tp = (TagType)int.Parse(xe.Attribute("Type").Value);
            Tagbase re = null;

            switch (tp)
            {
                case TagType.Bool:
                    re = new BoolTag();
                    break;
                case TagType.Byte:
                    re = new ByteTag();
                    break;
                case TagType.Short:
                    re = new ShortTag();
                    break;
                case TagType.UShort:
                    re = new UShortTag();
                    break;
                case TagType.Int:
                    re = new IntTag();
                    break;
                case TagType.UInt:
                    re = new UIntTag();
                    break;
                case TagType.Long:
                    re = new LongTag();
                    break;
                case TagType.ULong:
                    re = new ULongTag();
                    break;
                case TagType.Float:
                    re = new FloatTag();
                    break;
                case TagType.Double:
                    re = new DoubleTag();
                    break;
                case TagType.String:
                    re = new StringTag();
                    break;
                case TagType.DateTime:
                    re = new DateTimeTag();
                    break;
            }
            re.Id = int.Parse(xe.Attribute("Id").Value);
            re.Name = xe.Attribute("Name").Value;
            re.Group = xe.Attribute("Group")!=null? xe.Attribute("Group").Value:"";
            re.Desc = xe.Attribute("Desc") != null ? xe.Attribute("Desc").Value : "";
            re.LinkAddress = xe.Attribute("LinkAddress") != null ? xe.Attribute("LinkAddress").Value : "";
            return re;
        }
    }

}
