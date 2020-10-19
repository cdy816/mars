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
        public RecordType Type { get; set; } = RecordType.Timer;

        /// <summary>
        /// 变量类型
        /// </summary>
        public TagType TagType { get; set; }

        /// <summary>
        /// 压缩算法
        /// </summary>
        public int CompressType { get; set; } = 0;

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, double> Parameters { get; set; } = new Dictionary<string, double>();

        /// <summary>
        /// 定时记录周期,ms
        /// </summary>
        public int Circle { get; set; } = 1000;

        /// <summary>
        /// 驱动自主添加历史数据时
        /// 一秒内可记录的数据的数量
        /// </summary>
        public short MaxValueCountPerSecond { get; set; } = 1;
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
            xe.SetAttributeValue("MaxValueCountPerSecond", tag.MaxValueCountPerSecond);
            if (tag.Parameters != null && tag.Parameters.Count > 0)
            {
                XElement para = new XElement("Parameters");
                foreach(var vv in tag.Parameters)
                {
                    var vpp = new XElement("ParameterItem");
                    vpp.SetAttributeValue("Name", vv.Key.ToString());
                    vpp.SetAttributeValue("Value", vv.Value.ToString());
                    para.Add(vpp);
                }
                xe.Add(para);
            }
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
            hisTag.Circle = int.Parse(xe.Attribute("Circle").Value);
            hisTag.Type = (RecordType)(int.Parse(xe.Attribute("Type").Value));
            hisTag.TagType = (TagType)(int.Parse(xe.Attribute("TagType").Value));
            hisTag.CompressType = int.Parse(xe.Attribute("CompressType").Value);
            if(xe.Attribute("MaxValueCountPerSecond")!=null)
            hisTag.MaxValueCountPerSecond = short.Parse(xe.Attribute("MaxValueCountPerSecond").Value);
            if(xe.Element("Parameters") !=null)
            {
                Dictionary<string, double> dvals = new Dictionary<string, double>();
                foreach(var vv in xe.Element("Parameters").Elements())
                {
                    string skey = vv.Attribute("Name").Value;
                    double dval = Convert.ToDouble(vv.Attribute("Value").Value);
                    if(!dvals.ContainsKey(skey))
                    {
                        dvals.Add(skey, dval);
                    }
                }
                hisTag.Parameters = dvals;
            }
            return hisTag;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        public static HisTag Clone(this HisTag tag)
        {
            return tag.SaveToXML().LoadHisTagFromXML();
        }

    }


}
