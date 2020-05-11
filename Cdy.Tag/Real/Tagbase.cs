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
    /// 读写模式
    /// </summary>
    public enum ReadWriteMode
    {
        Write,
        Read,
        ReadWrite
    }

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
        public string Name { get; set; } = "";

        /// <summary>
        /// 组
        /// </summary>
        public string Group { get; set; } = "";

        /// <summary>
        /// 描述
        /// </summary>
        public string Desc { get; set; } = "";

        /// <summary>
        /// 外部管理IO的地址
        /// </summary>
        public string LinkAddress { get; set; } = "";

        /// <summary>
        /// 
        /// </summary>
        public long ValueAddress { get; set; }

        /// <summary>
        /// 值转换函数
        /// </summary>
        public IValueConvert Conveter { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public ReadWriteMode ReadWriteType{ get; set; }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="value"></param>
        ///// <returns></returns>
        //public abstract object ConvertValue<T>(object value);

    }

    /// <summary>
    /// 
    /// </summary>
    public abstract class NumberTagBase : Tagbase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public NumberTagBase()
        {
            MaxValue = AllowMaxValue;
            MinValue = AllowMinValue;
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public abstract double AllowMaxValue { get; }

        /// <summary>
        /// 
        /// </summary>
        public abstract double AllowMinValue { get; }

        /// <summary>
        /// 最大值
        /// </summary>
        public double MaxValue { get; set; }

        /// <summary>
        /// 最小值
        /// </summary>
        public double MinValue { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public abstract class FloatingTagBase : NumberTagBase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 小数位数
        /// </summary>
        public byte Precision { get; set; } = 6;
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
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
            xe.SetAttributeValue("ReadWriteType", (int)tag.ReadWriteType);
            if(tag.Conveter!=null)
            {
                xe.SetAttributeValue("Conveter", tag.Conveter.Name+":"+ tag.Conveter.SaveToString());
            }
            if(tag is NumberTagBase)
            {
                xe.SetAttributeValue("MaxValue", (tag as NumberTagBase).MaxValue);
                xe.SetAttributeValue("MinValue", (tag as NumberTagBase).MinValue);
            }
            if(tag is FloatingTagBase)
            {
                xe.SetAttributeValue("Precision", (tag as FloatingTagBase).Precision);
            }
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
                case TagType.IntPoint:
                    re = new IntPointTag();
                    break;
                case TagType.UIntPoint:
                    re = new UIntPointTag();
                    break;
                case TagType.LongPoint:
                    re = new LongPointTag();
                    break;
                case TagType.ULongPoint:
                    re = new ULongPointTag();
                    break;
                case TagType.IntPoint3:
                    re = new IntPoint3Tag();
                    break;
                case TagType.UIntPoint3:
                    re = new UIntPoint3Tag();
                    break;
                case TagType.LongPoint3:
                    re = new LongPoint3Tag();
                    break;
                case TagType.ULongPoint3:
                    re = new ULongPoint3Tag();
                    break;
            }
            re.Id = int.Parse(xe.Attribute("Id").Value);
            re.Name = xe.Attribute("Name").Value;
            re.Group = xe.Attribute("Group")!=null? xe.Attribute("Group").Value:"";
            re.Desc = xe.Attribute("Desc") != null ? xe.Attribute("Desc").Value : "";
            re.LinkAddress = xe.Attribute("LinkAddress") != null ? xe.Attribute("LinkAddress").Value : "";

            
            if(xe.Attribute("Conveter") !=null)
            {
                var vres= xe.Attribute("Conveter").Value;
                string[] sval = vres.Split(new char[] { ':' });
                var vtmp = ValueConvertManager.manager.GetConvert(sval[0]);
                if(vtmp!=null)
                {
                    re.Conveter = vtmp.LoadFromString(vres.Replace(sval[0]+":",""));
                }
            }

            if (xe.Attribute("ReadWriteType") != null)
            {
                re.ReadWriteType = (ReadWriteMode) int.Parse(xe.Attribute("ReadWriteType").Value);
            }

            if (re is NumberTagBase)
            {
                if (xe.Attribute("MaxValue") != null)
                {
                    (re as NumberTagBase).MaxValue = double.Parse(xe.Attribute("MaxValue").Value);
                }
                if (xe.Attribute("MinValue") != null)
                {
                    (re as NumberTagBase).MinValue = double.Parse(xe.Attribute("MinValue").Value);
                }
            }
            if (re is FloatingTagBase)
            {
                if (xe.Attribute("Precision") != null)
                {
                    (re as FloatingTagBase).Precision = byte.Parse(xe.Attribute("Precision").Value);
                }
            }
            return re;
        }
    }

}
