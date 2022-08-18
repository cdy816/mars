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

namespace Cdy.Tag
{
    /// <summary>
    /// 变量类型
    /// </summary>
    public enum TagType
    {
        /// <summary>
        /// 
        /// </summary>
        Bool,
        /// <summary>
        /// 
        /// </summary>
        Byte,
        /// <summary>
        /// 
        /// </summary>
        Short,
        /// <summary>
        /// 
        /// </summary>
        UShort,
        /// <summary>
        /// 
        /// </summary>
        Int,
        /// <summary>
        /// 
        /// </summary>
        UInt,
        /// <summary>
        /// 
        /// </summary>
        Long,
        /// <summary>
        /// 
        /// </summary>
        ULong,
        /// <summary>
        /// 
        /// </summary>
        Double,
        /// <summary>
        /// 
        /// </summary>
        Float,
        /// <summary>
        /// 
        /// </summary>
        DateTime,
        /// <summary>
        /// 
        /// </summary>
        String,
        /// <summary>
        /// 
        /// </summary>
        IntPoint,
        /// <summary>
        /// 
        /// </summary>
        UIntPoint,
        /// <summary>
        /// 
        /// </summary>
        LongPoint,
        /// <summary>
        /// 
        /// </summary>
        ULongPoint,
        /// <summary>
        /// 
        /// </summary>
        IntPoint3,
        /// <summary>
        /// 
        /// </summary>
        UIntPoint3,
        /// <summary>
        /// 
        /// </summary>
        LongPoint3,
        /// <summary>
        /// 
        /// </summary>
        ULongPoint3,
        /// <summary>
        /// 复合自定义类型
        /// </summary>
        Complex,
        ///// <summary>
        ///// 复合自定义类型类型
        ///// </summary>
        //ClassComplex
    }

    public static class TagTypeExtends
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagType"></param>
        /// <returns></returns>
        public static Cdy.Tag.Tagbase GetTag(this Cdy.Tag.TagType tagType)
        {
            switch (tagType)
            {
                case Cdy.Tag.TagType.Bool:
                    return new Cdy.Tag.BoolTag();
                case Cdy.Tag.TagType.Byte:
                    return new Cdy.Tag.ByteTag();
                case Cdy.Tag.TagType.DateTime:
                    return new Cdy.Tag.DateTimeTag();
                case Cdy.Tag.TagType.Double:
                    return new Cdy.Tag.DoubleTag();
                case Cdy.Tag.TagType.Float:
                    return new Cdy.Tag.FloatTag();
                case Cdy.Tag.TagType.Int:
                    return new Cdy.Tag.IntTag();
                case Cdy.Tag.TagType.Long:
                    return new Cdy.Tag.LongTag();
                case Cdy.Tag.TagType.Short:
                    return new Cdy.Tag.ShortTag();
                case Cdy.Tag.TagType.String:
                    return new Cdy.Tag.StringTag();
                case Cdy.Tag.TagType.UInt:
                    return new Cdy.Tag.UIntTag();
                case Cdy.Tag.TagType.ULong:
                    return new Cdy.Tag.ULongTag();
                case Cdy.Tag.TagType.UShort:
                    return new Cdy.Tag.UShortTag();
                case TagType.Complex:
                    return new Cdy.Tag.ComplexTag();
            }
            return null;
        }
    }

}
