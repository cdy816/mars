//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/1/2 10:04:50.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace DBInStudio.Desktop
{
    /// <summary>
    /// 
    /// </summary>
    public class TagViewModel : ViewModelBase
    {

        #region ... Variables  ...
        private Cdy.Tag.Tagbase mRealTagMode;
        private Cdy.Tag.HisTag mHisTagMode;

        private static string[] mTagTypeList;
        private static string[] mRecordTypeList;
        private static string[] mCompressTypeList;

        private bool mHasHisTag;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        static TagViewModel()
        {
            InitEnumType();
            mCompressTypeList = new string[] { "无", "无损压缩", "死区压缩", "斜率死区" };
        }

        public TagViewModel()
        {

        }

        public TagViewModel(Cdy.Tag.Tagbase realTag, Cdy.Tag.HisTag histag)
        {
            this.mRealTagMode = realTag;
            this.HisTagMode = histag;
        }

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 实时变量配置
        /// </summary>
        public Cdy.Tag.Tagbase RealTagMode
        {
            get
            {
                return mRealTagMode;
            }
            set
            {
                if (mRealTagMode != value)
                {
                    mRealTagMode = value;
                }
            }
        }

        /// <summary>
        /// 历史变量配置
        /// </summary>
        public Cdy.Tag.HisTag HisTagMode
        {
            get
            {
                return mHisTagMode;
            }
            set
            {
                if (mHisTagMode != value)
                {
                    mHisTagMode = value;
                   
                }
                mHasHisTag = mHisTagMode != null;
            }
        }

        /// <summary>
        /// 名字
        /// </summary>
        public string Name
        {
            get
            {
                return mRealTagMode != null ? mRealTagMode.Name : string.Empty;
            }
            set
            {
                if (mRealTagMode != null && mRealTagMode.Name != value)
                {
                    mRealTagMode.Name = value;
                    OnPropertChanged("Name");
                }
            }
        }

        /// <summary>
        /// 描述
        /// </summary>
        public string Desc
        {
            get
            {
                return mRealTagMode != null ? mRealTagMode.Desc : string.Empty;
            }
            set
            {
                if (mRealTagMode != null && mRealTagMode.Desc != value)
                {
                    mRealTagMode.Desc = value;
                    OnPropertChanged("Desc");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string[] TagTypeList
        {
            get
            {
                return mTagTypeList;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string[] RecordTypeList
        {
            get
            {
                return mRecordTypeList;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string[] CompressTypeList
        {
            get
            {
                return mCompressTypeList;
            }
        }


        /// <summary>
        /// 类型
        /// </summary>
        public int Type
        {
            get
            {
                return mRealTagMode != null ? (int)mRealTagMode.Type : -1;
            }
            set
            {
                if (mRealTagMode != null && (int)mRealTagMode.Type != value)
                {
                    ChangeTagType((Cdy.Tag.TagType)value);
                    OnPropertChanged("Type");
                }
            }
        }

        /// <summary>
        /// 组
        /// </summary>
        public string Group
        {
            get
            {
                return mRealTagMode != null ? mRealTagMode.Group : string.Empty;
            }
            set
            {
                if (mRealTagMode != null && mRealTagMode.Group != value)
                {
                    mRealTagMode.Group = value;
                    OnPropertChanged("Group");
                }
            }
        }

        /// <summary>
        /// 关联外部地址
        /// </summary>
        public string LinkAddress
        {
            get
            {
                return mRealTagMode != null ? mRealTagMode.LinkAddress : string.Empty;
            }
            set
            {
                if (mRealTagMode != null && mRealTagMode.LinkAddress != value)
                {
                    mRealTagMode.LinkAddress = value;
                    OnPropertChanged("LinkAddress");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public bool HasHisTag
        {
            get
            {
                return mHasHisTag;
            }
            set
            {
                if (mHasHisTag != value)
                {
                    mHasHisTag = value;
                    if(!value)
                    {
                        mHisTagMode = null;
                    }
                    else
                    {
                        mHisTagMode = new Cdy.Tag.HisTag() { Id = this.mRealTagMode.Id };
                        ChangeTagType(this.mRealTagMode.Type);
                    }
                }
                OnPropertChanged("HasHisTag");
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public int RecordType
        {
            get
            {
                return mHisTagMode != null ? (int)mHisTagMode.Type : -1;
            }
            set
            {
                if (mHisTagMode != null && (int)mHisTagMode.Type != value)
                {
                    mHisTagMode.Type = (Cdy.Tag.RecordType)value;
                    OnPropertChanged("RecordType");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int CompressType
        {
            get
            {
                return mHisTagMode != null ? mHisTagMode.CompressType : -1;
            }
            set
            {
                if (mHisTagMode != null && mHisTagMode.CompressType != value)
                {
                    mHisTagMode.CompressType = value;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public long CompressCircle
        {
            get
            {
                return mHisTagMode != null ? mHisTagMode.Circle : -1;
            }
            set
            {
                if (mHisTagMode != null && mHisTagMode.Circle != value)
                {
                    mHisTagMode.Circle = value;
                }
            }
        }


        #endregion ...Properties....

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private static void InitEnumType()
        {
            mTagTypeList = Enum.GetNames(typeof(Cdy.Tag.TagType));
            mRecordTypeList = Enum.GetNames(typeof(Cdy.Tag.RecordType));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagType"></param>
        private void ChangeTagType(Cdy.Tag.TagType tagType)
        {
            Cdy.Tag.Tagbase ntag = null;
            if (mHisTagMode == null) return;
            switch (tagType)
            {
                case Cdy.Tag.TagType.Bool:
                    ntag = new Cdy.Tag.BoolTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.Bool;
                    }
                    break;
                case Cdy.Tag.TagType.Byte:
                    ntag = new Cdy.Tag.ByteTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.Byte;
                    }
                    break;
                case Cdy.Tag.TagType.DateTime:
                    ntag = new Cdy.Tag.DateTimeTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.DateTime;
                    }
                    break;
                case Cdy.Tag.TagType.Double:
                    ntag = new Cdy.Tag.DoubleTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.Double;
                    }
                    break;
                case Cdy.Tag.TagType.Float:
                    ntag = new Cdy.Tag.FloatTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.Float;
                    }
                    break;
                case Cdy.Tag.TagType.Int:
                    ntag = new Cdy.Tag.IntTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.Int;
                    }
                    break;
                case Cdy.Tag.TagType.Long:
                    ntag = new Cdy.Tag.LongTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.Long;
                    }
                    break;
                case Cdy.Tag.TagType.Short:
                    ntag = new Cdy.Tag.ShortTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.Short;
                    }
                    break;
                case Cdy.Tag.TagType.String:
                    ntag = new Cdy.Tag.StringTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.String;
                    }
                    break;
                case Cdy.Tag.TagType.UInt:
                    ntag = new Cdy.Tag.UIntTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.UInt;
                    }
                    break;
                case Cdy.Tag.TagType.ULong:
                    ntag = new Cdy.Tag.ULongTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.ULong;
                    }
                    break;
                case Cdy.Tag.TagType.UShort:
                    ntag = new Cdy.Tag.UShortTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.UShort;
                    }
                    break;
                default:
                    break;
            }
            if (ntag != null)
            {
                RealTagMode = ntag;
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class TagGroupViewModel : ViewModelBase
    {

        #region ... Variables  ...
        private string mName;
        private System.Collections.ObjectModel.ObservableCollection<TagGroupViewModel> mChildren = new System.Collections.ObjectModel.ObservableCollection<TagGroupViewModel>();
        private bool mIsSelected = false;
        private bool mIsExpand = false;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public string Name
        {
            get
            {
                return mName;
            }
            set
            {
                if (mName != value)
                {
                    mName = value;
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public System.Collections.ObjectModel.ObservableCollection<TagGroupViewModel> Children
        {
            get
            {
                return mChildren;
            }
        }

        /// <summary>
            /// 被选中
            /// </summary>
        public bool IsSelected
        {
            get
            {
                return mIsSelected;
            }
            set
            {
                if (mIsSelected != value)
                {
                    mIsSelected = value;
                }
            }
        }

        /// <summary>
            /// 展开
            /// </summary>
        public bool IsExpand
        {
            get
            {
                return mIsExpand;
            }
            set
            {
                if (mIsExpand != value)
                {
                    mIsExpand = value;
                }
            }
        }



        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
