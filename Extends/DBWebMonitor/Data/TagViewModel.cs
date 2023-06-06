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
using System.Windows.Input;
using System.ComponentModel;
using Cdy.Tag;
using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc.ModelBinding.Validation;
using System.Runtime.Intrinsics.X86;

namespace DBWebMonitor
{
    /// <summary>
    /// 
    /// </summary>
    public class TagViewModel : ViewModelBase, ISelectable
    {

        #region ... Variables  ...
        protected Cdy.Tag.Tagbase mRealTagMode;
        protected Cdy.Tag.HisTag mHisTagMode;

        public static string[] mTagTypeList;
        public static string[] mTagTypeList2;
        public static string[] mRecordTypeList;
        public static string[] mCompressTypeList;
        public static string[] mReadWriteModeList;

        /// <summary>
        /// 
        /// </summary>
        public static Dictionary<string,Tuple<string[],string>> Drivers;

        private string mDriverName;
        private string mRegistorName;

        private bool mHasHisTag;

        private string[] mRegistorList=new string[0];

        private Action mAdvanceEditCommand;

        private bool mIsSelected;

        private object mValue="";
        private byte mQuality= 63;

        protected System.Collections.ObjectModel.ObservableCollection<TagViewModel> mSubItems = new System.Collections.ObjectModel.ObservableCollection<TagViewModel>();

        private string mRegistorEditType = "";

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
            mCompressTypeList = new string[] 
            {
                "无损",
                "死区",
                "斜率"
            };
        }

        public TagViewModel()
        {

        }

        public TagViewModel(Cdy.Tag.Tagbase realTag, Cdy.Tag.HisTag histag)
        {
            this.RealTagMode = realTag;
            this.HisTagMode = histag;
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public MarsProxy Proxy { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public TagViewModel Parent { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public System.Collections.ObjectModel.ObservableCollection<TagViewModel> SubItems
        {
            get
            {
                return mSubItems;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsChanged { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsSelected 
        { 
            get { return mIsSelected; } 
            set 
            {
                if (mIsSelected != value)
                {
                    mIsSelected = value;
                }
                OnPropertyChanged("IsSelected"); 
            }
        }


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

                    var tmax = (int)TagType.ULongPoint3;
                    if ((int)value.Type > tmax)
                    {
                        var cls = (mRealTagMode as ComplexTag).LinkComplexClass;
                        if(mTagTypeList.Contains(cls))
                        {
                            mType = mTagTypeList.AsSpan().IndexOf(cls);
                        }
                    }
                    else
                    {
                        mType = (int)value.Type;
                    }
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
        /// 
        /// </summary>
        public int Id
        {
            get
            {
                return mRealTagMode != null ? mRealTagMode.Id : -1;
            }
            set
            {
                if(mRealTagMode!=null)
                mRealTagMode.Id = value;
                if (mHisTagMode != null)
                    mHisTagMode.Id = value;
                OnPropertyChanged("Id");
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
                }
                OnPropertyChanged("Name");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string Database { get; set; }

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
                    IsChanged = true;
                    OnPropertyChanged("Desc");
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
        public string[] TagTypeList2
        {
            get
            {
                return mTagTypeList2;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string[] ReadWriteModeList
        {
            get
            {
                return mReadWriteModeList;
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
            /// 
            /// </summary>
        public string[] RegistorList
        {
            get
            {
                
                return mRegistorList;
            }
            set
            {
                if (mRegistorList != value)
                {
                    mRegistorList = value;
                }
                OnPropertyChanged("RegistorList");
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string RegistorEditType
        {
            get
            {
                return mRegistorEditType;
            }
            set
            {
                if (mRegistorEditType != value)
                {
                    mRegistorEditType = value;
                    OnPropertyChanged("RegistorEditType");
                }
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public string[] DriverList
        {
            get
            {
                if (Drivers != null)
                {
                    return Drivers.Keys.ToArray();
                }
                else
                {
                    return new string[0];
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsClassDefine { get; set; }

        private int mType = 0;

        /// <summary>
        /// 类型
        /// </summary>
        public int Type
        {
            get
            {
                return mType;
            }
            set
            {
                if (mRealTagMode != null && (int)mRealTagMode.Type != value)
                {
                    var tmax = (int)TagType.ULongPoint3;
                    bool ischg = false; 
                    if (value > tmax)
                    {
                        if (CanChangedTypeDelegate==null || CanChangedTypeDelegate(Cdy.Tag.TagType.Complex, mTagTypeList[value]))
                        {
                            mType = value;
                            ChangeTagType(Cdy.Tag.TagType.Complex);
                            IsChanged = true;
                            ischg = true;
                        }
                    }
                    else
                    {
                        mType = value;
                        ChangeTagType((Cdy.Tag.TagType)value);
                        IsChanged = true;
                        ischg = true;
                    }

                    OnPropertyChanged("Type");
                    OnPropertyChanged("TypeString");
                    OnPropertyChanged("IsNumberTag");
                    OnPropertyChanged("Precision");
                    OnPropertyChanged("MaxValue");
                    OnPropertyChanged("MinValue");
                    OnPropertyChanged("IsComplexTag");
                    OnPropertyChanged("RecordType");
                    OnPropertyChanged("RecordTypeString");
                    OnPropertyChanged("HasHisTag");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsComplexTag { get { return mRealTagMode is ComplexTag; } }

        /// <summary>
        /// 
        /// </summary>
        public string TypeString
        {
            get
            {
                return  (mRealTagMode is ComplexTag)?((mRealTagMode as ComplexTag).LinkComplexClass) : mRealTagMode.Type.ToString();
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
                    IsChanged = true;
                    OnPropertyChanged("Group");
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
                    IsChanged = true;
                    OnPropertyChanged("LinkAddress");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string DriverName
        {
            get
            {
                return mDriverName;
            }
            set
            {
                if (mDriverName != value)
                {
                    mDriverName = value;
                    mRegistorName = string.Empty;
                    IsChanged = true;
                    LinkAddress = DriverName + ":" + RegistorName;
                    if(Drivers!=null&&Drivers.ContainsKey(mDriverName))
                    {
                        RegistorList = Drivers[mDriverName].Item1;
                        RegistorEditType = Drivers[mDriverName].Item2;
                    }
                    else
                    {
                        RegistorList = new string[0];
                        RegistorEditType = "";
                    }
                    OnPropertyChanged("DriverName");
                    OnPropertyChanged("RegistorName");
                    Proxy?.RefreshAction();
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string RegistorName
        {
            get
            {
                return mRegistorName;
            }
            set
            {
                if (mRegistorName != value)
                {
                    mRegistorName = value;
                    LinkAddress = DriverName + ":" + RegistorName;
                    IsChanged = true;
                    OnPropertyChanged("RegistorName");
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
                if (mHasHisTag != value&&!IsComplexTag)
                {
                    mHasHisTag = value;
                    if(!value)
                    {
                        mHisTagMode = null;
                    }
                    else
                    {
                        mHisTagMode = new Cdy.Tag.HisTag() { Id = this.mRealTagMode.Id,TagType=mRealTagMode.Type };
                    }
                    IsChanged = true;
                }
                OnPropertyChanged("RecordType");
                OnPropertyChanged("CompressCircle");
                OnPropertyChanged("MaxValueCountPerSecond");
                OnPropertyChanged("CompressType");
                OnPropertyChanged("HasHisTag");
                OnPropertyChanged("IsTimerRecord");
                OnPropertyChanged("IsDriverRecord");
                OnPropertyChanged("RecordTypeString");
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
                    IsChanged = true;
                    OnPropertyChanged("RecordType");
                    OnPropertyChanged("RecordTypeString");
                    OnPropertyChanged("IsTimerRecord");
                    OnPropertyChanged("IsDriverRecord");

                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string ConvertString
        {
            get
            {
                return mRealTagMode.Conveter != null ? mRealTagMode.Conveter.SeriseToString() : string.Empty;
            }
            set
            {
                ;
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public IValueConvert Convert
        {
            get
            {
                return mRealTagMode.Conveter;
            }
            set
            {
                if (mRealTagMode.Conveter != value)
                {
                    mRealTagMode.Conveter = value;
                    IsChanged = true;
                }
                OnPropertyChanged("Convert");
                OnPropertyChanged("ConvertString");
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public int ReadWriteMode
        {
            get
            {
                return (int)mRealTagMode.ReadWriteType;
            }
            set
            {
                if ((int)mRealTagMode.ReadWriteType != value)
                {
                    mRealTagMode.ReadWriteType = (Cdy.Tag.ReadWriteMode)value;
                    IsChanged = true;
                }
                OnPropertyChanged("ReadWriteMode");
                OnPropertyChanged("ReadWriteModeString");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public double AllowMaxValue
        {
            get
            {
                return mRealTagMode is Cdy.Tag.NumberTagBase ? (mRealTagMode as Cdy.Tag.NumberTagBase).AllowMaxValue : 0;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public double AllowMinValue
        {
            get
            {
                return mRealTagMode is Cdy.Tag.NumberTagBase ? (mRealTagMode as Cdy.Tag.NumberTagBase).AllowMinValue : 0;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public double MaxValue
        {
            get
            {
                return mRealTagMode is Cdy.Tag.NumberTagBase ? (mRealTagMode as Cdy.Tag.NumberTagBase).MaxValue : 0;
            }
            set
            {
                if (mRealTagMode is Cdy.Tag.NumberTagBase)
                {
                    if (value <= AllowMaxValue)
                    {
                        (mRealTagMode as Cdy.Tag.NumberTagBase).MaxValue = value;
                    }
                    IsChanged = true;
                    OnPropertyChanged("MaxValue");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public double MinValue
        {
            get
            {
                return mRealTagMode is Cdy.Tag.NumberTagBase ? (mRealTagMode as Cdy.Tag.NumberTagBase).MinValue : 0;
            }
            set
            {
                if (mRealTagMode is Cdy.Tag.NumberTagBase)
                {
                    if(value>=AllowMinValue)
                    (mRealTagMode as Cdy.Tag.NumberTagBase).MinValue = value;
                    IsChanged = true;
                    OnPropertyChanged("MinValue");
                }
            }
        }

        public bool IsNumberTag
        {
            get
            {
                return mRealTagMode!=null && mRealTagMode is Cdy.Tag.NumberTagBase;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public byte Precision
        {
            get
            {
                return mRealTagMode is Cdy.Tag.FloatingTagBase ? (mRealTagMode as Cdy.Tag.FloatingTagBase).Precision : (byte)0;
            }
            set
            {
                if (mRealTagMode is Cdy.Tag.FloatingTagBase)
                {
                    (mRealTagMode as Cdy.Tag.FloatingTagBase).Precision = value;
                    IsChanged = true;
                    OnPropertyChanged("Precision");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string Unit
        {
            get
            {
                return mRealTagMode.Unit;
            }
            set
            {
                if(mRealTagMode.Unit!=value)
                {
                    mRealTagMode.Unit = value;
                    IsChanged=true;
                    OnPropertyChanged("Unit");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string ExtendField1
        {
            get
            {
                return mRealTagMode.ExtendField1;
            }
            set
            {
                if(mRealTagMode.ExtendField1 != value)
                {
                    mRealTagMode.ExtendField1 = value;
                    IsChanged = true;
                    OnPropertyChanged("ExtendField");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string Area
        {
            get
            {
                return mRealTagMode != null ? mRealTagMode.Area : "";
            }
            set
            {
                if(mRealTagMode != null && mRealTagMode.Area!=value)
                {
                    mRealTagMode.Area = value;
                    IsChanged = true;
                    OnPropertyChanged("Area");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public bool IsFloatingTag
        {
            get
            {
                return mRealTagMode!=null && mRealTagMode is Cdy.Tag.FloatingTagBase;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public string RecordTypeString
        {
            get
            {
                return mHisTagMode!=null? (mHisTagMode.Type.ToString()):string.Empty;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string ReadWriteModeString
        {
            get
            {
                return (((Cdy.Tag.ReadWriteMode)ReadWriteMode).ToString());
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public int CompressType
        {
            get
            {
                return mHisTagMode != null ? mHisTagMode.CompressType-1 : -1;
            }
            set
            {
                var val = value + 1;
                if (mHisTagMode != null && mHisTagMode.CompressType != val)
                {
                    mHisTagMode.CompressType = val;
                    IsChanged = true;
                    OnPropertyChanged("CompressType");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int CompressCircle
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
                    IsChanged = true;
                    OnPropertyChanged("CompressCircle");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public short MaxValueCountPerSecond
        {
            get
            {
                return mHisTagMode != null ? mHisTagMode.MaxValueCountPerSecond : (short)1;
            }
            set
            {
                if (mHisTagMode != null && mHisTagMode.MaxValueCountPerSecond != value)
                {
                    mHisTagMode.MaxValueCountPerSecond = value;
                    IsChanged = true;
                    OnPropertyChanged("MaxValueCountPerSecond");
                }
            }
        }

       

        /// <summary>
        /// 
        /// </summary>
        public object Value
        {
            get
            {
                return mValue;
            }
            set
            {
                if (mValue != value)
                {
                    mValue = value;
                    OnPropertyChanged("Value");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public byte Quality
        {
            get
            {
                return mQuality;
            }
            set
            {
                if (mQuality != value)
                {
                    mQuality = value;
                    OnPropertyChanged("Quality");
                }
            }
        }

        private string mUpdateTime="";
        public string UpdateTime
        {
            get
            {
                return mUpdateTime;
            }
            set
            {
                if(mUpdateTime != value)
                {
                    mUpdateTime = value;
                    OnPropertyChanged("UpdateTime");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public Func<string,bool> CheckAvaiableNameDelegate { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Func<TagViewModel,bool> UpdateDelegate { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Func<TagType,string,bool> CanChangedTypeDelegate { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Func<string, List<TagViewModel>> QueryTagSubTagsDelegate { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Action AdvanceEditCommand
        {
            get
            {
                if(mAdvanceEditCommand==null)
                {
                    mAdvanceEditCommand = () => {
                        var cmd = ServiceLocator.Locator.Resolve(this.RegistorEditType+ "Command");
                        if(cmd!=null && cmd is Func<string,string>)
                        {
                            RegistorName = (cmd as Func<string,string>)(this.RegistorName);
                        }
                    };
                }
                return mAdvanceEditCommand;
            }
        }

        #endregion ...Properties....

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void SetTagNane(string name)
        {
            mRealTagMode.Name = name;
        }

        /// <summary>
        /// 
        /// </summary>
        public void RefreshHisTag()
        {
            OnPropertyChanged("CompressType");
            OnPropertyChanged("CompressCircle");
            OnPropertyChanged("RecordType");
            OnPropertyChanged("RecordTypeString");
            OnPropertyChanged("IsTimerRecord");
            OnPropertyChanged("IsDriverRecord");
            OnPropertyChanged("MaxValueCountPerSecond");
        }

        /// <summary>
        /// 
        /// </summary>
        private static void InitEnumType()
        {
            var vlist = Enum.GetNames(typeof(Cdy.Tag.TagType)).ToList();
            vlist.Remove("Complex");
            //vlist.Remove("ClassComplex");
            mTagTypeList2 =vlist.ToArray();
            mTagTypeList = vlist.ToArray();

            mRecordTypeList = Enum.GetNames(typeof(Cdy.Tag.RecordType)).Select(e => e).ToArray();
            mReadWriteModeList = Enum.GetNames(typeof(Cdy.Tag.ReadWriteMode)).Select(e => e).ToArray();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ll"></param>
        public static void UpdateTagType(List<string> ll)
        {
            var vlist = Enum.GetNames(typeof(Cdy.Tag.TagType)).ToList();
            vlist.Remove("Complex");
            //vlist.Remove("ClassComplex");
            vlist.AddRange(ll);
            mTagTypeList = vlist.ToArray();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagType"></param>
        private void ChangeTagType(Cdy.Tag.TagType tagType)
        {
            Cdy.Tag.Tagbase ntag = null;
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

                case Cdy.Tag.TagType.IntPoint:
                    ntag = new Cdy.Tag.IntPointTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.IntPoint;
                    }
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    ntag = new Cdy.Tag.IntPoint3Tag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.IntPoint3;
                    }
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    ntag = new Cdy.Tag.UIntPointTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.UIntPoint;
                    }
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    ntag = new Cdy.Tag.UIntPoint3Tag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.UIntPoint3;
                    }
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    ntag = new Cdy.Tag.LongPointTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.LongPoint;
                    }
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    ntag = new Cdy.Tag.LongPoint3Tag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.LongPoint3;
                    }
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    ntag = new Cdy.Tag.ULongPointTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.ULongPoint;
                    }
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    ntag = new Cdy.Tag.ULongPoint3Tag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.ULongPoint3;
                    }
                    break;
                case TagType.Complex:
                    ntag = new Cdy.Tag.ComplexTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group,LinkComplexClass = mTagTypeList[mType] };
                    if (mHisTagMode != null)
                    {
                        HisTagMode = null;
                        
                    }
                    break;
                //case TagType.ClassComplex:
                //    ntag = new Cdy.Tag.ComplexClassTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group, LinkComplexClass = mTagTypeList[mType] };
                //    if (mHisTagMode != null)
                //    {
                //        HisTagMode = null;
                //    }
                //    break;

                default:
                    break;
            }
            if (ntag != null)
            {
                RealTagMode = ntag;
            }
            IsChanged = true;
        }


        private void CompressParameterModel_PropertyChanged(object sender, System.ComponentModel.PropertyChangedEventArgs e)
        {
            if (mHisTagMode != null)
            {
               // mHisTagMode.Parameters = (sender as CompressParameterModelBase).Parameters;
                IsChanged = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public TagViewModel Clone()
        {
            Cdy.Tag.Tagbase ntag = null;
            Cdy.Tag.HisTag htag = null;
            if(mHisTagMode != null)
            {
                htag = new Cdy.Tag.HisTag() { Id = mHisTagMode.Id, Circle = mHisTagMode.Circle,MaxValueCountPerSecond=mHisTagMode.MaxValueCountPerSecond, CompressType = mHisTagMode.CompressType, TagType = mHisTagMode.TagType, Type = mHisTagMode.Type };

                if (this.mHisTagMode.Parameters != null)
                {
                    htag.Parameters = new Dictionary<string, double>();
                    foreach(var vv in mHisTagMode.Parameters)
                    {
                        htag.Parameters.Add(vv.Key, vv.Value);
                    }
                }
            }

            switch (this.mRealTagMode.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    ntag = new Cdy.Tag.BoolTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group,Conveter = mRealTagMode.Conveter!=null?mRealTagMode.Conveter.Clone():null };
                    break;
                case Cdy.Tag.TagType.Byte:
                    ntag = new Cdy.Tag.ByteTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group, Conveter = mRealTagMode.Conveter != null ? mRealTagMode.Conveter.Clone() : null };

                    break;
                case Cdy.Tag.TagType.DateTime:
                    ntag = new Cdy.Tag.DateTimeTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group, Conveter = mRealTagMode.Conveter != null ? mRealTagMode.Conveter.Clone() : null };

                    break;
                case Cdy.Tag.TagType.Double:
                    ntag = new Cdy.Tag.DoubleTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group, Conveter = mRealTagMode.Conveter != null ? mRealTagMode.Conveter.Clone() : null };

                    break;
                case Cdy.Tag.TagType.Float:
                    ntag = new Cdy.Tag.FloatTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group, Conveter = mRealTagMode.Conveter != null ? mRealTagMode.Conveter.Clone() : null };

                    break;
                case Cdy.Tag.TagType.Int:
                    ntag = new Cdy.Tag.IntTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group, Conveter = mRealTagMode.Conveter != null ? mRealTagMode.Conveter.Clone() : null };

                    break;
                case Cdy.Tag.TagType.Long:
                    ntag = new Cdy.Tag.LongTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group, Conveter = mRealTagMode.Conveter != null ? mRealTagMode.Conveter.Clone() : null };

                    break;
                case Cdy.Tag.TagType.Short:
                    ntag = new Cdy.Tag.ShortTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group, Conveter = mRealTagMode.Conveter != null ? mRealTagMode.Conveter.Clone() : null };

                    break;
                case Cdy.Tag.TagType.String:
                    ntag = new Cdy.Tag.StringTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group, Conveter = mRealTagMode.Conveter != null ? mRealTagMode.Conveter.Clone() : null };

                    break;
                case Cdy.Tag.TagType.UInt:
                    ntag = new Cdy.Tag.UIntTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group, Conveter = mRealTagMode.Conveter != null ? mRealTagMode.Conveter.Clone() : null };

                    break;
                case Cdy.Tag.TagType.ULong:
                    ntag = new Cdy.Tag.ULongTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group, Conveter = mRealTagMode.Conveter != null ? mRealTagMode.Conveter.Clone() : null };

                    break;
                case Cdy.Tag.TagType.UShort:
                    ntag = new Cdy.Tag.UShortTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group, Conveter = mRealTagMode.Conveter != null ? mRealTagMode.Conveter.Clone() : null };

                    break;
                case Cdy.Tag.TagType.IntPoint:
                    ntag = new Cdy.Tag.IntPointTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.IntPoint;
                    }
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    ntag = new Cdy.Tag.IntPoint3Tag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.IntPoint3;
                    }
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    ntag = new Cdy.Tag.UIntPointTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.UIntPoint;
                    }
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    ntag = new Cdy.Tag.UIntPoint3Tag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.UIntPoint3;
                    }
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    ntag = new Cdy.Tag.LongPointTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.LongPoint;
                    }
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    ntag = new Cdy.Tag.LongPoint3Tag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.LongPoint3;
                    }
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    ntag = new Cdy.Tag.ULongPointTag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.ULongPoint;
                    }
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    ntag = new Cdy.Tag.ULongPoint3Tag() { Id = this.mRealTagMode.Id, Name = mRealTagMode.Name, Desc = mRealTagMode.Desc, LinkAddress = mRealTagMode.LinkAddress, Group = mRealTagMode.Group };
                    if (mHisTagMode != null)
                    {
                        mHisTagMode.TagType = Cdy.Tag.TagType.ULongPoint3;
                    }
                    break;
                case TagType.Complex:
                    ntag = new Cdy.Tag.ComplexTag() { Id = this.mRealTagMode.Id, Desc = mRealTagMode.Desc, LinkComplexClass = (this.mRealTagMode as ComplexTag).LinkComplexClass, Group = mRealTagMode.Group, Name = mRealTagMode.Name };
                    break;
                default:
                    break;
            }

            if(IsNumberTag)
            {
                (ntag as NumberTagBase).MaxValue = (mRealTagMode as NumberTagBase).MaxValue;
                (ntag as NumberTagBase).MinValue = (mRealTagMode as NumberTagBase).MinValue;
            }

            if(IsFloatingTag)
            {
                (ntag as FloatingTagBase).Precision = (mRealTagMode as FloatingTagBase).Precision;
            }

            return new TagViewModel(ntag, htag) { Database = this.Database };
        }

        protected virtual TagViewModel NewItem(Tagbase tag,HisTag histag)
        {
            return new TagViewModel(tag, histag);
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Dispose()
        {
            this.Parent = null;
            this.Proxy = null;
            mRealTagMode = null;
            mHisTagMode = null;
            if (mSubItems != null)
            {
                foreach (var vv in mSubItems)
                {
                    vv.Dispose();
                }
            }
            base.Dispose();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public class RowTagViewModel : TagViewModel
    {
        /// <summary>
        /// 
        /// </summary>
        public int UID { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string UName { get { return UID+Name; } }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="realTag"></param>
        /// <param name="histag"></param>
        public RowTagViewModel(Cdy.Tag.Tagbase realTag, Cdy.Tag.HisTag histag):base(realTag,histag)
        {

        }

        /// <summary>
        /// 
        /// </summary>
        public RowTagViewModel():base()
        {
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public RowTagViewModel Clone()
        {
            var bse = base.Clone();
            return new RowTagViewModel(bse.RealTagMode, bse.HisTagMode);
        }

        protected override TagViewModel NewItem(Tagbase tag, HisTag histag)
        {
            return new RowTagViewModel(tag, histag);
        }

    }


    public class ViewModelBase : INotifyPropertyChanged, IDisposable
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        protected void OnPropertyChanged(string name)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
        }

        /// <summary>
        /// 
        /// </summary>
        public virtual void Dispose()
        {
        }

        #endregion ...Methods...

        #region ... Interfaces ...
        /// <summary>
        /// 
        /// </summary>
        public event PropertyChangedEventHandler PropertyChanged;
        #endregion ...Interfaces...

    }
}
