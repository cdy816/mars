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
using Cdy.Tag;
using DBRuntimeMonitor.ViewModel;

namespace DBRuntimeMonitor
{
    /// <summary>
    /// 
    /// </summary>
    public class TagViewModel : ViewModelBase
    {

        #region ... Variables  ...
        private Cdy.Tag.Tagbase? mRealTagMode;
        private Cdy.Tag.HisTag? mHisTagMode;

        public static string[]? mTagTypeList;
        public static string[]? mRecordTypeList;
        public static string[]? mCompressTypeList;
        public static string[]? mReadWriteModeList;

        /// <summary>
        /// 
        /// </summary>
        public static Dictionary<string, string[]> Drivers = new Dictionary<string, string[]>();

        private string? mDriverName;
        private string? mRegistorName;

        private bool mHasHisTag;

        private string[]? mRegistorList;

        private ICommand? mHisValueCommand;

        private bool mIsSelected;

        private object? mValue;
        private byte mQuality= 63;

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
                Res.Get("NoneCompress"),
                Res.Get("LosslessCompress"),
                Res.Get("DeadAreaCompress"),
                Res.Get("SlopeCompress")
            };
        }

        /// <summary>
        /// 
        /// </summary>
        public TagViewModel()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="realTag"></param>
        /// <param name="histag"></param>
        public TagViewModel(Cdy.Tag.Tagbase realTag, Cdy.Tag.HisTag histag)
        {
            this.mRealTagMode = realTag;
            this.HisTagMode = histag;
            CheckLinkAddress();
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public TagGroupDetailViewModel? Owner { get; set; }

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
                mIsSelected = value; 
                OnPropertyChanged("IsSelected"); 
            }
        }

        /// <summary>
        /// 是否新建
        /// </summary>
        public bool IsNew { get; set; }


        /// <summary>
        /// 实时变量配置
        /// </summary>
        public Cdy.Tag.Tagbase? RealTagMode
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
        public Cdy.Tag.HisTag? HisTagMode
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
                return mRealTagMode != null ? mRealTagMode.FullName : string.Empty;
            }
            set
            {
                if (mRealTagMode != null && mRealTagMode.Name != value && CheckAvaiableName(value))
                {
                    mRealTagMode.Name = value;
                    IsChanged = true;
                }
                OnPropertyChanged("Name");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string? Database { get; set; }

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
                    return null;
                }
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
                    IsChanged = true;
                    OnPropertyChanged("Type");
                    OnPropertyChanged("TypeString");
                    OnPropertyChanged("IsNumberTag");
                    OnPropertyChanged("Precision");
                    OnPropertyChanged("MaxValue");
                    OnPropertyChanged("MinValue");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string TypeString
        {
            get
            {
                return mRealTagMode.Type.ToString();
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
                        RegistorList = Drivers[mDriverName];
                    }
                    else
                    {
                        RegistorList = null;
                    }
                    OnPropertyChanged("DriverName");
                    OnPropertyChanged("RegistorName");
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
                if (mHasHisTag != value)
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
        public bool IsTimerRecord
        {
            get
            {
                return RecordType == (int)(Cdy.Tag.RecordType.Timer);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsDriverRecord
        {
            get
            {
                return RecordType == (int)(Cdy.Tag.RecordType.Driver);
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
                return mHisTagMode!=null? Res.Get(mHisTagMode.Type.ToString()):string.Empty;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string ReadWriteModeString
        {
            get
            {
                return Res.Get(((Cdy.Tag.ReadWriteMode)ReadWriteMode).ToString());
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

        /// <summary>
        /// 
        /// </summary>
        public ICommand HisValueCommand
        {
            get
            {
                if(mHisValueCommand==null)
                {
                    mHisValueCommand = new RelayCommand(() => {
                        HisTagViewer htv = new HisTagViewer();
                        htv.Client = Owner?.Client;
                        htv.TagId = this.Id;
                        htv.TagName = this.mRealTagMode.FullName;
                        htv.TagType = this.mRealTagMode.Type;
                        htv.ShowDialog();
                    });
                }
                return mHisValueCommand;
            }
        }

        #endregion ...Properties....

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private bool CheckAvaiableName(string name)
        {
            return !name.Contains(".");
        }

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
        private void CheckLinkAddress()
        {
            if (string.IsNullOrEmpty(LinkAddress))
            {
                return;
            }
            else
            {
                string[] str = LinkAddress.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                if (str.Length == 0) return;

                mDriverName = str[0];
                if(str.Length>1)
                {
                    mRegistorName = LinkAddress.Substring(mDriverName.Length+1);
                }

                if (Drivers != null && Drivers.ContainsKey(mDriverName))
                {
                    RegistorList = Drivers[mDriverName];
                }
                else
                {
                    RegistorList = null;
                }

            }
        }

        /// <summary>
        /// 
        /// </summary>
        private static void InitEnumType()
        {
            mTagTypeList = Enum.GetNames(typeof(Cdy.Tag.TagType));
            mRecordTypeList = Enum.GetNames(typeof(Cdy.Tag.RecordType)).Select(e => Res.Get(e)).ToArray();
            mReadWriteModeList = Enum.GetNames(typeof(Cdy.Tag.ReadWriteMode)).Select(e=>Res.Get(e)).ToArray();
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string SaveToCSVString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(mRealTagMode.Id+",");
            sb.Append(mRealTagMode.Name + ",");
            sb.Append(mRealTagMode.Desc + ",");
            sb.Append(mRealTagMode.Group + ",");
            sb.Append(mRealTagMode.Type + ",");
            sb.Append(mRealTagMode.LinkAddress + ",");
            sb.Append((int)mRealTagMode.ReadWriteType + ",");
            if (mRealTagMode.Conveter != null)
                sb.Append(mRealTagMode.Conveter.SeriseToString() + ",");
            else
            {
                sb.Append(",");
            }
            if (mRealTagMode is NumberTagBase)
            {
                sb.Append((mRealTagMode as NumberTagBase).MaxValue.ToString() + ",");
                sb.Append((mRealTagMode as NumberTagBase).MinValue.ToString() + ",");
            }
            else
            {
                sb.Append(",");
                sb.Append(",");
            }
            if (mRealTagMode is FloatingTagBase)
            {
                sb.Append((mRealTagMode as FloatingTagBase).Precision + ",");
            }
            else
            {
                sb.Append(",");
            }
            if (this.mHisTagMode!=null)
            {
                sb.Append(mHisTagMode.Type + ",");
                sb.Append(mHisTagMode.Circle + ",");
                sb.Append(mHisTagMode.CompressType + ",");
                sb.Append(mHisTagMode.MaxValueCountPerSecond + ",");
                if (mHisTagMode.Parameters!=null)
                {
                    foreach(var vv in mHisTagMode.Parameters)
                    {
                        sb.Append(vv.Key + ",");
                        sb.Append(vv.Value + ",");
                    }
                }
            }
            sb.Length = sb.Length > 0 ? sb.Length - 1 : sb.Length;
            return sb.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static TagViewModel LoadFromCSVString(string val)
        {
            string[] stmp = val.Split(new char[] { ',' });
            Cdy.Tag.TagType tp = (Cdy.Tag.TagType)Enum.Parse(typeof(Cdy.Tag.TagType),stmp[4]);
            var realtag = TagTypeExtends.GetTag(tp);

            realtag.Id = int.Parse(stmp[0]);
            realtag.Name = stmp[1];
            realtag.Desc = stmp[2];
            realtag.Group = stmp[3];
            realtag.LinkAddress = stmp[5];
            realtag.ReadWriteType = (ReadWriteMode)(int.Parse(stmp[6]));
            if (stmp[7] != null)
            {
                realtag.Conveter = stmp[7].DeSeriseToValueConvert();
            }

            if (realtag is NumberTagBase)
            {
                (realtag as NumberTagBase).MaxValue = double.Parse(stmp[8], System.Globalization.NumberStyles.Any);
                (realtag as NumberTagBase).MinValue = double.Parse(stmp[9], System.Globalization.NumberStyles.Any);
            }

            if (realtag is FloatingTagBase)
            {
                (realtag as FloatingTagBase).Precision = byte.Parse(stmp[10]);
            }
            if (stmp.Length > 11)
            {
                Cdy.Tag.HisTag histag = new HisTag();
                histag.Type = (Cdy.Tag.RecordType)Enum.Parse(typeof(Cdy.Tag.RecordType), stmp[11]);

                histag.Circle = int.Parse(stmp[12]);
                histag.CompressType = int.Parse(stmp[13]);
                histag.Parameters = new Dictionary<string, double>();
                histag.TagType = realtag.Type;
                histag.Id = realtag.Id;
                histag.MaxValueCountPerSecond = short.Parse(stmp[14]);

                for (int i=15;i<stmp.Length;i++)
                {
                    string skey = stmp[i];
                    if(string.IsNullOrEmpty(skey))
                    {
                        break;
                    }
                    double dval = double.Parse(stmp[i + 1]);

                    if(!histag.Parameters.ContainsKey(skey))
                    {
                        histag.Parameters.Add(skey, dval);
                    }

                    i++;
                }
                return new TagViewModel(realtag, histag);
            }

            return new TagViewModel(realtag, null);


        }

        /// <summary>
        /// 
        /// </summary>
        public override void Dispose()
        {
            Owner = null;
            mRealTagMode = null;
            mHisTagMode = null;
            base.Dispose();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }






}
