//==============================================================
//  Copyright (C) 2020 Chongdaoyang Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/9/15 21:23:53 .
//  Version 1.0
//  CDYWORK
//==============================================================

using DBDevelopClientApi;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace DBInStudio.Desktop.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class DatabaseSettingConfigViewModel : ViewModelBase, IModeSwitch
    {

        #region ... Variables  ...
        private int mServerPort = 0;

        private ObservableCollection<DriverSetViewModel> mChildren = new ObservableCollection<DriverSetViewModel>();


        private string mDataPath;

        private string mDataBackupPath;

        private bool mHisDataPathIsCustom;

        private bool mHisDataPathIsDefault;

        private int mKeepTime;

        private int mKeepNoZipFileDays = -1;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...



        /// <summary>
        /// 
        /// </summary>
        public int KeepTime
        {
            get
            {
                return mKeepTime;
            }
            set
            {
                if (mKeepTime != value)
                {
                    mKeepTime = value;
                    OnPropertyChanged("KeepTime");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public bool IsEnableZipFile
        {
            get
            {
                return mKeepNoZipFileDays>=0;
            }
            set
            {
                if (value)
                {
                    KeepNoZipFileDays = 7;
                }
                else
                {
                    KeepNoZipFileDays = -1;
                }
                OnPropertyChanged("IsEnableZipFile");
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public int KeepNoZipFileDays
        {
            get
            {
                return mKeepNoZipFileDays;
            }
            set
            {
                if (mKeepNoZipFileDays != value)
                {
                    mKeepNoZipFileDays = value;
                    OnPropertyChanged("KeepNoZipFileDays");
                }
            }
        }




        /// <summary>
        /// 
        /// </summary>
        public string DataPath
        {
            get
            {
                return mDataPath;
            }
            set
            {
                if (mDataPath != value)
                {
                    mDataPath = value;
                    OnPropertyChanged("DataPath");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string DataBackupPath
        {
            get
            {
                return mDataBackupPath;
            }
            set
            {
                if (mDataBackupPath != value)
                {
                    mDataBackupPath = value;
                    OnPropertyChanged("DataBackupPath");
                }
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public ObservableCollection<DriverSetViewModel> Children
        {
            get
            {
                return mChildren;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int ServerPort
        {
            get
            {
                return mServerPort;
            }
            set
            {
                if (mServerPort != value)
                {
                    if (DevelopServiceHelper.Helper.SetRealServerPort(this.Database, value))
                    {
                        mServerPort = value;
                    }
                    OnPropertyChanged("ServerPort");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool HisDataPathIsDefault
        {
            get
            {
                return mHisDataPathIsDefault;
            }
            set
            {
                if(value)
                {
                    DataPath = string.Empty;
                }
                mHisDataPathIsDefault = value;
                OnPropertyChanged("HisDataPathIsDefault");
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public bool HisDataPathIsCustom
        {
            get
            {
                return mHisDataPathIsCustom;
            }
            set
            {
                if (mHisDataPathIsCustom != value)
                {
                    mHisDataPathIsCustom = value;
                }
                OnPropertyChanged("HisDataPathIsCustom");
            }
        }



        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void Init()
        {
            mServerPort = DevelopServiceHelper.Helper.GetRealServerPort(this.Database);
            OnPropertyChanged("ServerPort");

            var dds = DevelopServiceHelper.Helper.GetRegistorDrivers(this.Database).Keys;
            mChildren.Clear();

            var setting = DevelopServiceHelper.Helper.GetHisSetting(this.Database);

            DataPath = setting.Item1;
            DataBackupPath = setting.Item2;
            HisDataPathIsDefault = string.IsNullOrEmpty(DataPath);
            HisDataPathIsCustom = !HisDataPathIsDefault;

            KeepTime = setting.Item3;
            KeepNoZipFileDays = setting.Item4;

            foreach (var vv in dds)
            {
                var ss = DevelopServiceHelper.Helper.GetDriverSetting(this.Database, vv);
                if (ss != null && ss.Count > 0)
                {
                    DriverSetViewModel dsm = new DriverSetViewModel() { Name = vv };
                    dsm.Init(ss);
                    mChildren.Add(dsm);
                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

        /// <summary>
        /// 
        /// </summary>
        public void Active()
        {
            Init();
        }

        /// <summary>
        /// 
        /// </summary>
        public void DeActive()
        {
            foreach (var vv in mChildren)
            {
                var item = vv.ToDictionary();
                DevelopServiceHelper.Helper.UpdateDriverSetting(this.Database, vv.Name, item);
            }

            DevelopServiceHelper.Helper.UpdateHisSetting(this.Database, DataPath, DataBackupPath, KeepTime,KeepNoZipFileDays);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class DriverSetViewModel : ViewModelBase
    {

        #region ... Variables  ...

        private string mName;

        private ObservableCollection<DriverSettingItem> mChildren = new ObservableCollection<DriverSettingItem>();

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
                    OnPropertyChanged("Name");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ObservableCollection<DriverSettingItem> Children
        {
            get
            {
                return mChildren;
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="vals"></param>
        public void Init(Dictionary<string, string> vals)
        {
            mChildren.Clear();
            foreach (var vv in vals)
            {
                mChildren.Add(new DriverSettingItem() { Name = vv.Key, Value = vv.Value });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, string> ToDictionary()
        {
            Dictionary<string, string> dtmp = new Dictionary<string, string>();
            foreach (var vv in mChildren)
            {
                dtmp.Add(vv.Name, vv.Value);
            }
            return dtmp;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public class DriverSettingItem : ViewModelBase
    {

        #region ... Variables  ...
        private string mValue;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string DisplayName
        {
            get
            {
                return Res.Get(Name)+":";
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public string Value
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


        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
