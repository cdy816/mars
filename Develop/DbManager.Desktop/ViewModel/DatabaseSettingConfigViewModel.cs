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

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

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
