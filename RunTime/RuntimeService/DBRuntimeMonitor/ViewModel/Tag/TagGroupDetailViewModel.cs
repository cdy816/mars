//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/4 18:51:48.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Xml.Linq;

namespace DBRuntimeMonitor.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class TagGroupDetailViewModel:ViewModelBase, IModeSwitch
    {

        #region ... Variables  ...
        private RootTagGroupViewModel mGroupModel;
        private System.Collections.ObjectModel.ObservableCollection<TagViewModel> mSelectGroupTags = new System.Collections.ObjectModel.ObservableCollection<TagViewModel>();

        private Dictionary<int, TagViewModel> mTagModels = new Dictionary<int, TagViewModel>();

        private TagViewModel? mCurrentSelectTag;

        private int mTotalPageNumber = 0;
        private int mCurrentPageIndex = 0;

        private bool mIsLoading = false;

        private int mTagCount = 0;

        private string mFilterKeyName = string.Empty;

        private bool mTagTypeFilterEnable;

        private int mFilterType = -1;

        private bool mReadWriteModeFilterEnable;

        private int mFilterReadWriteMode = -1;

        private bool mRecordFilterEnable;

        private bool mTimerRecordFilterEnable=true;

        private bool mValueChangedRecordFilterEnable;

        private bool mCompressFilterEnable;

        private int mFilterCompressType;

        private bool mDriverFilterEnable;

        private string mFilterDriver;

        private bool mRegistorFilterEnable;

        private string mFilterRegistorName = string.Empty;

        private Dictionary<string, string> mFilters = new Dictionary<string, string>();

        private bool mEnableFilter = true;

        private RootTagGroupViewModel? mNode;

        private DBHighApi.ApiClient? mClient;

        private int mServerPort = 0;
        private string mUserName = "";
        private string mPassword = "";

        private const int TagCountPerPage = 1000;

        private string mCurrentGroup = "";

        private List<string>? mGroupList;

        private System.Timers.Timer? mScanTimer;

        private int mViewStart;
        private int mViewEnd;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public DBHighApi.ApiClient? Client
        {
            get
            {
                return mClient;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public RootTagGroupViewModel Node
        {
            get
            {
                return mNode;
            }
            set
            {
                if (mNode != value)
                {
                    mNode = value;
                    OnPropertyChanged("Node");
                }
            }
        }

      

        /// <summary>
        /// 
        /// </summary>
        public string FilterRegistorName
        {
            get
            {
                return mFilterRegistorName;
            }
            set
            {
                if (mFilterRegistorName != value)
                {
                    mFilterRegistorName = value;
                    NewQueryTags();
                    OnPropertyChanged("FilterRegistorName");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public bool RegistorFilterEnable
        {
            get
            {
                return mRegistorFilterEnable;
            }
            set
            {
                if (mRegistorFilterEnable != value)
                {
                    mRegistorFilterEnable = value;
                    NewQueryTags();
                    if (!value) mFilterRegistorName = string.Empty;
                    OnPropertyChanged("RegistorFilterEnable");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public string FilterDriver
        {
            get
            {
                return mFilterDriver;
            }
            set
            {
                if (mFilterDriver != value)
                {
                    mFilterDriver = value;
                    NewQueryTags();
                    if (DriverList != null && TagViewModel.Drivers.ContainsKey(value))
                    {
                        RegistorList = TagViewModel.Drivers[value];
                    }
                    else
                    {
                        RegistorList = null;
                    }
                    OnPropertyChanged("RegistorList");
                    OnPropertyChanged("FilterDriver");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public bool DriverFilterEnable
        {
            get
            {
                return mDriverFilterEnable;
            }
            set
            {
                if (mDriverFilterEnable != value)
                {
                    mDriverFilterEnable = value;
                    NewQueryTags();
                    if (!value) mFilterDriver = string.Empty;
                    OnPropertyChanged("DriverFilterEnable");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public int FilterCompressType
        {
            get
            {
                return mFilterCompressType;
            }
            set
            {
                if (mFilterCompressType != value)
                {
                    mFilterCompressType = value;
                    NewQueryTags();
                    OnPropertyChanged("FilterCompressType");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public bool CompressFilterEnable
        {
            get
            {
                return mCompressFilterEnable;
            }
            set
            {
                if (mCompressFilterEnable != value)
                {
                    mCompressFilterEnable = value;
                    NewQueryTags();
                    OnPropertyChanged("CompressFilterEnable");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public bool ValueChangedRecordFilterEnable
        {
            get
            {
                return mValueChangedRecordFilterEnable;
            }
            set
            {
                if (mValueChangedRecordFilterEnable != value)
                {
                    mValueChangedRecordFilterEnable = value;
                    mTimerRecordFilterEnable = !value;
                    if (value) NewQueryTags();
                }
                OnPropertyChanged("ValueChangedRecordFilterEnable");
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public bool TimerRecordFilterEnable
        {
            get
            {
                return mTimerRecordFilterEnable;
            }
            set
            {
                if (mTimerRecordFilterEnable != value)
                {
                    mTimerRecordFilterEnable = value;
                    mValueChangedRecordFilterEnable = !value;
                    if (value) NewQueryTags();
                }
                OnPropertyChanged("TimerRecordFilterEnable");
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public bool RecordFilterEnable
        {
            get
            {
                return mRecordFilterEnable;
            }
            set
            {
                if (mRecordFilterEnable != value)
                {
                    mRecordFilterEnable = value;
                    NewQueryTags();
                    OnPropertyChanged("RecordFilterEnable");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public int FilterReadWriteMode
        {
            get
            {
                return mFilterReadWriteMode;
            }
            set
            {
                if (mFilterReadWriteMode != value)
                {
                    mFilterReadWriteMode = value;
                    NewQueryTags();
                }
                OnPropertyChanged("FilterReadWriteMode");
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public bool ReadWriteModeFilterEnable
        {
            get
            {
                return mReadWriteModeFilterEnable;
            }
            set
            {
                if (mReadWriteModeFilterEnable != value)
                {
                    mReadWriteModeFilterEnable = value;
                    if (!value)
                    {
                        mFilterReadWriteMode = -1;
                        NewQueryTags();
                    }
                    OnPropertyChanged("ReadWriteModeFilterEnable");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public int FilterType
        {
            get
            {
                return mFilterType;
            }
            set
            {
                if (mFilterType != value)
                {
                    mFilterType = value;
                    NewQueryTags();
                }
                OnPropertyChanged("FilterType");
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public string FilterKeyName
        {
            get
            {
                return mFilterKeyName;
            }
            set
            {
                if (mFilterKeyName != value)
                {
                    mFilterKeyName = value;
                    NewQueryTags();
                }
                OnPropertyChanged("FilterKeyName");
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string CurrentGroup
        {
            get
            {
                return mCurrentGroup;
            }
            set
            {
                if (mCurrentGroup != value)
                {
                    mCurrentGroup = value;
                    OnPropertyChanged("CurrentGroup");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public List<string> GroupList
        {
            get
            {
                return mGroupList;
            }
            set
            {
                if (mGroupList != value)
                {
                    mGroupList = value;
                    OnPropertyChanged("GroupList");
                }
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public bool TagTypeFilterEnable
        {
            get
            {
                return mTagTypeFilterEnable;
            }
            set
            {
                if (mTagTypeFilterEnable != value)
                {
                    mTagTypeFilterEnable = value;
                    if (!value)
                    {
                        mFilterType = -1;
                        NewQueryTags();
                    }
                }
                OnPropertyChanged("TagTypeFilterEnable");
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public bool IsLoading
        {
            get
            {
                return mIsLoading;
            }
            set
            {
                if (mIsLoading != value)
                {
                    mIsLoading = value;
                    OnPropertyChanged("IsLoading");
                }
            }
        }

        


        /// <summary>
        /// 
        /// </summary>
        public TagViewModel? CurrentSelectTag
        {
            get
            {
                return mCurrentSelectTag;
            }
            set
            {
                if (mCurrentSelectTag != value)
                {
                    mCurrentSelectTag = value;
                    OnPropertyChanged("CurrentSelectTag");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public System.Collections.ObjectModel.ObservableCollection<TagViewModel> SelectGroupTags
        {
            get
            {
                return mSelectGroupTags;
            }
            set
            {
                mSelectGroupTags = value;
                OnPropertyChanged("SelectGroupTags");
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public int TagCount
        {
            get
            {
                return mTagCount;
            }
            set
            {
                if (mTagCount != value)
                {
                    mTagCount = value;
                    OnPropertyChanged("TagCount");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public bool EnableFilter
        {
            get
            {
                return mEnableFilter;
            }
            set
            {
                if (mEnableFilter != value)
                {
                    mEnableFilter = value;
                    OnPropertyChanged("EnableFilter");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public string[] TagTypeList { get { return TagViewModel.mTagTypeList; } }

        /// <summary>
        /// 
        /// </summary>
        public string[] ReadWriteModeList { get { return TagViewModel.mReadWriteModeList; } }

        public string[] CompressTypeList { get { return TagViewModel.mCompressTypeList; } }

        public string[] DriverList { get { return TagViewModel.Drivers.Keys.ToArray(); } }

        /// <summary>
        /// 
        /// </summary>
        public string[]? RegistorList { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DataGrid? grid { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsEnable { get { return mClient != null && mClient.IsLogin; } }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <param name="end"></param>
        public void UpdateViewPort(int start,int end)
        {
            mViewStart = start;
            mViewEnd = end;
        }


        /// <summary>
        /// 
        /// </summary>
        private void NewQueryTags()
        {
            EnableFilter = false;
            Task.Run(() => {
                BuildFilters();
                mTotalPageNumber = -1;
                ContinueQueryTags();
                Application.Current?.Dispatcher.Invoke(new Action(() => {
                    EnableFilter = true;
                }));
            });
        }

        private void BuildFilters()
        {
            mFilters.Clear();
            if(!string.IsNullOrEmpty(this.FilterKeyName))
            {
                mFilters.Add("keyword", FilterKeyName);
            }
            if(this.TagTypeFilterEnable)
            {
                mFilters.Add("type", this.FilterType.ToString());
            }
            if(this.ReadWriteModeFilterEnable)
            {
                mFilters.Add("readwritetype", FilterReadWriteMode.ToString());
            }

            //
            if(!string.IsNullOrEmpty(CurrentGroup))
            {
                mFilters.Add("group", CurrentGroup);
            }

            if (this.RecordFilterEnable)
            {
                if (this.TimerRecordFilterEnable && this.ValueChangedRecordFilterEnable)
                {
                    mFilters.Add("recordtype", "3");
                }
                else if (this.TimerRecordFilterEnable)
                {
                    mFilters.Add("recordtype", "0");
                }
                else if (this.ValueChangedRecordFilterEnable)
                {
                    mFilters.Add("recordtype", "1");
                }
                else
                {
                    mFilters.Add("recordtype", "3");
                }
            }

            if(this.CompressFilterEnable)
            {
                mFilters.Add("compresstype", FilterCompressType.ToString());
            }

            string stmp = "";
            if(this.DriverFilterEnable)
            {
                stmp = this.FilterDriver;
            }
            if(this.RegistorFilterEnable)
            {
                stmp += "." + this.FilterRegistorName;
            }
            if(!string.IsNullOrEmpty(stmp))
            {
                mFilters.Add("linkaddress", stmp);
            }

        }



        /// <summary>
        /// 
        /// </summary>
        public void ContinueLoadData()
        {
            if (!IsLoading)
            {
                IsLoading = true;
                System.Threading.Tasks.Task.Run(() => { ContinueQueryTags(); IsLoading = false; });
            }
        }

        private bool mIsBusy = false;

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool CanContinueLoadData()
        {
            return mTotalPageNumber < 0 || mCurrentPageIndex < mTotalPageNumber;
        }

        /// <summary>
        /// 
        /// </summary>
        private void ContinueQueryTags()
        {
            if (mIsBusy) return;

            mIsBusy = true;
            if (mTotalPageNumber <0)
            {
                Application.Current.Dispatcher.Invoke(new Action(() => {
                    lock (SelectGroupTags)
                    {
                       foreach(var vv in SelectGroupTags)
                        {
                            vv.Dispose();
                        }
                        SelectGroupTags.Clear();
                        mTagModels.Clear();
                    }
                }));
                
                mCurrentPageIndex = 0;
                
                var vv = mClient.QueryTags(mFilters, out int tagcount, 0, TagCountPerPage);

                mTotalPageNumber = tagcount / TagCountPerPage;
                mTotalPageNumber = tagcount % TagCountPerPage > 0 ? mTotalPageNumber++ : mTotalPageNumber;

                if (vv != null)
                {
                    lock (SelectGroupTags)
                    {
                        foreach (var vvv in vv)
                        {
                            TagViewModel model = new TagViewModel(vvv.Value.Item1, vvv.Value.Item2) { Database = Node.Model.DatabseName,Owner=this };
                            if (!(model.RealTagMode is ComplexTag))
                            {
                                Application.Current?.Dispatcher.Invoke(new Action(() =>
                                {
                                    SelectGroupTags.Add(model);
                                    mTagModels.Add(model.Id, model);
                                }));
                            }
                        }
                    }
                    
                }
                TagCount = tagcount;
            }
            else
            {
                if (mTotalPageNumber > mCurrentPageIndex+1)
                {
                    mCurrentPageIndex++;
                    var vv = mClient.QueryTags(mFilters, out int tagcount, mCurrentPageIndex*TagCountPerPage, TagCountPerPage);
                    //var vv = DevelopServiceHelper.Helper.QueryTagByGroup(this.GroupModel.Database, this.GroupModel.FullName, mCurrentPageIndex, out totalcount,out count, mFilters);
                    if (vv != null)
                    {
                        lock (SelectGroupTags)
                        {
                            foreach (var vvv in vv)
                            {
                                TagViewModel model = new TagViewModel(vvv.Value.Item1, vvv.Value.Item2) { Database = Node.Model.DatabseName };
                                if (!(model.RealTagMode is ComplexTag))
                                {
                                    Application.Current?.Dispatcher.BeginInvoke(new Action(() =>
                                    {

                                        SelectGroupTags.Add(model);
                                        mTagModels.Add(model.Id, model);
                                    }));
                                }
                            }
                        }
                    }

                    TagCount = tagcount;
                }
            }
           
            mIsBusy = false;
        }

        /// <summary>
        /// 刷新实时值
        /// </summary>
        private void RefreshRealValue()
        {
            if(mClient!=null && mClient.IsLogin)
            {
                IEnumerable<int> ids=null;

                lock (SelectGroupTags)
                {
                    var vcount=mTagModels.Count;
                    try
                    {
                        if (vcount > mViewStart)
                        {
                            ids = mTagModels.Keys.Skip(mViewStart).Take(Math.Min(vcount, mViewEnd) - mViewStart);
                        }
                    }
                    catch
                    {

                    }
                }

                if (ids != null && ids.Any())
                {
                   var vals =  mClient.GetRealDataValueAndQualityOnly(ids);
                    if(vals != null && vals.Count>0)
                    {
                        foreach (var vv in vals)
                        {
                            lock (SelectGroupTags)
                            {
                                mTagModels.TryGetValue(vv.Key, out TagViewModel? tag);
                                if (tag != null)
                                {
                                    tag.Value = vv.Value.Item1;
                                    tag.Quality = vv.Value.Item2;
                                }
                            }
                           
                        }
                        
                    }
                }
            }
            
        }
        

        /// <summary>
        /// 
        /// </summary>
        public void Active()
        {
            mIsDeActive = false;
            mTotalPageNumber = -1;

            string sip = mNode.Model.HostAddress;
            mUserName = mNode.Model.UserName;
            mPassword = mNode.Model.Password;

            Task.Run(() => {

                var sport = (mNode.Parent as DatabaseViewModel).GetDataServerPort(() => { return mIsDeActive; });
                if (sport > 0)
                {
                    mClient = new DBHighApi.ApiClient() { Port = mServerPort };
                    mClient.PropertyChanged += MClient_PropertyChanged;
                    mClient.Open(sip, sport);
                }
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void MClient_PropertyChanged(object? sender, System.ComponentModel.PropertyChangedEventArgs e)
        {
            if(e.PropertyName== "IsConnected")
            {
                if(mClient.IsConnected)
                {
                    Task.Run(() => {
                        mClient.Login(mUserName, mPassword);
                        if (mClient.IsLogin)
                        {
                            var re = mClient.ListALlTagGroup();
                            if (re != null)
                            {
                                re.Insert(0, "");
                                GroupList = re.Distinct().ToList();
                            }
                            ContinueQueryTags();

                            mScanTimer = new System.Timers.Timer(1000);
                            mScanTimer.Elapsed += MScanTimer_Elapsed;
                            mScanTimer.Start();
                            OnPropertyChanged("IsEnable");
                        }
                    });
                }
                else
                {
                    OnPropertyChanged("IsEnable");
                }
            }
        }

        private void MScanTimer_Elapsed(object? sender, System.Timers.ElapsedEventArgs e)
        {
            if (mScanTimer == null) return;

            mScanTimer.Elapsed -= MScanTimer_Elapsed;
            RefreshRealValue();
            if(mScanTimer != null)
            mScanTimer.Elapsed += MScanTimer_Elapsed;
        }

        private bool mIsDeActive = false;

        /// <summary>
        /// 
        /// </summary>
        public void DeActive()
        {
            mIsDeActive = true;
            try
            {
                if (mClient != null)
                {
                    mClient.PropertyChanged -= MClient_PropertyChanged;
                    mClient.Close();
                }

                if(mScanTimer!=null)
                {
                    mScanTimer.Stop();
                    mScanTimer.Elapsed -= MScanTimer_Elapsed;
                    mScanTimer = null;
                }
            }
            catch
            {

            }
        }

     
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
