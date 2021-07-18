//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/6/12 11:32:13.
//  Version 1.0
//  种道洋
//==============================================================

using DBDevelopClientApi;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Input;

namespace DBInStudio.Desktop.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class PermissionGroupManagerViewModel : WindowViewModelBase
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private ObservableCollection<GroupItemModel> mSelectGroups = new ObservableCollection<GroupItemModel>();

        /// <summary>
        /// 
        /// </summary>
        private ObservableCollection<GroupItemModel> mAllGroups = new ObservableCollection<GroupItemModel>();

        private List<string> mGroups;

        private ICommand mAddCommand;

        private ICommand mRemoveCommand;

        private ICommand mAddAllCommand;

        private ICommand mRemoveAllCommand;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public PermissionGroupManagerViewModel()
        {
            DefaultWidth = 720;
            DefaultHeight = 450;
            Title = Res.Get("Permission");
        }

        #endregion ...Constructor...

        #region ... Properties ...

        public ICommand AddAllCommand
        {
            get
            {
                if(mAddAllCommand==null)
                {
                    mAddAllCommand = new RelayCommand(() => {
                        foreach (var vv in mAllGroups)
                        {
                            mSelectGroups.Add(vv);
                        }
                        mAllGroups.Clear();
                    }, () => { return mAllGroups.Count > 0; });
                }
                return mAddAllCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand RemoveAllCommand
        {
            get
            {
                if(mRemoveAllCommand==null)
                {
                    mRemoveAllCommand = new RelayCommand(() => {
                        foreach (var vv in mSelectGroups)
                        {
                            mAllGroups.Add(vv);
                        }
                        mSelectGroups.Clear();
                    }, () => { return mSelectGroups.Count > 0; });
                }
                return mRemoveAllCommand;
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public ICommand AddCommand
        {
            get
            {
                if(mAddCommand==null)
                {
                    mAddCommand = new RelayCommand(() => { 
                        foreach(var vv in mAllGroups.Where(e=>e.IsSelected).ToList())
                        {
                            mSelectGroups.Add(vv);
                            mAllGroups.Remove(vv);
                        }
                    },()=> { return mAllGroups.Where(e => e.IsSelected).Count() > 0; });
                }
                return mAddCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand RemoveCommand
        {
            get
            {
                if(mRemoveCommand==null)
                {
                    mRemoveCommand = new RelayCommand(() => {
                        foreach (var vv in mSelectGroups.Where(e => e.IsSelected).ToList())
                        {
                            mAllGroups.Add(vv);
                            mSelectGroups.Remove(vv);
                        }
                    },()=> { return mSelectGroups.Where(e => e.IsSelected).Count() > 0; });
                }
                return mRemoveCommand;
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public ObservableCollection<GroupItemModel> SelectGroups
        {
            get
            {
                return mSelectGroups;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ObservableCollection<GroupItemModel> AllGroups
        {
            get
            {
                return mAllGroups;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public List<string> Groups { get { return mGroups; } set { mGroups = value;
                InitGroups();
            } }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>

        private void InitGroups()
        {
            mSelectGroups.Clear();
            foreach(var vv in mGroups)
            {
                mSelectGroups.Add(new GroupItemModel() { Name = vv, IsSelected = false });
            }
            if(!string.IsNullOrEmpty(this.Database))
            {
                var vg = DevelopServiceHelper.Helper.QueryTagGroups(Database);
                foreach(var vv in vg)
                {
                    if(!mGroups.Contains(vv.Item1))
                    {
                        mAllGroups.Add(new GroupItemModel() { Name = vv.Item1, IsSelected = false });
                    }
                }
            }
           


        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override bool OKCommandProcess()
        {
            this.mGroups = mSelectGroups.Select(e => e.Name).ToList();
            return base.OKCommandProcess();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public class GroupItemModel : ViewModelBase
    {

        #region ... Variables  ...
        private bool mIsSelected = false;
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
                    OnPropertyChanged("IsSelected");
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
