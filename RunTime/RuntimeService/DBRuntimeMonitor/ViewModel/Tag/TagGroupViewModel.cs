//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/6/11 16:18:22.
//  Version 1.0
//  种道洋
//==============================================================

using DBRuntimeMonitor.ViewModel;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace DBRuntimeMonitor
{

    public class RootTagGroupViewModel : HasChildrenTreeItemViewModel
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private DBHighApi.ApiClient mDataClient;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public RootTagGroupViewModel()
        {
            Name = Res.Get("Tag");
        }
        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public override string FullName => string.Empty;

        /// <summary>
        /// 
        /// </summary>
        public Database Model { get; set; }

        /// <summary>
            /// 
            /// </summary>
        public DBHighApi.ApiClient DataClient
        {
            get
            {
                return mDataClient;
            }
            set
            {
                if (mDataClient != value)
                {
                    mDataClient = value;
                    OnPropertyChanged("DataClient");
                }
            }
        }




        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CanRemove()
        {
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CanCopy()
        {
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="oldName"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public override bool OnRename(string oldName, string newName)
        {
            return true;
        }

        public override ViewModelBase GetModel(ViewModelBase mode)
        {
            if (mode is TagGroupDetailViewModel)
            {
                (mode as TagGroupDetailViewModel).Node = this;
                return mode;
            }
            else
            {
                return new TagGroupDetailViewModel() { Node = this };
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class LogViewModel : HasChildrenTreeItemViewModel
    {

        #region ... Variables  ...
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public LogViewModel()
        {
            Name = Res.Get("Log");
        }
        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public override string FullName => string.Empty;

        /// <summary>
        /// 
        /// </summary>
        public Database Model { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CanRemove()
        {
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CanCopy()
        {
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="oldName"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public override bool OnRename(string oldName, string newName)
        {
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mode"></param>
        /// <returns></returns>
        public override ViewModelBase GetModel(ViewModelBase mode)
        {
            if (mode is LogDetailViewModel)
            {
                (mode as LogDetailViewModel).Node = this;
                return mode;
            }
            else
            {
                return new LogDetailViewModel() { Node = this };
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


}
