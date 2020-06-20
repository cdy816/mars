//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/6/14 15:55:42.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace DBInStudio.Desktop.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class ImportModeSelectViewModel: WindowViewModelBase
    {

        #region ... Variables  ...

        private int mMode = 0;

        private bool mIsAddMode;

        private bool mIsReplaceMode;

        private bool mIsReplace = true;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public ImportModeSelectViewModel()
        {
            DefaultWidth = 300;
            DefaultHeight = 60;
            Title = Res.Get("ImportMode");
        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
            /// 
            /// </summary>
        public bool IsReplace
        {
            get
            {
                return mIsReplace;
            }
            set
            {
                if (mIsReplace != value)
                {
                    mIsReplace = value;
                    if (value) Mode = 0;
                    OnPropertyChanged("IsReplace");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public bool IsReplaceMode
        {
            get
            {
                return mIsReplaceMode;
            }
            set
            {
                if (mIsReplaceMode != value)
                {
                    mIsReplaceMode = value;
                    if (value) Mode = 1;
                    OnPropertyChanged("IsReplaceMode");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public bool IsAddMode
        {
            get
            {
                return mIsAddMode;
            }
            set
            {
                if (mIsAddMode != value)
                {
                    mIsAddMode = value;
                    if (value) Mode = 2;
                    OnPropertyChanged("IsAddMode");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public int Mode
        {
            get
            {
                return mMode;
            }
            set
            {
                if (mMode != value)
                {
                    mMode = value;
                    OnPropertyChanged("Mode");
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
