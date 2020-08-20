//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/3/29 21:38:53.
//  Version 1.0
//  种道洋
//==============================================================

using DBDevelopClientApi;
using System;
using System.Collections.Generic;
using System.Text;

namespace DBInStudio.Desktop.ViewModel
{
    public class NewDatabaseViewModel : WindowViewModelBase
    {

        #region ... Variables  ...
        private string mName;
        private string mDesc;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        public NewDatabaseViewModel()
        {
            DefaultWidth = 400;
            DefaultHeight = 120;
            Title = Res.Get("NewDatabase");
            IsEnableMax = false;
        }
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
                    if (string.IsNullOrEmpty(mDesc)) Desc = value;
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string Desc
        {
            get
            {
                return mDesc;
            }
            set
            {
                if (mDesc != value)
                {
                    mDesc = value;
                }
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override bool CanOKCommandProcess()
        {
            return !string.IsNullOrEmpty(Name);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override bool OKCommandProcess()
        {
            if(!DevelopServiceHelper.Helper.NewDatabase(Name,Desc))
            {
                System.Windows.MessageBox.Show(Res.Get("NewDatabaseFailed"));
                return false;
            }
            return base.OKCommandProcess();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
