//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/3/28 10:54:26.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Input;
using DBInStudio.Desktop.ViewModel;

namespace DBInStudio.Desktop
{
    public class MainViewModel:ViewModelBase
    {

        #region ... Variables  ...
        private ICommand mLoginCommand;
        private string mDatabase = string.Empty;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public ICommand LoginCommand
        {
            get
            {
                if(mLoginCommand==null)
                {
                    mLoginCommand = new RelayCommand(() => {
                        Login();
                    });
                }
                return mLoginCommand;
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...
        /// <summary>
        /// 
        /// </summary>
        private void Login()
        {
            LoginViewModel login = new LoginViewModel();
            if(login.ShowDialog().Value)
            {
                ListDatabaseViewModel ldm = new ListDatabaseViewModel();
                if(ldm.ShowDialog().Value)
                {
                    mDatabase = ldm.SelectDatabase.Name;
                }
            }
        }
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
