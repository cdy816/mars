//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/3/28 11:05:41.
//  Version 1.0
//  种道洋
//==============================================================

using DBDevelopClientApi;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Text;
using System.Windows;
using System.Windows.Input;

namespace DBInStudio.Desktop.ViewModel
{
    public class LoginViewModel:WindowViewModelBase
    {

        #region ... Variables  ...
        private string mServer="127.0.0.1"; 
        private string mUserName;
        private string mPassword;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        public LoginViewModel()
        {
            Title = Res.Get("Login");
            DefaultWidth = 500;
            DefaultHeight = 180;
            IsEnableMax = false;
        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
            /// 
            /// </summary>
        public string Server
        {
            get
            {
                return mServer;
            }
            set
            {
                if (mServer != value)
                {
                    mServer = value;
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public string UserName
        {
            get
            {
                return mUserName;
            }
            set
            {
                if (mUserName != value)
                {
                    mUserName = value;
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string Password
        {
            get
            {
                return mPassword;
            }
            set
            {
                if (mPassword != value)
                {
                    mPassword = value;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string LoginUserId { get; set; }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>

        protected override bool OKCommandProcess()
        {
            if(!Login())
            {
                System.Windows.MessageBox.Show(Res.Get("LoginFailed"));
                return false;
            }
            return base.OKCommandProcess();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private bool Login()
        {
            try
            {
                CheckLocalServerRun();
                LoginUserId = DevelopServiceHelper.Helper.Login(Server, UserName, Password);
            }
            catch(Exception ex)
            {
                MessageBox.Show(ex.Message);
                return false;
            }
            return !string.IsNullOrEmpty(LoginUserId);
        }

        private void CheckLocalServerRun()
        {
            if(mServer=="127.0.0.1"||mServer=="localhost")
            {
               var pps = Process.GetProcessesByName("DBInStudioServer");
                if(pps==null||pps.Length==0)
                {
                    Process.Start("DBInStudioServer.exe").WaitForExit(20000);
                }
            }
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
