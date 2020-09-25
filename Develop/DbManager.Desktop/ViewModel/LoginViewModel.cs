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
using System.Linq;
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
        private string mPassword="";
        private List<string> mIPList;
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
            mIPList = new List<string>() { mServer };
            LoadCachIPs();
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public List<string> IPList
        {
            get
            {
                return mIPList;
            }
        }


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
            else
            {
                SaveIpCach();
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
                if(!mIPList.Contains(mServer))
                {
                    mIPList.Add(mServer);
                }
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

        /// <summary>
        /// 
        /// </summary>
        private void CheckLocalServerRun()
        {
            if(mServer=="127.0.0.1"||mServer=="localhost")
            {
               var pps = Process.GetProcessesByName("DBInStudioServer");
              
                if(pps==null||pps.Length==0)
                {
                    try
                    {
                        var vfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "DBInStudioServer.exe");
                        Process.Start(vfile).WaitForExit(5000);
                    }
                    catch
                    {

                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void SaveIpCach()
        {
            StringBuilder sb = new StringBuilder();
            foreach(var vv in mIPList)
            {
                sb.AppendLine(vv);
            }
            string sfile = System.IO.Path.Combine( System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "login.cach");
            System.IO.File.WriteAllText(sfile, sb.ToString());
        }


        private void LoadCachIPs()
        {
            string sfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "login.cach");
            if(System.IO.File.Exists(sfile))
            {
                string[] sfiles = System.IO.File.ReadAllLines(sfile);
                mIPList.AddRange(sfiles);
                mIPList = mIPList.Distinct().ToList();
            }
        }

      

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
