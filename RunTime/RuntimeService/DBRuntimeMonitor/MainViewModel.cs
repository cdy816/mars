//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/3/28 10:54:26.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using DBRuntimeMonitor.ViewModel;
using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using System.Windows;
using System.Windows.Input;
using System.Xml.Linq;

namespace DBRuntimeMonitor
{
    public class MainViewModel : ViewModelBase, IProcessNotify, IParent
    {

        #region ... Variables  ...

        private ViewModelBase mContentViewModel;

        private bool mIsCanOperate = true;

        private double mProcessNotify;

        private Visibility mNotifyVisiblity = Visibility.Hidden;

        private bool mIsLogin;


        private System.Timers.Timer mCheckRunningTimer;

        private ICommand mAddCommand;

        private ICommand mRemoveCommand;

        private ICommand mManagerCommand;

        private ICommand mStartCommand;

        private ICommand mStopCommand;

        private ICommand mReRunCommand;


        private System.Collections.ObjectModel.ObservableCollection<DatabaseViewModel> mDatabaseItems = new System.Collections.ObjectModel.ObservableCollection<DatabaseViewModel>();

        private TreeItemViewModel mCurrentSelectItem;

        private MarInfoViewModel mInfo;

        private DatabaseViewModel mCurrentDatabase;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public MainViewModel()
        {
            ValueConvertManager.manager.Init();

            ServiceLocator.Locator.Registor<IProcessNotify>(this);
            mCheckRunningTimer = new System.Timers.Timer(1000);

            mInfo = new MarInfoViewModel();
            mContentViewModel = mInfo;
            LoadConfig();
        }


        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public DatabaseViewModel CurrentDatabase
        {
            get
            {
                return mCurrentDatabase;
            }
            set
            {
                if (mCurrentDatabase != value)
                {
                    mCurrentDatabase = value;
                    OnPropertyChanged("CurrentDatabase");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand ReRunCommand
        {
            get
            {
                if(mReRunCommand==null)
                {
                    mReRunCommand = new RelayCommand(() => {
                        CurrentDatabase.ReStart();
                    }, () => { return CurrentDatabase != null && CurrentDatabase.IsRunning; });
                }
                return mReRunCommand;
            }
        }


        /// <summary>
        /// 
        /// </summary>

        public ICommand StopCommand
        {
            get
            {
                if(mStopCommand==null)
                {
                    mStopCommand = new RelayCommand(() => { 
                    CurrentDatabase.Stop();
                    }, () => { return CurrentDatabase != null && CurrentDatabase.IsRunning; });
                }
                return mStopCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand StartCommand
        {
            get
            {
                if(mStartCommand==null)
                {
                    mStartCommand = new RelayCommand(() => { 
                    CurrentDatabase.Start();
                    }, () => { return CurrentDatabase != null && !CurrentDatabase.IsRunning; });
                }
                return mStartCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand ManagerCommand
        {
            get
            {
                if(mManagerCommand==null)
                {
                    mManagerCommand = new RelayCommand(() => {
                        if(CurrentDatabase != null)
                            CurrentDatabase.Manager();
                        else
                        {
                            new DatabaseViewModel(null).Manager();
                        }
                    });
                }
                return mManagerCommand;
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public TreeItemViewModel CurrentSelectItem
        {
            get
            {
                return mCurrentSelectItem;
            }
            set
            {
                if (value != null)
                {
                    if (mCurrentSelectItem != value && value.IsEnable)
                    {
                        value.OnEnable = null;

                        mCurrentSelectItem = value;
                        SelectContentModel();

                        OnPropertyChanged("CurrentSelectItem");
                    }
                    else if (!value.IsEnable)
                    {
                        value.OnEnable = (val, sender) =>
                        {
                            if (val)
                            {
                                Application.Current.Dispatcher.Invoke(() =>
                                {
                                    CurrentSelectItem = sender;
                                });
                            }
                        };
                    }
                }
                else
                {
                    mCurrentSelectItem = value;
                    SelectContentModel();
                    OnPropertyChanged("CurrentSelectItem");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public System.Collections.ObjectModel.ObservableCollection<DatabaseViewModel> DatabaseItems
        {
            get
            {
                return mDatabaseItems;
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

                        DatabaseManagerViewModel dmv = new DatabaseManagerViewModel(Res.Get("AddDatabase"));
                        if(dmv.ShowDialog().Value)
                        {
                            this.mDatabaseItems.Add(new DatabaseViewModel(dmv.GetDatabase()) { Owner = this });
                            SaveConfig();
                        }
                    });
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
                        CurrentDatabase.RemoveCommand.Execute(null);
                    }, () => { return CurrentDatabase != null; });
                }
                return mRemoveCommand;
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public Visibility NotifyVisiblity
        {
            get
            {
                return mNotifyVisiblity;
            }
            set
            {
                if (mNotifyVisiblity != value)
                {
                    mNotifyVisiblity = value;
                    OnPropertyChanged("NotifyVisiblity");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public double ProcessNotify
        {
            get
            {
                return mProcessNotify;
            }
            set
            {
                if (mProcessNotify != value)
                {
                    mProcessNotify = value;
                    OnPropertyChanged("ProcessNotify");
                    OnPropertyChanged("ProcessNotifyPercent");
                }
            }
        }

        public double ProcessNotifyPercent
        {
            get
            {
                return mProcessNotify / 100;
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public bool IsCanOperate
        {
            get
            {
                return mIsCanOperate;
            }
            set
            {
                if (mIsCanOperate != value)
                {
                    mIsCanOperate = value;
                    OnPropertyChanged("IsCanOperate");
                }
            }
        }

        


        /// <summary>
        /// 
        /// </summary>
        public ViewModelBase ContentViewModel
        {
            get
            {
                return mContentViewModel;
            }
            set
            {
                if(mContentViewModel!=value)
                {
                    mContentViewModel = value;
                    OnPropertyChanged("ContentViewModel");
                    OnPropertyChanged("MonitorVisibily");
                    OnPropertyChanged("MonitorString");
                }
            }
        }

      

        /// <summary>
        /// 
        /// </summary>
        public bool IsLogin
        {
            get
            {
                return mIsLogin;
            }
            set
            {
                if (mIsLogin != value)
                {
                    mIsLogin = value;
                    OnPropertyChanged("IsLogin");
                    OnPropertyChanged("IsLoginOut");
                }
            }
        }

        public bool IsLoginOut
        {
            get
            {
                return !IsLogin;
            }
        }




        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void LoadConfig()
        {
            string sfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "DBRuntimeMonitor.ch");
            if(System.IO.File.Exists(sfile))
            {
                XElement xx = XElement.Load(sfile);
                foreach(var vv in xx.Elements())
                {
                    Database dd = new Database();
                    if(vv.Attribute("Databse") !=null)
                    {
                        dd.DatabseName = vv.Attribute("Databse").Value;
                    }
                    if (vv.Attribute("UserName") != null)
                    {
                        dd.UserName = vv.Attribute("UserName").Value;
                    }
                    if (vv.Attribute("HostAddress") != null)
                    {
                        dd.HostAddress = vv.Attribute("HostAddress").Value;
                    }
                    if (vv.Attribute("Password") != null)
                    {
                        dd.Password = RSADecrypt(vv.Attribute("Password").Value);
                    }
                    this.mDatabaseItems.Add(new DatabaseViewModel(dd) { Owner=this});
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void SaveConfig()
        {
            string sfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "DBRuntimeMonitor.ch");
            XElement xx = new XElement("DBRuntimeMonitor");
            foreach(var vv in this.mDatabaseItems)
            {
                XElement xe = new XElement("Databases");
                xe.SetAttributeValue("Databse", vv.Model.DatabseName);
                xe.SetAttributeValue("UserName", vv.Model.UserName);
                xe.SetAttributeValue("HostAddress", vv.Model.HostAddress);
                xe.SetAttributeValue("Password", RSAEncryption(vv.Model.Password));
                xx.Add(xe);
            }
            xx.Save(sfile);
        }

        /// <summary> 
        /// RSA加密数据 
        /// </summary> 
        /// <param name="express">要加密数据</param> 
        /// <param name="KeyContainerName">密匙容器的名称</param> 
        /// <returns></returns> 
        public static string RSAEncryption(string express, string KeyContainerName = null)
        {

            System.Security.Cryptography.CspParameters param = new System.Security.Cryptography.CspParameters();
            param.KeyContainerName = KeyContainerName ?? "Mars_CDY"; //密匙容器的名称，保持加密解密一致才能解密成功
            using (System.Security.Cryptography.RSACryptoServiceProvider rsa = new System.Security.Cryptography.RSACryptoServiceProvider(param))
            {
                byte[] plaindata = System.Text.Encoding.Default.GetBytes(express);//将要加密的字符串转换为字节数组
                byte[] encryptdata = rsa.Encrypt(plaindata, false);//将加密后的字节数据转换为新的加密字节数组
                return Convert.ToBase64String(encryptdata);//将加密后的字节数组转换为字符串
            }
        }
        /// <summary> 
        /// RSA解密数据 
        /// </summary> 
        /// <param name="express">要解密数据</param> 
        /// <param name="KeyContainerName">密匙容器的名称</param> 
        /// <returns></returns> 
        public static string RSADecrypt(string ciphertext, string KeyContainerName = null)
        {
            try
            {
                System.Security.Cryptography.CspParameters param = new System.Security.Cryptography.CspParameters();
                param.KeyContainerName = KeyContainerName ?? "Mars_CDY"; //密匙容器的名称，保持加密解密一致才能解密成功
                using (System.Security.Cryptography.RSACryptoServiceProvider rsa = new System.Security.Cryptography.RSACryptoServiceProvider(param))
                {
                    byte[] encryptdata = Convert.FromBase64String(ciphertext);
                    byte[] decryptdata = rsa.Decrypt(encryptdata, false);
                    return System.Text.Encoding.Default.GetString(decryptdata);
                }
            }
            catch
            {
                return String.Empty;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void SelectContentModel()
        {



            if (ContentViewModel is IModeSwitch)
            {
                (ContentViewModel as IModeSwitch).DeActive();
            }

            if (CurrentSelectItem != null)
            {
                ContentViewModel = CurrentSelectItem.GetModel(ContentViewModel);
                if(CurrentSelectItem is DatabaseViewModel)
                {
                    CurrentDatabase = CurrentSelectItem as DatabaseViewModel;
                }
                else
                {
                    if(CurrentSelectItem.Parent is DatabaseViewModel)
                    {
                        CurrentDatabase = CurrentSelectItem.Parent as DatabaseViewModel;
                    }
                }
            }
            else
            {
                ContentViewModel = null;
            }

            if (ContentViewModel is IModeSwitch)
            {
                (ContentViewModel as IModeSwitch).Active();
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void Manager_RefreshNameEvent(object sender, EventArgs e)
        {
            OnPropertyChanged("UserName");
        }

       

        /// <summary>
        /// 
        /// </summary>
        public void BeginShowNotify()
        {
            Application.Current?.Dispatcher.BeginInvoke(new Action(() => {

                NotifyVisiblity = Visibility.Visible;
            }));
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public void ShowNotifyValue(double val)
        {
            Application.Current?.Dispatcher.BeginInvoke(new Action(() => {
                if (val > 100)
                {
                    ProcessNotify = 100;
                }
                else
                {
                    ProcessNotify = val;
                }
            }));
            
        }

        /// <summary>
        /// 
        /// </summary>
        public void EndShowNotify()
        {
            Application.Current?.Dispatcher.BeginInvoke(new Action(() => {
                NotifyVisiblity = Visibility.Hidden;
                ProcessNotify = 0;
            }));
           
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        public void Remove(object target)
        {
            if(target is  DatabaseViewModel && mDatabaseItems.Contains(target as DatabaseViewModel))
            {
                mDatabaseItems.Remove(target as DatabaseViewModel);
                SaveConfig();
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    ///// <summary>
    ///// 
    ///// </summary>
    //public interface ITagGroupAdd
    //{
    //    bool AddGroup(string parent);
    //}

    /// <summary>
    /// 
    /// </summary>
    public interface IProcessNotify
    {
        /// <summary>
        /// 
        /// </summary>
        void BeginShowNotify();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        void ShowNotifyValue(double val);
        /// <summary>
        /// 
        /// </summary>
        void EndShowNotify();
    }

    /// <summary>
    /// 
    /// </summary>
    public interface IParent
    {
        void Remove(object target);
        void SaveConfig();
    }


}
