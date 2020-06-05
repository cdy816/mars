//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/31 10:21:05.
//  Version 1.0
//  种道洋
//==============================================================
using DBInStudio.Desktop;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace HighSpeedApiDemo
{
    /// <summary>
    /// 
    /// </summary>
    public class MainViewModel:ModelBase
    {

        #region ... Variables  ...
        private string mIp="127.0.0.1";
        private ICommand mConnectCommand;
        private ICommand mStopCommand;
        private List<TagItemInfo> mTags = new List<TagItemInfo>();
        DBHighApi.ApiClient clinet;
        private Thread mScanThread;
        private bool mExited = false;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public MainViewModel()
        {
            Init();
        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public List<TagItemInfo> Tags
        {
            get
            {
                return mTags;
            }
        }


        /// <summary>
            /// 
            /// </summary>
        public string Ip
        {
            get
            {
                return mIp;
            }
            set
            {
                if (mIp != value)
                {
                    mIp = value;
                    OnPropertyChanged("Ip");
                }
            }
        }

        public ICommand ConnectCommand
        {
            get
            {
                if(mConnectCommand==null)
                {
                    mConnectCommand = new RelayCommand(() => {
                        Connect();
                    },()=> { return !clinet.IsConnected; });
                }
                return mConnectCommand;
            }
        }

        public ICommand StopCommand
        {
            get
            {
                if(mStopCommand==null)
                {
                    mStopCommand = new RelayCommand(() => {
                        mExited = true;
                        clinet.Close();
                        
                    });
                }
                return mStopCommand;
            }
        }



        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void Init()
        {
            for(int i=1;i<1000;i++)
            {
                mTags.Add(new TagItemInfo() { Id = i, Value = "0" });
            }
            clinet = new DBHighApi.ApiClient();
            clinet.TagValueChangedCallBack = (val) =>
            {
                foreach (var vv in val)
                {
                    if(vv.Key<mTags.Count)
                    mTags[vv.Key].Value = vv.Value.Item1.ToString();
                }
            };
            clinet.PropertyChanged += Clinet_PropertyChanged;
        }

        private void Clinet_PropertyChanged(object sender, PropertyChangedEventArgs e)
        {
            if(e.PropertyName== "IsConnected")
            {
                if(clinet.IsConnected)
                {
                    clinet.Login("Admin", "Admin");
                    clinet.RegistorTagValueCallBack(500, 999);
                    mScanThread = new Thread(ScanProcess);
                    mScanThread.IsBackground = true;
                    mScanThread.Start();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void Connect()
        {
            clinet.Connect(mIp, 14332);
           
        }

        /// <summary>
        /// 
        /// </summary>
        private void ScanProcess()
        {
            while (!mExited)
            {
                UpdateValue();
                Thread.Sleep(1000);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void UpdateValue()
        {
            if (!clinet.IsLogin) return;
            var vals = clinet.GetRealDataValueOnly(0, 500);
            if (vals != null)
            {
                for (int i = 0; i < 500; i++)
                {
                    if(vals.ContainsKey(i))
                    mTags[i].Value = vals[i].ToString();
                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public class TagItemInfo : ModelBase
    {
        private string mValue;
        public int Id { get; set; }

        public string Value { get { return mValue; } set { mValue = value; OnPropertyChanged("Value"); } }

    }


    /// <summary>
    /// 
    /// </summary>
    public class ModelBase : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler PropertyChanged;

       protected  void OnPropertyChanged(string name)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
        }
    }

}
