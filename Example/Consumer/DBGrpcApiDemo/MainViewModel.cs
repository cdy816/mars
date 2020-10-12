//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/31 10:21:05.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace DBGrpcApiDemo
{
    /// <summary>
    /// 
    /// </summary>
    public class MainViewModel:ModelBase
    {

        #region ... Variables  ...
        private string mIp="127.0.0.1";
        private ICommand mLoginCommand;
        private ICommand mStopCommand;
        private Dictionary<string,TagItemInfo> mTags = new Dictionary<string, TagItemInfo>();
        DBGrpcApi.Client clinet;
        private Thread mScanThread;
        private bool mExited = false;

        private string mUserName="Admin";
        public string mPassword="Admin";
        private int mPort = 14333;

        private ICommand mSetTagValueCommand;

        private ICommand mQueryHisDataCommand;

        private DateTime mStartTime = DateTime.Now.Date;

        private DateTime mEndTime;

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
            mEndTime = mStartTime.AddDays(1);
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
                return mTags.Values.ToList();
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
                    OnPropertyChanged("UserName");
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
                    OnPropertyChanged("Password");
                }
            }
        }


        /// <summary>
            /// 
            /// </summary>
        public int Port
        {
            get
            {
                return mPort;
            }
            set
            {
                if (mPort != value)
                {
                    mPort = value;
                    OnPropertyChanged("Port");
                }
            }
        }


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
                        Start();
                    });
                }
                return mLoginCommand;
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
                        clinet.Logout();
                       // clinet.Close();
                        
                    });
                }
                return mStopCommand;
            }
        }

        /// <summary>
        /// Id
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Value
        /// </summary>
        public double Value { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public ICommand SetTagValueCommand
        {
            get
            {
                if (mSetTagValueCommand == null)
                {
                    mSetTagValueCommand = new RelayCommand(() => {

                        clinet.SetTagValue(Id,  Value);
                    });
                }
                return mSetTagValueCommand;
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public DateTime StartTime
        {
            get
            {
                return mStartTime;
            }
            set
            {
                if (mStartTime != value)
                {
                    mStartTime = value;
                    OnPropertyChanged("StartTime");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public DateTime EndTime
        {
            get
            {
                return mEndTime;
            }
            set
            {
                if (mEndTime != value)
                {
                    mEndTime = value;
                    OnPropertyChanged("EndTime");
                }
            }
        }


        public ICommand QueryHisDataCommand
        {
            get
            {
                if (mQueryHisDataCommand == null)
                {
                    mQueryHisDataCommand = new RelayCommand(() =>
                    {
                        var vals = clinet.ReadAllHisValue(new List<string> { "tag2" }, StartTime, EndTime);
                        if (vals != null&&vals.Count>0)
                        {
                            MessageBox.Show("读取历史数据个数:" + vals.First().Value.Count);
                        }
                    });
                }
                return mQueryHisDataCommand;
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
                mTags.Add("Double.Double" + i,new TagItemInfo() { Id = i, Name= "Double.Double" + i,Value = "0" });
            }
        }

        public void Start()
        {
            clinet = new DBGrpcApi.Client(Ip, Port);
            clinet.Login(UserName, Password);
            mScanThread = new Thread(ScanProcess);
            mScanThread.IsBackground = true;
            mScanThread.Start();
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
            if (!clinet.IsLogined) return;
            var vals = clinet.ReadRealValue(mTags.Keys.ToList());
            if (vals != null)
            {
                foreach(var vv in vals)
                {
                    mTags[vv.Key].Value = vv.Value.ToString();
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
        public string Name { get; set; }
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
