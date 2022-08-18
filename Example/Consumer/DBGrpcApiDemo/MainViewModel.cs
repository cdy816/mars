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

        private ICommand mQueryRealCommand;

        private DateTime mStartTime = DateTime.Now.Date;

        private DateTime mEndTime;

        private bool mIsReadStatistics = false;

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
        public ICommand QueryRealCommand
        {

            get
            {
                if(mQueryRealCommand== null)
                {
                    mQueryRealCommand = new RelayCommand(() => {
                        QueryRealValue();
                    });
                }
                return mQueryRealCommand;
            }
        }

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
        public bool IsReadStatistics
        {
            get
            {
                return mIsReadStatistics;
            }
            set
            {
                if (mIsReadStatistics != value)
                {
                    mIsReadStatistics = value;
                    OnPropertyChanged("IsReadStatistics");
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

                        Dictionary<string, string> values = new Dictionary<string, string>();
                        string[] ss = Id.Split(",");
                        foreach(var vv in ss)
                        {
                            values.Add(vv, Value.ToString());
                        }
                        clinet.SetTagValue(values);
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
                        if (mIsReadStatistics)
                        {
                            var vals = clinet.ReadStatisticsValue(new List<string> { mTags.First().Key }, StartTime, EndTime);
                            if(vals!=null && vals.Count>0)
                            {
                                int count = 0;
                                string sfile = System.IO.Path.GetTempFileName();
                                using (var stream = System.IO.File.Open(sfile, System.IO.FileMode.OpenOrCreate))
                                {
                                    using (var vss = new System.IO.StreamWriter(stream))
                                    {
                                        foreach (var vv in vals)
                                        {
                                            vss.WriteLine(vv.Key);
                                            foreach (var vvv in vv.Value)
                                            {
                                                vss.WriteLine(vvv.Time + "," + vvv.AvgValue + "," + vvv.MaxValueTime + "," + vvv.MaxValue + "," + vvv.MinValueTime + "," + vvv.MinValue);
                                                count++;
                                            }
                                        }
                                    }
                                }
                                System.IO.File.Move(sfile, sfile.Replace(".tmp", ".txt"));

                                MessageBox.Show("读取历史数据个数:" + count + " 详情查看历史文件:" + sfile.Replace(".tmp", ".txt"));
                            }
                        }
                        else
                        {
                            var vals = clinet.ReadAllHisValue(new List<string> { mTags.First().Key }, StartTime, EndTime);
                            if (vals != null && vals.Count > 0)
                            {

                                int count = 0;
                                string sfile = System.IO.Path.GetTempFileName();
                                using (var stream = System.IO.File.Open(sfile, System.IO.FileMode.OpenOrCreate))
                                {
                                    using (var vss = new System.IO.StreamWriter(stream))
                                    {
                                        foreach (var vv in vals)
                                        {
                                            vss.WriteLine(vv.Key);
                                            foreach (var vvv in vv.Value)
                                            {
                                                vss.WriteLine(vvv.Time + "," + vvv.Value);
                                                count++;
                                            }
                                        }
                                    }
                                }
                                System.IO.File.Move(sfile, sfile.Replace(".tmp", ".txt"));

                                MessageBox.Show("读取历史数据个数:" + count + " 详情查看历史文件:" + sfile.Replace(".tmp", ".txt"));
                            }
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

        private void QueryRealValue()
        {
            if(clinet!=null)
            {
                var vals = clinet.ReadRealValueAndQuality(new List<string> { Id });
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
