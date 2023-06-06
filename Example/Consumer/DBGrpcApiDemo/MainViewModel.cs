//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/31 10:21:05.
//  Version 1.0
//  种道洋
//==============================================================
using Cdy.Tag;
using Google.Protobuf.WellKnownTypes;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Linq;
using System.Net.NetworkInformation;
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
    public class MainViewModel : ModelBase
    {

        #region ... Variables  ...
        private string mIp = "127.0.0.1";
        private ICommand mLoginCommand;
        private ICommand mStopCommand;
        private Dictionary<string, TagItemInfo> mTags = new Dictionary<string, TagItemInfo>();
        DBGrpcApi.Client clinet;
        private Thread mScanThread;
        private bool mExited = false;

        private string mUserName = "Admin";
        public string mPassword = "Admin";
        private int mPort = 14333;

        private ICommand mSetTagValueCommand;

        private ICommand mQueryHisDataCommand;

        private ICommand mQueryHisData2Command;

        private ICommand mQueryRealCommand;

        private DateTime mStartTime = DateTime.Now.Date;

        private DateTime mEndTime;

        private bool mIsReadStatistics = false;

        public string mSqlExp = "select tag1 from a where time>='2023-04-29 0:0:0' and time < '2023-04-29 23:59:59'";
        
        private ICommand mSqlQueryCommand;

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
        public string SqlExp
        {
            get
            {
                return mSqlExp;
            }
            set
            {
                if (mSqlExp != value)
                {
                    mSqlExp = value;
                    OnPropertyChanged("SqlExp");
                }
            }
        }

        public ICommand SqlQueryCommand
        {
            get
            {
                if (mSqlQueryCommand == null)
                {
                    mSqlQueryCommand = new RelayCommand(() => {
                        ExecuteSql(mSqlExp);
                    });
                }
                return mSqlQueryCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand QueryRealCommand
        {

            get
            {
                if (mQueryRealCommand == null)
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
                if (mLoginCommand == null)
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
                if (mStopCommand == null)
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
                        foreach (var vv in ss)
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
                                                vss.WriteLine(vvv.Time + "," + vvv.AvgValue + "," + vvv.MaxValueTime + "," + vvv.MaxValue + "," + vvv.MinValueTime + "," + vvv.MinValue);
                                                count++;
                                            }
                                        }
                                    }
                                }
                                System.IO.File.Move(sfile, sfile.Replace(".tmp", ".txt"));
                                Process.Start("explorer.exe", sfile.Replace(".tmp", ".txt"));
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
                                Process.Start("explorer.exe", sfile.Replace(".tmp", ".txt"));
                            }
                        }
                    });
                }
                return mQueryHisDataCommand;
            }
        }

        public ICommand QueryHisData2Command
        {
            get
            {
                if (mQueryHisData2Command == null)
                {
                    mQueryHisData2Command = new RelayCommand(() =>
                    {
                        var vals = clinet.ReadHisValueByIgnorSystemExit(new List<string> { mTags.First().Key }, StartTime, EndTime,new TimeSpan(0,0,10),Cdy.Tag.QueryValueMatchType.Previous);
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
                            Process.Start("explorer.exe", sfile.Replace(".tmp", ".txt"));
                        }
                    });
                }
                return mQueryHisData2Command;
            }
        }
    

        private ICommand mSetTagStateCommand;
        public ICommand SetTagStateCommand
        {
            get
            {
                if (mSetTagStateCommand == null)
                {
                    mSetTagStateCommand = new RelayCommand(() => {
                        var vals = new Dictionary<string,short>();
                        string[] ss = Id.Split(",");
                        foreach (var vv in ss)
                        {
                            vals.Add(vv, (short)DateTime.Now.Second);
                        }
                      
                        clinet.SetTagState(vals);
                    });
                }
                return mSetTagStateCommand;
            }
        }

        private ICommand mSetTagExtendField2Command;
        public ICommand SetTagExtendField2Command
        {
            get
            {
                if (mSetTagExtendField2Command == null)
                {
                    mSetTagExtendField2Command = new RelayCommand(() => {
                        var vals = new Dictionary<string, long>();
                        string[] ss = Id.Split(",");
                        foreach (var vv in ss)
                        {
                            vals.Add(vv, (short)DateTime.Now.Second);
                        }
                        clinet.SetTagExtendField2(vals);
                    });
                }
                return mSetTagExtendField2Command;
            }
        }

        private ICommand mGetTagStateCommand;
        public ICommand GetTagStateCommand
        {
            get
            {
                if (mGetTagStateCommand == null)
                {
                    mGetTagStateCommand = new RelayCommand(() => {
                        var vlist = new List<string>();
                        vlist.AddRange(Id.Split(","));

                        var vals = clinet.ReadTagState(vlist);
                        if (vals != null && vals.Count > 0)
                        {
                            MessageBox.Show(vals.First().Value.ToString());
                        }
                    });
                }
                return mGetTagStateCommand;
            }
        }

        private ICommand mGetTagExtendField2Command;
        public ICommand GetTagExtendField2Command
        {
            get
            {
                if (mGetTagExtendField2Command == null)
                {
                    mGetTagExtendField2Command = new RelayCommand(() => {
                        var vlist = new List<string>();
                        vlist.AddRange(Id.Split(","));

                        var vals = clinet.ReadTagExtendField2(vlist);
                        if (vals != null && vals.Count > 0)
                        {
                            MessageBox.Show(vals.First().Value.ToString());
                        }
                    });
                }
                return mGetTagExtendField2Command;
            }
        }

        private ICommand mQueryMaxValueCommand;

        public ICommand QueryMaxValueCommand
        {
            get
            {
                if (mQueryMaxValueCommand == null)
                {
                    mQueryMaxValueCommand = new RelayCommand(() => {
                        var vals = clinet.FindNumberTagMaxValue(mTags.First().Key, StartTime, EndTime);
                        StringBuilder sb = new StringBuilder();
                        sb.Append(vals.Item1+" > ");
                        foreach(var vv in vals.Item2)
                        {
                            sb.Append(vv + ",");
                        }
                        MessageBox.Show(sb.ToString());
                    });
                   

                }
                return mQueryMaxValueCommand;
            }
        }

        private ICommand mQueryAvgValueCommand;
        public ICommand QueryAvgValueCommand
        {
            get
            {
                if (mQueryAvgValueCommand == null)
                {
                    mQueryAvgValueCommand = new RelayCommand(() => {
                        var vals = clinet.FindNumberTagAvgValue(mTags.First().Key, StartTime, EndTime);
                        MessageBox.Show(vals.ToString());
                    });
                }
                return mQueryAvgValueCommand;
            }
        }

        private ICommand mCalKeepTimeCommand;
        public ICommand CalKeepTimeCommand
        {
            get
            {
                if (mCalKeepTimeCommand == null)
                {
                    mCalKeepTimeCommand = new RelayCommand(() =>
                    {
                        var vals = clinet.CalTagValueKeepTime(mTags.First().Key, StartTime, EndTime, Cdy.Tag.NumberStatisticsType.GreatValue, Value, 0);
                        MessageBox.Show(vals.ToString());
                    });
                }
                return mCalKeepTimeCommand;
            }
        }

        private ICommand mQueryGreatValueCommand;
        public ICommand QueryGreatValueCommand
        {
            get
            {
                if(mQueryGreatValueCommand == null)
                {
                    mQueryGreatValueCommand = new RelayCommand(() =>
                    {
                        var vals = clinet.FindTagValue(mTags.First().Key, StartTime, EndTime, Cdy.Tag.NumberStatisticsType.GreatValue, Value, 0.1);
                        MessageBox.Show(vals.ToString());

                        var vals2 = clinet.FindTagValues(mTags.First().Key, StartTime, EndTime, Cdy.Tag.NumberStatisticsType.GreatValue, Value, 0.1);
                        if (vals2 != null)
                        {
                            StringBuilder sb = new StringBuilder();
                            foreach (var v in vals2)
                            {
                                sb.AppendLine(v.Key + "," + v.Value);
                            }
                            MessageBox.Show(sb.ToString());
                        }
                    });
                }
                return mQueryGreatValueCommand;
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
                mTags.Add("tag" + i,new TagItemInfo() { Id = i, Name= "tag" + i,Value = "0" });
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

        private void ExecuteSql(string sql)
        {
            var vals = clinet.ReadValueBySql(sql);
            if (vals is List<List<string>>)
            {
                foreach (var vv in (vals as List<List<string>>))
                {
                    StringBuilder sb = new StringBuilder();
                    foreach (var vv2 in vv)
                    {
                        sb.Append($",{vv2}");
                    }
                    Debug.WriteLine(sb.ToString());
                }
            }
            else if (vals is string[])
            {
                foreach (var vv in vals as string[])
                {
                    Debug.Write(vv + ",");
                }
                Debug.WriteLine("");

            }
            else if (vals is List<RealTagValueWithTimer2>)
            {
                foreach (var vv in vals as List<RealTagValueWithTimer2>)
                {
                    Debug.WriteLine(vv.Value + ",");
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
