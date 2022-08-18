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

        private ICommand mSetTagValueCommand;

        private ICommand mQueryComplexValueCommand;

        private List<int> mIds;

        private List<int> mHours;

        private List<int> mSeconds;

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

        public ICommand QueryComplexValueCommand
        {
            get
            {
                if(mQueryComplexValueCommand == null)
                {
                    mQueryComplexValueCommand = new RelayCommand(() => {
                        QueryComplexRealValue();
                    });
                }
                return mQueryComplexValueCommand;
            }
        }

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
                        mExited = true;
                        clinet.Close();
                        
                    });
                }
                return mStopCommand;
            }
        }

        /// <summary>
        /// Id
        /// </summary>
        public int Id { get; set; }

        private double mValue;

        /// <summary>
        /// Value
        /// </summary>
        public double Value { get { return mValue; } set { mValue = value;OnPropertyChanged("Value"); } }



        private double mModifyValue;

        public double ModifyValue { get { return mModifyValue; } set { mModifyValue = value;OnPropertyChanged("ModifyValue"); } }

        private DateTime mModifyDate = DateTime.Now.Date;
        public DateTime ModifyDate { get { return mModifyDate; } set { mModifyDate = value;OnPropertyChanged("ModifyDate"); } }

        private int mModifyStartTime=0;

        public int ModifyStartTime { get { return mModifyStartTime; } set { mModifyStartTime = value;OnPropertyChanged("ModifyStartTime"); } }

        private int mModifyEndTime=1;
        public int ModifyEndTime { get { return mModifyEndTime; } set { mModifyEndTime = value;OnPropertyChanged("ModifyEndTime"); } }


        private int mModifyStartSecond = 0;

        public int ModifyStartSecond { get { return mModifyStartSecond; } set { mModifyStartSecond = value; OnPropertyChanged("ModifyStartSecond"); } }

        private int mModifyEndSecond = 60;
        public int ModifyEndSecond { get { return mModifyEndSecond; } set { mModifyEndSecond = value; OnPropertyChanged("ModifyEndSecond"); } }

        /// <summary>
        /// 
        /// </summary>
        public ICommand SetTagValueCommand
        {
            get
            {
                if(mSetTagValueCommand == null)
                {
                    mSetTagValueCommand = new RelayCommand(() => {

                        clinet.SetTagValue(Id, (int)(Cdy.Tag.TagType.Double), Value);
                    });
                }
                return mSetTagValueCommand;
            }
        }

        private ICommand mDeleteHisValueCommand;

        /// <summary>
        /// 
        /// </summary>
        public ICommand DeleteHisValueCommand
        {
            get
            {
                if (mDeleteHisValueCommand == null)
                {
                    mDeleteHisValueCommand = new RelayCommand(() => {
                        DateTime stime = ModifyDate.AddHours(ModifyStartTime).AddMinutes(ModifyStartSecond);
                        DateTime etime = ModifyDate.AddHours(ModifyEndTime).AddMinutes(ModifyEndSecond);
                        clinet.DeleteHisValue(Id,"Admin","Admin message",stime,etime);
                    });
                }
                return mDeleteHisValueCommand;
            }
        }

        private ICommand mModifyHisValueCommand;

        /// <summary>
        /// 
        /// </summary>
        public ICommand ModifyHisValueCommand
        {
            get
            {
                if(mModifyHisValueCommand == null)
                {
                    mModifyHisValueCommand = new RelayCommand(() => {
                        DateTime stime = ModifyDate.AddHours(ModifyStartTime).AddMinutes(ModifyStartSecond);
                        DateTime etime = ModifyDate.AddHours(ModifyEndTime).AddMinutes(ModifyEndSecond);

                        List<Cdy.Tag.TagHisValue<object>> vals = new List<Cdy.Tag.TagHisValue<object>>();
                        DateTime stmp = stime;
                        while(stmp<=etime)
                        {
                            vals.Add(new Cdy.Tag.TagHisValue<object>() { Quality=0,Time=stmp,Value = ModifyValue+1});
                            stmp=stmp.AddSeconds(1);
                        }

                        clinet.ModifyHisData(Id,Cdy.Tag.TagType.Double, "Admin", "Admin message", vals);
                    });
                }
                return mModifyHisValueCommand;
            }
        }


        private ICommand mQueryAllValueCommand;

        public ICommand QueryAllValueCommand
        {
            get
            {
                if (mQueryAllValueCommand == null)
                {
                    mQueryAllValueCommand = new RelayCommand(() =>
                    {
                        DateTime stime = ModifyDate.AddHours(ModifyStartTime).AddMinutes(ModifyStartSecond);
                        DateTime etime = ModifyDate.AddHours(ModifyEndTime).AddMinutes(ModifyEndSecond);

                        var vals = clinet.QueryAllHisValue<double>(Id,stime,etime);
                        if(vals!=null)
                        {
                            StringBuilder sb = new StringBuilder(); 
                            foreach(var val in vals.ListAvaiableValues())
                            {
                                sb.AppendLine($"{ val.Time } { val.Value } { val.Quality }");
                            }
                            ResultDialog rd = new ResultDialog();
                            rd.Result = sb.ToString();
                            rd.Show();
                        }
                    });
                }
                return mQueryAllValueCommand;
            }
        }

        private ICommand mQueryHisValueCommand;

        /// <summary>
        /// 
        /// </summary>
        public ICommand QueryHisValueCommand
        {
            get
            {
                if(mQueryHisValueCommand == null)
                {
                    mQueryHisValueCommand = new RelayCommand(() => {
                        DateTime stime = ModifyDate.AddHours(ModifyStartTime).AddMinutes(ModifyStartSecond);
                        DateTime etime = ModifyDate.AddHours(ModifyEndTime).AddMinutes(ModifyEndSecond);


                        List<DateTime> ltmp = new List<DateTime>();
                        DateTime dtmp = stime;
                        while (dtmp < etime)
                        {
                            ltmp.Add(dtmp);
                            dtmp = dtmp.AddSeconds(10);
                        }

                        var vals = clinet.QueryHisValueForTimeSpan<double>(Id, stime, etime,new TimeSpan(0,0,10),Cdy.Tag.QueryValueMatchType.Previous);
                        if (vals != null)
                        {
                            StringBuilder sb = new StringBuilder();
                            foreach (var val in vals.ListAvaiableValues())
                            {
                                sb.AppendLine($"{ val.Time } { val.Value } { val.Quality }");
                            }
                            ResultDialog rd = new ResultDialog();
                            rd.Result = sb.ToString();
                            rd.Show();
                        }
                    });
                }
                return mQueryHisValueCommand;
            }
        }

        private ICommand mQueryHisValue2Command;

        /// <summary>
        /// 
        /// </summary>
        public ICommand QueryHisValue2Command
        {
            get
            {
                if (mQueryHisValue2Command == null)
                {
                    mQueryHisValue2Command = new RelayCommand(() => {
                        DateTime stime = ModifyDate.AddHours(ModifyStartTime).AddMinutes(ModifyStartSecond);
                        DateTime etime = ModifyDate.AddHours(ModifyEndTime).AddMinutes(ModifyEndSecond);


                        List<DateTime> ltmp = new List<DateTime>();
                        DateTime dtmp = stime;
                        while (dtmp < etime)
                        {
                            ltmp.Add(dtmp);
                            dtmp = dtmp.AddSeconds(10);
                        }

                        var vals = clinet.QueryHisValueAtTimes<double>(Id, ltmp, Cdy.Tag.QueryValueMatchType.Previous);
                        if (vals != null)
                        {
                            StringBuilder sb = new StringBuilder();
                            foreach (var val in vals.ListAvaiableValues())
                            {
                                sb.AppendLine($"{ val.Time } { val.Value } { val.Quality }");
                            }
                            ResultDialog rd = new ResultDialog();
                            rd.Result = sb.ToString();
                            rd.Show();
                        }
                    });
                }
                return mQueryHisValue2Command;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public List<int> Hours
        {
            get
            {
                return mHours;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public List<int> Seconds
        {
            get
            {
                return mSeconds;
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void QueryComplexRealValue()
        {
            var vals = clinet.GetComplextTagRealData(Id,true);
            if(vals != null)
            {

            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void Init()
        {

            mHours = new List<int>();
            for(int i = 0; i < 24; i++)
            {
                mHours.Add(i);
            }

            mSeconds = new List<int>();
            for(int i=0;i< 60;i++)
            {
                mSeconds.Add(i);
            }

            mIds = new List<int>();
            for(int i=1;i<=100000;i++)
            {
                mTags.Add(new TagItemInfo() { Id = i, Value = "0" });
                mIds.Add(i);
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
                    Task.Run(() => {
                        clinet.Login("Admin", "Admin");
                        clinet.RegistorTagValueCallBack(500, 999);
                        InitFunTest();
                        mScanThread = new Thread(ScanProcess);
                        mScanThread.IsBackground = true;
                        mScanThread.Start();
                    });
                   
                }
            }
        }

        private void InitFunTest()
        {
            clinet.GetTagIds(new List<string>() { "Double.Double1" });
            clinet.ListAllTag();
            var grps = clinet.ListALlTagGroup();
            clinet.ListTagByGroup("Double");
        }

        /// <summary>
        /// 
        /// </summary>
        private void Connect()
        {
            clinet.Open(mIp, 14332);
           
        }

        /// <summary>
        /// 
        /// </summary>
        private void ScanProcess()
        {
            while (!mExited)
            {
                UpdateValue();
                Thread.Sleep(500);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void UpdateValue()
        {
            if (!clinet.IsLogin)
            {
                clinet.Login("Admin", "Admin");
                return;
            }
            Stopwatch sw = new Stopwatch();
            sw.Start();

            var vals = clinet.GetRealDataValueOnly(mIds, true);
            sw.Stop();
            Debug.WriteLine($"time : { sw.ElapsedMilliseconds }");
            if (vals != null)
            {
                for (int i = 0; i < 50000; i++)
                {
                    if (vals.ContainsKey(i))
                        mTags[i].Value = vals[i]?.ToString();
                }
            }

            sw.Restart();

            var avals = clinet.GetRealData(mIds, true);
            sw.Stop();
            Debug.WriteLine($"time : { sw.ElapsedMilliseconds }");
            if (avals != null)
            {
                for (int i = 50000; i < 100000; i++)
                {
                    if (avals.ContainsKey(i))
                        mTags[i].Value = avals[i].Item1?.ToString();
                }
            }


            //sw.Restart();

            //var aqvals = clinet.GetRealDataValueAndQualityOnly(mIds, true);
            sw.Stop();
            //Debug.WriteLine($"time : { sw.ElapsedMilliseconds }");
            //if (aqvals != null)
            //{
            //    for (int i = 0; i < 50000; i++)
            //    {
            //        if (aqvals.ContainsKey(i))
            //            mTags[i].Value = aqvals[i].Item1?.ToString();
            //    }
            //}
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
