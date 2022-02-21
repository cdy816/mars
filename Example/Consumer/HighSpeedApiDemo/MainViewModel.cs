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

        private List<int> mIds;

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
                if(mSetTagValueCommand == null)
                {
                    mSetTagValueCommand = new RelayCommand(() => {

                        clinet.SetTagValue(Id, (int)(Cdy.Tag.TagType.Double), Value);
                    });
                }
                return mSetTagValueCommand;
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void Init()
        {
            mIds = new List<int>();
            for(int i=1;i<100000;i++)
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
          
            var vals = clinet.GetRealDataValueOnly(mIds,true);
            sw.Stop();
            Debug.WriteLine($"time : { sw.ElapsedMilliseconds }");
            if (vals != null)
            {
                for (int i = 0; i < 50000; i++)
                {
                    if(vals.ContainsKey(i))
                    mTags[i].Value = vals[i]?.ToString();
                }
            }

            sw.Restart();

            var avals = clinet.GetRealData(mIds, true);
            sw.Stop();
            Debug.WriteLine($"time : { sw.ElapsedMilliseconds }");
            if (avals != null)
            {
                for (int i = 0; i < 50000; i++)
                {
                    if (avals.ContainsKey(i))
                        mTags[i].Value = avals[i].Item1?.ToString();
                }
            }


            sw.Restart();

            var aqvals = clinet.GetRealDataValueAndQualityOnly(mIds, true);
            sw.Stop();
            Debug.WriteLine($"time : { sw.ElapsedMilliseconds }");
            if (aqvals != null)
            {
                for (int i = 0; i < 50000; i++)
                {
                    if (aqvals.ContainsKey(i))
                        mTags[i].Value = aqvals[i].Item1?.ToString();
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
