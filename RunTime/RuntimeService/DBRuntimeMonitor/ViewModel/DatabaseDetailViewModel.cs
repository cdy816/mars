using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace DBRuntimeMonitor.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class DatabaseDetailViewModel:ViewModelBase, IModeSwitch
    {

        #region ... Variables  ...
        
        private double mCup = 0;
        private double mTotalMemory = 1;
        private double mUsedMemory = 0;

        private double mSendBytes = 0;
        private double mReceiveBytes = 0;

        private System.Collections.ObjectModel.ObservableCollection<DriverInfo> mDiskInfo = new System.Collections.ObjectModel.ObservableCollection<DriverInfo>();

        private Dictionary<string, DriverInfo> mDriverInfoDic = new Dictionary<string, DriverInfo>();

        private DatabaseViewModel mNode;

        private System.Timers.Timer mScanTimer;

        private string mOSVersion = "";
        private string mDotnetVersion = "";
        private int mProcessCount = 0;
        private string mMachineName = "";
        private string mIs64Bit = "";

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public System.Collections.ObjectModel.ObservableCollection<DriverInfo> DiskInfo
        {
            get
            {
                return mDiskInfo;
            }
            set
            {
                mDiskInfo = value;
                OnPropertyChanged("DiskInfo");
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string OSVersion
        {
            get
            {
                return mOSVersion;
            }
            set
            {
                if (mOSVersion != value)
                {
                    mOSVersion = value;
                    OnPropertyChanged("OSVersion");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string DotnetVersion
        {
            get
            {
                return mDotnetVersion;
            }
            set
            {
                if (mDotnetVersion != value)
                {
                    mDotnetVersion = value;
                    OnPropertyChanged("DotnetVersion");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public int ProcessCount
        {
            get
            {
                return mProcessCount;
            }
            set
            {
                if (mProcessCount != value)
                {
                    mProcessCount = value;
                    OnPropertyChanged("ProcessCount");
                }
            }
        }


        /// <summary>
            /// 
            /// </summary>
        public string Is64Bit
        {
            get
            {
                return mIs64Bit;
            }
            set
            {
                if (mIs64Bit != value)
                {
                    mIs64Bit = value;
                    OnPropertyChanged("Is64Bit");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string MachineName
        {
            get
            {
                return mMachineName;
            }
            set
            {
                if (mMachineName != value)
                {
                    mMachineName = value;
                    OnPropertyChanged("MachineName");
                }
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public double Cup
        {
            get
            {
                return mCup;
            }
            set
            {
                if (mCup != value)
                {
                    mCup = value;
                    OnPropertyChanged("Cup");
                    OnPropertyChanged("CPUString");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string CPUString
        {
            get
            {
                return this.Cup.ToString("f0") + "%";
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string MemoryString
        {
            get
            {
                return $"{this.UsedMemory}/{this.TotalMemory}";
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public double TotalMemory
        {
            get
            {
                return mTotalMemory;
            }
            set
            {
                if (mTotalMemory != value)
                {
                    mTotalMemory = value;
                    OnPropertyChanged("TotalMemory");
                    OnPropertyChanged("MemoryString");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public double UsedMemory
        {
            get
            {
                return mUsedMemory;
            }
            set
            {
                if (mUsedMemory != value)
                {
                    mUsedMemory = value;
                    OnPropertyChanged("UsedMemory");
                    OnPropertyChanged("MemoryString");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public double SendBytes
        {
            get
            {
                return mSendBytes;
            }
            set
            {
                if (mSendBytes != value)
                {
                    mSendBytes = value;
                    OnPropertyChanged("SendBytes");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public double ReceiveBytes
        {
            get
            {
                return mReceiveBytes;
            }
            set
            {
                if (mReceiveBytes != value)
                {
                    mReceiveBytes = value;
                    OnPropertyChanged("ReceiveBytes");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public DatabaseViewModel Node
        {
            get
            {
                return mNode;
            }
            set
            {
                if (mNode != value)
                {
                    mNode = value;
                    OnPropertyChanged("Node");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Active()
        {
            Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void DeActive()
        {
            Stop();
        }



        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void Start()
        {
            mScanTimer = new System.Timers.Timer(1000);
            mScanTimer.Elapsed += MScanTimer_Elapsed;
            mScanTimer.Start();
            Task.Run(() => {
                UpdateLocalResource();
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        /// <exception cref="NotImplementedException"></exception>
        private void MScanTimer_Elapsed(object? sender, System.Timers.ElapsedEventArgs e)
        {
            if (mScanTimer == null) return;

            mScanTimer.Elapsed -= MScanTimer_Elapsed;
            UpdateLocalResource();
            if(mScanTimer != null)
            mScanTimer.Elapsed += MScanTimer_Elapsed;
        }

        private void UpdateLocalResource()
        {
            try
            {
                if (mNode != null && mNode.ServerClient != null && mNode.ServerClient.IsLogined)
                {
                    var minfo = mNode.ServerClient.GetMachineInfo();
                    if (minfo != null)
                    {
                        Application.Current.Dispatcher.BeginInvoke(() =>
                        {
                            this.MachineName = minfo.MachineName;
                            this.OSVersion = minfo.OSVersion;
                            this.DotnetVersion = minfo.DotnetVersion;
                            this.Is64Bit = minfo.Is64Bit ? "64" : "32";
                            this.ProcessCount = minfo.ProcessCount;
                        });
                    }

                    var rs = mNode.ServerClient.GetHostResource();
                    Application.Current.Dispatcher.BeginInvoke(() =>
                    {
                        this.Cup = rs.CPU;
                        this.TotalMemory = Math.Round(rs.MemoryTotal, 1);
                        this.UsedMemory = Math.Round(rs.MemoryUsed, 1);
                        try
                        {
                            this.SendBytes = Math.Round(double.Parse(rs.Network.Send), 1);
                            this.ReceiveBytes = Math.Round(double.Parse(rs.Network.Receive), 1);
                        }
                        catch
                        {

                        }
                    });


                    var disinfo = mNode.ServerClient.GetDiskInfo(mNode.Name);
                    if (disinfo != null)
                    {
                        Application.Current.Dispatcher.BeginInvoke(() =>
                        {
                            DriverInfo dinfo = null;
                            lock (mDiskInfo)
                            {
                                foreach (var vv in disinfo)
                                {
                                    if (mDriverInfoDic.ContainsKey(vv.UsedFor))
                                    {
                                        dinfo = mDriverInfoDic[vv.UsedFor];
                                    }
                                    else
                                    {
                                        dinfo = new DriverInfo() { Name = vv.UsedFor, Label = vv.Label };
                                        mDriverInfoDic.Add(dinfo.Name, dinfo);
                                        mDiskInfo.Add(dinfo);
                                    }
                                    dinfo.Label = vv.Label;
                                    dinfo.TotalDisk = Math.Round(vv.Total, 1);
                                    dinfo.UsedDisk = Math.Round(vv.Used, 1);
                                    if (vv.Total > 0)
                                        dinfo.UsedPercent = (vv.Used / vv.Total) * 100;
                                    else
                                    {
                                        dinfo.UsedPercent = 0;
                                    }
                                }
                            }
                        });
                    }
                }
            }
            catch(Exception ex)
            {
                mNode.ReInit();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void Stop()
        {
            if(mScanTimer != null)
            {
                mScanTimer.Stop();
                mScanTimer.Dispose();
                mScanTimer = null;
            }
            lock (mDiskInfo)
            {
                mDriverInfoDic.Clear();
                mDiskInfo.Clear();
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public class DriverInfo : ViewModelBase
    {

        private string mName = "";
        private double mTotalDisk = 1;
        private double mUsedDisk = 0;
        private string mLabel = "";

        /// <summary>
        /// 
        /// </summary>
        public string Name
        {
            get
            {
                return mName;
            }
            set
            {
                if (mName != value)
                {
                    mName = value;
                    OnPropertyChanged("Name");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string Label
        {
            get
            {
                return mLabel;
            }
            set
            {
                if (mLabel != value)
                {
                    mLabel = value;
                    OnPropertyChanged("Label");
                    OnPropertyChanged("Visiable");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public Visibility Visiable
        {
            get
            {
                return !string.IsNullOrEmpty(mLabel) ? Visibility.Visible : Visibility.Collapsed;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public double TotalDisk
        {
            get
            {
                return mTotalDisk;
            }
            set
            {
                if (mTotalDisk != value)
                {
                    mTotalDisk = value;
                    OnPropertyChanged("TotalDisk");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public double UsedDisk
        {
            get
            {
                return mUsedDisk;
            }
            set
            {
                if (mUsedDisk != value)
                {
                    mUsedDisk = value;
                    OnPropertyChanged("UsedDisk");
                }
            }
        }

        private double mUsedPercent = 0;

        public double UsedPercent { get { return mUsedPercent; }set { mUsedPercent = value;OnPropertyChanged("UsedPercent"); } }


    }

}
