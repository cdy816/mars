﻿using Cdy.Tag;
using DirectAccessDriver.ClientApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DirectAccessGrpc
{
    public class DirectAccessProxy
    {

        #region ... Variables  ...

        public static DirectAccessProxy Proxy = new DirectAccessProxy();

        private string mIp;
        private int mPort;
        private string mUserName;
        private string mPassword;

        private int mServerPort = 10081;

        private DirectAccessDriver.ClientApi.DriverProxy mClient;

        private bool mIsConnected = false;

        private Dictionary<string, Tuple<int, byte>> mTagCach = new Dictionary<string, Tuple<int, byte>>();

        public object RealSyncLocker { get; set; } = new object();

        public object HisSyncLocker { get; set; } = new object();

        private Thread mMonitorThread;
        private bool mIsConnectedChanged = false;
        bool isClosed = false;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, Tuple<int, byte>> TagCach
        {
            get
            {
                return mTagCach;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsConnected
        {
            get
            {
                return mIsConnected;
            }
            set
            {
                if (mIsConnected != value)
                {
                    mIsConnected = value;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int ServerPort
        {
            get
            {
                return mServerPort;
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public DirectAccessProxy Load()
        {
            string sfileName = Assembly.GetEntryAssembly().Location;
            string spath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(sfileName), "Config");
            sfileName = System.IO.Path.Combine(spath, System.IO.Path.GetFileNameWithoutExtension(sfileName) + ".cfg");

            if (System.IO.File.Exists(sfileName))
            {
                XElement xx = XElement.Load(sfileName);
                if (xx.Element("ProxyClient") == null)
                    return this;
                var xe = xx.Element("ProxyClient");
                if (xe.Attribute("Ip") != null)
                {
                    mIp = xe.Attribute("Ip").Value;
                }

                if (xe.Attribute("Port") != null)
                {
                    mPort = int.Parse(xe.Attribute("Port").Value);
                }

                if (xe.Attribute("LoginUser") != null)
                {
                    mUserName = xe.Attribute("LoginUser").Value;
                }

                if (xe.Attribute("LoginPassword") != null)
                {
                    mPassword = xe.Attribute("LoginPassword").Value;
                }

                if(xx.Attribute("ServerPort") !=null)
                {
                    mServerPort = int.Parse(xx.Attribute("ServerPort").Value);
                }
            }
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        public DirectAccessProxy Start()
        {
            mClient = new DirectAccessDriver.ClientApi.DriverProxy();
            //mClient.PropertyChanged += MClient_PropertyChanged;
            mClient.DatabaseChanged = (his, real) => {
                Task.Run(() => { InitTagInfos(); });
            };
            mClient.Open(mIp, mPort);
            mMonitorThread = new Thread(MonitorProcess);
            mMonitorThread.IsBackground = true;
            mMonitorThread.Start();
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Close()
        {
            isClosed = true;
            mClient.Close();
        }

        private void MonitorProcess()
        {
            while(!isClosed)
            {
                if (!mClient.IsLogin)
                {
                    if (mClient.IsConnected)
                    {
                        var re = mClient.Login(mUserName, mPassword);
                        if (re)
                        {
                            InitTagInfos();
                            IsConnected = true;
                            Cdy.Tag.LoggerService.Service.Info("DataAccess", $"Login {mClient.ServerIp}:{mClient.Port}  sucessfull.");
                            mIsConnectedChanged = false;
                        }
                        else
                        {
                            IsConnected = false;
                            Cdy.Tag.LoggerService.Service.Info("DataAccess", $"Login {mClient.ServerIp}:{mClient.Port}  failed.");
                        }
                    }
                    else if (IsConnected)
                    {
                        IsConnected = false;
                        SecurityManager.Manager.Clear();
                        Cdy.Tag.LoggerService.Service.Info("DataAccess", $"Login {mClient.ServerIp}:{mClient.Port}  failed.");
                    }
                    Thread.Sleep(2000);
                }
                else
                {
                    mClient.Hart();
                    Thread.Sleep(3000);
                }
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="sender"></param>
        ///// <param name="e"></param>
        //private void MClient_PropertyChanged(object sender, System.ComponentModel.PropertyChangedEventArgs e)
        //{
        //    if(e.PropertyName== "IsConnected")
        //    {
        //        mIsConnectedChanged = true;
        //        //if(mClient.IsConnected)
        //        //{
        //        //    if(!mClient.IsLogin)
        //        //    {
        //        //        Task.Run(() => {
        //        //            var re = mClient.Login(mUserName, mPassword);
        //        //            if (re)
        //        //            {
        //        //                InitTagInfos();
        //        //                IsConnected = true;
        //        //                Cdy.Tag.LoggerService.Service.Info("DataAccess", $"Login {mClient.ServerIp}:{mClient.Port}  sucessfull.");
        //        //            }
        //        //            else
        //        //            {
        //        //                IsConnected = false;
        //        //                Cdy.Tag.LoggerService.Service.Info("DataAccess", $"Login {mClient.ServerIp}:{mClient.Port}  failed.");
        //        //            }
        //        //        });

        //        //    }
        //        //    else
        //        //    {
        //        //        IsConnected = true;
        //        //        Cdy.Tag.LoggerService.Service.Info("DataAccess", $"Login {mClient.ServerIp}:{mClient.Port}  sucessfull.");
        //        //    }
        //        //}
        //        //else
        //        //{
        //        //    SecurityManager.Manager.Clear();
        //        //    IsConnected = false;
        //        //    Cdy.Tag.LoggerService.Service.Warn("DataAccess", $"Login {mClient.ServerIp}:{mClient.Port}  failed.");
        //        //}
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public string Login(string user,string password)
        {
            if(IsConnected)
            {
                var re = mClient.Login2(user, password);
                return re;
            }
            return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        private void InitTagInfos()
        {
            lock (HisSyncLocker)
                lock (RealSyncLocker)
                {
                    try
                    {
                        mTagCach.Clear();
                        var vids = mClient.QueryAllTagIdAndNames();
                        if (vids != null && vids.Count > 0)
                        {
                            foreach (var vv in vids)
                            {
                                if (mTagCach.ContainsKey(vv.Value.Item1))
                                {
                                    mTagCach[vv.Value.Item1] = new Tuple<int, byte>(vv.Key, vv.Value.Item2);
                                }
                                else
                                {
                                    mTagCach.Add(vv.Value.Item1, new Tuple<int, byte>(vv.Key, vv.Value.Item2));
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {

                    }
                }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool UpdateData(RealDataBuffer values)
        {
            if(IsConnected)
            {
                return mClient.SetTagRealAndHisValue(values);
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool UpdateHisData(HisDataBuffer values,int count)
        {
            if (IsConnected)
            {
                return mClient.SetMutiTagHisValue(values,count,10000);
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool UpdateHisData(Dictionary<int, IEnumerable<TagValueAndType>> values)
        {
            if (IsConnected)
            {
                return mClient.SetMutiTagHisValue(values, 10000);
            }
            return true;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}