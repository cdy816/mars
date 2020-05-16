using DBRunTime.ServiceApi;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DbInRunWebApi
{
    public class DbServerProxy:INotifyPropertyChanged
    {

        #region ... Variables  ...

        public static DbServerProxy Proxy = new DbServerProxy();

        ApiClient dbClient;

        private bool mIsConnected;

        private ManualResetEvent resetEvent;

        private Thread mScanThread;

        private string mIp;
        private int mPort = 0;

        public event PropertyChangedEventHandler PropertyChanged;

        private bool mIsClosed = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public DbServerProxy()
        {
            Init();
            resetEvent = new ManualResetEvent(false);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public string UserName { get; set; } = "Admin";

        /// <summary>
        /// 
        /// </summary>
        public string Password { get; set; } = "Admin";

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
                mIsConnected = value;
                OnPropertyChanged("IsConnected");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ApiClient NetworkClient
        {
            get
            {
                return dbClient;
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void ConnectProcess()
        {
            Thread.Sleep(1000);
            if(dbClient.IsConnected)
            {
               IsConnected = dbClient.Login(UserName, Password);
            }
            else
            {
                dbClient.Connect(mIp, mPort);
            }
            resetEvent.Set();
            while (!mIsClosed)
            {
                resetEvent.WaitOne();
                if (mIsClosed) break;
                if (!mIsConnected)
                {
                    if (dbClient.IsConnected)
                    {
                        IsConnected = dbClient.Login(UserName, Password);
                    }
                    else if(dbClient.NeedReConnected)
                    {
                        dbClient.Connect(mIp, mPort);
                    }
                    Thread.Sleep(1000);
                }
                else
                {
                    resetEvent.Reset();
                }
              
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void Init()
        {
            dbClient = new ApiClient();
            dbClient.PropertyChanged += DbClient_PropertyChanged;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void DbClient_PropertyChanged(object sender, PropertyChangedEventArgs e)
        {
            if(e.PropertyName== "IsConnected")
            {
                if (!dbClient.IsConnected)
                    resetEvent.Set();
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="port"></param>
        public void Connect(string ip,int port)
        {
            mIp = ip;
            mPort = port;
            dbClient.Connect(ip, port);
            mScanThread = new Thread(ConnectProcess);
            mScanThread.IsBackground = true;
            mScanThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Close()
        {
            mIsClosed = true;
            resetEvent.Set();
            dbClient.PropertyChanged -= DbClient_PropertyChanged;
            dbClient.Close();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string GetRunnerDatabase()
        {
            return dbClient.GetRunnerDatabase(dbClient.LoginId);
        }

        #endregion ...Methods...

        #region ... Interfaces ...
        protected void OnPropertyChanged(string name)
        {
            if(PropertyChanged!=null)
            {
                PropertyChanged(this, new PropertyChangedEventArgs(name) );
            }
        }
        #endregion ...Interfaces...
    }
}
