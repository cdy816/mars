using Cdy.Tag;
using DbWebApiProxy.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DbInRunWebApi
{
    /// <summary>
    /// 
    /// </summary>
    public class DatabaseRunner
    {

        #region ... Variables  ...
        
        /// <summary>
        /// 
        /// </summary>
        public static DatabaseRunner Manager = new DatabaseRunner();

        Cdy.Tag.RealEnginer realEnginer;

        private string mDatabaseName;

        private RealDatabase mRealDatabase;

        private SecurityRunner mSecurityRunner;

        private DbServerProxy mProxy;

        private NetTransformDriver mDriver;

        private string mIp="127.0.0.1";
        private int mPort = 14330;

        private string mUserName="Admin";
        private string mPassword="Admin";

        private Thread mMonitorThread;

        private ManualResetEvent resetEvent = new ManualResetEvent(false);

        private bool mIsClosed = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        public bool IsReady { get; set; } = false;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Load()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            mProxy = new DbServerProxy() { UserName = mUserName, Password = mPassword };
            mProxy.Connect(mIp, mPort);
            mProxy.PropertyChanged += MProxy_PropertyChanged;
            
            mMonitorThread = new Thread(MonitorThreadPro);
            mMonitorThread.IsBackground = true;
            mMonitorThread.Start();


        }

        /// <summary>
        /// 
        /// </summary>
        public void Close()
        {
            mIsClosed = true;
            resetEvent.Set();
            mProxy.Close();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void MProxy_PropertyChanged(object sender, System.ComponentModel.PropertyChangedEventArgs e)
        {
            Task.Run(() => {
                if (mProxy.IsConnected)
                {
                    resetEvent.Set();
                }
                else
                {
                    if (!string.IsNullOrEmpty(mDatabaseName))
                    {
                        CloseDatabase();
                    }
                }

            });
            
        }

        /// <summary>
        /// 
        /// </summary>
        private void MonitorThreadPro()
        {
            while(!mIsClosed)
            {
                resetEvent.WaitOne();
                if (mIsClosed) break;
                while (!mIsClosed)
                {
                    if(mProxy.IsConnected)
                    {
                        string sname = mProxy.GetRunnerDatabase();
                        if(!string.IsNullOrEmpty(sname))
                        {
                            Load(sname);
                            break;
                        }
                    }
                    Thread.Sleep(2000);
                }
                resetEvent.Reset();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        public void Load(string database)
        {
            if (database == mDatabaseName) return;

            if (mDriver != null)
                mDriver.Stop();

            if (System.IO.Path.IsPathRooted(database))
            {
                this.mDatabaseName = System.IO.Path.GetFileNameWithoutExtension(database);
            }
            else
            {
                this.mDatabaseName = database;
            }
            PathHelper.helper.CheckDataPathExist();
            if (CheckDatabaseExist(mDatabaseName))
            {
                var mDatabase = new DatabaseSerise().Load(mDatabaseName);
                this.mRealDatabase = mDatabase.RealDatabase;
                mSecurityRunner = new SecurityRunner() { Document = mDatabase.Security };
            }
            realEnginer = new RealEnginer(mRealDatabase);
            realEnginer.Init();

            RegistorInterface();
            IsReady = true;

            mDriver = new NetTransformDriver() { Client = mProxy.NetworkClient };
            mDriver.Start(realEnginer);
        }

        /// <summary>
        /// 
        /// </summary>
        private void RegistorInterface()
        {
            ServiceLocator.Locator.Registor<IRealData>(realEnginer);
            ServiceLocator.Locator.Registor<IRealDataNotify>(realEnginer);
            ServiceLocator.Locator.Registor<IRealDataNotifyForProducter>(realEnginer);
            ServiceLocator.Locator.Registor<IRealTagComsumer>(realEnginer);
            ServiceLocator.Locator.Registor<ITagManager>(mRealDatabase);
            ServiceLocator.Locator.Registor<IRuntimeSecurity>(mSecurityRunner);
            
        }

        private bool CheckDatabaseExist(string name)
        {
            return System.IO.File.Exists(PathHelper.helper.GetDataPath(name, name + ".db"));
        }

        /// <summary>
        /// 
        /// </summary>
        public void CloseDatabase()
        {
            realEnginer = null;
            realEnginer.Dispose();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
