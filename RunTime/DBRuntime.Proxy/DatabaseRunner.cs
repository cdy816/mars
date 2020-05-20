using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DBRuntime.Proxy
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

        private NetTransformWorkMode mWorkMode = NetTransformWorkMode.Poll;

        private int mPollCircle = 1000;

        private bool mUseStandardHisDataServer = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        static DatabaseRunner()
        {
            //注册日志
            ServiceLocator.Locator.Registor<ILog>(new ConsoleLogger());
        }

        #endregion ...Constructor...

        #region ... Properties ...

        public bool IsReady { get; set; } = false;

        /// <summary>
        /// 
        /// </summary>
        public DbServerProxy Proxy
        {
            get
            {
                return mProxy;
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Load()
        {
            
            string sfileName = Assembly.GetEntryAssembly().Location;
            string spath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(sfileName), "Config");
            sfileName = System.IO.Path.Combine(spath, System.IO.Path.GetFileNameWithoutExtension(sfileName) + ".cfg");

            if(System.IO.File.Exists(sfileName))
            {
                XElement xe = XElement.Load(sfileName);
                if (xe.Element("ProxyClient") == null)
                    return;
                xe = xe.Element("ProxyClient");
                if(xe.Attribute("Ip")!=null)
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

                if (xe.Attribute("WorkMode") != null)
                {
                    mWorkMode = (NetTransformWorkMode) int.Parse(xe.Attribute("WorkMode").Value);
                }

                if (xe.Attribute("PollCircle") != null)
                {
                    mPollCircle = int.Parse(xe.Attribute("PollCircle").Value);
                }

                if (xe.Attribute("IsUseStandardHisDataServer") != null)
                {
                    mUseStandardHisDataServer = bool.Parse(xe.Attribute("IsUseStandardHisDataServer").Value);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            mProxy = new DbServerProxy() { UserName = mUserName, Password = mPassword,IsUseStandardHisDataServer= mUseStandardHisDataServer };
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
                            CheckAndLoadDatabase(sname);
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
        private void CheckAndLoadDatabase(string database)
        {
            string[] sbase = database.Split(new char[] { ',' });
            Load(sbase[0], sbase[1] + sbase[2]);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        public void Load(string database,string checkKey)
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

                string skey = mDatabase.RealDatabase.Version + mDatabase.RealDatabase.UpdateTime;

                if(skey!=checkKey)
                {
                    LoggerService.Service.Warn("Proxy","代理使用的数据库和服务器使用的数据库不一致.");
                }

                this.mRealDatabase = mDatabase.RealDatabase;
                mSecurityRunner = new SecurityRunner() { Document = mDatabase.Security };
            }
            realEnginer = new RealEnginer(mRealDatabase);
            realEnginer.Init();

            RegistorInterface();
            IsReady = true;

            mDriver = new NetTransformDriver() { Client = mProxy.NetworkClient ,WorkMode=mWorkMode,PollCircle=mPollCircle};
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
