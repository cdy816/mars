using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
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

        //private HisDatabase mHisDatabase;

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

        public delegate void IsReadyDelegate(bool value);

        public event IsReadyDelegate IsReadyEvent;

        public event EventHandler ValueUpdateEvent;

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
                    LoggerService.Service.Info("DatabaseProxy", "服务器连接成功!",ConsoleColor.Cyan);
                }
                else
                {
                    LoggerService.Service.Warn("DatabaseProxy", "服务器连接断开!");
                }
                resetEvent.Set();

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
                resetEvent.Reset();
                if (mIsClosed) break;

                if (mProxy.IsConnected)
                {
                    try
                    {
                        string sname = mProxy.GetRunnerDatabase();
                        if (!string.IsNullOrEmpty(sname))
                        {
                            CheckAndLoadDatabase(sname);
                        }
                        else
                        {
                            Thread.Sleep(1000);
                            resetEvent.Set();
                            continue;
                        }
                    }
                    catch
                    {
                        Thread.Sleep(1000);
                        resetEvent.Set();
                        continue;
                    }
                }
                else
                {
                    if (!string.IsNullOrEmpty(mDatabaseName))
                    {
                        CloseDatabase();
                    }
                }
                IsReadyEvent?.Invoke(mProxy.IsConnected);
                Thread.Sleep(2000);
                
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void CheckAndLoadDatabase(string database)
        {
            try
            {
                string[] sbase = database.Split(new char[] { ',' });
                if (!string.IsNullOrEmpty(sbase[0]) && !string.IsNullOrEmpty(sbase[1]))
                    Load(sbase[0], sbase[1] + sbase[2]);

                //IsReadyEvent?.Invoke(true);
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("CheckAndLoadDatabase", ex.Message);
            }
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
            if (CheckDatabaseExist(mDatabaseName) && IsRunInLocal())
            {
                LoggerService.Service.Info("DatabaseRunner", "开始从本地加载数据库:" + mDatabaseName);
                var mDatabase = new RealDatabaseSerise().LoadByName(mDatabaseName);

                string skey = mDatabase.Version + mDatabase.UpdateTime;

                if (skey != checkKey)
                {
                    LoggerService.Service.Warn("Proxy", "代理使用的数据库和服务器使用的数据库不一致,将从网络进行加载");
                    this.mRealDatabase = mProxy.LoadRealDatabase();
                    mSecurityRunner = new SecurityRunner() { Document = mProxy.LoadSecurity() };
                }
                else
                {
                    this.mRealDatabase = mDatabase;
                    mSecurityRunner = new SecurityRunner() { Document = new SecuritySerise().LoadByName(mDatabaseName) };
                }
                LoggerService.Service.Info("DatabaseRunner", "从本地加载数据库完成");
            }
            else
            {
                LoggerService.Service.Info("DatabaseRunner", "开始从远程加载数据库");
                this.mRealDatabase = mProxy.LoadRealDatabase();
                mSecurityRunner = new SecurityRunner() { Document = mProxy.LoadSecurity() };
                LoggerService.Service.Info("DatabaseRunner", "从远程加载数据库完成");
            }

            realEnginer = new RealEnginer(mRealDatabase);
            realEnginer.Init();

            RegistorInterface();
            IsReady = true;

            mDriver = new NetTransformDriver() { Client = mProxy.NetworkClient ,WorkMode=mWorkMode,PollCircle=mPollCircle};
            mDriver.ValueUpdateEvent += MDriver_ValueUpdateEvent;

            mProxy.NetworkClient.DatabaseChangedAction = (realchanged, hischanged, securitychanged) => { 
                
                if(IsRunInLocal())
                {
                    //Stopwatch sw = new Stopwatch();
                    //sw.Start();
                    LoggerService.Service.Info("DatabaseRunner", "开始从本地加载数据库:" + mDatabaseName);

                    if (realchanged)
                    {
                        mDriver.Stop();

                        var mDatabase = new RealDatabaseSerise().LoadByName(mDatabaseName);
                        this.mRealDatabase = mDatabase;

                        var oldeng = realEnginer;
                        realEnginer = new RealEnginer(this.mRealDatabase);
                        realEnginer.Init();

                        
                        mDriver.Start(realEnginer);

                        oldeng.Dispose();
                    }

                    if (securitychanged)
                    {
                        mSecurityRunner.Document = new SecuritySerise().LoadByName(mDatabaseName);
                    }

                    RegistorInterface();
                    //sw.Stop();

                    LoggerService.Service.Info("DatabaseRunner", "从本地加载数据库完成");
                }
                else
                {
                    //Stopwatch sw = new Stopwatch();
                    //sw.Start();

                    if (realchanged)
                    {
                        mDriver.Stop();

                        LoggerService.Service.Info("DatabaseRunner", "开始从远程加载数据库");
                        //通过远程下载数据库
                        this.mRealDatabase = mProxy.LoadRealDatabase();

                        var oldeng = realEnginer;
                        realEnginer = new RealEnginer(this.mRealDatabase);
                        realEnginer.Init();
                        
                        mDriver.Start(realEnginer);
                        oldeng.Dispose();
                        LoggerService.Service.Info("DatabaseRunner", "从远程加载数据库完成");
                    }

                    if (securitychanged)
                    {
                        LoggerService.Service.Info("DatabaseRunner", "开始从远程加载安全配置");
                        mSecurityRunner.Document = mProxy.LoadSecurity();
                        LoggerService.Service.Info("DatabaseRunner", "从远程加载安全配置完成");
                    }

                    RegistorInterface();
                    //sw.Stop();


                }
            };

            mDriver.Start(realEnginer);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private bool IsRunInLocal()
        {
            return mIp.StartsWith("127.0.0.") || mIp == "0.0.0.0" || Dns.GetHostAddresses(Dns.GetHostName()).Select(e => e.AddressFamily.ToString()).Contains(mIp);
        }

        private void MDriver_ValueUpdateEvent(object sender, EventArgs e)
        {
            ValueUpdateEvent?.Invoke(this, e);
        }

        /// <summary>
        /// 
        /// </summary>
        private void RegistorInterface()
        {
            ServiceLocator.Locator.Registor<IRealData>(realEnginer);
            ServiceLocator.Locator.Registor<IRealDataNotify>(realEnginer);
            ServiceLocator.Locator.Registor<IRealDataNotifyForProducter>(realEnginer);
            ServiceLocator.Locator.Registor<IRealTagConsumer>(realEnginer);
            ServiceLocator.Locator.Registor<ITagManager>(mRealDatabase);
            //ServiceLocator.Locator.Registor<IHisTagQuery>(mHisDatabase);
            
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
            if (mDriver != null)
            {
                mDriver.Stop();
                mDriver = null;
            }
            if (realEnginer != null)
            {
                realEnginer.Dispose();
                realEnginer = null;
            }
            mDatabaseName = string.Empty;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
