//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using Cdy.Tag.Driver;
using DBRuntime;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class Runner
    {

        #region ... Variables  ...

        public static string CurrentDatabase = "";

        public static string CurrentDatabaseVersion = "";

        public static string CurrentDatabaseLastUpdateTime = "";

        /// <summary>
        /// 
        /// </summary>
        public static Runner RunInstance = new Runner();

        /// <summary>
        /// 
        /// </summary>
        private string mDatabaseName = "local";

        private Database mDatabase;

        private RealDatabase mRealDatabase;

        private HisDatabase mHisDatabase;

        private RealEnginer realEnginer;

        private HisEnginer2 hisEnginer;

        private CompressEnginer2 compressEnginer;

        private SeriseEnginer2 seriseEnginer;

        private DataFileManager mHisFileManager;

        private QuerySerivce querySerivce;

        private SecurityRunner mSecurityRunner;

        private bool mIsStarted = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        static Runner()
        {
            RDDCManager.Manager.StartTime = DateTime.Now;
        }

        /// <summary>
        /// 
        /// </summary>
        public Runner()
        {
            RDDCManager.Manager.SwitchWorkStateAction = new Func<WorkState, bool>((state) => 
            {
                if (mIsStarted)
                {
                    if (state == WorkState.Primary)
                    {

                        return SwitchToPrimary();
                    }
                    else
                    {
                        return SwitchToStandby();
                    }
                }
                else
                {
                    while (!mIsStarted) System.Threading.Thread.Sleep(100);
                    if (state == WorkState.Primary)
                    {

                        return SwitchToPrimary();
                    }
                    else
                    {
                        return SwitchToStandby();
                    }
                }
            });
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public WorkState State
        {
            get
            {
                return RDDCManager.Manager.CurrentState;
            }
        }


        /// <summary>
        /// 数据库存访路径
        /// </summary>
        public string DatabasePath { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Database Database { get { return mDatabase; } }

        /// <summary>
        /// 
        /// </summary>
        public bool IsStarted
        {
            get
            {
                return mIsStarted;
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
           
            //注册日志
            ServiceLocator.Locator.Registor<ILog>(new ConsoleLogger());

            //注册线性转换器
            ValueConvertManager.manager.Registor(new LinerConvert());

            DataFileSeriserManager.manager.Init();
            CPUAssignHelper.Helper.Init();
        }

        /// <summary>
        /// 
        /// </summary>
        private void InitPath()
        {
            PathHelper.helper.CheckDataPathExist();
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private bool CheckDatabaseExist(string name)
        {
            return System.IO.File.Exists(PathHelper.helper.GetDataPath(name, name + ".db"));
        }

        /// <summary>
        /// 
        /// </summary>
        private void LoadDatabase()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            this.mDatabase = new DatabaseSerise().Load(mDatabaseName);
            this.mRealDatabase = this.mDatabase.RealDatabase;
            this.mHisDatabase = this.mDatabase.HisDatabase;
            CurrentDatabaseVersion = this.mRealDatabase.Version;
            CurrentDatabase = mRealDatabase.Name;
            CurrentDatabaseLastUpdateTime = mRealDatabase.UpdateTime;

           sw.Stop();
            LoggerService.Service.Info("LoadDatabase", "load " +mDatabaseName +" take " + sw.ElapsedMilliseconds.ToString() +" ms");
        }

        /// <summary>
        /// 重新加载数据库
        /// </summary>
        public void ReStartDatabase()
        {

            LoggerService.Service.Info("ReStartDatabase", "start to hot restart database.",ConsoleColor.DarkYellow);

            Stopwatch sw = new Stopwatch();
            sw.Start();

            var db = new DatabaseSerise().Load(mDatabaseName);
            List<Tagbase> ltmp = new List<Tagbase>();
            List<HisTag> htmp = new List<HisTag>();
            foreach(var vv in db.RealDatabase.Tags.Where(e=>!this.mRealDatabase.Tags.ContainsKey(e.Key)))
            {
                ltmp.Add(vv.Value);
            }

            foreach (var vv in db.HisDatabase.HisTags.Where(e => !this.mHisDatabase.HisTags.ContainsKey(e.Key)))
            {
                htmp.Add(vv.Value);
            }

            LoggerService.Service.Info("ReStartDatabase", "reload " + mDatabaseName + " take " + sw.ElapsedMilliseconds.ToString() + " ms");
            compressEnginer.WaitForReady();

            sw.Reset();
            sw.Start();
            hisEnginer.Pause();
            
            realEnginer.Lock();
            realEnginer.ReLoadTags(ltmp,db.RealDatabase);
            realEnginer.UnLock();

            hisEnginer.ReLoadTags(htmp, db.HisDatabase);
            compressEnginer.ReSizeTagCompress(htmp);

            hisEnginer.Resume();

            this.mDatabase = db;
            this.mRealDatabase = db.RealDatabase;
            this.mHisDatabase = db.HisDatabase;

            CurrentDatabaseVersion = db.Version;
            CurrentDatabase = db.Name;
            CurrentDatabaseLastUpdateTime = mRealDatabase.UpdateTime;
            sw.Stop();
            LoggerService.Service.Info("ReStartDatabase", "ReInit" + mDatabaseName + " take " + sw.ElapsedMilliseconds.ToString() + " ms");


            LoggerService.Service.Info("ReStartDatabase", "hot restart database finish.", ConsoleColor.DarkYellow);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        private async Task<bool> InitAsync(string database)
        {

            if (System.IO.Path.IsPathRooted(database))
            {
                this.mDatabaseName = System.IO.Path.GetFileNameWithoutExtension(database);
                this.DatabasePath = System.IO.Path.GetDirectoryName(database);
            }
            else
            {
                this.mDatabaseName = database;
            }
            InitPath();

            if (CheckDatabaseExist(mDatabaseName))
            {
                LoadDatabase();

                mHisFileManager = new DataFileManager(mDatabaseName);
                mHisFileManager.TagCountOneFile = mHisDatabase.Setting.TagCountOneFile;

                var task = mHisFileManager.Int();
                realEnginer = new RealEnginer(mRealDatabase);
                realEnginer.Init();
                ServiceLocator.Locator.Registor<IRealData>(realEnginer);
                ServiceLocator.Locator.Registor<IRealDataNotify>(realEnginer);
                ServiceLocator.Locator.Registor<IRealDataNotifyForProducter>(realEnginer);
                ServiceLocator.Locator.Registor<IRealTagConsumer>(realEnginer);
                ServiceLocator.Locator.Registor<IRealTagProduct>(realEnginer);

                hisEnginer = new HisEnginer2(mHisDatabase, realEnginer);
                hisEnginer.MergeMemoryTime = mHisDatabase.Setting.DataBlockDuration * 60;
                hisEnginer.LogManager = new LogManager2() { Database = mDatabaseName,Parent=hisEnginer };
                hisEnginer.Init();
                ServiceLocator.Locator.Registor<IHisEngine2>(hisEnginer);

                compressEnginer = new CompressEnginer2();
                compressEnginer.TagCountOneFile = mHisDatabase.Setting.TagCountOneFile;
                compressEnginer.Init();
                ServiceLocator.Locator.Registor<IDataCompress2>(compressEnginer);

                seriseEnginer = new SeriseEnginer2() { DatabaseName = database };
                seriseEnginer.FileDuration = mHisDatabase.Setting.FileDataDuration;
                seriseEnginer.BlockDuration = mHisDatabase.Setting.DataBlockDuration;
                seriseEnginer.TagCountOneFile = mHisDatabase.Setting.TagCountOneFile;
                seriseEnginer.DataSeriser = mHisDatabase.Setting.DataSeriser;
                seriseEnginer.Init();
                ServiceLocator.Locator.Registor<IDataSerialize2>(seriseEnginer);

                querySerivce = new QuerySerivce(this.mDatabaseName);

                mSecurityRunner = new SecurityRunner() { Document = mDatabase.Security };

                RegistorInterface();

                DriverManager.Manager.Init(realEnginer);

                HisQueryManager.Instance.Registor(mDatabaseName);
                HisQueryManager.Instance.StartMonitor();

                await task;

                return true;
            }
            else
            {
                LoggerService.Service.Erro("Runner", string.Format(DBRuntime.Res.Get("databasenotexist"), mDatabaseName));
                return false;
            }
           

        }

        /// <summary>
        /// 
        /// </summary>
        private void RegistorInterface()
        {
           
            ServiceLocator.Locator.Registor<IHisQuery>(querySerivce);

            ServiceLocator.Locator.Registor<ITagManager>(mRealDatabase);

            ServiceLocator.Locator.Registor<IRuntimeSecurity>(mSecurityRunner);
        }

        /// <summary>
        /// 
        /// </summary>
        public void ReStart()
        {
            Stop();
            Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start(int port = 14330)
        {
            StartAsync("local");
        }

        /// <summary>
        /// 启动
        /// </summary>
        /// <param name="database"></param>
        public async void StartAsync(string database,int port = -1)
        {
            LoggerService.Service.EnableLogger = true;
            LoggerService.Service.Info("Runner", " 数据库 " + database+" 开始启动");


            RDDCManager.Manager.Load(database);
            RDDCManager.Manager.Start();

            var re = await InitAsync(database);
            if (!re)
            {
                return;
            }

            int pt = port;
            if(pt<1)
            {
                pt = mDatabase.Setting.RealDataServerPort;
            }

            DBRuntime.Api.DataService.Service.Start(pt);

            if (RDDCManager.Manager.CurrentState == WorkState.Primary)
            {
                seriseEnginer.Start();
                compressEnginer.Start();
                hisEnginer.Start();
                DriverManager.Manager.Start();
            }

            mIsStarted = true;
            LoggerService.Service.Info("Runner", " 数据库 " + database + " 启动完成");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mode"></param>
        public bool Switch(WorkState mode)
        {
            if (RDDCManager.Manager.EnableRDDC)
            {
                if (RDDCManager.Manager.CurrentState != mode)
                {
                    return RDDCManager.Manager.ManualSwitchTo(mode);
                }
            }
            else
            {
                LoggerService.Service.Info("Runner", " Rddc is not enable.");
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        private bool SwitchToStandby()
        {
            try
            {
                hisEnginer.Stop();
                compressEnginer.Stop();
                seriseEnginer.Stop();
                DriverManager.Manager.Stop();
            }
            catch
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        private bool SwitchToPrimary()
        {
            try
            {
                DriverManager.Manager.Start();
                seriseEnginer.Start();
                compressEnginer.Start();
                hisEnginer.Start();
            }
            catch
            {
                return false;
            }
            return true;
        }


        /// <summary>
        /// 停止
        /// </summary>
        public  void Stop()
        {
            LoggerService.Service.EnableLogger = true;

            DBRuntime.Api.DataService.Service.Stop();
            hisEnginer.Stop();
            DriverManager.Manager.Stop();
            compressEnginer.Stop();
            seriseEnginer.Stop();

            hisEnginer.Dispose();
            compressEnginer.Dispose();
            seriseEnginer.Dispose();

            mIsStarted = false;
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
