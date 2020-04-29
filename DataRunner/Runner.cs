//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
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

        private HisEnginer hisEnginer;

        private CompressEnginer compressEnginer;

        private SeriseEnginer seriseEnginer;

        private DataFileManager mHisFileManager;

        private QuerySerivce querySerivce;

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
            //注册日志
            ServiceLocator.Locator.Registor<ILog>(new ConsoleLogger());
        }

        #endregion ...Constructor...

        #region ... Properties ...

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
        private void InitPath()
        {
            PathHelper.helper.CheckDataPathExist();
        }
        
        /// <summary>
        /// 
        /// </summary>
        private void LoadDatabase()
        {
            this.mDatabase = new DatabaseSerise().Load(mDatabaseName);
            this.mRealDatabase = this.mDatabase.RealDatabase;
            this.mHisDatabase = this.mDatabase.HisDatabase;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        private async Task InitAsync(string database)
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

            LoadDatabase();

            mHisFileManager = new DataFileManager(mDatabaseName);
            mHisFileManager.TagCountOneFile = mHisDatabase.Setting.TagCountOneFile;

            var task = mHisFileManager.Int();
            realEnginer = new RealEnginer(mRealDatabase);
            realEnginer.Init();

            hisEnginer = new HisEnginer(mHisDatabase, realEnginer);
            hisEnginer.MergeMemoryTime = mHisDatabase.Setting.DataBlockDuration * 60;
            hisEnginer.Init();

            compressEnginer = new CompressEnginer(hisEnginer.MegerMemorySize);
            compressEnginer.TagCountOneFile = mHisDatabase.Setting.TagCountOneFile;

            seriseEnginer = new SeriseEnginer() { DatabaseName = database };
            seriseEnginer.FileDuration = mHisDatabase.Setting.FileDataDuration;
            seriseEnginer.BlockDuration = mHisDatabase.Setting.DataBlockDuration;
            seriseEnginer.TagCountOneFile = mHisDatabase.Setting.TagCountOneFile;
            seriseEnginer.DataSeriser = mHisDatabase.Setting.DataSeriser;

            querySerivce = new QuerySerivce(this.mDatabaseName);

            RegistorInterface();

            DriverManager.Manager.Init(realEnginer);

            await task;

        }

        /// <summary>
        /// 
        /// </summary>
        private void RegistorInterface()
        {
            ServiceLocator.Locator.Registor<IRealData>(realEnginer);
            ServiceLocator.Locator.Registor<IRealDataNotify>(realEnginer);

            ServiceLocator.Locator.Registor<IHisEngine>(hisEnginer);

            ServiceLocator.Locator.Registor<IDataCompress>(compressEnginer);
            ServiceLocator.Locator.Registor<IDataSerialize>(seriseEnginer);

            ServiceLocator.Locator.Registor<IHisQuery>(querySerivce);

            ServiceLocator.Locator.Registor<ITagManager>(mRealDatabase);
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
        public void Start()
        {
            StartAsync("local");
            
        }

        /// <summary>
        /// 启动
        /// </summary>
        /// <param name="database"></param>
        public async void StartAsync(string database)
        {
            LoggerService.Service.Info("Runner", " 数据库 " + database+" 开始启动");
            await InitAsync(database);
            seriseEnginer.Start();
            compressEnginer.Start();
            hisEnginer.Start();
            DriverManager.Manager.Start();
            mIsStarted = true;
            LoggerService.Service.Info("Runner", " 数据库 " + database + " 启动完成");
        }


        /// <summary>
        /// 停止
        /// </summary>
        public  void Stop()
        {
            hisEnginer.Stop();
            compressEnginer.Stop();
            seriseEnginer.Stop();
            mIsStarted = false;
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
