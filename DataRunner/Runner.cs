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
        private string mDatabaseName="local";

        private Database mDatabase;

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

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 数据库存访路径
        /// </summary>
        public string DatabasePath { get; set; }

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
            if (!string.IsNullOrEmpty(DatabasePath))
            {
                PathHelper.helper.SetDataBasePath(DatabasePath);
            }
            else
            {
                PathHelper.helper.SetDataBasePath(this.mDatabaseName);
            }
            PathHelper.helper.CheckDataPathExist();
        }
        
        /// <summary>
        /// 
        /// </summary>
        private void LoadRealDatabase()
        {
            DatabaseManager.Manager.LoadByName(mDatabaseName);
            this.mDatabase = DatabaseManager.Manager.Database;
        }

        /// <summary>
        /// 
        /// </summary>
        private void LoadHisDatabase()
        {
            HisDatabaseManager.Manager.LoadByName(mDatabaseName);
            mHisDatabase = HisDatabaseManager.Manager.Database;
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

            mHisFileManager = new DataFileManager(mDatabaseName);

            var task = mHisFileManager.Int();

            LoadRealDatabase();
            LoadHisDatabase();

            realEnginer = new RealEnginer(mDatabase);
            realEnginer.Init();

            hisEnginer = new HisEnginer(mHisDatabase, realEnginer);
            hisEnginer.MemoryCachTime = mHisDatabase.Setting.DataBlockDuration * 60;
            hisEnginer.Init();

            compressEnginer = new CompressEnginer(hisEnginer.CurrentMemory.Length);

            var sf = DataFileSeriserManager.manager.GetSeriser( mHisDatabase.Setting.DataSeriser);
            seriseEnginer = new SeriseEnginer(sf) { DatabaseName = database };
            seriseEnginer.FileDuration = mHisDatabase.Setting.FileDataDuration;
            seriseEnginer.BlockDuration = mHisDatabase.Setting.DataBlockDuration;

            querySerivce = new QuerySerivce(this.mDatabaseName);

            RegistorInterface();

            await task;

        }

        /// <summary>
        /// 
        /// </summary>
        private void RegistorInterface()
        {
            //注册日志
            ServiceLocator.Locator.Registor<ILog>(new ConsoleLogger());

            ServiceLocator.Locator.Registor<IRealData>(realEnginer);
            ServiceLocator.Locator.Registor<IRealDataNotify>(realEnginer);

            ServiceLocator.Locator.Registor<IDataCompress>(compressEnginer);
            ServiceLocator.Locator.Registor<IDataSerialize>(seriseEnginer);

            ServiceLocator.Locator.Registor<IHisQuery>(querySerivce);

            ServiceLocator.Locator.Registor<ITagQuery>(mDatabase);
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
            await InitAsync(database);
            seriseEnginer.Start();
            compressEnginer.Start();
            hisEnginer.Start();
            mIsStarted = true;
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
