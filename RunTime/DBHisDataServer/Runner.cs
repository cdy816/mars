//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/17 17:53:01.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Text;

namespace DBHisDataServer
{
    /// <summary>
    /// 
    /// </summary>
    public class Runner
    {

        #region ... Variables  ...
        private Database mDatabase;
        private RealDatabase mRealDatabase;
        private string mDatabaseName = "local";
        private SecurityRunner mSecurityRunner;
        private QuerySerivce querySerivce;
        private string mPrimaryDataPath;
        private string mBackDataPath;

        public static Runner Instance = new Runner();

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

        private bool Init(string database)
        {

            ServiceLocator.Locator.Registor<ILog>(new ConsoleLogger());

            if (System.IO.Path.IsPathRooted(database))
            {
                this.mDatabaseName = System.IO.Path.GetFileNameWithoutExtension(database);
                this.DatabasePath = System.IO.Path.GetDirectoryName(database);
            }
            else
            {
                this.mDatabaseName = database;
            }
            PathHelper.helper.CheckDataPathExist();

            if (CheckDatabaseExist(mDatabaseName))
            {
                LoadDatabase();



                querySerivce = new QuerySerivce(this.mDatabaseName);

                mSecurityRunner = new SecurityRunner() { Document = mDatabase.Security };



                DataFileSeriserManager.manager.Init();
                CompressUnitManager2.Manager.Init();
                HisQueryManager.Instance.Registor(mDatabaseName, mPrimaryDataPath,mBackDataPath);
                HisQueryManager.Instance.StartMonitor();

                RegistorInterface();
                return true;
            }
            else
            {
                LoggerService.Service.Erro("Runner", "database " + database + " is not exist.");
                return false;
            }
        }

        private bool CheckDatabaseExist(string name)
        {
            return System.IO.File.Exists(PathHelper.helper.GetDataPath(name, name + ".db"));
        }

        /// <summary>
        /// 
        /// </summary>
        private void LoadDatabase()
        {
            this.mDatabase = new DatabaseSerise().Load(mDatabaseName);
            this.mRealDatabase = this.mDatabase.RealDatabase;
            mPrimaryDataPath = this.mDatabase.HisDatabase.Setting.HisDataPathPrimary;
            mBackDataPath = this.mDatabase.HisDatabase.Setting.HisDataPathBack;
        }

        private void RegistorInterface()
        {
            ServiceLocator.Locator.Registor<ITagManager>(mRealDatabase);
            ServiceLocator.Locator.Registor<IRuntimeSecurity>(mSecurityRunner);
            ServiceLocator.Locator.Registor<IHisQuery>(querySerivce);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start(string database)
        {
            var re =  Init(database);
            if (!re)
            {
                return;
            }
            DBRuntime.Api.DataService.Service.Start(14329);
            //mSecurityRunner.Start();
            mIsStarted = true;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            DBRuntime.Api.DataService.Service.Stop();
            mSecurityRunner.Stop();
            mIsStarted = false;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
