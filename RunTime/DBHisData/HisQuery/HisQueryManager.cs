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

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class HisQueryManager
    {

        #region ... Variables  ...
        
        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, DataFileManager> mManagers = new Dictionary<string, DataFileManager>();

        /// <summary>
        /// 
        /// </summary>
        public static HisQueryManager Instance = new HisQueryManager();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public DataFileManager GetFileManager(string name)
        {
            if (mManagers.ContainsKey(name))
                return mManagers[name];
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public async void Registor(string name,string primaryDataPath="",string backupDataPath="")
        {
            if (!mManagers.ContainsKey(name))
            {
                DataFileManager dataFile = new DataFileManager(name) { PrimaryHisDataPath = primaryDataPath, BackHisDataPath = backupDataPath };
                await dataFile.Int();
                mManagers.Add(name, dataFile);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool CheckDatabaseIsRegistor(string name)
        {
            return mManagers.ContainsKey(name);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="manager"></param>
        public void Registor(string name,DataFileManager manager)
        {
            if (!mManagers.ContainsKey(name))
            {
                mManagers.Add(name, manager);
            }
            else
            {
                mManagers[name] = manager;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void StartMonitor()
        {
            foreach(var vv in mManagers)
            {
                vv.Value.Start();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void StopMonitor()
        {
            foreach(var vv in mManagers)
            {
                vv.Value.Stop();
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
