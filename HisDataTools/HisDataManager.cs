//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/12 15:35:17.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Text;

namespace HisDataTools
{
    /// <summary>
    /// 
    /// </summary>
    public class HisDataManager
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static HisDataManager Manager = new HisDataManager();
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public string SelectDatabase { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<string> Databases { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            DataFileSeriserManager.manager.Init();
            CompressUnitManager.Manager.Init();
            ScanDatabase();
        }

        /// <summary>
        /// 
        /// </summary>
        public void ScanDatabase()
        {
            List<string> bds = new List<string>();
            string dbpath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "Data");
            if (System.IO.Directory.Exists(dbpath))
            {
                foreach (var vv in new System.IO.DirectoryInfo(dbpath).EnumerateDirectories())
                {
                    bds.Add(vv.Name);
                }
            }
            Databases = bds;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
