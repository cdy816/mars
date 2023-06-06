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
using System.Collections.Specialized;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class PathHelper
    {

        #region ... Variables  ...

        private string mAppPath;

        private string mDataPath;

        private string mBasePath = "";

        public static PathHelper helper = new PathHelper();

        private string mDatabaseName;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        
        /// <summary>
        /// 
        /// </summary>
        public PathHelper()
        {
            mAppPath = System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location);
            mBasePath = mAppPath;
            LoadBasePathConfig();
            mDataPath = System.IO.Path.Combine(mBasePath, "Data");
        }

        #endregion ...Constructor...

        #region ... Properties ...
        
        /// <summary>
        /// 
        /// </summary>
        public string AppPath
        {
            get
            {
                return mAppPath;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string DataPath
        {
            get
            {
                return mDataPath;
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void LoadBasePathConfig()
        {
            string sfile = System.IO.Path.Combine(mAppPath, "BasePath.cfg");
            {
               if(System.IO.File.Exists(sfile))
                {
                    string txt= System.IO.File.ReadAllText(sfile,Encoding.UTF8);
                    if(!string.IsNullOrWhiteSpace(txt) && System.IO.Directory.Exists(txt))
                    {
                        mBasePath = txt;
                    }
                }
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="path"></param>
        //public void SetDataBasePath(string path)
        //{
        //    if (System.IO.Path.IsPathRooted(path))
        //    {
        //        this.mDataPath = path;
        //    }
        //    else
        //    {
        //        if (mDatabaseName != path)
        //        {
        //            mDatabaseName = path;
        //            this.mDataPath = System.IO.Path.Combine(mDataPath, path);
        //        }
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        public void CheckDataPathExist()
        {
            if(!System.IO.Directory.Exists(mDataPath))
            {
                System.IO.Directory.CreateDirectory(mDataPath);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        public string GetDataPath(string databaseName,string path)
        {
            return System.IO.Path.Combine(mDataPath,databaseName, path);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public string GetDatabasePath(string database)
        {
            return System.IO.Path.Combine(mDataPath, database);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public string GetApplicationFilePath(params string[] file)
        {
            string spath = mAppPath;
            foreach(var vv in file)
            {
                spath = System.IO.Path.Combine(spath, vv);
            }
            return spath;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
