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
    public class PathHelper
    {

        #region ... Variables  ...

        private string mAppPath;

        private string mDataPath;

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
            mDataPath = System.IO.Path.Combine(mAppPath,"Data");
        }

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        public void SetDataBasePath(string path)
        {
            if (System.IO.Path.IsPathRooted(path))
            {
                this.mDataPath = path;
            }
            else
            {
                if (mDatabaseName != path)
                {
                    mDatabaseName = path;
                    this.mDataPath = System.IO.Path.Combine(mDataPath, path);
                }
            }
        }

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
        public string GetDataPath(string path)
        {
            return System.IO.Path.Combine(mDataPath, path);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
