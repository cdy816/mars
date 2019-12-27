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
            mDataPath = mAppPath;
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
            this.mDataPath = path;
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
