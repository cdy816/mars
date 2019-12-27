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
        public void Registor(string name)
        {
            if (!mManagers.ContainsKey(name))
            {
                DataFileManager dataFile = new DataFileManager(name);
                dataFile.Int();
                mManagers.Add(name, dataFile);
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
