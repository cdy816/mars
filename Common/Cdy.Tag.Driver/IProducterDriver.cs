using System;
using System.Collections.Generic;

namespace Cdy.Tag.Driver
{
    /// <summary>
    /// 
    /// </summary>
    public interface IProducterDriver
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        string Name { get; }

        /// <summary>
        /// 
        /// </summary>
        string[] Registors { get; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        bool Init();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        bool Start(IRealTagProduct tagQuery);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        bool Stop();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        Dictionary<string,string> GetConfig(string database);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="config"></param>
        void UpdateConfig(string database, Dictionary<string, string> config);

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
