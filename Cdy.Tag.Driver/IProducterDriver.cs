using System;

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
        bool Start(IRealTagProduct tagQuery);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        bool Stop();

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
