using System;

namespace Cdy.Tag.Consume
{
    public interface IConsumeDriver
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


        #endregion ...Properties...

        #region ... Methods    ...


        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        bool Start();

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
