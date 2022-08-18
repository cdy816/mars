using System;
using System.Collections.Generic;
using System.Text;

namespace DBRuntimeMonitor.ViewModel
{
    public interface IModeSwitch
    {

        #region ... Variables  ...

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
        void Active();

        /// <summary>
        /// 
        /// </summary>
        void DeActive();

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public interface RefreshCurrentUser
    {
        void RefreshCurrentUser();
    }

}
