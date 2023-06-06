using DBRuntimeMonitor.ViewModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBRuntimeMonitor
{
    /// <summary>
    /// 
    /// </summary>
    public class AlarmNodeViewModel : HasChildrenTreeItemViewModel
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public AlarmNodeViewModel()
        {
            Name = Res.Get("Alarm");
        }
        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public override string FullName => string.Empty;

        /// <summary>
        /// 
        /// </summary>
        public Database Model { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsGrpc { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Port { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CanRemove()
        {
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CanCopy()
        {
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="oldName"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public override bool OnRename(string oldName, string newName)
        {
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mode"></param>
        /// <returns></returns>
        public override ViewModelBase GetModel(ViewModelBase mode)
        {
            if (mode is AlarmDetailViewModel)
            {
                (mode as AlarmDetailViewModel).Node = this;
                return mode;
            }
            else
            {
                return new AlarmDetailViewModel() { Node = this };
            }
        }
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
