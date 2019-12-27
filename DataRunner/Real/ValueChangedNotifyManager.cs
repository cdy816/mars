using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class ValueChangedNotifyManager:IDisposable
    {

        #region ... Variables  ...

        private Dictionary<string, ValueChangedNotifyProcesser> mProcesser = new Dictionary<string, ValueChangedNotifyProcesser>();

        /// <summary>
        /// 
        /// </summary>
        public static ValueChangedNotifyManager Manager = new ValueChangedNotifyManager();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public ValueChangedNotifyManager()
        {
           
        }


        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public ValueChangedNotifyProcesser GetNotifier(string name)
        {
            if (mProcesser.ContainsKey(name))
            {
                return mProcesser[name];
            }
            else
            {
                mProcesser.Add(name, new ValueChangedNotifyProcesser());
                return mProcesser[name];
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void DisposeNotifier(string name)
        {
            if (mProcesser.ContainsKey(name))
            {
                var pp = mProcesser[name];
                pp.Close();
                pp.Dispose();
                mProcesser.Remove(name);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void UpdateValue(int id)
        {
            foreach (var vv in mProcesser)
            {
                vv.Value.UpdateValue(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        public void UpdateValue(List<int> ids)
        {
            foreach (var vv in mProcesser)
            {
                vv.Value.UpdateValue(ids);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void NotifyChanged()
        {
            foreach(var vv in mProcesser)
            {
                vv.Value.NotifyChanged();
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...
        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            foreach(var vv in mProcesser)
            {
                vv.Value.Dispose();
            }
            mProcesser.Clear();
        }

        #endregion ...Interfaces...
    }
}
