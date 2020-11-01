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
using System.Threading;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class ComsumerValueChangedNotifyManager : IDisposable
    {

        #region ... Variables  ...

        private Dictionary<string, ValueChangedNotifyProcesser> mProcesser = new Dictionary<string, ValueChangedNotifyProcesser>();

        /// <summary>
        /// 
        /// </summary>
        public static ComsumerValueChangedNotifyManager Manager = new ComsumerValueChangedNotifyManager();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public ComsumerValueChangedNotifyManager()
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
        public void UpdateValue(IEnumerable<int> ids)
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
