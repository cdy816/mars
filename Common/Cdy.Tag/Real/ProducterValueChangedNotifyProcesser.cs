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
using System.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 变量值改变通知处理
    /// </summary>
    public class ProducterValueChangedNotifyProcesser : IDisposable
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, int> mRegistorTagIds = new Dictionary<int, int>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, object> mChangedIds = null;

        /// <summary>
        /// 
        /// </summary>
        private ManualResetEvent resetEvent;

        /// <summary>
        /// 
        /// </summary>
        private Thread mProcessThread;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagIds"></param>
        public delegate void ValueChangedDelagete(Dictionary<int,object> values);

        private object mLockObject = new object();

        private bool mIsAll = false;

        private bool mIsClosed = false;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public ProducterValueChangedNotifyProcesser()
        {
            resetEvent = new ManualResetEvent(false);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 名称
        /// </summary>
        public string Name { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public ValueChangedDelagete ValueChanged { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            mChangedIds = new Dictionary<int,object>((int)(mRegistorTagIds.Count * 1.2));
            mProcessThread = new Thread(ThreadProcess);
            mProcessThread.IsBackground = true;
            mProcessThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Close()
        {
            mIsClosed = true;
            resetEvent.Set();
            //if (mProcessThread != null)
            //{
            //    mProcessThread.Abort();
            //    mProcessThread = null;
            //}
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void UpdateValue(int id,object value)
        {
            if (mIsAll || mRegistorTagIds.ContainsKey(id))
            {
                lock (mLockObject)
                {
                    if (mChangedIds.ContainsKey(id))
                    {
                        mChangedIds[id] = value;
                    }
                    else
                    {
                        mChangedIds.Add(id, value);
                    }
                }
            }
        }


        /// <summary>
        /// 注册需要监视的变量集合
        /// </summary>
        /// <param name="id"></param>
        public void Registor(int id)
        {
            if (id == -1)
            {
                mIsAll = true;
                return;
            }

            if (!mRegistorTagIds.ContainsKey(id))
                mRegistorTagIds.Add(id,0);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        public void Registor(List<int> ids)
        {
            foreach(var id in ids)
            {
                if (!mRegistorTagIds.ContainsKey(id))
                    mRegistorTagIds.Add(id, 0);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void NotifyChanged()
        {
            resetEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>
        private void ThreadProcess()
        {
            while(true)
            {
                resetEvent.WaitOne();
                if (mIsClosed) break;
                resetEvent.Reset();
                if (mChangedIds.Count > 0)
                {
                    lock (mLockObject)
                    {
                        ValueChanged?.Invoke(mChangedIds);
                        mChangedIds.Clear();
                    }
                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...


        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            ValueChanged = null;
            mIsClosed = true;
            resetEvent.Set();
            resetEvent.Close();
        }
        #endregion ...Interfaces...
    }
}
