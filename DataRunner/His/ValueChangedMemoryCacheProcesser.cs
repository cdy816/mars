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
    /// 
    /// </summary>
    public class ValueChangedMemoryCacheProcesser:IDisposable
    {

        #region ... Variables  ...


        /// <summary>
        /// 定时记录对象集合
        /// </summary>
        private Dictionary<int, HisRunTag> mTags = new Dictionary<int, HisRunTag>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, bool> mChangedTags = new Dictionary<int, bool>();

        public const int MaxTagCount = 100000;

        private int mCurrentCount = 0;

        private bool mIsClosed = false;

        private ManualResetEvent resetEvent;

        private ManualResetEvent closedEvent;

        private Thread mRecordThread;

        

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public ValueChangedMemoryCacheProcesser()
        {
            resetEvent = new ManualResetEvent(false);
            closedEvent = new ManualResetEvent(false);
        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public Action<HisRunTag>    PreProcess { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public Action AfterProcess { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        private DateTime mLastUpdateTime;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        public void Notify(DateTime time)
        {
            mLastUpdateTime = time;
            resetEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            //注册值改变处理
            ServiceLocator.Locator.Resolve<IRealDataNotify>().Subscribe(this.Name, new ValueChangedNotifyProcesser.ValueChangedDelagete((ids) => {
                foreach(var vv in ids)
                {
                    if(mChangedTags.ContainsKey(vv))
                    {
                        mChangedTags[vv] = true;
                    }
                }
            }), new Func<List<int>>(() => { return mTags.Keys.ToList(); }));

            mRecordThread = new Thread(ThreadProcess);
            mRecordThread.IsBackground=true;
            mRecordThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            mIsClosed = true;
            closedEvent.WaitOne();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        public bool AddTag(HisRunTag tag)
        {
            if (mCurrentCount < MaxTagCount)
            {
                mTags.Add(tag.Id,tag);
                mCurrentCount++;
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Clear()
        {
            mTags.Clear();
            mCurrentCount = 0;
        }

        /// <summary>
        /// 
        /// </summary>
        public void WriteHeader()
        {
            foreach (var vv in mTags.Values)
            {
                vv.UpdateHeader();
            }
        }

        /// <summary>
        /// 记录所有值
        /// </summary>
        public void RecordAllValue(DateTime time)
        {
            try
            {
                mLastUpdateTime = time;
                foreach (var vv in mTags)
                {
                    if (mChangedTags.ContainsKey(vv.Key) && mChangedTags[vv.Key])
                    {
                        mTags[vv.Key].UpdateValue(mLastUpdateTime);
                    }
                    else
                    {
                        vv.Value.AppendValue(mLastUpdateTime);
                    }
                }
            }
            catch
            {

            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void ThreadProcess()
        {
            closedEvent.Reset();
            while (!mIsClosed)
            {
                resetEvent.WaitOne();
                resetEvent.Reset();
                if (mIsClosed) break;
                try
                {
                    foreach (var vv in mChangedTags)
                    {
                        if (vv.Value)
                        {
                            mTags[vv.Key].UpdateValue(mLastUpdateTime);
                        }
                        else
                        {
                            mTags[vv.Key].UpdateNone();
                        }
                        mChangedTags[vv.Key] = false;
                    }
                }
                catch
                {

                }
                resetEvent.Reset();
            }
            closedEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            resetEvent.Close();
            closedEvent.Close();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
