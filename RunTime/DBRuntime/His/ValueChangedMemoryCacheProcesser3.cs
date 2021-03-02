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
using System.Collections.Concurrent;

namespace Cdy.Tag
{
    /// <summary>
    /// 值改变记录，记录周期同定时记录的周期的一样1s
    /// 每隔1s毫秒检查一次变量是否改变，如果改变则记录。
    /// 变化周期超过1s的情况，则会被忽略
    /// </summary>
    public class ValueChangedMemoryCacheProcesser3:IDisposable
    {

        #region ... Variables  ...


        /// <summary>
        /// 定时记录对象集合
        /// </summary>
        private Dictionary<int, HisRunTag> mTags = new Dictionary<int, HisRunTag>();

        /// <summary>
        /// 
        /// </summary>
        private ConcurrentDictionary<int, bool> mChangedTags = new ConcurrentDictionary<int, bool>();

        public static  int MaxTagCount = 100000;

        private int mCurrentCount = 0;

        private bool mIsClosed = false;

        private ManualResetEvent resetEvent;

        private ManualResetEvent closedEvent;

        private Thread mRecordThread;

        private bool mIsStarted = false;

        private object mLockObj = new object();

        private ValueChangedNotifyProcesser mValueChangedNotifier;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public ValueChangedMemoryCacheProcesser3()
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
        public int Count { get { return mCurrentCount; } }

        /// <summary>
        /// 
        /// </summary>
        private DateTime mLastUpdateTime;

        private int mLastUpdateSecond = -1;

        /// <summary>
        /// 
        /// </summary>
        public bool IsStarted { get { return mIsStarted; } }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        public void Notify(DateTime time)
        {
            mLastUpdateTime = time;
            //if(mLastUpdateTime.Second!=mLastUpdateSecond)
            //{
                mLastUpdateSecond = mLastUpdateTime.Second;
                resetEvent.Set();
            //}
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            //注册值改变处理
            mValueChangedNotifier = ServiceLocator.Locator.Resolve<IRealDataNotify>().SubscribeValueChangedForConsumer(this.Name, new ValueChangedNotifyProcesser.ValueChangedDelegate((ids,len) => {
                for(int i=0;i<len;i++)
                {
                    lock(mLockObj)
                    mChangedTags[ids[i]] = true;
                }
                //LoggerService.Service.Info("TagChanged", "变化变量数:"+ids.Length);
            }),null,new Func<IEnumerable<int>>(() => { return  mTags.Keys; }),RealDataNotifyType.Tag);

            foreach(var vv in mTags.Keys)
            {
                mChangedTags.TryAdd(vv, false);
            }

            mRecordThread = new Thread(ThreadProcess);
            mRecordThread.IsBackground=true;
            mRecordThread.Priority = ThreadPriority.Highest;
            mRecordThread.Start();

            mIsStarted = true;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            mIsClosed = true;
            try{
                resetEvent.Set();
                closedEvent.WaitOne();
                Clear();
                mIsStarted = false;
            }
            catch
            {

            }
            ServiceLocator.Locator.Resolve<IRealDataNotify>().UnSubscribeValueChangedForConsumer(this.Name);
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

                mChangedTags.TryAdd(tag.Id, false);
                mValueChangedNotifier?.Registor(tag.Id);

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
        /// <param name="tag"></param>
        public void Remove(HisRunTag tag)
        {
            if(mTags.ContainsKey(tag.Id))
            {
                mTags.Remove(tag.Id);
                mCurrentCount--;
            }
            bool btmp = false;
            if(mChangedTags.ContainsKey(tag.Id))
            {
                mChangedTags.Remove(tag.Id, out btmp);
            }
            mValueChangedNotifier?.UnRegistor(tag.Id);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Clear()
        {
            mTags.Clear();
            mChangedTags.Clear();
            mCurrentCount = 0;
        }

        /// <summary>
        /// 
        /// </summary>
        private void ThreadProcess()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray1);

            closedEvent.Reset();
            while (!mIsClosed)
            {
                resetEvent.WaitOne();
                resetEvent.Reset();
                if (mIsClosed) break;
                
                try
                {
                    int tim = (int)((mLastUpdateTime - HisRunTag.StartTime).TotalMilliseconds / HisEnginer3.MemoryTimeTick);
                    if (mChangedTags.Count > 0)
                    {
                        foreach (var vv in mChangedTags)
                        {
                            if (vv.Value)
                            {
                                lock (mLockObj)
                                {
                                    mTags[vv.Key].UpdateChangedValue3(tim);
                                    mChangedTags[vv.Key] = false;
                                }
                            }
                        }
                    }
                }
                catch(Exception ex)
                {
                    LoggerService.Service.Erro("ValueChangedMemoryCacheProcesser", ex.Message);
                }
                resetEvent.Reset();
            }
            closedEvent.Set();
            LoggerService.Service.Info("ValueChangedMemoryCacheProcesser", Name + " 退出");
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            resetEvent.Close();
            closedEvent.Close();
            mChangedTags.Clear();
            mTags.Clear();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
