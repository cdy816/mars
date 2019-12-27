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
    public class TimerMemoryCacheProcesser:IDisposable
    {

        #region ... Variables  ...


        /// <summary>
        /// 定时记录对象集合
        /// </summary>
        private Dictionary<long, List<HisRunTag>> mTimerTags = new Dictionary<long, List<HisRunTag>>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<long, long> mCount = new Dictionary<long, long>();

        public const int MaxTagCount = 100000;

        private int mCurrentCount = 0;

        private bool mIsClosed = false;

        private ManualResetEvent resetEvent;

        private ManualResetEvent closedEvent;

        private Thread mRecordThread;

        private DateTime mLastUpdateTime;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public TimerMemoryCacheProcesser()
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
                var cc = tag.Circle;
                if (mTimerTags.ContainsKey(cc))
                {
                    mTimerTags[cc].Add(tag);
                }
                else
                {
                    mTimerTags.Add(cc, new List<HisRunTag>() { tag });
                    mCount.Add(cc, 0);
                }
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
            mTimerTags.Clear();
            mCount.Clear();
            mCurrentCount = 0;
        }
        
        /// <summary>
        /// 
        /// </summary>
        public void WriteHeader()
        {
            foreach(var vv in mTimerTags.Values)
            {
                foreach(var vvv in vv)
                {
                    vvv.UpdateHeader();
                }
            }
        }

        /// <summary>
        /// 记录所有值
        /// </summary>
        public void RecordAllValue()
        {
            try
            {
                foreach (var vv in mCount.Keys)
                {
                    mCount[vv] += HisEnginer.MemoryTimeTick;
                    if (mCount[vv] >= vv)
                    {
                        mCount[vv] = 0;
                        ProcessTags(mTimerTags[vv]);
                    }
                    else
                    {
                        ProcessAppendValue(mTimerTags[vv]);
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
                    foreach (var vv in mCount.Keys)
                    {
                        mCount[vv] += HisEnginer.MemoryTimeTick;
                        if (mCount[vv] >= vv)
                        {
                            mCount[vv] = 0;
                            ProcessTags(mTimerTags[vv]);
                        }
                        else
                        {
                            ProcessTagsNone(mTimerTags[vv]);
                        }
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
        /// <param name="tags"></param>
        private void ProcessAppendValue(List<HisRunTag> tags)
        {
            foreach (var vv in tags)
            {
                vv.AppendValue();
            }
        }

        /// <summary>
        /// 记录一组
        /// </summary>
        /// <param name="tags"></param>
        private void ProcessTags(List<HisRunTag> tags)
        {
            foreach(var vv in tags)
            {
                vv.UpdateValue(mLastUpdateTime);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tags"></param>
        private void ProcessTagsNone(List<HisRunTag> tags)
        {
            foreach (var vv in tags)
            {
                vv.UpdateNone();
            }
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
