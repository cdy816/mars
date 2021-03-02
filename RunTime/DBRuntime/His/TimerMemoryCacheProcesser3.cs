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
using System.Diagnostics;

namespace Cdy.Tag
{
    /// <summary>
    /// 定时记录处理
    /// </summary>
    public class TimerMemoryCacheProcesser3:IDisposable
    {

        #region ... Variables  ...


        /// <summary>
        /// 定时记录对象集合
        /// </summary>
        private Dictionary<long, List<HisRunTag>> mTimerTags = new Dictionary<long, List<HisRunTag>>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<long, DateTime> mCount = new Dictionary<long, DateTime>();

        public static int MaxTagCount = 100000;

        private int mCurrentCount = 0;

        private bool mIsClosed = false;

        private ManualResetEvent resetEvent;

        private ManualResetEvent closedEvent;

        private Thread mRecordThread;

        private DateTime mLastUpdateTime;

        private bool mIsBusy = false;

        private int mBusyCount = 0;

        private bool mIsStarted = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public TimerMemoryCacheProcesser3()
        {
            resetEvent = new ManualResetEvent(false);
            closedEvent = new ManualResetEvent(false);
        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public Action<HisRunTag>  PreProcess { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public Action AfterProcess { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Id { get; set; }

        public bool IsStarted { get { return mIsStarted; } }

        /// <summary>
        /// 
        /// </summary>
        public int Count { get { return mCurrentCount; } }

        ///// <summary>
        ///// 
        ///// </summary>
        //public bool  NeedReInit { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        public void UpdateLastUpdateDatetime(DateTime time)
        {
            mLastUpdateTime = time;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        public void Notify(DateTime time)
        {
            if (mIsBusy)
            {
                mBusyCount++;
                if (mBusyCount > 4)
                    LoggerService.Service.Warn("Record", "TimerMemoryCacheProcesser" + Id + " 出现阻塞:" + mBusyCount);
            }
            else
            {
                mBusyCount = 0;
            }
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
            mRecordThread.Priority = ThreadPriority.Highest;
            mRecordThread.Start();
            mLastUpdateTime = DateTime.UtcNow;
            mIsStarted = true;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            mIsClosed = true;
            resetEvent.Set();
            closedEvent.WaitOne();
            mIsStarted = false;
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
                    mCount.Add(cc, DateTime.UtcNow);
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
        /// <param name="tag"></param>
        public void Remove(HisRunTag tag)
        {
            if(mTimerTags.ContainsKey(tag.Circle))
            {
                mTimerTags[tag.Circle].Remove(tag);
                mCurrentCount--;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="oldcircle"></param>
        public void UpdateCircle(HisRunTag tag,long oldcircle)
        {
            if (mTimerTags.ContainsKey(oldcircle) && mTimerTags[oldcircle].Contains(tag))
            {
                if (mTimerTags.ContainsKey(oldcircle))
                {
                    mTimerTags[oldcircle].Remove(tag);
                }

                var cc = tag.Circle;
                if (mTimerTags.ContainsKey(cc))
                {
                    mTimerTags[cc].Add(tag);
                }
                else
                {
                    mTimerTags.Add(cc, new List<HisRunTag>() { tag });
                    mCount.Add(cc, DateTime.UtcNow);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool CanAddTag()
        {
            return mCurrentCount < MaxTagCount;
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

        private DateTime mLastProcessTime;

        /// <summary>
        /// 
        /// </summary>
        private void ThreadProcess()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray1);
            closedEvent.Reset();
            var vkeys = mCount.Keys.ToArray();
            var vdd = DateTime.UtcNow;
            foreach(var vv in vkeys)
            {
                mCount[vv] = vdd.AddMilliseconds(vv);
            }

            mLastProcessTime = DateTime.Now;

            while (!mIsClosed)
            {
                resetEvent.WaitOne();
                resetEvent.Reset();
                if (mIsClosed) 
                    break;

                //if(NeedReInit)
                //{
                //    NeedReInit = false;
                //    vkeys = mCount.Keys.ToArray();
                //    vdd = DateTime.UtcNow;
                //    foreach (var vv in vkeys)
                //    {
                //        mCount[vv] = vdd.AddMilliseconds(vv);
                //    }
                //}

                var dnow = DateTime.Now;

                if ((mLastProcessTime - dnow).TotalMilliseconds > 900)
                {
                    LoggerService.Service.Warn("TimerMemoryCacheProcesser", "定时记录超时 "+ (mLastProcessTime - dnow).TotalMilliseconds);
                }

                mLastProcessTime = dnow;

                try
                {
                    mIsBusy = true;
                    var vdata = mLastUpdateTime;
                    foreach (var vv in vkeys)
                    {
                        if (vdata >= mCount[vv])
                        {
                            do
                            {
                                mCount[vv] = mCount[vv].AddMilliseconds(vv);
                            }
                            while (mCount[vv] <= vdata);
                            ProcessTags(mTimerTags[vv]);
                        }
                    }
                    
                    mIsBusy = false;
                }
                catch
                {

                }
                resetEvent.Reset();
            }
            closedEvent.Set();
            LoggerService.Service.Info("TimerMemoryCacheProcesser",  Id + " 退出");

        }


        /// <summary>
        /// 记录一组变量
        /// </summary>
        /// <param name="tags"></param>
        private void ProcessTags(List<HisRunTag> tags)
        {            
            int tim = (int)((mLastUpdateTime - HisRunTag.StartTime).TotalMilliseconds / HisEnginer3.MemoryTimeTick);
            foreach (var vv in tags)
            {
                vv.UpdateValue3(tim);
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
