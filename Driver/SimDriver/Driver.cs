using Cdy.Tag;
using Cdy.Tag.Driver;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SimDriver
{
    public class Driver : IProducterDriver
    {

        #region ... Variables  ...

        Dictionary<string, List<Tagbase>> mTagIdCach = new Dictionary<string, List<Tagbase>>();

        Dictionary<string, List<int>> mManualRecordTagCach = new Dictionary<string, List<int>>();

        //private System.Timers.Timer mScanTimer;

        private short mNumber = 0;

        private bool mBoolNumber = false;

        private IRealTagProduct mTagService;

        private ITagHisValueProduct mTagHisValueService;

        //private bool mIsBusy = false;

        //private StreamWriter mWriter;

        private DateTime mLastProcessTime = DateTime.Now;

        //private int mBusyCount = 0;

        private bool mIsSecond = false;

        //private int mTickCount = 0;

        private Thread mScanThread;

        private bool mIsClosed = false;

        private ManualResetEvent mCosEvent;
        private ManualResetEvent mSinEvent;
        private ManualResetEvent mStepEvent;
        private ManualResetEvent mSteppointEvent;
        private ManualResetEvent mSquareEvent;
        private ManualResetEvent mDatetimeEvent;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public Driver()
        {
            //var vfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), DateTime.Now.ToString("yyyyMMddHHmmss") + ".log");
            //mWriter = new StreamWriter(  System.IO.File.Open(vfile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));

            mCosEvent = new ManualResetEvent(false);
            mSinEvent = new ManualResetEvent(false);
            mStepEvent = new ManualResetEvent(false);
            mSquareEvent = new ManualResetEvent(false);
            mDatetimeEvent = new ManualResetEvent(false);
            mSteppointEvent = new ManualResetEvent(false);

            CPUAssignHelper.Helper.Init();

        }



        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public string Name => "Sim";

        /// <summary>
        /// 
        /// </summary>
        public string[] Registors
        {
            get
            {
                return new string[] { "cos", "sin", "step","steppoint", "square","datetime" };
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="sval"></param>
        //private void Log(string sval)
        //{
        //    mWriter.WriteLine(sval);
        //    mWriter.Flush();
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagQuery"></param>
        private void InitTagCach(IRealTagProduct tagQuery)
        {
            mTagIdCach = tagQuery.GetTagsByLinkAddress(new List<string>() { "Sim:cos", "Sim:sin", "Sim:step", "Sim:steppoint", "Sim:square","Sim:datetime" });

            mTagHisValueService = ServiceLocator.Locator.Resolve<ITagHisValueProduct>();

            foreach (var vv in mTagIdCach)
            {
                mManualRecordTagCach.Add(vv.Key, mTagHisValueService.GetTagRecordType(vv.Value.Select(e => e.Id).ToList()).Where(e=>e.Value == RecordType.Driver).Select(e=>e.Key).ToList());
            }

        }

        private Thread mCosThread;
        private Thread mSinThread;
        private Thread mStepThread;
        private Thread mStepPointThread;
        private Thread mSquareThread;
        private Thread mDatetimeThread;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagQuery"></param>
        /// <returns></returns>
        public bool Start(IRealTagProduct tagQuery, ITagHisValueProduct tagHisValueService)
        {
            mIsClosed = false;
            mTagService = tagQuery;
            mTagHisValueService = tagHisValueService;
            InitTagCach(tagQuery);
            mScanThread = new Thread(ScanThreadPro);
            mScanThread.IsBackground = true;
            mScanThread.Start();
            //mScanTimer = new System.Timers.Timer(100);
            //mScanTimer.Elapsed += MScanTimer_Elapsed;
            //mScanTimer.Start();
            mCosThread = new Thread(CosThreadPro);
            mCosThread.IsBackground = true;
            mCosThread.Start();

            mSinThread = new Thread(SinThreadPro);
            mSinThread.IsBackground = true;
            mSinThread.Start();


            mStepThread = new Thread(StepThreadPro);
            mStepThread.IsBackground = true;
            mStepThread.Start();

            mStepPointThread = new Thread(StepPointThreadPro);
            mStepPointThread.IsBackground = true;
            mStepPointThread.Start();

            mSquareThread = new Thread(SquareThreadPro);
            mSquareThread.IsBackground = true;
            mSquareThread.Start();


            mDatetimeThread = new Thread(DateTimeThreadPro);
            mDatetimeThread.IsBackground = true;
            mDatetimeThread.Start();

            return true;
        }

        private double mMaxProcessTimeSpan = 0;
        //private double mSelfProcessTimeSpan = 0;
        private int mFinishCount = 6;
        private object mLockObj = new object();

        //private Stopwatch mCosStopwatch;
        //private Stopwatch mSinStopwatch;
        //private Stopwatch mStepStopwatch;
        /// <summary>
        /// 
        /// </summary>
        private void CosThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            List<Tagbase> vv = mTagIdCach.ContainsKey("Sim:cos") ? mTagIdCach["Sim:cos"] : null;
            List<int> vvr = mManualRecordTagCach.ContainsKey("Sim:cos") ? mManualRecordTagCach["Sim:cos"] : null;
            //mCosStopwatch = new Stopwatch();
            while (!mIsClosed)
            {
                mCosEvent.WaitOne();
                mCosEvent.Reset();
                //mCosStopwatch.Restart();
                //long ll = 0;
                double fval = Math.Cos(mNumber / 180.0 * Math.PI);
                if (vv!=null)
                {
                    mTagService.SetTagValue(vv,ref fval, 0);
                    //ll = mCosStopwatch.ElapsedMilliseconds;
                    mTagService.SubmiteNotifyChanged();
                }

                if (vvr != null && vvr.Count>0)
                {
                    TagValue tv = new TagValue() { Quality = 0, Time = DateTime.UtcNow, Value = fval };
                    foreach (var vvv in vvr)
                    {
                        mTagHisValueService.SetTagHisValue(vvv, tv);
                    }
                }

                //mCosStopwatch.Stop();

                //LoggerService.Service.Info("SimDriver", "设置变量耗时:" + ll + " 其他耗时:" + (mCosStopwatch.ElapsedMilliseconds - ll) + " count:" + vv.Count);

                var ts = (DateTime.Now - mLastProcessTime).TotalMilliseconds;
                lock (mLockObj)
                {
                    if (ts > mMaxProcessTimeSpan)
                    {
                        mMaxProcessTimeSpan = ts;
                        //mSelfProcessTimeSpan = mCosStopwatch.ElapsedMilliseconds;
                    }
                }

                Interlocked.Increment(ref mFinishCount);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void SinThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            List<Tagbase> vv = mTagIdCach.ContainsKey("Sim:sin") ? mTagIdCach["Sim:sin"] : null;
            List<int> vvr = mManualRecordTagCach.ContainsKey("Sim:sin") ? mManualRecordTagCach["Sim:sin"] : null;
            //mSinStopwatch = new Stopwatch();
            while (!mIsClosed)
            {
                mSinEvent.WaitOne();
                mSinEvent.Reset();
                //mSinStopwatch.Restart();
                double fval = Math.Sin(mNumber / 180.0 * Math.PI);
                if (vv != null)
                {
                    mTagService.SetTagValue(vv,ref fval, 0);
                    mTagService.SubmiteNotifyChanged();
                }

                if (vvr != null && vvr.Count > 0)
                {
                    TagValue tv = new TagValue() { Quality = 0, Time = DateTime.UtcNow, Value = fval };
                    foreach (var vvv in vvr)
                    {
                        mTagHisValueService.SetTagHisValue(vvv, tv);
                    }
                }
                //mSinStopwatch.Stop();
                var ts = (DateTime.Now - mLastProcessTime).TotalMilliseconds;
                lock (mLockObj)
                {
                    if (ts > mMaxProcessTimeSpan)
                    {
                        mMaxProcessTimeSpan = ts;
                        //mSelfProcessTimeSpan = mSinStopwatch.ElapsedMilliseconds;
                    }
                }
               
                Interlocked.Increment(ref mFinishCount);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void StepThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            List<Tagbase> vv = mTagIdCach.ContainsKey("Sim:step") ? mTagIdCach["Sim:step"] : null;
            List<int> vvr = mManualRecordTagCach.ContainsKey("Sim:step") ? mManualRecordTagCach["Sim:step"] : null;
            //mStepStopwatch = new Stopwatch();
            while (!mIsClosed)
            {
                mStepEvent.WaitOne();
                mStepEvent.Reset();
                //mStepStopwatch.Restart();
                if (vv != null)
                {
                    mTagService.SetTagValue(vv,ref mNumber, 0);
                    mTagService.SubmiteNotifyChanged();
                }

                if (vvr != null && vvr.Count > 0)
                {
                    TagValue tv = new TagValue() { Quality = 0, Time = DateTime.UtcNow, Value = mNumber };
                    foreach (var vvv in vvr)
                    {
                        mTagHisValueService.SetTagHisValue(vvv, tv);
                    }
                }
                //mStepStopwatch.Stop();
                var ts = (DateTime.Now - mLastProcessTime).TotalMilliseconds;
                lock (mLockObj)
                {
                    if (ts > mMaxProcessTimeSpan)
                    {
                        mMaxProcessTimeSpan = ts;
                        //mSelfProcessTimeSpan = mStepStopwatch.ElapsedMilliseconds;
                    }
                }

                Interlocked.Increment(ref mFinishCount);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void StepPointThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            List<Tagbase> vv = mTagIdCach.ContainsKey("Sim:steppoint") ? mTagIdCach["Sim:steppoint"] : null;
            List<int> vvr = mManualRecordTagCach.ContainsKey("Sim:steppoint") ? mManualRecordTagCach["Sim:steppoint"] : null;
            while (!mIsClosed)
            {
                mSteppointEvent.WaitOne();
                mSteppointEvent.Reset();
                var vpp = new IntPoint3Data(mNumber, mNumber, mNumber);
                if (vv != null)
                {
                    mTagService.SetTagValue(vv ,ref vpp, 0);
                    mTagService.SubmiteNotifyChanged();
                }

                if (vvr != null && vvr.Count > 0)
                {
                    TagValue tv = new TagValue() { Quality = 0, Time = DateTime.UtcNow, Value = new IntPoint3Data(mNumber, mNumber, mNumber) };
                    foreach (var vvv in vvr)
                    {
                        mTagHisValueService.SetTagHisValue(vvv, tv);
                    }
                }

                var ts = (DateTime.Now - mLastProcessTime).TotalMilliseconds;
                lock (mLockObj)
                {
                    if (ts > mMaxProcessTimeSpan)
                    {
                        mMaxProcessTimeSpan = ts;
                    }
                }

                Interlocked.Increment(ref mFinishCount);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void SquareThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            List<Tagbase> vv = mTagIdCach.ContainsKey("Sim:square") ? mTagIdCach["Sim:square"] : null;
            List<int> vvr = mManualRecordTagCach.ContainsKey("Sim:square") ? mManualRecordTagCach["Sim:square"] : null;
            while (!mIsClosed)
            {
                mSquareEvent.WaitOne();
                mSquareEvent.Reset();
                if (vv != null)
                {
                    mTagService.SetTagValue(vv,ref mBoolNumber, 0);
                    mTagService.SubmiteNotifyChanged();
                }

                if (vvr != null && vvr.Count > 0)
                {
                    TagValue tv = new TagValue() { Quality = 0, Time = DateTime.UtcNow, Value = mBoolNumber };
                    foreach (var vvv in vvr)
                    {
                        mTagHisValueService.SetTagHisValue(vvv, tv);
                    }
                }

                var ts = (DateTime.Now - mLastProcessTime).TotalMilliseconds;
                lock (mLockObj)
                {
                    if (ts > mMaxProcessTimeSpan)
                    {
                        mMaxProcessTimeSpan = ts;
                    }
                }
                Interlocked.Increment(ref mFinishCount);
            }
        }


        private void DateTimeThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            List<Tagbase> vv = mTagIdCach.ContainsKey("Sim:datetime") ? mTagIdCach["Sim:datetime"] : null;
            List<int> vvr = mManualRecordTagCach.ContainsKey("Sim:datetime") ? mManualRecordTagCach["Sim:datetime"] : null;
            while (!mIsClosed)
            {
                mDatetimeEvent.WaitOne();
                mDatetimeEvent.Reset();
                if (vv != null)
                {
                    DateTime dnow = DateTime.Now;
                    mTagService.SetTagValue(vv,ref dnow, 0);
                    mTagService.SubmiteNotifyChanged();
                }

                if (vvr != null && vvr.Count > 0)
                {
                    TagValue tv = new TagValue() { Quality = 0, Time = DateTime.UtcNow, Value = DateTime.Now };
                    foreach (var vvv in vvr)
                    {
                        mTagHisValueService.SetTagHisValue(vvv, tv);
                    }
                }

                var ts = (DateTime.Now - mLastProcessTime).TotalMilliseconds;
                lock (mLockObj)
                {
                    if (ts > mMaxProcessTimeSpan)
                    {
                        mMaxProcessTimeSpan = ts;
                    }
                }

                Interlocked.Increment(ref mFinishCount);
            }
        }

     
        private void ScanThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            while (!mIsClosed)
            {
                DateTime time = DateTime.Now;

                if(mFinishCount>5 && mLastProcessTime.Second != time.Second)
                {
                    mFinishCount = 0;
                }
                else
                {
                    Thread.Sleep(100);
                    continue;
                }

                if (mMaxProcessTimeSpan > 1000)
                {
                    LoggerService.Service.Warn("Sim Driver", "出现阻塞 更新耗时:" + mMaxProcessTimeSpan + " ms");
                }
                else if ((mNumber % 10 == 0))
                {
                    LoggerService.Service.Debug("Sim Driver", "上次更新数据耗时: " + mMaxProcessTimeSpan + " ms");
                }

                mMaxProcessTimeSpan = 0;
                //mSelfProcessTimeSpan = 0;

                mLastProcessTime = time;

                mNumber++;
                mNumber = mNumber >= (short)360 ? (short)0 : mNumber;
                mIsSecond = true;
                if (mNumber % 60 == 0)
                {
                    mBoolNumber = !mBoolNumber;
                }

                mSinEvent.Set();
                mCosEvent.Set();
                mSquareEvent.Set();
                mDatetimeEvent.Set();
                mStepEvent.Set();
                mSteppointEvent.Set();

                Thread.Sleep(100);            
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Stop()
        {
            //mScanTimer.Stop();
            mIsClosed = true;
           // mScanThread.Abort();
            //mWriter.Close();
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public Dictionary<string, string> GetConfig(string database)
        {
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="config"></param>
        public void UpdateConfig(string database, Dictionary<string, string> config)
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Init()
        {
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="arg"></param>
        public void OnRealTagChanged(TagChangedArg arg)
        {
            
            var tags  = arg.AddedTags.Where(e => e.Value.StartsWith("Sim"));

            foreach (var vv in tags)
            {
                var vtag = mTagService.GetTagById(vv.Key);
                if (mTagIdCach.ContainsKey(vv.Value))
                {
                    mTagIdCach[vv.Value].Add(vtag);
                }
                else
                {
                    mTagIdCach.Add(vv.Value, new List<Tagbase>() { vtag });
                }
            }

            var cgtags = arg.ChangedTags;

            foreach (var vvt in mTagIdCach)
            {
                vvt.Value.RemoveAll((tag) => { return cgtags.ContainsKey(tag.Id); });
            }

            tags = arg.ChangedTags.Where(e => e.Value.StartsWith("Sim"));

            foreach (var vv in tags)
            {
                var vtag = mTagService.GetTagById(vv.Key);
                if (mTagIdCach.ContainsKey(vv.Value))
                {
                    mTagIdCach[vv.Value].Add(vtag);
                }
                else
                {
                    mTagIdCach.Add(vv.Value, new List<Tagbase>() { vtag });
                }
            }

            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="arg"></param>
        public void OnHisTagChanged(HisTagChangedArg arg)
        {
            if (arg.ChangedTags != null && arg.ChangedTags.Count()>0)
            {
                mManualRecordTagCach.Clear();
                mTagHisValueService = ServiceLocator.Locator.Resolve<ITagHisValueProduct>();

                foreach (var vv in mTagIdCach)
                {
                    mManualRecordTagCach.Add(vv.Key, mTagHisValueService.GetTagRecordType(vv.Value.Select(e => e.Id).ToList()).Where(e => e.Value == RecordType.Driver).Select(e => e.Key).ToList());
                }
            }
            else
            {
                var atags = arg.AddedTags.ToList();
                var typ = mTagHisValueService.GetTagRecordType(arg.AddedTags.ToList());
                for (int i = 0; i < atags.Count; i++)
                {
                    var tag = mTagService.GetTagById(atags[i]);
                    if (typ[i] == RecordType.Driver && tag.LinkAddress.StartsWith("Sim"))
                    {
                        if (mManualRecordTagCach.ContainsKey(tag.LinkAddress))
                        {
                            mManualRecordTagCach[tag.LinkAddress].Add(tag.Id);
                        }
                        else
                        {
                            mManualRecordTagCach.Add(tag.LinkAddress, new List<int>() { tag.Id });
                        }
                    }
                }
            }
        }



        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...



    }
}
