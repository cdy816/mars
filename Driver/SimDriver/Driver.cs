using Cdy.Tag;
using Cdy.Tag.Driver;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

namespace SimDriver
{
    public class Driver : Cdy.Tag.Driver.IProducterDriver
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

        private StreamWriter mWriter;

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
            var vfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), DateTime.Now.ToString("yyyyMMddHHmmss") + ".log");
            mWriter = new StreamWriter(  System.IO.File.Open(vfile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));

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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sval"></param>
        private void Log(string sval)
        {
            mWriter.WriteLine(sval);
            mWriter.Flush();
        }

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

        /// <summary>
        /// 
        /// </summary>
        private void CosThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            List<Tagbase> vv = mTagIdCach.ContainsKey("Sim:cos") ? mTagIdCach["Sim:cos"] : null;
            List<int> vvr = mManualRecordTagCach.ContainsKey("Sim:cos") ? mManualRecordTagCach["Sim:cos"] : null;
            while (!mIsClosed)
            {
                mCosEvent.WaitOne();
                mCosEvent.Reset();
                double fval = Math.Cos(mNumber / 180.0 * Math.PI);
                if (vv!=null)
                {
                    mTagService.SetTagValue<double>(vv,ref fval, 0);
                    mTagService.SubmiteNotifyChanged();
                }

                if (vvr != null && !mIsSecond)
                {
                    TagValue tv = new TagValue() { Quality = 0, Time = DateTime.UtcNow, Value = fval };
                    foreach (var vvv in vvr)
                    {
                        mTagHisValueService.SetTagHisValue(vvv, tv);
                    }
                    
                }

                mLastProcessTime = DateTime.Now;
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
            while (!mIsClosed)
            {
                mSinEvent.WaitOne();
                mSinEvent.Reset();
                double fval = Math.Sin(mNumber / 180.0 * Math.PI);
                if (vv != null)
                {
                    mTagService.SetTagValue<double>(vv,ref fval, 0);
                    mTagService.SubmiteNotifyChanged();
                }

                if (vvr != null && !mIsSecond)
                {
                    TagValue tv = new TagValue() { Quality = 0, Time = DateTime.UtcNow, Value = fval };
                    foreach (var vvv in vvr)
                    {
                        mTagHisValueService.SetTagHisValue(vvv, tv);
                    }
                   
                }

                mLastProcessTime = DateTime.Now;
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
            while (!mIsClosed)
            {
                mStepEvent.WaitOne();
                mStepEvent.Reset();
                if (vv != null)
                {
                    mTagService.SetTagValue(vv,ref mNumber, 0);
                    mTagService.SubmiteNotifyChanged();
                }

                if (vvr != null && !mIsSecond)
                {
                    TagValue tv = new TagValue() { Quality = 0, Time = DateTime.UtcNow, Value = mNumber };
                    foreach (var vvv in vvr)
                    {
                        mTagHisValueService.SetTagHisValue(vvv, tv);
                    }
                }

                mLastProcessTime = DateTime.Now;
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

                if (vvr != null && !mIsSecond)
                {
                    TagValue tv = new TagValue() { Quality = 0, Time = DateTime.UtcNow, Value = new IntPoint3Data(mNumber, mNumber, mNumber) };
                    foreach (var vvv in vvr)
                    {
                        mTagHisValueService.SetTagHisValue(vvv, tv);
                    }
                }

                mLastProcessTime = DateTime.Now;
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

                if (vvr != null && !mIsSecond)
                {
                    TagValue tv = new TagValue() { Quality = 0, Time = DateTime.UtcNow, Value = mBoolNumber };
                    foreach (var vvv in vvr)
                    {
                        mTagHisValueService.SetTagHisValue(vvv, tv);
                    }
                }

                mLastProcessTime = DateTime.Now;
            }
        }


        private void DateTimeThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            List<Tagbase> vv = mTagIdCach.ContainsKey("Sim:datetime") ? mTagIdCach["Sim:datetime"] : null;
            while (!mIsClosed)
            {
                mSquareEvent.WaitOne();
                mSquareEvent.Reset();
                if (vv != null)
                {
                    mTagService.SetTagValue(vv, DateTime.Now, 0);
                    mTagService.SubmiteNotifyChanged();
                }
                mLastProcessTime = DateTime.Now;
            }
        }

     
        private void ScanThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            while (!mIsClosed)
            {
                DateTime time = DateTime.Now;
                var dtime = (time - mLastProcessTime).TotalMilliseconds;
                if (dtime > 1000)
                {
                    LoggerService.Service.Warn("Sim Driver", "出现阻塞");
                }
                else if (mNumber % 10 == 0 || dtime > 1000)
                {
                    LoggerService.Service.Debug("Sim Driver", "距离上次更新时间差:"+ dtime);
                }

                mLastProcessTime = time;
                if (!mIsSecond)
                {
                    mNumber++;
                    mNumber = mNumber >= (short)360 ? (short)0 : mNumber;
                    mIsSecond = true;

                    if (mNumber % 60 == 0)
                    {
                        mBoolNumber = !mBoolNumber;
                    }
                }
                else
                {
                    mIsSecond = false;
                }

                mSinEvent.Set();
                mCosEvent.Set();
                mSquareEvent.Set();
                mDatetimeEvent.Set();
                mStepEvent.Set();
                mSteppointEvent.Set();

                Thread.Sleep(500);

                
            }
        }

//        /// <summary>
//        /// 
//        /// </summary>
//        /// <param name="sender"></param>
//        /// <param name="e"></param>
//        private void MScanTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
//        {
//            mTickCount++;
//            if (mIsBusy)
//            {
//                mBusyCount++;
//                if (mBusyCount >= 10)
//                {
//                    mBusyCount = 0;
//                    LoggerService.Service.Warn("Sim Driver", "出现阻塞");
//                }
//                return;
//            }
//            mBusyCount = 0;
//            mIsBusy = true;
//            DateTime time = DateTime.Now;
//            if (mTickCount <5)
//            {
//                mIsBusy = false;
//                return;
//            }
//            else
//            {
//                mTickCount = 0;
//            }

//            mLastProcessTime = time;
//            if(!mIsSecond)
//            {
//                mNumber++;
//                mNumber = mNumber > (short)360 ? (short)0 : mNumber;
//                mIsSecond = true;
//            }
//            else
//            {
//                mIsSecond = false;
//            }
            

//            if (mNumber % 100 == 0) mBoolNumber = !mBoolNumber;

//            double fval = Math.Cos(mNumber / 180.0 * Math.PI);
//            double sval = Math.Sin(mNumber / 180.0 * Math.PI);

//#if DEBUG
//            Stopwatch sw = new Stopwatch();
//            sw.Start();
//            Log("Sim:Sin " + fval + " " + "Sim:Cos " + sval + " " + "Sim:step " + mNumber + "  " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));

//#endif


//            System.Threading.Tasks.Parallel.ForEach(mTagIdCach, (vv) =>
//            {
//                if (vv.Key == "Sim:cos")
//                {
//                    mTagService.SetTagValue(vv.Value, fval);
//                }
//                else if (vv.Key == "Sim:sin")
//                {
//                    mTagService.SetTagValue(vv.Value, sval);
//                }
//                else if (vv.Key == "Sim:step")
//                {
//                    mTagService.SetTagValue(vv.Value, mNumber);
//                }
//                else if(vv.Key == "Sim:steppoint")
//                {
//                    mTagService.SetPointValue(vv.Value,mNumber,mNumber,mNumber);
//                }
//                else if (vv.Key == "Sim:square")
//                {
//                    mTagService.SetPointValue(vv.Value, mBoolNumber);
//                }
//            });

//#if DEBUG
//            sw.Stop();

//            LoggerService.Service.Info("Sim Driver", "set value elapsed:" + sw.ElapsedMilliseconds + " total count:" + mNumber + " cos:" + Math.Cos(mNumber / 180.0 * Math.PI) + " sin:" + Math.Sin(mNumber / 180.0 * Math.PI));
//#endif
//            mIsBusy = false;
//        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Stop()
        {
            //mScanTimer.Stop();
            mIsClosed = true;
           // mScanThread.Abort();
            mWriter.Close();
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

        public bool Init()
        {
            return true;
        }



        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...



    }
}
