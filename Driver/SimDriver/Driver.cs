﻿using Cdy.Tag;
using Cdy.Tag.Driver;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace SimDriver
{
    public class Driver : Cdy.Tag.Driver.IProducterDriver
    {

        #region ... Variables  ...

        System.Collections.Generic.Dictionary<string, List<Tagbase>> mTagIdCach = new Dictionary<string, List<Tagbase>>();

        //private System.Timers.Timer mScanTimer;

        private short mNumber = 0;

        private bool mBoolNumber = false;

        private IRealTagProduct mTagService;

        //private bool mIsBusy = false;

        private StreamWriter mWriter;

        private DateTime mLastProcessTime = DateTime.Now;

        //private int mBusyCount = 0;

        private bool mIsSecond = false;

        //private int mTickCount = 0;

        private Thread mScanThread;

        private bool mIsClosed = false;

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
                return new string[] { "cos", "sin", "step","steppoint", "square" };
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
            mTagIdCach = tagQuery.GetTagsByLinkAddress(new List<string>() { "Sim:cos", "Sim:sin", "Sim:step", "Sim:steppoint", "Sim:square" });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagQuery"></param>
        /// <returns></returns>
        public bool Start(IRealTagProduct tagQuery)
        {
            mTagService = tagQuery;
            InitTagCach(tagQuery);
            mScanThread = new Thread(ScanThreadPro);
            mScanThread.IsBackground = true;
            mScanThread.Start();
            //mScanTimer = new System.Timers.Timer(100);
            //mScanTimer.Elapsed += MScanTimer_Elapsed;
            //mScanTimer.Start();
            return true;
        }

        private void ScanThreadPro()
        {
            while (!mIsClosed)
            {
                //mTickCount++;
                DateTime time = DateTime.Now;
                //if (mTickCount < 5)
                //{
                //    mIsBusy = false;
                //    Thread.Sleep(100);
                //    continue;
                //}
                //else
                //{
                //    mTickCount = 0;
                //}

                if((mLastProcessTime-time).TotalSeconds>1000)
                {
                    LoggerService.Service.Warn("Sim Driver", "出现阻塞");
                }

                mLastProcessTime = time;
                if (!mIsSecond)
                {
                    mNumber++;
                    mNumber = mNumber >= (short)360 ? (short)0 : mNumber;
                    mIsSecond = true;
                }
                else
                {
                    mIsSecond = false;
                }


                if (mNumber % 100 == 0) mBoolNumber = !mBoolNumber;

                double fval = Math.Cos(mNumber / 180.0 * Math.PI);
                double sval = Math.Sin(mNumber / 180.0 * Math.PI);

//#if DEBUG
            Stopwatch sw = new Stopwatch();
            sw.Start();
                //if(mNumber%10==0)
            Log("Sim:Sin " + fval + " " + "Sim:Cos " + sval + " " + "Sim:step " + mNumber + "  " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));

//#endif


                System.Threading.Tasks.Parallel.ForEach(mTagIdCach, (vv) =>
                {
                    if (vv.Key == "Sim:cos")
                    {
                        mTagService.SetTagValue(vv.Value, fval);
                    }
                    else if (vv.Key == "Sim:sin")
                    {
                        mTagService.SetTagValue(vv.Value, sval);
                    }
                    else if (vv.Key == "Sim:step")
                    {
                        mTagService.SetTagValue(vv.Value, mNumber);
                    }
                    else if (vv.Key == "Sim:steppoint")
                    {
                        mTagService.SetPointValue(vv.Value, mNumber, mNumber, mNumber);
                    }
                    else if (vv.Key == "Sim:square")
                    {
                        mTagService.SetTagValue(vv.Value, mBoolNumber);
                    }
                });
                mTagService.SubmiteNotifyChanged();
//#if DEBUG
            sw.Stop();
                if (mNumber % 5 == 0 && mIsSecond)
                    LoggerService.Service.Info("Sim Driver", "set value elapsed:" + sw.ElapsedMilliseconds);
                //#endif

                int delay = (int)(500 - (DateTime.Now - mLastProcessTime).TotalMilliseconds);
                if(delay < 0)
                {
                    delay = 1;
                }
                Thread.Sleep(delay);
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

        

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
        

        
    }
}