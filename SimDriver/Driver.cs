using Cdy.Tag;
using Cdy.Tag.Driver;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace SimDriver
{
    public class Driver : Cdy.Tag.Driver.ITagDriver
    {

        #region ... Variables  ...

        System.Collections.Generic.Dictionary<string, List<int>> mTagIdCach = new Dictionary<string, List<int>>();

        private System.Timers.Timer mScanTimer;

        private short mNumber = 0;

        private IRealTagDriver mTagService;

        private bool mIsBusy = false;

        private StreamWriter mWriter;

        private DateTime mLastProcessTime = DateTime.Now;

        private int mBusyCount = 0;

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
                return new string[] { "cos", "sin", "step" };
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
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagQuery"></param>
        private void InitTagCach(IRealTagDriver tagQuery)
        {
            mTagIdCach = tagQuery.GetTagsByLinkAddress(new List<string>() { "Sim:cos", "Sim:sin", "Sim:step" });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagQuery"></param>
        /// <returns></returns>
        public bool Start(IRealTagDriver tagQuery)
        {
            mTagService = tagQuery;
            InitTagCach(tagQuery);
            mScanTimer = new System.Timers.Timer(100);
            mScanTimer.Elapsed += MScanTimer_Elapsed;
            mScanTimer.Start();
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void MScanTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
           
            if (mIsBusy)
            {
                mBusyCount++;
                if(mBusyCount>=10)
                LoggerService.Service.Warn("Sim Driver", "出现阻塞");
                return;
            }
            mBusyCount = 0;
            mIsBusy = true;
            DateTime time = DateTime.Now;
            if ((time - mLastProcessTime).Seconds < 1)
            {
                mIsBusy = false;
                return;
            }
            mLastProcessTime = time;

            mNumber++;
            mNumber = mNumber > (short)360 ? (short)0 : mNumber;
//#if DEBUG
//            Stopwatch sw = new Stopwatch();
//            sw.Start();
//#endif
            double fval = Math.Cos(mNumber / 180.0 * Math.PI);
            double sval = Math.Sin(mNumber / 180.0 * Math.PI);

            Log("Sim:Sin " + fval + " " + "Sim:Cos " + sval + " " + "Sim:step " + mNumber + "  " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            //foreach (var vv in mTagIdCach)
            //{
            //    if (vv.Key == "Sim:cos")
            //    {
            //        mTagService.SetTagValue(vv.Value, fval);
            //    }
            //    else if (vv.Key == "Sim:sin")
            //    {
            //        mTagService.SetTagValue(vv.Value, sval);
            //    }
            //    else if (vv.Key == "Sim:step")
            //    {
            //        mTagService.SetTagValue(vv.Value, mNumber);
            //    }
            //}

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
            });

//#if DEBUG
//            sw.Stop();

//            LoggerService.Service.Info("Sim Driver", "set value elapsed:" + sw.ElapsedMilliseconds + " total count:" + mNumber + " cos:" + Math.Cos(mNumber / 180.0 * Math.PI) + " sin:" + Math.Sin(mNumber / 180.0 * Math.PI));
//#endif
            mIsBusy = false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Stop()
        {
            mScanTimer.Stop();
            mWriter.Close();
            return true;
        }

        

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
        

        
    }
}
