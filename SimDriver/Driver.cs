using Cdy.Tag;
using Cdy.Tag.Driver;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;

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
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public string Name => "Sim";

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
            mScanTimer = new System.Timers.Timer(1000);
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
                LoggerService.Service.Warn("Sim Driver", "出现阻塞");
                return;
            }
            mIsBusy = true;

            mNumber++;
            mNumber = mNumber > (short)360 ? (short)0 : mNumber;
//#if DEBUG
//            Stopwatch sw = new Stopwatch();
//            sw.Start();
//#endif
            foreach(var vv in mTagIdCach)
            {
                if (vv.Key == "Sim:cos")
                {
                    double fval = Math.Cos(mNumber / 180.0 * Math.PI);
                    mTagService.SetTagValue(vv.Value, fval);
                }
                else if (vv.Key == "Sim:sin")
                {
                    double fval = Math.Sin(mNumber / 180.0 * Math.PI);
                    mTagService.SetTagValue(vv.Value, fval);
                }
                else if (vv.Key == "Sim:step")
                {
                    mTagService.SetTagValue(vv.Value, mNumber);
                }
            }
//#if DEBUG
//            sw.Stop();

//            LoggerService.Service.Info("Sim Driver","set value elapsed:"+ sw.ElapsedMilliseconds +" total count:"+mNumber+" cos:"+ Math.Cos(mNumber / 180.0 * Math.PI)+" sin:"+ Math.Sin(mNumber / 180.0 * Math.PI));
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
            return true;
        }

        

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
        

        
    }
}
