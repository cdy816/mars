using Cdy.Tag.Driver;
using System;
using System.Collections;
using System.Collections.Generic;

namespace SimDriver
{
    public class Driver : Cdy.Tag.Driver.ITagDriver
    {

        #region ... Variables  ...

        System.Collections.Generic.Dictionary<string, List<int>> mTagIdCach = new Dictionary<string, List<int>>();

        private System.Timers.Timer mScanTimer;

        private short mNumber = 0;

        private IRealTagDriver mTagService;

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

        #endregion ...Properties...

        #region ... Methods    ...

        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagQuery"></param>
        private void InitTagCach(IRealTagDriver tagQuery)
        {
            mTagIdCach = tagQuery.GetTagsByLinkAddress(new List<string>() { "cos", "sin","step" });
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
            mNumber++;
            mNumber = mNumber > (short)360 ? (short)0 : mNumber;
            foreach(var vv in mTagIdCach)
            {
                if(vv.Key=="cos")
                {
                    double fval = Math.Cos(mNumber / 180 * Math.PI);
                    mTagService.SetTagValue(vv.Value, fval);
                }
                else if(vv.Key=="sim")
                {
                    double fval = Math.Sin(mNumber / 180 * Math.PI);
                    mTagService.SetTagValue(vv.Value, fval);
                }
                else if(vv.Key=="step")
                {
                    mTagService.SetTagValue(vv.Value, mNumber);
                }
            }
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
