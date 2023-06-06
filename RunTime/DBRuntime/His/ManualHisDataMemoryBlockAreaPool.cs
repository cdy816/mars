using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBRuntime.His
{
    public class ManualHisDataMemoryBlockAreaPool
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static ManualHisDataMemoryBlockAreaPool Instance = new ManualHisDataMemoryBlockAreaPool();

        private Queue<ManualHisDataMemoryBlockArea> Cache = new Queue<ManualHisDataMemoryBlockArea>();
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        public int MaxCount = 10000;
        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ManualHisDataMemoryBlockArea Get()
        {
            lock (Cache)
            {
                if (Cache.Count > 0) return Cache.Dequeue();
                else
                {
                    return new ManualHisDataMemoryBlockArea();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="area"></param>
        public void Return(ManualHisDataMemoryBlockArea area)
        {
            lock (Cache)
            {
                if (Cache.Count < MaxCount)
                {
                    area.Dispose();
                    Cache.Enqueue(area);
                }
                else
                {
                    area.Dispose();
                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
