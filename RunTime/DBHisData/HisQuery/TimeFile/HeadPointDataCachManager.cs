using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using System.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 用于缓存数据区指针
    /// </summary>
    public class HeadPointDataCachManager
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, HeadDataPointCachItem> mCacheDatas = new Dictionary<string, HeadDataPointCachItem>();

        /// <summary>
        /// 
        /// </summary>
        public static HeadPointDataCachManager Manager = new HeadPointDataCachManager();


        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="address"></param>
        /// <returns></returns>
        public MarshalMemoryBlock GetMemory(DataFileSeriserbase datafile, long address,int len)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            try
            {
                string skey = System.IO.Path.GetFileNameWithoutExtension(datafile.FileName) + address;
                if (mCacheDatas.ContainsKey(skey))
                {
                    mCacheDatas[skey].LastAccessTime = DateTime.Now;
                    return mCacheDatas[skey].DataBlock;
                }
                else
                {
                    DateTime dnow = DateTime.Now;
                    var mh = datafile.Read(address, len);
                    mCacheDatas.Add(skey,new HeadDataPointCachItem() { DataBlock = mh, LastAccessTime = dnow, Name = skey });

                    if (mCacheDatas.Count > 24)
                    {
                        Task.Run(() => {
                            try
                            {
                                foreach (var vv in mCacheDatas.Values.OrderBy(e => e.LastAccessTime))
                                {
                                    if ((dnow - vv.LastAccessTime).TotalDays >= 1)
                                    {
                                        mCacheDatas.Remove(vv.Name);
                                    }
                                    vv.DataBlock.Dispose();
                                }
                            }
                            catch
                            {

                            }
                        });

                    }

                    return mh;
                }
            }
            finally
            {
                sw.Stop();
                Debug.Print("获取头部指针:" + sw.ElapsedMilliseconds);
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class HeadDataPointCachItem
    {
        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime LastAccessTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public MarshalMemoryBlock DataBlock { get; set; }

    }

}
