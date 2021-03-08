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
        public void ClearMemoryCach(string datafile)
        {
            lock (mCacheDatas)
            {
                string skey = System.IO.Path.GetFileNameWithoutExtension(datafile);
                foreach (var vv in mCacheDatas.Where(e => e.Key.StartsWith(skey)))
                {
                   var vvb = mCacheDatas[vv.Key];
                    vvb.DataBlock?.Dispose();
                    vvb.DataBlock = null;
                }
            }
        }

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
                lock (mCacheDatas)
                {
                    string skey = System.IO.Path.GetFileNameWithoutExtension(datafile.FileName) + address;
                    if (mCacheDatas.ContainsKey(skey) && mCacheDatas[skey]!=null && mCacheDatas[skey].DataBlock!=null)
                    {
                        mCacheDatas[skey].LastAccessTime = DateTime.Now;
                        return mCacheDatas[skey].DataBlock;
                    }
                    else
                    {
                        DateTime dnow = DateTime.Now;
                        var mh = datafile.Read(address, len);

                        if (!mCacheDatas.ContainsKey(skey))
                        {
                            mCacheDatas.Add(skey, new HeadDataPointCachItem() { DataBlock = mh, LastAccessTime = dnow, Name = skey });
                        }
                        else
                        {
                            mCacheDatas[skey].DataBlock = mh;
                        }
                        if (mCacheDatas.Count > 24)
                        {
                            Task.Run(() =>
                            {
                                try
                                {
                                    lock (mCacheDatas)
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
                                }
                                catch
                                {

                                }
                            });

                        }

                        return mh;
                    }
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
