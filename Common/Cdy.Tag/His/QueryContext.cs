using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public  class QueryContext:Dictionary<string, object>,IDisposable
    {
        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, BlockKeyHisValueItem> mCachKeyValues = new Dictionary<string, BlockKeyHisValueItem>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, KeyHisValueItem> mFileCachKeyValues = new Dictionary<string, KeyHisValueItem>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string,long[]> mTagHeadCache = new Dictionary<string, long[]>();

        /// <summary>
        /// 
        /// </summary>
        public string CurrentFile { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int CurrentBlock { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public object LastValue { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime LastTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public byte LastQuality { get; set; }

        public object FirstValue { get; set; }

        public DateTime FirstTime { get; set; }

        public byte FirstQuality { get; set; }

        public void RegistorLastFileKeyHisValue<T>(string key,object value)
        {
            if(mFileCachKeyValues.ContainsKey(key))
            {
                mFileCachKeyValues[key].NextValue = value;
            }
            else
            {
                mFileCachKeyValues.Add(key,new KeyHisValueItem() { NextValue = value });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void RegistorFirstFileKeyHisValue<T>(string key,object value)
        {
            if (mFileCachKeyValues.ContainsKey(key))
            {
                mFileCachKeyValues[key].PreValue = value;
            }
            else
            {
                mFileCachKeyValues.Add(key, new KeyHisValueItem() { PreValue = value });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool HasFileKeyHisValueRegistor(string key)
        {
            return mFileCachKeyValues.ContainsKey(key);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public object GetLastFileKeyHisValueRegistor(string key)
        {
            if (mFileCachKeyValues.ContainsKey(key))
            {
                return mFileCachKeyValues[key].NextValue;
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public object GetFirstFileKeyHisValueRegistor(string key)
        {
            if (mFileCachKeyValues.ContainsKey(key))
            {
                return mFileCachKeyValues[key].PreValue;
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="item"></param>
        public void RegistorLastKeyHisValue<T>(string key, int blockid, T value, DateTime time, byte quality)
        {
            lock (mCachKeyValues)
            {
                if (mCachKeyValues.ContainsKey(key))
                {
                    mCachKeyValues[key].CheckAndLastValue(blockid,time,quality,value);
                }
                else
                {
                    var bk = new BlockKeyHisValueItem();
                    bk.CheckAndLastValue(blockid,time,quality,value);

                    mCachKeyValues.Add(key, bk);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="id"></param>
        /// <param name="blockid"></param>
        /// <param name="value"></param>
        /// <param name="time"></param>
        public void RegistorFirstKeyHisValue<T>(string key,int blockid,T value,DateTime time,byte quality)
        {
            lock (mCachKeyValues)
            {
                if (mCachKeyValues.ContainsKey(key))
                {
                    mCachKeyValues[key].CheckAndPreValue(blockid, time, quality, value);
                }
                else
                {
                    var bk = new BlockKeyHisValueItem();
                    bk.CheckAndPreValue(blockid, time, quality, value);
                    mCachKeyValues.Add(key, bk);
                }
            }
        }

        /// <summary>
        /// 缓存结束值供下次使用
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        public void RegistorLastKeyHisValue<T>(T value,DateTime time,byte quality)
        {
            RegistorLastKeyHisValue<T>(CurrentFile,CurrentBlock,value,time,quality);
        }

        /// <summary>
        /// 缓存开始值，供下次使用
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        public void RegistorFirstKeyHisValue<T>(T value,DateTime time,byte quality)
        {
            RegistorFirstKeyHisValue<T>(CurrentFile, CurrentBlock, value, time, quality);
        }

        /// <summary>
        /// 从缓冲中读取当前文件的当前数据块的最后一个值
        /// </summary>
        /// <returns></returns>
        public object GetBlockLastValue()
        {
            if(mCachKeyValues.ContainsKey(CurrentFile))
            {
                return mCachKeyValues[CurrentFile].GetBlockLastValue(CurrentBlock);
            }
            return null;
        }

        /// <summary>
        /// 从缓冲中 读取某个数据块的第最后一个值
        /// </summary>
        /// <param name="file"></param>
        /// <param name="blockid"></param>
        /// <returns></returns>
        public object GetBlockLastValue(string file,int blockid)
        {
            if (mCachKeyValues.ContainsKey(file))
            {
               return mCachKeyValues[file].GetBlockLastValue(blockid);
            }
            return null;
        }

        /// <summary>
        /// 从缓冲中读取当前文件的当前数据块的第一个值
        /// </summary>
        /// <returns></returns>
        public object GetBlockFirstValue()
        {
            if (mCachKeyValues.ContainsKey(CurrentFile))
            {
               return mCachKeyValues[CurrentFile].GetBlockFirstValue(CurrentBlock);
            }
            return null;
        }

        /// <summary>
        /// 从缓冲中读取某个数据块的第一个值
        /// </summary>
        /// <param name="file"></param>
        /// <param name="blockid"></param>
        /// <returns></returns>
        public object GetBlockFirstValue(string file, int blockid)
        {
            if (mCachKeyValues.ContainsKey(file))
            {
                return mCachKeyValues[file].GetBlockFirstValue(blockid);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool IsLastQualityClosed()
        {
            return LastQuality == (byte)QualityConst.Close;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            foreach(var key in mCachKeyValues)
            {
                key.Value.Dispose();
            }
            mCachKeyValues.Clear();
            mFileCachKeyValues.Clear();
            this.Clear();
        }

        public void RegisorHeadPoint(string sfile,long[] pointers)
        {
            lock (mCachKeyValues)
            {
                if (mTagHeadCache.ContainsKey(sfile))
                {
                    mTagHeadCache[sfile] = pointers;
                }
                else
                {
                    mTagHeadCache.Add(sfile, pointers);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sfile"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public long GetHeadPoint(string sfile, int index)
        {
            if (mTagHeadCache.ContainsKey(sfile))
            {
                return mTagHeadCache[sfile][index];
            }
            else
            {
                return -1;
            }
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class BlockKeyHisValueItem:IDisposable
    {
        private Dictionary<int,KeyHisValueItem> mCachKeyValues = new Dictionary<int,KeyHisValueItem>();

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<int,KeyHisValueItem> CachKeyValues
        {
            get
            {
               return mCachKeyValues;
            }
            set
            {
                mCachKeyValues = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="blockid"></param>
        /// <returns></returns>
        public object GetBlockLastValue(int blockid)
        {
            if(mCachKeyValues.ContainsKey(blockid))
            {
                return mCachKeyValues[blockid].NextValue;
            }
            return null ;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="blockid"></param>
        /// <returns></returns>
        public object GetBlockFirstValue(int blockid)
        {
            if (mCachKeyValues.ContainsKey(blockid))
            {
                return mCachKeyValues[blockid].PreValue;
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="blockid"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <param name="value"></param>
        public void CheckAndLastValue<T>(int blockid,DateTime time,byte quality,T value)
        {
            if (mCachKeyValues.ContainsKey(blockid))
            {
                if (mCachKeyValues[blockid].NextValue != null)
                {
                    var oval = (TagHisValue<T>)mCachKeyValues[blockid].NextValue;
                    if (oval.Time > time) return;
                }
                mCachKeyValues[blockid].NextValue = new TagHisValue<T>() { Value=value ,Quality=quality,Time=time};
            }
            else
            {
                mCachKeyValues.Add(blockid, new KeyHisValueItem() { NextValue = new TagHisValue<T>() { Value = value, Time = time, Quality = quality } });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="blockid"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <param name="value"></param>
        public void CheckAndPreValue<T>(int blockid, DateTime time, byte quality, T value)
        {
            if (mCachKeyValues.ContainsKey(blockid))
            {
                if (mCachKeyValues[blockid].PreValue != null)
                {
                    var oval = (TagHisValue<T>)mCachKeyValues[blockid].PreValue;
                    if (oval.Time < time) return;
                }
                mCachKeyValues[blockid].PreValue = new TagHisValue<T>() { Value = value, Quality = quality, Time = time };
            }
            else
            {
                mCachKeyValues.Add(blockid, new KeyHisValueItem() { PreValue = new TagHisValue<T>() { Value = value, Time = time, Quality = quality } });
            }
        }

        public void Dispose()
        {
            //foreach(var vv in mCachKeyValues)
            //{

            //}
            mCachKeyValues.Clear();
        }
    }


    public class KeyHisValueItem
    {

        /// <summary>
        /// 
        /// </summary>
        public object PreValue { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public object NextValue { get; set; }
    }
}
