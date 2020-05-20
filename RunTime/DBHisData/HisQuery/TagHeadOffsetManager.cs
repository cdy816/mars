//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 变量头部偏移管理
    /// </summary>
    public class TagHeadOffsetManager
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static TagHeadOffsetManager manager = new TagHeadOffsetManager();

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<HeadOffsetKey,Tuple<Dictionary<int, int>,long>> mHeadOffsets = new Dictionary<HeadOffsetKey, Tuple<Dictionary<int, int>, long>>();

        public Dictionary<string, Dictionary<int, long>> LogHeadOffsets = new Dictionary<string, Dictionary<int, long>>();

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
        /// <param name="name"></param>
        /// <returns></returns>
        public bool Contains(string name)
        {
            return LogHeadOffsets.ContainsKey(name);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="addrs"></param>
        public void AddLogHead(string name,Dictionary<int,long> addrs)
        {
            if (!LogHeadOffsets.ContainsKey(name))
            {
                LogHeadOffsets.Add(name, addrs);
            }
            else
            {
                LogHeadOffsets[name] = addrs;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void RemoveLogHead(string name)
        {
            if(LogHeadOffsets.ContainsKey(name))
            {
                LogHeadOffsets.Remove(name);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sum"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public bool Contains(long sum,int count)
        {
            HeadOffsetKey key = new HeadOffsetKey(count, sum);
            return mHeadOffsets.ContainsKey(key);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sum"></param>
        /// <param name="count"></param>
        /// <param name="offset"></param>
        public void Add(long sum,int count,Dictionary<int, int> offset,long blockPointer)
        {
            HeadOffsetKey key = new HeadOffsetKey(count, sum);
            mHeadOffsets.Add(key,new Tuple<Dictionary<int, int>, long>(offset, blockPointer));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sum"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Tuple<Dictionary<int,int>,long> Get(long sum,int count)
        {
            HeadOffsetKey key = new HeadOffsetKey(count, sum);
            return mHeadOffsets[key];
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public Dictionary<int, long> Get(string key)
        {
            return LogHeadOffsets[key];
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public struct HeadOffsetKey
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <param name="sum"></param>
        public HeadOffsetKey(int count,long sum)
        {
            Count = count;
            Sum = sum;
        }

        /// <summary>
        /// 
        /// </summary>
        public int Count { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public long Sum { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            HeadOffsetKey target = (HeadOffsetKey)obj;
            return this.Count == target.Count && this.Sum == target.Sum;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return Sum.GetHashCode()+Count.GetHashCode();
        }
    }

}
