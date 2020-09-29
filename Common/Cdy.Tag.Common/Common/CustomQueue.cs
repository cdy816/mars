//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/23 23:58:40.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cdy.Tag
{
    public class CustomQueue<T>
    {

        #region ... Variables  ...
        private T[] mColections;
        private int mCount = 0;
        private object mReadLockObj = new object();
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        public CustomQueue(int count)
        {
            mColections = new T[count];
            mCount = count;
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public void CheckAndResize(int size)
        {
            if(size>mCount)
            {
                mColections = new T[size];
                mCount = size;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public int WriteIndex { get; set; } = -1;

        /// <summary>
        /// 
        /// </summary>
        public int ReadIndex { get; set; } = 0;

        /// <summary>
        /// 
        /// </summary>
        public int Length { get { return mCount; } }

        #endregion ...Properties...

        #region ... Methods    ...
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Insert(T value)
        {
            lock(mColections)
            mColections[++WriteIndex] = value;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="index"></param>
        public void InsertAt(T value,int index)
        {
            mColections[index] = value;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool Contains(T value)
        {
            return mColections.Contains(value);
        }

        /// <summary>
        /// 
        /// </summary>
        public T IncRead()
        {
            lock (mReadLockObj)
            {
                if (ReadIndex <= WriteIndex)
                {
                    return mColections[ReadIndex++];
                }
                else
                {
                    ReadIndex++;
                    return default(T);
                }
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public T DescRead()
        {
            lock (mReadLockObj)
            {
                if (ReadIndex >= 0)
                {
                    return mColections[ReadIndex--];
                }
                else
                {
                    ReadIndex--;
                    return default(T);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public T Read()
        {
            return mColections[ReadIndex];
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public T Read(int index)
        {
            return mColections[index];
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public T Remove()
        {
            if (WriteIndex < 0)
            {
                WriteIndex = -1;
                return default(T);
            }
            else
            {
                var re = mColections[WriteIndex];
                --WriteIndex;
                return re;
            }

        }

        public T Get(int index)
        {
            return mColections[index];
        }

        /// <summary>
        /// 
        /// </summary>
        public void Reset()
        {
            WriteIndex = -1;
            ReadIndex = 0;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
