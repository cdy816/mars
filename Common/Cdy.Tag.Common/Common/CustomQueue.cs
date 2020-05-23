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
using System.Text;

namespace Cdy.Tag
{
    public class CustomQueue<T>
    {

        #region ... Variables  ...
        private T[] mColections;
        private int mCount = 0;
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



        public int Index { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Insert(T value)
        {
            Index++;
            mColections[Index] = value;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public T Remove()
        {
            Index--;
            if (Index < 0)
            {
                Index = -1;
                return default(T);
            }
            else
            {
                return mColections[Index];
            }
        }

        public T Get(int index)
        {
            return mColections[index];
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
