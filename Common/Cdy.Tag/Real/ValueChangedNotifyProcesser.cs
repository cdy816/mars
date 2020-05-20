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
using System.Threading;
using System.Linq;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    public class BlockItem
    {
        /// <summary>
        /// 
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int StartAddress { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int EndAddress { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsDirty { get; set; }
    }


    /// <summary>
    /// 变量值改变通知处理
    /// </summary>
    public class ValueChangedNotifyProcesser:IDisposable
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, bool> mRegistorTagIds = new Dictionary<int, bool>();

        private Dictionary<int, BlockItem> mBlockChangeds = new Dictionary<int, BlockItem>();

        /// <summary>
        /// 
        /// </summary>
        private int[] mChangedIds = null;

        /// <summary>
        /// 
        /// </summary>
        private ManualResetEvent resetEvent;

        /// <summary>
        /// 
        /// </summary>
        private Thread mProcessThread;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagIds"></param>
        public delegate void ValueChangedDelegate(int[] tagIds);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="blockids"></param>
        public delegate void BlockChangedDelegate(BlockItem block);

        private int mLenght = 0;

        private object mLockObject = new object();

        private bool mIsAll = false;

        private DateTime mLastNotiyTime = DateTime.Now;

        private bool mIsClosed = false;

        public const int BlockSize = 10000;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public ValueChangedNotifyProcesser()
        {
            resetEvent = new ManualResetEvent(false);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 名称
        /// </summary>
        public string Name { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public ValueChangedDelegate ValueChanged { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public BlockChangedDelegate BlockChanged { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            if (mIsAll)
            {
                mChangedIds = new int[1024];
            }
            else
            {
                mChangedIds = new int[(int)(mRegistorTagIds.Count * 1.2)];
            }
            mProcessThread = new Thread(ThreadProcess);
            mProcessThread.IsBackground = true;
            mProcessThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Close()
        {
            mIsClosed = true;
            resetEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void UpdateValue(int id)
        {
            if (mIsAll || mRegistorTagIds.ContainsKey(id))
            {
                lock (mLockObject)
                {
                    if (mIsAll)
                    {
                        lock (mBlockChangeds)
                            mBlockChangeds[id / BlockSize].IsDirty = true;
                    }
                    else
                    {
                        mChangedIds[mLenght++] = id;
                        if (mLenght >= mChangedIds.Length)
                        {
                            ReAllocMemory((int)(mLenght * 1.2));
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 重新分配内存块
        /// </summary>
        /// <param name="len"></param>
        private void ReAllocMemory(int len)
        {
            int[] ltmp = new int[len];
            Array.Copy(mChangedIds, 0, ltmp, 0, mChangedIds.Length);
            mChangedIds = ltmp;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        public void UpdateValue(List<int> ids)
        {
            if (!mIsAll)
            {
                if ((mLenght + ids.Count) > mChangedIds.Length)
                {
                    ReAllocMemory((int)((mLenght + ids.Count) * 1.2));
                }
            }

            foreach (var id in ids)
            {
                if (mIsAll || mRegistorTagIds.ContainsKey(id))
                {
                    lock (mChangedIds)
                    {
                        //int blockid = id / BlockSize;
                        if (mIsAll)
                        {
                            lock (mBlockChangeds)
                                mBlockChangeds[id / BlockSize].IsDirty = true;
                        }
                        else
                        {
                            mChangedIds[mLenght++] = id;
                            mLenght = mLenght >= mChangedIds.Length ? mChangedIds.Length - 1 : mLenght;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="min"></param>
        /// <param name="max"></param>
        public void RegistorAll()
        {
            mIsAll = true;
        }

        /// <summary>
        /// 注册需要监视的变量集合
        /// </summary>
        /// <param name="id"></param>
        public void Registor(int id)
        {
            if (!mRegistorTagIds.ContainsKey(id))
                mRegistorTagIds.Add(id, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        public void Registor(List<int> ids)
        {
            foreach(var id in ids)
            {
                if (!mRegistorTagIds.ContainsKey(id))
                    mRegistorTagIds.Add(id, false);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void NotifyChanged()
        {
            resetEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>
        private void ThreadProcess()
        {
            while(!mIsClosed)
            {
                resetEvent.WaitOne();
                if (mIsClosed) break;

                resetEvent.Reset();
                if (ValueChanged!=null && mLenght > 0)
                {
                    int[] vtmp = null;
                    lock (mLockObject)
                    {
                        vtmp = new int[mLenght];
                        Array.Copy(mChangedIds, 0, vtmp, 0, mLenght);
                        mLenght = 0;
                    }
                    ValueChanged?.Invoke(vtmp);
                }
                if (BlockChanged != null)
                {
                    foreach (var vv in mBlockChangeds)
                    {
                        if (vv.Value.IsDirty)
                        {
                            lock (mBlockChangeds)
                            {
                                BlockChanged?.Invoke(vv.Value);
                                vv.Value.IsDirty = false;
                            }
                        }
                    }

                }
                Thread.Sleep(10);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="maxid"></param>
        /// <param name="getAddress"></param>
        public void BuildBlock(int maxid,Func<int,int> getAddress)
        {
            int count = maxid / BlockSize;
            count = maxid % BlockSize > 0 ? count + 1 : count;
            for(int i=0;i<count;i++)
            {
                int start = getAddress(i * BlockSize);
                int end = getAddress(i * BlockSize + BlockSize);

                mBlockChangeds.Add(i, new BlockItem() { Id = i, StartAddress = start, EndAddress = end });
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...


        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            ValueChanged = null;
            resetEvent.Close();
            resetEvent = null;
        }
        #endregion ...Interfaces...
    }
}
