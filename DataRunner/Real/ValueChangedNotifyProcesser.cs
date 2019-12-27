using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 变量值改变通知处理
    /// </summary>
    public class ValueChangedNotifyProcesser:IDisposable
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, int> mRegistorTagIds = new Dictionary<int, int>();

        /// <summary>
        /// 
        /// </summary>
        private long[] mChangedIds = null;

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
        public delegate void ValueChangedDelagete(int[] tagIds);

        private int mLenght = 0;

        private object mLockObject = new object();

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
        public ValueChangedDelagete ValueChanged { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {


            mChangedIds = new long[(int)(mRegistorTagIds.Count * 1.2)];
            mProcessThread = new Thread(ThreadProcess);
            mProcessThread.IsBackground = true;
            mProcessThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Close()
        {
            if (mProcessThread != null)
            {
                mProcessThread.Abort();
                mProcessThread = null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void UpdateValue(int id)
        {
            if (mRegistorTagIds.ContainsKey(id))
            {
                lock (mLockObject)
                {
                    mChangedIds[mLenght++] = id;
                    if(mLenght>=mChangedIds.Length)
                    {
                        ReAllocMemory((int)(mLenght * 1.2));
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
            long[] ltmp = new long[len];
            Array.Copy(mChangedIds, 0, ltmp, 0, mChangedIds.Length);
            mChangedIds = ltmp;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        public void UpdateValue(List<int> ids)
        {
            foreach (var id in ids)
            {
                if (mRegistorTagIds.ContainsKey(id))
                {
                    lock (mChangedIds)
                    {
                        mChangedIds[mLenght++] = id;
                        mLenght = mLenght >= mChangedIds.Length ? mChangedIds.Length - 1 : mLenght;
                    }
                }
            }
        }

        /// <summary>
        /// 注册需要监视的变量集合
        /// </summary>
        /// <param name="id"></param>
        public void Registor(int id)
        {
            if (!mRegistorTagIds.ContainsKey(id))
                mRegistorTagIds.Add(id,0);
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
                    mRegistorTagIds.Add(id, 0);
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
            while(true)
            {
                resetEvent.WaitOne();
                resetEvent.Reset();
                if (mLenght > 0)
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
