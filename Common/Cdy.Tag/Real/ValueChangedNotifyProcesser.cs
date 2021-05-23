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
using System.Buffers;

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
    /// 
    /// </summary>
    public class Databuffer
    {

        /// <summary>
        /// 
        /// </summary>
        public int Length { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int[] Buffer { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public Databuffer(int size)
        {
            Buffer = new int[size];
        }

        /// <summary>
        /// 
        /// </summary>
        public Databuffer():this(1024)
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="len"></param>
        public void ReSize(int len)
        {
            if (len > Buffer.Length)
            {
                int[] ltmp = new int[len];
                Array.Copy(Buffer, 0, ltmp, 0, Buffer.Length);
                Buffer = ltmp;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void AppendValue(int value)
        {
            if(Length<Buffer.Length-1)
            {
                Buffer[Length++] = value;
            }
            else
            {
                ReSize((int)(Buffer.Length * 1.2));
                Buffer[Length++] = value;
            }
        }

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
        private Databuffer mChangedIds = null;

        /// <summary>
        /// 
        /// </summary>
        private Databuffer mChangedId1 = null;

        /// <summary>
        /// 
        /// </summary>
        private Databuffer mChangedId2 = null;



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
        public delegate void ValueChangedDelegate(int[] tagIds,int len);

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

        public const int BlockSize = 500000;

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

        /// <summary>
        /// 
        /// </summary>
        public RealDataNotifyType NotifyType { get; set; }

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
        public Action BlockChangedNotify { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int MemorySize { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            if (mIsAll)
            {
                if (NotifyType == RealDataNotifyType.All || NotifyType == RealDataNotifyType.Tag)
                {
                    var vcount = ServiceLocator.Locator.Resolve<ITagManager>().ListAllTags().Count();
                    mChangedId2 = new Databuffer((int)(vcount * 1.2));
                    mChangedId1 = new Databuffer((int)(vcount * 1.2));
                }
                else
                {
                    mChangedId2 = new Databuffer();
                    mChangedId1 = new Databuffer();
                }
                mChangedIds = mChangedId1;
            }
            else
            {
                mChangedId1 = new Databuffer((int)(mRegistorTagIds.Count * 1.2));
                mChangedId2 = new Databuffer((int)(mRegistorTagIds.Count * 1.2));
                mChangedIds = mChangedId1;
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
            if (this.NotifyType == RealDataNotifyType.All || this.NotifyType == RealDataNotifyType.Block)
            {
                lock (mBlockChangeds)
                    mBlockChangeds[id / BlockSize].IsDirty = true;
            }

            if ((this.NotifyType == RealDataNotifyType.All || this.NotifyType == RealDataNotifyType.Tag))
            {
                if (mIsAll || mRegistorTagIds.ContainsKey(id))
                {
                    lock (mLockObject)
                    {
                        mChangedIds.AppendValue(id);
                    }
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        public void UpdateValue(IEnumerable<int> ids)
        {
            lock (mLockObject)
            {
                if (this.NotifyType == RealDataNotifyType.Tag || this.NotifyType == RealDataNotifyType.All)
                {
                    if ((mChangedIds.Buffer.Length + ids.Count()) > mChangedIds.Length)
                    {
                        mChangedIds.ReSize((int)((mLenght + ids.Count()) * 1.2));
                    }
                }
                foreach (var id in ids)
                {
                    UpdateValue(id);
                }
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="min"></param>
        ///// <param name="max"></param>
        //public void RegistorAll()
        //{
        //    mIsAll = true;
        //}

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
        /// <param name="id"></param>
        public void UnRegistor(int id)
        {
            if (mRegistorTagIds.ContainsKey(id))
                mRegistorTagIds.Remove(id);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        public void Registor(IEnumerable<int> ids)
        {
            if (ids == null) return;

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
            int i = 0;
            while (!mIsClosed)
            {
                resetEvent.WaitOne();
                if (mIsClosed) break;

                resetEvent.Reset();

                if (NotifyType == RealDataNotifyType.Tag || NotifyType == RealDataNotifyType.All)
                {
                    if (ValueChanged != null && mChangedIds.Length > 0)
                    {
                        var vids = mChangedIds;
                        lock (mLockObject)
                        {
                            if (mChangedIds == mChangedId1)
                            {
                                mChangedIds = mChangedId2;
                            }
                            else
                            {
                                mChangedIds = mChangedId1;
                            }
                        }
                        ValueChanged?.Invoke(vids.Buffer, vids.Length);
                        vids.Length = 0;
                    }
                }

                if (NotifyType == RealDataNotifyType.All || NotifyType == RealDataNotifyType.Block)
                {
                    if ((BlockChanged != null) && i % 10 == 0)
                    {
                        i = 0;
                        lock (mBlockChangeds)
                        {
                            foreach (var vv in mBlockChangeds)
                            {
                                if (vv.Value.IsDirty)
                                {
                                    vv.Value.IsDirty = false;
                                    BlockChanged?.Invoke(vv.Value);
                                }
                            }
                        }
                    }
                }
                i++;

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
            lock (mBlockChangeds)
            {
                mBlockChangeds.Clear();
                int count = maxid / BlockSize;
                count = maxid % BlockSize > 0 ? count + 1 : count;
                for (int i = 0; i <= count; i++)
                {
                    int start = getAddress(i * BlockSize);
                    int end = getAddress(i * BlockSize + BlockSize);

                    mBlockChangeds.Add(i, new BlockItem() { Id = i, StartAddress = start, EndAddress = end });
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="maxid"></param>
        /// <param name="getAddress"></param>
        public void UpdateBlock(int maxid, Func<int, int> getAddress)
        {
            lock (mBlockChangeds)
            {
                int count = maxid / BlockSize;
                count = maxid % BlockSize > 0 ? count + 1 : count;
                for (int i = 0; i <= count; i++)
                {
                    int start = getAddress(i * BlockSize);
                    int end = getAddress(i * BlockSize + BlockSize);
                    if (!mBlockChangeds.ContainsKey(i))
                    {
                        mBlockChangeds.Add(i, new BlockItem() { Id = i, StartAddress = start, EndAddress = end });
                    }
                    else
                    {
                        var block = mBlockChangeds[i];
                        block.StartAddress = start;
                        block.EndAddress = end;
                    }
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
