//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/14 11:01:03.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace DBRuntime.RDDC
{
    public abstract class RDDCServerProcessBase : IDisposable
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, Queue<IByteBuffer>> mDatasCach = new Dictionary<string, Queue<IByteBuffer>>();

        private Thread mProcessThread;

        private ManualResetEvent resetEvent;

        private bool mIsClosed = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public abstract byte FunId { get; }

        /// <summary>
        /// 
        /// </summary>
        public RDDCDataService Parent { get; set; }



        #endregion ...Properties...

        #region ... Methods    ...


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        protected IByteBuffer ToByteBuffer(byte id, string value)
        {
            var re = BufferManager.Manager.Allocate(id, value.Length*2);
            re.WriteString(value);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        protected IByteBuffer ToByteBuffer(byte id, byte value)
        {
            var re = BufferManager.Manager.Allocate(id, 1);
            re.WriteByte(value);
            return re;
        }

        protected IByteBuffer ToByteBuffer(byte id, long value)
        {
            var re = BufferManager.Manager.Allocate(id, 1);
            re.WriteLong(value);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        public virtual void ProcessData(string client, IByteBuffer data)
        {
            data.Retain();
            if (mDatasCach.ContainsKey(client))
            {
                mDatasCach[client].Enqueue(data);
            }
            else
            {
                var vq = new Queue<IByteBuffer>();
                vq.Enqueue(data);
                mDatasCach.Add(client, vq);
            }
            resetEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>
        private void DataProcess()
        {
            while (!mIsClosed)
            {
                resetEvent.WaitOne();
                if (mIsClosed) return;
                resetEvent.Reset();
                foreach (var vv in mDatasCach)
                {
                    while(vv.Value.Count>0)
                    {
                        var dd = vv.Value.Dequeue();
                        ProcessSingleData(vv.Key, dd);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        protected virtual void ProcessSingleData(string client, IByteBuffer data)
        {
            data.ReleaseBuffer();
        }


        public virtual void Start()
        {
            resetEvent = new ManualResetEvent(false);
            mProcessThread = new Thread(DataProcess);
            mProcessThread.IsBackground = true;
            mProcessThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public virtual void Stop()
        {
            mIsClosed = true;
            resetEvent.Set();
            resetEvent.Close();
        }

        /// <summary>
        /// 
        /// </summary>
        public virtual void Dispose()
        {
            Parent = null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public virtual void OnClientDisconnected(string id)
        {

        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
