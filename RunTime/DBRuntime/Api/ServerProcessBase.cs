//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/14 11:01:03.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using Cheetah;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace DBRuntime.Api
{
    public abstract class ServerProcessBase:IDisposable
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, Queue<ByteBuffer>> mDatasCach = new Dictionary<string, Queue<ByteBuffer>>();

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
        public DataService Parent { get; set; }



        #endregion ...Properties...

        #region ... Methods    ...


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        protected ByteBuffer ToByteBuffer(byte id, string value)
        {
            var re = Parent.Allocate(id, value.Length*2);
            re.Write(value);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        protected ByteBuffer ToByteBuffer(byte id, byte value)
        {
            var re = Parent.Allocate(id, 1);
            re.Write(value);
            return re;
        }

        protected ByteBuffer ToByteBuffer(byte id, byte value,byte value2)
        {
            var re = Parent.Allocate(id, 2);
            re.WriteByte(value);
            re.WriteByte(value2);
            return re;
        }

        protected ByteBuffer ToByteBuffer(byte id, long value)
        {
            var re = Parent.Allocate(id, 8);
            re.Write(value);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="stream"></param>
        /// <returns></returns>
        protected ByteBuffer ToByteBuffer(byte id,byte[] values)
        {
            var re = Parent.Allocate(id, (int)values.Length);
            re.Write(values, 0, (int)values.Length);
            return re;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        public virtual void ProcessData(string client, ByteBuffer data)
        {
            data.IncRef();
            if (mDatasCach.ContainsKey(client))
            {
                mDatasCach[client].Enqueue(data);
            }
            else
            {
                var vq = new Queue<ByteBuffer>();
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
                try
                {
                    foreach (var vv in mDatasCach)
                    {
                        while (vv.Value.Count > 0)
                        {
                            var dd = vv.Value.Dequeue();
                            if (dd != null)
                                ProcessSingleData(vv.Key, dd);
                        }
                    }
                }
                catch
                {

                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        protected virtual void ProcessSingleData(string client, ByteBuffer data)
        {
            data.UnlockAndReturn();
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
