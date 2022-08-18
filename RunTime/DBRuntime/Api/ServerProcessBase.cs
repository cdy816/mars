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
using System.Linq;

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

        private List<string> mClients = new List<string>();

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
            if (Parent != null)
            {
                var re = Parent.Allocate(id, (int)values.Length);
                re.Write(values, 0, (int)values.Length);
                return re;
            }
            return null;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        public virtual void ProcessData(string client, ByteBuffer data)
        {
            if (mDatasCach.ContainsKey(client))
            {
                lock (mDatasCach)
                {
                    mDatasCach[client].Enqueue(data);
                   
                }
            }
            else
            {
                var vq = new Queue<ByteBuffer>();
                vq.Enqueue(data);

                lock (mDatasCach)
                    mDatasCach.Add(client, vq);

                lock (mClients)
                    mClients.Add(client);
            }
            resetEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>
        private void DataProcess()
        {
            //KeyValuePair<string, Queue<ByteBuffer>>[] vvv;
            ByteBuffer bb;

            string sname = "";
            Queue<ByteBuffer> datas = null;

            while (!mIsClosed)
            {
                resetEvent.WaitOne();
                if (mIsClosed) return;
                resetEvent.Reset();
                try
                {
                    for (int i = 0; i < mClients.Count; i++)
                    {
                        sname = "";
                        datas = null;
                        lock (mClients)
                        {
                            if (i < mClients.Count)
                            {
                                sname = mClients[i];
                            }
                        }

                        if (!string.IsNullOrEmpty(sname))
                        {
                            lock (mDatasCach)
                            {
                                if (mDatasCach.ContainsKey(sname))
                                {
                                    datas = mDatasCach[sname];
                                }
                            }
                            if (datas != null)
                            {
                                if (datas.Count > 5)
                                {
                                    LoggerService.Service.Warn("ServerProcess", $"{sname} {this.FunId} 数据积压 {datas.Count}");
                                }

                                while (datas.Count > 0)
                                {
                                    lock (mDatasCach)
                                        bb = datas.Dequeue();
                                    ProcessSingleData(sname, bb);
                                }
                            }
                        }
                    }
                    //lock(mDatasCach)
                    //{
                    //    vvv = mDatasCach.ToArray();
                    //}
                    //foreach (var vv in mDatasCach.ToArray())
                    //{
                    //    while (vv.Value.Count > 0)
                    //    {
                    //        lock (mDatasCach)
                    //        {
                    //            bb = vv.Value.Dequeue();
                    //        }
                    //        if (bb != null)
                    //            ProcessSingleData(vv.Key, bb);
                    //    }
                    //}
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
        public void RemoveClient(string client)
        {
            lock (mClients)
            {
                if (mClients.Contains(client))
                {
                    mClients.Remove(client);
                }
            }

            lock (mDatasCach)
            {
                if (mDatasCach.ContainsKey(client))
                {
                    foreach (var vv in mDatasCach[client])
                    {
                        vv.UnlockAndReturn();
                    }
                    mDatasCach.Remove(client);
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
            if (resetEvent != null)
            {
                resetEvent.Set();
                resetEvent.Close();
            }
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
            RemoveClient(id);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
