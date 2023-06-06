//==============================================================
//  Copyright (C) 2022  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/1/2 11:00:57.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using Cheetah;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DirectAccessDriver
{
    public abstract class ServerProcessBase:IDisposable
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        protected Dictionary<string, Queue<ByteBuffer>> mDatasCach = new Dictionary<string, Queue<ByteBuffer>>();

        private Thread mProcessThread;

        private ManualResetEvent resetEvent;

        private bool mIsClosed = false;

        private List<string> mClients = new List<string>();


        private Dictionary<string, ClientProcess> mClientProcess = new Dictionary<string, ClientProcess>();

        //Cdy.Tag.Common.ThreadPool mProcessPools;


        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        public ServerProcessBase()
        {
            //mProcessPools = new Cdy.Tag.Common.ThreadPool();
            //int pcount = Environment.ProcessorCount / 2;
            //pcount=pcount==0?1 : pcount;
            //mProcessPools.ThreadCount = (byte)pcount;
        }
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

        /// <summary>
        /// 
        /// </summary>
        public bool IsPause { get; set; } = false;

        /// <summary>
        /// 
        /// </summary>
        public virtual bool IsEnableMutiThread { get { return true; } }

        #endregion ...Properties...

        #region ... Methods    ...


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        protected ByteBuffer ToByteBuffer(byte id, string value)
        {
            var re = Parent.Allocate(id, value.Length*2+4);
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
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
        /// <param name="cid"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        protected ByteBuffer ToByteBuffer(byte id, byte cid,long value)
        {
            var re = Parent.Allocate(id, 8+1);
            re.Write(cid);
            re.Write(value);
            return re;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="cid"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        protected ByteBuffer ToByteBuffer(byte id, long value,byte value2)
        {
            var re = Parent.Allocate(id, 8 + 1);
            re.Write(value);
            re.Write(value2);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="cid"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        protected ByteBuffer ToByteBuffer(byte id, byte cid, string value)
        {
            var re = Parent.Allocate(id, value.Length * 2 + 4 + 1);
            re.Write(cid);
            re.Write(value);
            return re;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="value2"></param>
        /// <returns></returns>
        protected ByteBuffer ToByteBuffer(byte id, byte value, byte value2)
        {
            var re = Parent.Allocate(id, 2);
            re.Write(value);
            re.Write(value2);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        public virtual void ProcessData(string client, ByteBuffer data)
        {
            //data.IncRef();
            try
            {
                if (IsEnableMutiThread)
                {
                    if (mClientProcess.ContainsKey(client))
                    {
                        mClientProcess[client].AppendDatas(data);
                    }
                    else
                    {
                        var vp = new ClientProcess() { Parent = this, Name = client };
                        lock (mClientProcess)
                        {
                            mClientProcess.Add(client, vp);
                            vp.AppendDatas(data);
                            vp.Start();
                        }
                    }
                }
                else
                {
                    if (mDatasCach.ContainsKey(client))
                    {
                        mDatasCach[client].Enqueue(data);
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
                    CheckDataBusy(client);
                    resetEvent.Set();
                }
            }
            catch
            {

            }
           
        }

        /// <summary>
        /// 
        /// </summary>
        public virtual void CheckDataBusy(string client)
        {

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
            if (IsEnableMutiThread)
            {
                lock (mClientProcess)
                {
                    if (mClientProcess.ContainsKey(client))
                    {
                        var vp = mClientProcess[client];
                        mClientProcess.Remove(client);
                        Task.Run(() =>
                        {
                            vp.Stop();
                        });
                    }
                }
            }
            else
            {
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
        }

        /// <summary>
        /// 
        /// </summary>
        private void DataProcess()
        {
            string sname="";
            Queue<ByteBuffer> datas = null;
            while (!mIsClosed)
            {
                resetEvent.WaitOne();
                if (mIsClosed) return;
                resetEvent.Reset();

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
                            //Stopwatch sw = new Stopwatch();
                            //sw.Start();
                            //Debug.Print("开始实时数据请求:" + FunId +"  " + datas.Count);
                            while (datas.Count > 0)
                            {
                                //mProcessPools.Registor(DataProcess, sname, datas.Dequeue());
                                ProcessSingleData(sname, datas.Dequeue());
                            }
                            //sw.Stop();
                            //Debug.Print("结束实时数据请求:" + sw.ElapsedMilliseconds);
                        }
                    }
                }
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="paramters"></param>
        //private void DataProcess(object[] paramters)
        //{
        //    ProcessSingleData(paramters[0] as string, paramters[1] as ByteBuffer);
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        public virtual void ProcessSingleData(string client, ByteBuffer data)
        {
            data.UnlockAndReturn();
        }

        /// <summary>
        /// 
        /// </summary>
        public virtual void Start()
        {
            //mProcessPools.Name=this.GetType().Name;
            //mProcessPools.Start();
            mIsClosed = false;
            if (IsEnableMutiThread)
            {

            }
            else
            {
                resetEvent = new ManualResetEvent(false);
                mProcessThread = new Thread(DataProcess);
                mProcessThread.IsBackground = true;
                mProcessThread.Start();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public virtual void Stop()
        {
            mIsClosed = true;
            if (IsEnableMutiThread)
            {
                lock(mClientProcess)
                {
                    foreach(var vv in mClientProcess)
                    {
                        vv.Value.Stop();
                    }
                    mClientProcess.Clear();
                }
            }
            else
            {
                resetEvent.Set();
                resetEvent.Close();
            }
            //mProcessPools.Stop();
        }

        /// <summary>
        /// 
        /// </summary>
        public virtual void Dispose()
        {
            Parent = null;
            //mProcessPools.Stop();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public virtual void OnClientConnected(string id)
        {

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

    /// <summary>
    /// 
    /// </summary>
    public class ClientProcess
    {
        private bool mIsClosed = false;

        private ManualResetEvent resetEvent;

        private Task mExecuteTask;

        public ClientProcess()
        {
            resetEvent= new ManualResetEvent(false);
        }

        /// <summary>
            /// 
            /// </summary>
        public ServerProcessBase Parent
        {
            get;
            set;
        }


        public string Name { get; set; }

        public Queue<ByteBuffer> DatasCach { get; set; }=new Queue<ByteBuffer>();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        public void AppendDatas(ByteBuffer data)
        {
            try
            {
                lock (DatasCach)
                {
                    DatasCach.Enqueue(data);
                }
                resetEvent.Set();
            }
            catch(Exception ex)
            {
                LoggerService.Service.Warn($"DirectAccess ", $"{this.Name} {ex.Message} { ex.StackTrace}");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void DataProcess()
        {
            while (!mIsClosed)
            {
                resetEvent.WaitOne();
                if (mIsClosed) return;
                resetEvent.Reset();
                ByteBuffer data;
                while (DatasCach.Count > 0 && !mIsClosed)
                {
                    try
                    {
                        lock (DatasCach)
                        {
                            data = DatasCach.Dequeue();
                        }
                        Parent?.ProcessSingleData(this.Name, data);
                    }
                    catch
                    {

                    }
                    Thread.Sleep(1);
                }
            }
        }

        /// <summary>
        /// 开始
        /// </summary>
        public void Start()
        {
            mIsClosed = false;
            mExecuteTask = Task.Run(() => {
                DataProcess();
            });
        }

        /// <summary>
        /// 停止
        /// </summary>
        public void Stop()
        {
            try
            {
                mIsClosed = true;
                resetEvent.Set();
                if (mExecuteTask != null)
                {
                    mExecuteTask.Wait();
                    mExecuteTask = null;
                }
                resetEvent.Dispose();
                Parent = null;

                if (DatasCach != null)
                {
                    DatasCach.Clear();
                    DatasCach = null;
                }
            }
            catch
            {

            }
        }


    }

}
