using Cdy.Tag;
using Cdy.Tag.Driver;
using DBRunTime.ServiceApi;
//using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cheetah;

namespace DBRuntime.Proxy
{
    public enum NetTransformWorkMode
    {
        //主动获取模式
        Poll,
        //服务器主动推送
        Push
    }

    public class NetTransformDriver
    {

        #region ... Variables  ...
        
        private IRealTagProduct mServier;

        private Queue<ByteBuffer> mCachDatas = new Queue<ByteBuffer>();

        private ManualResetEvent resetEvent;

        private bool mIsClosed = false;

        private Thread mScanThread;

        private Dictionary<long, long> mSyncMemoryCach = new Dictionary<long, long>();

        /// <summary>
        /// 
        /// </summary>
        public const byte RealDataBlockPush = 1;
        public const byte RealDataPush = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public ApiClient Client { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public NetTransformWorkMode WorkMode { get; set; }

        /// <summary>
        /// 查询周期
        /// </summary>
        public int PollCircle { get; set; } = 1000;

        public event EventHandler ValueUpdateEvent;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public Action<bool,bool,bool> ReloadDatabaseAction { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="buffer"></param>
        /// <returns></returns>
        protected string ReadString(ByteBuffer buffer)
        {
            //return buffer.ReadString(buffer.ReadInt(), Encoding.Unicode);
            return buffer.ReadString(Encoding.Unicode);
        }

        /// <summary>
        /// 
        /// </summary>
        private void RemoteDataudpatePro()
        {
            while (!mIsClosed)
            {
                if (WorkMode == NetTransformWorkMode.Push)
                {
                    resetEvent.WaitOne();
                    if (mIsClosed) break;
                    resetEvent.Reset();
                    if (mCachDatas != null)
                    {
                        int icount = mCachDatas.Count;
                        while (mCachDatas.Count > 0)
                        {
                            // ProcessSingleBufferData(mCachDatas.Dequeue());
                            ProcessBufferData(mCachDatas.Dequeue());
                        }
                        ValueUpdateEvent?.Invoke(this, null);
                    }
                }
                else
                {
                    DateTime stime = DateTime.Now;
                    Client.SyncRealMemory(mSyncMemoryCach);
                    double span = (DateTime.Now - stime).TotalMilliseconds;
                    int sleeptime = span > PollCircle ? 1 : (int)(PollCircle - span);
                    ValueUpdateEvent?.Invoke(this, null);
                    Thread.Sleep(sleeptime);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void ProcessBufferData(ByteBuffer block)
        {

            if (block == null) return;
            var vtp = block.ReadByte();
            if (vtp == RealDataBlockPush)
            {
                //LoggerService.Service.Info("ProcessBufferData", "block data");
                ProcessBlockBufferData(block);
            }
            else if (vtp == RealDataPush)
            {
                ProcessSingleBufferDataByMemoryCopy(block);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void ProcessBlockBufferData(ByteBuffer block)
        {
            var realenginer = (ServiceLocator.Locator.Resolve<IRealTagConsumer>() as RealEnginer);

            if (realenginer == null || realenginer.Memory == null) return;

            var start = block.ReadInt();
            var size = block.ReadInt();
            //LoggerService.Service.Info("ProcessBlockBufferData", "block start" +start +", size:"+size);

            try
            {
                if (realenginer.Memory!=null &&(start + size < realenginer.Memory.Length))
                {
                    block.CopyTo(realenginer.MemoryHandle, block.ReadIndex, start, size);
                   // Buffer.BlockCopy(block.Array, block.ArrayOffset + block.ReaderIndex, realenginer.Memory, start, size);
                }
                else
                {
                    //内存数据不匹配，需要重新加载数据库
                    //ReloadDatabaseAction?.BeginInvoke(true,true,true,null,null);
                }
               
            }
            catch
            {

            }
            block.ReadIndex += size;
            block.UnlockAndReturn();
            //block.SetReaderIndex(block.ReaderIndex + size);
            //block.ReleaseBuffer();

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void ProcessSingleBufferDataByMemoryCopy(ByteBuffer block)
        {
            var realenginer = (ServiceLocator.Locator.Resolve<IRealTagConsumer>() as RealEnginer);
            var mTagManager = ServiceLocator.Locator.Resolve<ITagManager>();

            var count = block.ReadInt();
            for (int i = 0; i < count; i++)
            {

                var vid = block.ReadInt();
                if (vid < 0)
                {
                    Debug.Print("Invaild value!");
                }
                if (mTagManager != null)
                {
                    var tag = mTagManager.GetTagById(vid);
                    if (tag != null && realenginer.Memory!=null)
                    {
                        try
                        {
                            block.CopyTo(realenginer.MemoryHandle, block.ReadIndex, tag.ValueAddress, tag.ValueSize);
                           // Buffer.BlockCopy(block.Array, block.ArrayOffset + block.ReaderIndex, realenginer.Memory, (int)tag.ValueAddress, tag.ValueSize);
                        }
                        catch
                        {

                        }
                        block.ReadIndex += tag.ValueSize;
                        //block.SetReaderIndex(block.ReaderIndex + tag.ValueSize);
                    }
                }
            }
            block.UnlockAndReturn();
        }

        private void ProcessSingleBufferData(ByteBuffer block)
        {
            if (block == null) return;
            var count = block.ReadInt();
            for (int i = 0; i < count; i++)
            {
                var vid = block.ReadInt();
                if (vid < 0)
                {
                    Debug.Print("Invaild value!");
                }
                var typ = block.ReadByte();
                object value = null;
                switch (typ)
                {
                    case (byte)TagType.Bool:
                        value = block.ReadByte();
                        break;
                    case (byte)TagType.Byte:
                        value = block.ReadByte();
                        break;
                    case (byte)TagType.Short:
                        value = block.ReadShort();
                        break;
                    case (byte)TagType.UShort:
                        value = (ushort)block.ReadShort();
                        break;
                    case (byte)TagType.Int:
                        value = block.ReadInt();
                        break;
                    case (byte)TagType.UInt:
                        value =  Convert.ToUInt32(block.ReadInt());
                        break;
                    case (byte)TagType.Long:
                        value = block.ReadLong();
                        break;
                    case (byte)TagType.ULong:
                        value = (ulong)block.ReadLong();
                        break;
                    case (byte)TagType.Float:
                        value = block.ReadFloat();
                        break;
                    case (byte)TagType.Double:
                        value = block.ReadDouble();
                        break;
                    case (byte)TagType.String:
                        value = ReadString(block);
                        break;
                    case (byte)TagType.DateTime:
                        var tick = block.ReadLong();
                        value = new DateTime(tick);
                        break;
                    case (byte)TagType.IntPoint:
                        value = new IntPointData(block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.UIntPoint:
                        value = new UIntPointData(block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.IntPoint3:
                        value = new IntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.UIntPoint3:
                        value = new UIntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.LongPoint:
                        value = new LongPointData(block.ReadLong(), block.ReadLong());
                        break;
                    case (byte)TagType.ULongPoint:
                        value = new ULongPointData(block.ReadLong(), block.ReadLong());
                        break;
                    case (byte)TagType.LongPoint3:
                        value = new LongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                        break;
                    case (byte)TagType.ULongPoint3:
                        value = new ULongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                        break;
                }
                var time = new DateTime(block.ReadLong());
                var qua = block.ReadByte();
                mServier.SetTagValue(vid,ref value, time, qua);
            }
            block.UnlockAndReturn();
        }


        ///// <summary>
        ///// 
        ///// </summary>
        //private void RegistorTag()
        //{
        //    if (Client != null)
        //    {
        //        Client.RegistorTagValueCallBack(-1, int.MaxValue, 5000);
        //        Client.ProcessDataPush = new ApiClient.ProcessDataPushDelegate((block) => {
        //            block.Retain();
        //            mCachDatas.Enqueue(block);
        //            resetEvent.Set();
        //        });
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        private void RegistorBlockTag()
        {
            if (Client != null)
            {
                Client.RegistorTagBlockValueCallBack();
                Client.ProcessDataPush = new ApiClient.ProcessDataPushDelegate((block) => {
                    try
                    {
                        mCachDatas.Enqueue(block);
                    }
                    catch
                    {

                    }
                    resetEvent.Set();
                });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void ReadAllData()
        {
            if (Client != null)
            {
                int maxid = ServiceLocator.Locator.Resolve<ITagManager>().MaxTagId();
                int minid = ServiceLocator.Locator.Resolve<ITagManager>().MinTagId();
                int len = maxid - minid;
                int countpercall = 10000;

                int executed = 0;
                while (executed < len)
                {
                    var ex = Math.Min(len - executed, countpercall);
                    var res = Client.GetRealData(executed, executed + ex);
                    ProcessSingleBufferData(res);
                    executed += len;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagQuery"></param>
        /// <returns></returns>
        public bool Start(IRealTagProduct tagQuery)
        {
            mIsClosed = false;
            mServier = tagQuery;
            resetEvent = new ManualResetEvent(false);
            ServiceLocator.Locator.Resolve<IRealDataNotifyForProducter>().SubscribeValueChangedForProducter("NetTransformDriver", ProcessValueChanged,
                new Func<List<int>>(() => { return new List<int>() { -1 }; })
                );

            if (WorkMode == NetTransformWorkMode.Push)
            {
                RegistorBlockTag();
            }
            //RegistorTag();


            mSyncMemoryCach.Clear();

            int synccount = 500000;
            int start = 0;
            int end = synccount;
            var addrs = (mServier as RealEnginer).IdAndValueAddress;

            var vcount = addrs.Count;
            long startaddress = 0, endaddress = 0;

            while (start<vcount)
            {
                startaddress = addrs.ElementAt(start).Value;

                if (end >= vcount)
                {
                    end = vcount;
                    endaddress = (mServier as RealEnginer).UsedSize;
                }
                else
                {
                    endaddress = addrs.ElementAt(end).Value;
                }

                mSyncMemoryCach.Add(startaddress, endaddress);

                start = end;
                end += synccount;

            }

            Client.SyncRealMemory(mSyncMemoryCach);
            mScanThread = new Thread(RemoteDataudpatePro);
            mScanThread.IsBackground = true;
            mScanThread.Start();
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        private void ProcessValueChanged(Dictionary<int, object> values)
        {
            List<int> ids = new List<int>(values.Count);
            List<byte> typs = new List<byte>(values.Count);
            List<object> vals = new List<object>();
            foreach (var vv in values)
            {
                var vatg = mServier.GetTagById(vv.Key);
                ids.Add(vv.Key);
                typs.Add((byte)vatg.Type);
                vals.Add(vv.Value);
                //var vatg = mServier.GetTagById(vv.Key);
                //if (Client != null)
                //{
                //    Client.SetTagValue(vv.Key, (byte)vatg.Type, vv.Value, 2000);
                //}
            }

            if(Client!=null && ids.Count>0)
            {
                Client.SetTagValue(ids, typs, vals, 5000);
            }

        }
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Stop()
        {
            ServiceLocator.Locator.Resolve<IRealDataNotifyForProducter>().UnSubscribeValueChangedForProducter("NetTransformDriver");
            mIsClosed = true;
            resetEvent.Set();
            while (mScanThread.IsAlive) Thread.Sleep(1);
            return true;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...


    }

    public static class ApiClientExtends
    {
        /// <summary>
        /// 
        /// </summary>
        public static void SyncRealMemory(this ApiClient Client,Dictionary<long,long> address)
        {
            //Stopwatch sw = new Stopwatch();
            //sw.Start();
            var realenginer = (ServiceLocator.Locator.Resolve<IRealTagConsumer>() as RealEnginer);
            if (Client != null && Client.IsConnected && realenginer.Memory!=null)
            {
                foreach (var vv in  address)
                {
                    try
                    {
                        var res = Client.SyncRealMemory((int)vv.Key, (int)vv.Value);
                        if (res != null)
                        {
                            int size = (int)res.ReadableCount;

                            res.CopyTo(realenginer.MemoryHandle, res.ReadIndex, vv.Key, size);

                            //Buffer.BlockCopy(res.Array, res.ArrayOffset + res.ReaderIndex, realenginer.Memory, (int)vv.Key, size);
                            //res.SetReaderIndex(res.ReaderIndex + size);
                            //res.ReleaseBuffer();


                            res.UnlockAndReturn();
                        }
                        else
                        {
                            break;
                        }
                    }
                    catch
                    {
                        break;
                    }
                }
            }
            //sw.Stop();
            //LoggerService.Service.Info("SyncRealMemory", "数据大小:" + (realenginer.Memory.Length / 1024 / 1024) + " 耗时: " + sw.ElapsedMilliseconds);
        }

        /// <summary>
        /// 
        /// </summary>
        public static void ReadAllDataByMemoryCopy(this ApiClient Client)
        {
            if (Client != null)
            {
                int maxid = ServiceLocator.Locator.Resolve<ITagManager>().MaxTagId();
                int minid = ServiceLocator.Locator.Resolve<ITagManager>().MinTagId();
                int len = maxid - minid;
                int countpercall = 10000;

                int executed = 0;
                while (executed < len)
                {
                    var ex = Math.Min(len - executed, countpercall);
                    var res = Client.GetRealDataByMemoryCopy(executed, executed + ex);
                    ProcessSingleBufferDataByMemoryCopy(executed, executed + ex, res);
                    executed += len;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private static void ProcessSingleBufferDataByMemoryCopy(int sid, int eid, ByteBuffer block)
        {
            if (block == null) return;
            var count = block.ReadInt();
            var tserver = ServiceLocator.Locator.Resolve<ITagManager>();
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            //int targetbaseoffset = block.ArrayOffset + block.ReaderIndex;
            for (int i = sid; i <= eid; i++)
            {
                var tag = tserver.GetTagById(i);

                Marshal.Copy(block.StartMemory, (service as RealEnginer).Memory, (int)tag.ValueAddress, tag.ValueSize);

                block.ReadIndex += tag.ValueSize;

                //Marshal.Copy(block.Array, targetbaseoffset, (service as RealEnginer).MemoryHandle + (int)tag.ValueAddress, tag.ValueSize);
                //block.SetReaderIndex(block.ReaderIndex + tag.ValueSize);
                //targetbaseoffset = block.ArrayOffset + block.ReaderIndex;
            }
            block.UnlockAndReturn();
            //block.ReleaseBuffer();
        }
    }

}
