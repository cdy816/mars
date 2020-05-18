using Cdy.Tag;
using Cdy.Tag.Driver;
using DBRunTime.ServiceApi;
using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DBRuntime.Proxy
{
    public enum NetTransformWorkMode
    {
        //主动获取模式
        Poll,
        //服务器主动推送
        Push
    }

    public class NetTransformDriver : Cdy.Tag.Driver.IProducterDriver
    {

        #region ... Variables  ...
        
        private IRealTagProducter mServier;

        private Queue<IByteBuffer> mCachDatas = new Queue<IByteBuffer>();

        private ManualResetEvent resetEvent;

        private bool mIsClosed = false;

        private Thread mScanThread;



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

        #endregion ...Properties...

        #region ... Methods    ...



        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

        public string Name => "NetTransformDriver";

        public string[] Registors => new string[0];

        protected string ReadString(IByteBuffer buffer)
        {
            return buffer.ReadString(buffer.ReadInt(), Encoding.Unicode);
        }

        /// <summary>
        /// 
        /// </summary>
        private void RemoteDataudpatePro()
        {
            while(!mIsClosed)
            {
                if (WorkMode == NetTransformWorkMode.Push)
                {
                    resetEvent.WaitOne();
                    if (mIsClosed) break;
                    resetEvent.Reset();
                    while (mCachDatas.Count > 0)
                    {
                        ProcessSingleBufferData(mCachDatas.Dequeue());
                    }
                }
                else
                {
                    DateTime stime = DateTime.Now;
                    Client.SyncRealMemory();
                    double span = (DateTime.Now - stime).TotalMilliseconds;
                    int sleeptime = span > PollCircle ? 1 : (int)(PollCircle - span);
                    Thread.Sleep(sleeptime);
                }
            }
        }

        private void ProcessSingleBufferData(IByteBuffer block)
        {
            if (block == null) return;
            var count = block.ReadInt();
            for (int i = 0; i < count; i++)
            {
                var vid = block.ReadInt();
                if(vid<0)
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
                        value = (uint)block.ReadInt();
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
                mServier.SetTagValue(vid, value, time, qua);
            }
            block.Release();
        }


        /// <summary>
        /// 
        /// </summary>
        private void RegistorTag()
        {
            if (Client != null)
            {
                Client.RegistorTagValueCallBack(-1, int.MaxValue, 5000);
                Client.ProcessDataPush = new ApiClient.ProcessDataPushDelegate((block) => {
                    block.Retain();
                    mCachDatas.Enqueue(block);
                    resetEvent.Set();
                });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void ReadAllData()
        {
            if(Client!=null)
            {
                int maxid = ServiceLocator.Locator.Resolve<ITagManager>().MaxTagId();
                int minid = ServiceLocator.Locator.Resolve<ITagManager>().MinTagId();
                int len = maxid - minid;
                int countpercall = 10000;

                int executed = 0;
                while(executed<len)
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
        public bool Start(IRealTagProducter tagQuery)
        {
            mServier = tagQuery;
            resetEvent = new ManualResetEvent(false);
            ServiceLocator.Locator.Resolve<IRealDataNotifyForProducter>().SubscribeProducter("NetTransformDriver", ProcessValueChanged,
                new Func<List<int>>(() => { return new List<int>() { -1 }; })
                );
            
            if (WorkMode == NetTransformWorkMode.Push)
                RegistorTag();

            Client.SyncRealMemory();
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
            foreach(var vv in values)
            {
                var vatg = mServier.GetTagById(vv.Key);
                if(Client!=null)
                {
                    Client.SetTagValue(vv.Key, (byte)vatg.Type, vv.Value,2000);
                }
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Stop()
        {
            mIsClosed = true;
            resetEvent.Set();
            return true;
        }
    }

    public static class ApiClientExtends
    {
        /// <summary>
        /// 
        /// </summary>
        public static void SyncRealMemory(this ApiClient Client)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            var realenginer = (ServiceLocator.Locator.Resolve<IRealTagComsumer>() as RealEnginer);
            if (Client != null && Client.IsConnected)
            {
                int i = 0;
                foreach (var vv in Client.SyncRealMemory(realenginer.Memory.Length))
                {
                    if (vv != null)
                    {
                        int size = vv.ReadableBytes;
                        Buffer.BlockCopy(vv.Array, vv.ArrayOffset + vv.ReaderIndex, realenginer.Memory, i, size);
                        vv.SetReaderIndex(vv.ReaderIndex + size);
                        i += size;
                    }
                    else
                    {
                        break;
                    }
                }
            }
            sw.Stop();
            LoggerService.Service.Info("SyncRealMemory", "数据大小:"+ (realenginer.Memory.Length/1024/1024) + " 耗时: "+ sw.ElapsedMilliseconds);
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
        private static void ProcessSingleBufferDataByMemoryCopy(int sid, int eid, IByteBuffer block)
        {
            if (block == null) return;
            var count = block.ReadInt();
            var tserver = ServiceLocator.Locator.Resolve<ITagManager>();
            var service = ServiceLocator.Locator.Resolve<IRealTagComsumer>();
            int targetbaseoffset = block.ArrayOffset + block.ReaderIndex;
            for (int i = sid; i <= eid; i++)
            {
                var tag = tserver.GetTagById(i);
                Marshal.Copy(block.Array, targetbaseoffset, (service as RealEnginer).MemoryHandle + (int)tag.ValueAddress, tag.ValueSize);
                block.SetReaderIndex(block.ReaderIndex + tag.ValueSize);
                targetbaseoffset = block.ArrayOffset + block.ReaderIndex;
            }
            block.Release();
        }
    }

}
