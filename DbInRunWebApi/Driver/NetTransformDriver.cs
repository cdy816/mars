using Cdy.Tag;
using Cdy.Tag.Driver;
using DBRunTime.ServiceApi;
using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DbWebApiProxy.Driver
{
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
        public DbClient Client { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...



        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

        public string Name => "NetTransformDriver";

        public string[] Registors => new string[0];

        protected string ReadString(IByteBuffer buffer)
        {
            return buffer.ReadString(buffer.ReadInt(), Encoding.UTF8);
        }

        /// <summary>
        /// 
        /// </summary>
        private void RemoteDataudpatePro()
        {
            while(!mIsClosed)
            {
                resetEvent.WaitOne();
                if (mIsClosed) break;
                resetEvent.Reset();
                while(mCachDatas.Count>0)
                {
                    ProcessSingleBufferData(mCachDatas.Dequeue());
                }
            }
        }

        private void ProcessSingleBufferData(IByteBuffer block)
        {
            var count = block.ReadInt();
            for (int i = 0; i < count; i++)
            {
                var vid = block.ReadInt();
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
                mServier.SetTagValue(vid, value);
            }
            block.Release();
        }

        private void RegistorTag()
        {
            if (Client != null)
            {
                Client.RegistorTagValueCallBack(-1, int.MaxValue, 5000);
                Client.ProcessDataPush = new DbClient.ProcessDataPushDelegate((block) => {
                    //block.re
                    block.Retain();
                    mCachDatas.Enqueue(block);
                });
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
            RegistorTag();
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
            throw new NotImplementedException();
        }
    }
}
