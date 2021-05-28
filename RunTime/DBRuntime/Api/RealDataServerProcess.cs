//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/14 11:00:38.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Cdy.Tag;
using Cheetah;

namespace DBRuntime.Api
{
    /// <summary>
    /// 
    /// </summary>
    public class RealDataServerProcess : ServerProcessBase
    {

        #region ... Variables  ...
        /// <summary>
        /// 获取实时值
        /// </summary>
        public const byte RequestRealData = 0;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestRealDataByMemoryCopy = 11;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestRealData2ByMemoryCopy = 12;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestRealData2 = 10;

        /// <summary>
        /// 请求所有数据
        /// </summary>
        public const byte RealMemorySync = 13;


        /// <summary>
        /// 设置实时值
        /// </summary>
        public const byte SetDataValue = 1;

        /// <summary>
        /// 值改变通知
        /// </summary>
        public const byte ValueChangeNotify = 2;

        /// <summary>
        /// 块改变通知
        /// </summary>
        public const byte BlockValueChangeNotify = 4;

        /// <summary>
        /// 清空值改变通知
        /// </summary>
        public const byte ResetValueChangeNotify = 3;

        /// <summary>
        /// 
        /// </summary>
        public const byte RealDataBlockPush = 1;
        public const byte RealDataPush = 0;

        private Dictionary<string,Tuple<HashSet<int>,bool>> mCallBackRegistorIds = new Dictionary<string, Tuple<HashSet<int>, bool>>();

        private Dictionary<string, bool> mBlockCallBackRegistorIds = new Dictionary<string, bool>();

        private Queue<Tuple<int[],int>> mChangedTags = new Queue<Tuple<int[], int>>(10);

        private Queue<BlockItem> mChangedBlocks = new Queue<BlockItem>();

        private Thread mScanThread;

        private ManualResetEvent resetEvent;

        private bool mIsClosed = false;
        private ITagManager mTagManager;
        private IRealTagConsumer mTagConsumer;

        private Dictionary<string, ByteBuffer> buffers = new Dictionary<string, ByteBuffer>();

        private Dictionary<string, int> mDataCounts = new Dictionary<string, int>();
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        public RealDataServerProcess()
        {
            mTagManager = ServiceLocator.Locator.Resolve<ITagManager>();
            mTagConsumer = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            
            ServiceLocator.Locator.Resolve<IRealDataNotify>().SubscribeValueChangedForConsumer("RealDataServerProcess", new ValueChangedNotifyProcesser.ValueChangedDelegate((ids,len) => {
                lock (mChangedTags)
                {
                    int[] vtmp = ArrayPool<int>.Shared.Rent(len);
                    Array.Copy(ids, 0, vtmp, 0, len);
                    mChangedTags.Enqueue(new Tuple<int[], int>(vtmp,len));
                }
                if(!mIsClosed)
                resetEvent.Set();
            }),new ValueChangedNotifyProcesser.BlockChangedDelegate((bids)=> {

                if (mBlockCallBackRegistorIds.Count > 0)
                {
                    lock (mChangedBlocks)
                        mChangedBlocks.Enqueue(bids);
                }
                if (!mIsClosed)
                {
                    resetEvent.Set();
                }
            }), null,RealDataNotifyType.Block);
            
        }

        #endregion ...Constructor...

        #region ... Properties ...

        public override byte FunId => ApiFunConst.RealDataRequestFun;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        protected  override void ProcessSingleData(string client, ByteBuffer data)
        {
            if(data.RefCount==0)
            {
                Debug.Print("invailed data buffer in RealDataServerProcess");
                return;
            }
            byte cmd = data.ReadByte();
            long id = data.ReadLong();
            if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(id))
            {
                switch (cmd)
                {
                    case RequestRealData:
                        ProcessGetRealData(client, data);
                        break;
                    case RequestRealDataByMemoryCopy:
                        ProcessGetRealDataByMemoryCopy(client, data);
                        break;
                    case RequestRealData2:
                        ProcessGetRealData2(client, data);
                        break;
                    case RequestRealData2ByMemoryCopy:
                        ProcessGetRealData2ByMemoryCopy(client, data);
                        break;
                    case RealMemorySync:
                        ProcessRealMemorySync(client, data);
                        break;
                    case SetDataValue:
                        ProcessSetRealData(client, data);
                        break;
                    case ValueChangeNotify:
                        ProcessValueChangeNotify(client, data);
                        break;
                    case BlockValueChangeNotify:
                        BlockProcessValueChangeNotify(client, data);
                        break;
                    case ResetValueChangeNotify:
                        ProcessResetValueChangedNotify(client, data);
                        break;
                }
            }
            else
            {
                Parent.AsyncCallback(client, FunId, new byte[1], 0);
            }
            base.ProcessSingleData(client, data);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessRealMemorySync(string clientId, ByteBuffer block)
        {
            int size = block.ReadInt();
            int start = block.ReadInt();
            var cc = ServiceLocator.Locator.Resolve<IRealTagConsumer>() as RealEnginer;
           
            if (start >= cc.Memory.Length) return;
            var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, 0);
            
            var ob = Parent.MemoryPool.GetBuffer();
            ob.IsPooled = false;
            ob.ReInit(cc.MemoryHandle + start, size);
            ob.WriteIndex = size;
            Parent.AsyncCallback(clientId, ob,re);
        }


        private ByteBuffer[] GetBlockSendBuffer2(BlockItem item)
        {
            int start = item.StartAddress;
            int size = item.EndAddress - start;
            var cc = ServiceLocator.Locator.Resolve<IRealTagConsumer>() as RealEnginer;
            if (start >= cc.Memory.Length) return null;

            //var re = Unpooled.Buffer(10);

            //re.WriteByte(ApiFunConst.RealDataPushFun);
            var re = Parent.Allocate(ApiFunConst.RealDataPushFun,  9);
            re.Write(RealDataBlockPush);
            re.Write(start);
            re.Write(size);
            
            var data = Parent.MemoryPool.GetBuffer();
            data.ReInit(cc.MemoryHandle + start, size);
            data.WriteIndex = size;

            return new ByteBuffer[] { data, re };

            //return Unpooled.CompositeBuffer().AddComponents(true,re, Unpooled.WrappedBuffer(cc.Memory, start, size));
       
           // Buffer.BlockCopy(cc.Memory, start, re.Array, re.ArrayOffset + re.WriterIndex, size);
           // re.SetWriterIndex(re.WriterIndex + size);
           // return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetRealData(string clientid, ByteBuffer block)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            int count = block.ReadInt();
            for (int i = 0; i < count; i++)
            {
                var id = block.ReadInt();
                byte typ = block.ReadByte();
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
                        value = block.ReadString();
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
                service.SetTagValueForConsumer(id, value);
            }
            Parent.AsyncCallback(clientid, ToByteBuffer(ApiFunConst.RealDataSetFun, (byte)1));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cc"></param>
        /// <param name="re"></param>
        private void ProcessRealDataByMemoryCopy(List<int> cc,ByteBuffer re)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            var tagservice = ServiceLocator.Locator.Resolve<ITagManager>();

            re.Write(cc.Count);
            //int targetbaseoffset =  re.ArrayOffset + re.WriterIndex;
            foreach(var vv in cc)
            {
                var tag = tagservice.GetTagById(vv);
                re.Write((service as RealEnginer).MemoryHandle + (int)tag.ValueAddress, tag.ValueSize);

                //Marshal.Copy((service as RealEnginer).MemoryHandle+ (int)tag.ValueAddress, re.Array, targetbaseoffset, tag.ValueSize);
                //re.SetWriterIndex(re.WriterIndex + tag.ValueSize);
                //targetbaseoffset = re.ArrayOffset + re.WriterIndex;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cc"></param>
        /// <param name="re"></param>
        private void ProcessRealData(List<int> cc,ByteBuffer re)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            re.Write(cc.Count);
            foreach (var vv in cc)
            {
                byte qu, type;
                DateTime time;
                object value;
                value = service.GetTagValue(vv, out qu, out time, out type);
                re.Write(vv);
                if (value != null)
                {
                    re.WriteByte(type);
                    switch (type)
                    {
                        case (byte)TagType.Bool:
                            re.Write(Convert.ToByte(value));
                            break;
                        case (byte)TagType.Byte:
                            re.Write(Convert.ToByte(value));
                            break;
                        case (byte)TagType.Short:
                            re.Write(Convert.ToInt16(value));
                            break;
                        case (byte)TagType.UShort:
                            re.Write(Convert.ToUInt16(value));
                            break;
                        case (byte)TagType.Int:
                            re.Write(Convert.ToInt32(value));
                            break;
                        case (byte)TagType.UInt:
                            re.Write(Convert.ToUInt32(value));
                            break;
                        case (byte)TagType.Long:
                        case (byte)TagType.ULong:
                            re.Write(Convert.ToInt64(value));
                            break;
                        case (byte)TagType.Float:
                            re.Write(Convert.ToSingle(value));
                            break;
                        case (byte)TagType.Double:
                            re.Write(Convert.ToDouble(value));
                            break;
                        case (byte)TagType.String:
                            string sval = value.ToString();
                            re.Write(sval);
                            //re.Write(sval.Length);
                            //re.Write(sval, Encoding.Unicode);
                            break;
                        case (byte)TagType.DateTime:
                            re.Write(((DateTime)value).Ticks);
                            break;
                        case (byte)TagType.IntPoint:
                            re.Write(((IntPointData)value).X);
                            re.Write(((IntPointData)value).Y);
                            break;
                        case (byte)TagType.UIntPoint:
                            re.Write((int)((UIntPointData)value).X);
                            re.Write((int)((UIntPointData)value).Y);
                            break;
                        case (byte)TagType.IntPoint3:
                            re.Write(((IntPoint3Data)value).X);
                            re.Write(((IntPoint3Data)value).Y);
                            re.Write(((IntPoint3Data)value).Z);
                            break;
                        case (byte)TagType.UIntPoint3:
                            re.Write((int)((UIntPoint3Data)value).X);
                            re.Write((int)((UIntPoint3Data)value).Y);
                            re.Write((int)((UIntPoint3Data)value).Z);
                            break;
                        case (byte)TagType.LongPoint:
                            re.Write(((LongPointData)value).X);
                            re.Write(((LongPointData)value).Y);
                            break;
                        case (byte)TagType.ULongPoint:
                            re.Write((long)((ULongPointData)value).X);
                            re.Write((long)((ULongPointData)value).Y);
                            break;
                        case (byte)TagType.LongPoint3:
                            re.Write(((LongPoint3Data)value).X);
                            re.Write(((LongPoint3Data)value).Y);
                            re.Write(((LongPoint3Data)value).Z);
                            break;
                        case (byte)TagType.ULongPoint3:
                            re.Write((long)((ULongPoint3Data)value).X);
                            re.Write((long)((ULongPoint3Data)value).Y);
                            re.Write((long)((ULongPoint3Data)value).Z);
                            break;
                    }

                    re.Write(time.Ticks);
                    re.WriteByte(qu);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void ProcessGetRealData(string clientId, ByteBuffer block)
        {
            int count = block.ReadInt();
            List<int> cc = new List<int>(count);
            for (int i = 0; i < count; i++)
            {
                cc.Add(block.ReadInt());
            }

            var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, count * 34);
            ProcessRealData(cc, re);
            Parent.AsyncCallback(clientId, re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void ProcessGetRealDataByMemoryCopy(string clientId, ByteBuffer block)
        {
            int count = block.ReadInt();
            List<int> cc = new List<int>(count);
            for (int i = 0; i < count; i++)
            {
                cc.Add(block.ReadInt());
            }

            var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, count * 34);
            ProcessRealDataByMemoryCopy(cc, re);
            Parent.AsyncCallback(clientId, re);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void ProcessGetRealData2(string clientId, ByteBuffer block)
        {
            int sid = block.ReadInt();
            int eid = block.ReadInt();
            List<int> cc = new List<int>(eid - sid);
            for (int i = sid; i <= eid; i++)
            {
                cc.Add(i);
            }
            var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, cc.Count * 34);
            ProcessRealData(cc, re);
            Parent.AsyncCallback(clientId, re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void ProcessGetRealData2ByMemoryCopy(string clientId, ByteBuffer block)
        {
            int sid = block.ReadInt();
            int eid = block.ReadInt();
            List<int> cc = new List<int>(eid - sid);
            for (int i = sid; i <= eid; i++)
            {
                cc.Add(i);
            }
            var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, cc.Count * 34);
            ProcessRealDataByMemoryCopy(cc, re);
            Parent.AsyncCallback(clientId, re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void BlockProcessValueChangeNotify(string clientId, ByteBuffer block)
        {
            try
            {
                
                if (mBlockCallBackRegistorIds.ContainsKey(clientId))
                {
                    mBlockCallBackRegistorIds[clientId] = true;
                }
                else
                {
                    mBlockCallBackRegistorIds.Add(clientId, true);
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void ProcessValueChangeNotify(string clientId, ByteBuffer block)
        {
            try
            {
                int minid = block.ReadInt();
                int maxid = block.ReadInt();
                bool isall = false;
                HashSet<int> ids = new HashSet<int>();
                if (minid < 0)
                {
                    ids.Add(-1);
                    isall = true;
                }
                else
                {
                    for (int i = minid; i <= maxid; i++)
                    {
                        ids.Add(i);
                    }
                }

                if (mCallBackRegistorIds.ContainsKey(clientId))
                {
                    mCallBackRegistorIds[clientId] = new Tuple<HashSet<int>, bool>( ids,isall);
                }
                else
                {
                    mCallBackRegistorIds.Add(clientId, new Tuple<HashSet<int>, bool>(ids, isall));
                }

                if (!mDataCounts.ContainsKey(clientId))
                {
                    mDataCounts.Add(clientId, 0);
                }
            }
            catch(Exception ex)
            {
                Debug.Print(ex.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void ProcessResetValueChangedNotify(string clientId, ByteBuffer block)
        {
            if (mCallBackRegistorIds.ContainsKey(clientId))
            {
                mCallBackRegistorIds.Remove(clientId);
            }
            if(mDataCounts.ContainsKey(clientId))
            {
                mDataCounts.Remove(clientId);
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="buffer"></param>
        ///// <param name="addr"></param>
        ///// <param name="size"></param>
        //private unsafe void CopyRealValue(IByteBuffer buffer,void* addr,int size,int id)
        //{
        //    buffer.WriteInt(id);
        //    int tagsize = buffer.WriterIndex + size;
        //    if (tagsize > buffer.Capacity)
        //        buffer.AdjustCapacity((int)(tagsize*1.1));

        //    Buffer.MemoryCopy(addr, (void*)(buffer.AddressOfPinnedMemory() + buffer.WriterIndex), size, size);
        //    buffer.SetWriterIndex(tagsize);
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="buffer"></param>
        ///// <param name="id"></param>
        //private unsafe void ProcessTagPushToClient(IByteBuffer buffer,int id)
        //{
        //    var tag = mTagManager.GetTagById(id);
        //    var addr = mTagConsumer.GetDataRawAddr(id);
        //    CopyRealValue(buffer, addr, tag.ValueSize, id);
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="id"></param>
        private void ProcessTagPush(ByteBuffer re,int id)
        {
            byte type,qu;
            DateTime mtime;
            object value = mTagConsumer.GetTagValue(id, out qu, out mtime, out type);
            re.Write(id);
            re.Write(type);
            switch (type)
            {
                case (byte)TagType.Bool:
                    re.Write(Convert.ToByte(value));
                    break;
                case (byte)TagType.Byte:
                    re.Write(Convert.ToByte(value));
                    break;
                case (byte)TagType.Short:
                    re.Write(Convert.ToUInt16(value));
                    break;
                case (byte)TagType.UShort:
                    re.Write(Convert.ToUInt16(value));
                    break;
                case (byte)TagType.Int:
                    re.Write(Convert.ToInt32(value));
                    break;
                case (byte)TagType.UInt:
                    re.Write(Convert.ToUInt32(value));
                    break;
                case (byte)TagType.Long:
                case (byte)TagType.ULong:
                    re.Write(Convert.ToInt64(value));
                    break;
                case (byte)TagType.Float:
                    re.Write(Convert.ToSingle(value));
                    break;
                case (byte)TagType.Double:
                    re.Write(Convert.ToDouble(value));
                    break;
                case (byte)TagType.String:
                    string sval = value.ToString();
                    re.Write(sval);
                    //re.Write(sval.Length);
                    //re.Write(sval, Encoding.Unicode);
                    break;
                case (byte)TagType.DateTime:
                    re.Write(((DateTime)value).Ticks);
                    break;
                case (byte)TagType.IntPoint:
                    re.Write(((IntPointData)value).X);
                    re.Write(((IntPointData)value).Y);
                    break;
                case (byte)TagType.UIntPoint:
                    re.Write((int)((UIntPointData)value).X);
                    re.Write((int)((UIntPointData)value).Y);
                    break;
                case (byte)TagType.IntPoint3:
                    re.Write(((IntPoint3Data)value).X);
                    re.Write(((IntPoint3Data)value).Y);
                    re.Write(((IntPoint3Data)value).Z);
                    break;
                case (byte)TagType.UIntPoint3:
                    re.Write((int)((UIntPoint3Data)value).X);
                    re.Write((int)((UIntPoint3Data)value).Y);
                    re.Write((int)((UIntPoint3Data)value).Z);
                    break;
                case (byte)TagType.LongPoint:
                    re.Write(((LongPointData)value).X);
                    re.Write(((LongPointData)value).Y);
                    break;
                case (byte)TagType.ULongPoint:
                    re.Write((long)((ULongPointData)value).X);
                    re.Write((long)((ULongPointData)value).Y);
                    break;
                case (byte)TagType.LongPoint3:
                    re.Write(((LongPoint3Data)value).X);
                    re.Write(((LongPoint3Data)value).Y);
                    re.Write(((LongPoint3Data)value).Z);
                    break;
                case (byte)TagType.ULongPoint3:
                    re.Write((long)((ULongPoint3Data)value).X);
                    re.Write((long)((ULongPoint3Data)value).Y);
                    re.Write((long)((ULongPoint3Data)value).Z);
                    break;
            }
            re.Write(mtime.Ticks);
            re.WriteByte(qu);
            //Debug.Print(buffer.WriterIndex.ToString());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="re"></param>
        /// <param name="id"></param>
        private void ProcessTagPushByMemoryCopy(ByteBuffer re, int id)
        {
            var tag = mTagManager.GetTagById(id);
            //re.WriteInt(id);
            re.Write(id);
            re.Write((mTagConsumer as RealEnginer).MemoryHandle + (int)tag.ValueAddress, tag.ValueSize);
            //Buffer.BlockCopy((mTagConsumer as RealEnginer).Memory, (int)tag.ValueAddress, re.Array, re.ArrayOffset + re.WriterIndex, tag.ValueSize);
            //re.SetWriterIndex(re.WriterIndex + tag.ValueSize);
        }


        /// <summary>
        /// 
        /// </summary>
        private void SendThreadPro()
        {
            Tuple<int[], int> changtags;
            while (!mIsClosed)
            {
                try
                {
                    resetEvent.WaitOne();
                    if (mIsClosed) return;
                    resetEvent.Reset();

                    while (mChangedTags.Count > 0)
                    {

                        changtags = mChangedTags.Dequeue();

                        if (mCallBackRegistorIds.Count == 0)
                        {
                            ArrayPool<int>.Shared.Return(changtags.Item1);
                            break;
                        }

                        var clients = mCallBackRegistorIds.ToArray();

                        foreach (var cb in clients)
                        {
                            var buffer = Parent.Allocate(Api.ApiFunConst.RealDataPushFun, changtags.Item2 * 64+5);
                            buffer.Write(RealDataPush);
                            buffer.Write(0);
                            buffers.Add(cb.Key, buffer);
                            mDataCounts[cb.Key]= 0;
                        }

                        foreach (var vv in changtags.Item1)
                        {
                            foreach (var vvc in clients)
                            {
                                if (vvc.Value.Item2 || vvc.Value.Item1.Contains(vv))
                                {
                                    ProcessTagPushByMemoryCopy(buffers[vvc.Key], vv);
                                    mDataCounts[vvc.Key]++;
                                }
                            }
                        }

                        foreach (var cb in buffers)
                        {
                            //cb.Value.MarkWriterIndex();
                            //cb.Value.SetWriterIndex(2);

                            var ind = cb.Value.WriteIndex;
                            cb.Value.WriteIndex = 2;
                            cb.Value.Write(mDataCounts[cb.Key]);
                            cb.Value.WriteIndex = ind;
                            //cb.Value.WriteInt(mDataCounts[cb.Key]);
                            //cb.Value.ResetWriterIndex();

                            Parent.PushRealDatatoClient(cb.Key, cb.Value);
                        }
                        buffers.Clear();

                        ArrayPool<int>.Shared.Return(changtags.Item1);
                    }

                    if(mBlockCallBackRegistorIds.Count>0)
                    {
                        //Stopwatch sw = new Stopwatch();
                        //sw.Start();
                        int count = 0;
                        BlockItem vv;
                        while (mChangedBlocks.Count>0)
                        {
                            lock (mChangedBlocks)
                                 vv = mChangedBlocks.Dequeue();
                            if (vv == null) continue;

                            var buffer = GetBlockSendBuffer2(vv);
                            foreach (var vvb in mBlockCallBackRegistorIds.ToArray())
                            {
                              //  buffer.Retain();
                                Parent.PushRealDatatoClient(vvb.Key, buffer);
                                //buffer.ReleaseBuffer();
                            }

                            //while(buffer.ReferenceCount>0)
                            //buffer.ReleaseBuffer();
                            count++;
                        }
                        Thread.Sleep(10);
                        //sw.Stop();
                        //LoggerService.Service.Erro("RealDataServerProcess", "推送数据耗时" + sw.ElapsedMilliseconds + " 大小:" + count);
                    }
                    else
                    {
                        mChangedBlocks.Clear();
                    }
                }
                catch(Exception ex)
                {
                    LoggerService.Service.Erro("RealDataServerProcess", ex.StackTrace);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>

        public override void Start()
        {
           
            base.Start();
            resetEvent = new ManualResetEvent(false);
            mScanThread = new Thread(SendThreadPro);
            mScanThread.IsBackground = true;
            mScanThread.Start();
           
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Stop()
        {
            mIsClosed = true;
            resetEvent.Set();
            resetEvent.Close();
            base.Stop();
        }

        public override void OnClientDisconnected(string id)
        {
            if(mBlockCallBackRegistorIds.ContainsKey(id))
            {
                mBlockCallBackRegistorIds.Remove(id);
            }
            if(mCallBackRegistorIds.ContainsKey(id))
            {
                mCallBackRegistorIds.Remove(id);
            }
            base.OnClientDisconnected(id);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
