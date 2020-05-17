﻿//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/14 11:00:38.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Cdy.Tag;
using DotNetty.Buffers;

namespace DBRuntime.Api
{
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
        public const byte RequestRealData2 = 10;

        /// <summary>
        /// 设置实时值
        /// </summary>
        public const byte SetDataValue = 1;

        /// <summary>
        /// 值改变通知
        /// </summary>
        public const byte ValueChangeNotify = 2;

        /// <summary>
        /// 清空值改变通知
        /// </summary>
        public const byte ResetValueChangeNotify = 3;

        private Dictionary<string,Tuple<HashSet<int>,bool>> mCallBackRegistorIds = new Dictionary<string, Tuple<HashSet<int>, bool>>();

        private SortedDictionary<int, bool> mChangedTags = new SortedDictionary<int, bool>();

        private Thread mScanThread;

        private ManualResetEvent resetEvent;

        private bool mIsClosed = false;
        private ITagManager mTagManager;
        private IRealTagComsumer mTagConsumer;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        public RealDataServerProcess()
        {
            ServiceLocator.Locator.Resolve<IRealDataNotify>().SubscribeConsumer("RealDataServerProcess", new ValueChangedNotifyProcesser.ValueChangedDelagete((ids) => {
                foreach (var vv in ids)
                {
                    if (mChangedTags.ContainsKey(vv))
                    {
                        mChangedTags[vv] = true;
                    }
                    else
                    {
                        mChangedTags.Add(vv, true);
                    }
                }
                if(!mIsClosed)
                resetEvent.Set();
            }), new Func<List<int>>(() => { return new List<int>() { -1 }; }));
            mTagManager = ServiceLocator.Locator.Resolve<ITagManager>();
            mTagConsumer = ServiceLocator.Locator.Resolve<IRealTagComsumer>();
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
        protected  override void ProcessSingleData(string client, IByteBuffer data)
        {
            if(data.ReferenceCount==0)
            {
                Debug.Print("invailed data buffer in RealDataServerProcess");
                return;
            }
            byte cmd = data.ReadByte();
            string id = data.ReadString();
            if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(id))
            {
                switch (cmd)
                {
                    case RequestRealData:
                        ProcessGetRealData(client, data);
                        break;
                    case RequestRealData2:
                        ProcessGetRealData2(client, data);
                        break;
                    case SetDataValue:
                        ProcessSetRealData(client, data);
                        break;
                    case ValueChangeNotify:
                        ProcessValueChangeNotify(client, data);
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
        private void ProcessSetRealData(string clientid, IByteBuffer block)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagComsumer>();
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
            Parent.AsyncCallback(clientid, ToByteBuffer(ApiFunConst.RealDataRequestFun, (byte)1));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cc"></param>
        /// <param name="re"></param>
        private void ProcessRealData(List<int> cc,IByteBuffer re)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagComsumer>();
            re.WriteInt(cc.Count);
            foreach (var vv in cc)
            {
                byte qu, type;
                DateTime time;
                object value;
                value = service.GetTagValue(vv, out qu, out time, out type);
                re.WriteInt(vv);
                if (value != null)
                {
                    re.WriteByte(type);
                    switch (type)
                    {
                        case (byte)TagType.Bool:
                            re.WriteByte((byte)value);
                            break;
                        case (byte)TagType.Byte:
                            re.WriteByte((byte)value);
                            break;
                        case (byte)TagType.Short:
                            re.WriteShort((short)value);
                            break;
                        case (byte)TagType.UShort:
                            re.WriteUnsignedShort((ushort)value);
                            break;
                        case (byte)TagType.Int:
                            re.WriteInt((int)value);
                            break;
                        case (byte)TagType.UInt:
                            re.WriteInt((int)value);
                            break;
                        case (byte)TagType.Long:
                        case (byte)TagType.ULong:
                            re.WriteLong((long)value);
                            break;
                        case (byte)TagType.Float:
                            re.WriteFloat((float)value);
                            break;
                        case (byte)TagType.Double:
                            re.WriteDouble((double)value);
                            break;
                        case (byte)TagType.String:
                            string sval = value.ToString();
                            re.WriteInt(sval.Length);
                            re.WriteString(sval, Encoding.Unicode);
                            break;
                        case (byte)TagType.DateTime:
                            re.WriteLong(((DateTime)value).Ticks);
                            break;
                        case (byte)TagType.IntPoint:
                            re.WriteInt(((IntPointData)value).X);
                            re.WriteInt(((IntPointData)value).Y);
                            break;
                        case (byte)TagType.UIntPoint:
                            re.WriteInt((int)((UIntPointData)value).X);
                            re.WriteInt((int)((UIntPointData)value).Y);
                            break;
                        case (byte)TagType.IntPoint3:
                            re.WriteInt(((IntPoint3Data)value).X);
                            re.WriteInt(((IntPoint3Data)value).Y);
                            re.WriteInt(((IntPoint3Data)value).Z);
                            break;
                        case (byte)TagType.UIntPoint3:
                            re.WriteInt((int)((UIntPoint3Data)value).X);
                            re.WriteInt((int)((UIntPoint3Data)value).Y);
                            re.WriteInt((int)((UIntPoint3Data)value).Z);
                            break;
                        case (byte)TagType.LongPoint:
                            re.WriteLong(((LongPointData)value).X);
                            re.WriteLong(((LongPointData)value).Y);
                            break;
                        case (byte)TagType.ULongPoint:
                            re.WriteLong((long)((ULongPointData)value).X);
                            re.WriteLong((long)((ULongPointData)value).Y);
                            break;
                        case (byte)TagType.LongPoint3:
                            re.WriteLong(((LongPoint3Data)value).X);
                            re.WriteLong(((LongPoint3Data)value).Y);
                            re.WriteLong(((LongPoint3Data)value).Z);
                            break;
                        case (byte)TagType.ULongPoint3:
                            re.WriteLong((long)((ULongPoint3Data)value).X);
                            re.WriteLong((long)((ULongPoint3Data)value).Y);
                            re.WriteLong((long)((ULongPoint3Data)value).Z);
                            break;
                    }

                    re.WriteLong(time.Ticks);
                    re.WriteByte(qu);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void ProcessGetRealData(string clientId, IByteBuffer block)
        {
            int count = block.ReadInt();
            List<int> cc = new List<int>(count);
            for (int i = 0; i < count; i++)
            {
                cc.Add(block.ReadInt());
            }

            var re = BufferManager.Manager.Allocate(ApiFunConst.RealDataRequestFun, count * 34);
            ProcessRealData(cc, re);
            Parent.AsyncCallback(clientId, re);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void ProcessGetRealData2(string clientId, IByteBuffer block)
        {
            int sid = block.ReadInt();
            int eid = block.ReadInt();
            List<int> cc = new List<int>(eid - sid);
            for (int i = sid; i <= eid; i++)
            {
                cc.Add(i);
            }
            var re = BufferManager.Manager.Allocate(ApiFunConst.RealDataRequestFun, cc.Count * 34);
            ProcessRealData(cc, re);
            Parent.AsyncCallback(clientId, re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void ProcessValueChangeNotify(string clientId, IByteBuffer block)
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
        private void ProcessResetValueChangedNotify(string clientId, IByteBuffer block)
        {
            if (mCallBackRegistorIds.ContainsKey(clientId))
            {
                mCallBackRegistorIds.Remove(clientId);
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
        private void ProcessTagPush(IByteBuffer re,int id)
        {
            byte type,qu;
            DateTime mtime;
            object value = mTagConsumer.GetTagValue(id, out qu, out mtime, out type);
            re.WriteInt(id);
            re.WriteByte(type);
            switch (type)
            {
                case (byte)TagType.Bool:
                    re.WriteByte((byte)value);
                    break;
                case (byte)TagType.Byte:
                    re.WriteByte((byte)value);
                    break;
                case (byte)TagType.Short:
                    re.WriteShort((short)value);
                    break;
                case (byte)TagType.UShort:
                    re.WriteUnsignedShort((ushort)value);
                    break;
                case (byte)TagType.Int:
                    re.WriteInt((int)value);
                    break;
                case (byte)TagType.UInt:
                    re.WriteInt((int)value);
                    break;
                case (byte)TagType.Long:
                case (byte)TagType.ULong:
                    re.WriteLong((long)value);
                    break;
                case (byte)TagType.Float:
                    re.WriteFloat((float)value);
                    break;
                case (byte)TagType.Double:
                    re.WriteDouble((double)value);
                    break;
                case (byte)TagType.String:
                    string sval = value.ToString();
                    re.WriteInt(sval.Length);
                    re.WriteString(sval, Encoding.Unicode);
                    break;
                case (byte)TagType.DateTime:
                    re.WriteLong(((DateTime)value).Ticks);
                    break;
                case (byte)TagType.IntPoint:
                    re.WriteInt(((IntPointData)value).X);
                    re.WriteInt(((IntPointData)value).Y);
                    break;
                case (byte)TagType.UIntPoint:
                    re.WriteInt((int)((UIntPointData)value).X);
                    re.WriteInt((int)((UIntPointData)value).Y);
                    break;
                case (byte)TagType.IntPoint3:
                    re.WriteInt(((IntPoint3Data)value).X);
                    re.WriteInt(((IntPoint3Data)value).Y);
                    re.WriteInt(((IntPoint3Data)value).Z);
                    break;
                case (byte)TagType.UIntPoint3:
                    re.WriteInt((int)((UIntPoint3Data)value).X);
                    re.WriteInt((int)((UIntPoint3Data)value).Y);
                    re.WriteInt((int)((UIntPoint3Data)value).Z);
                    break;
                case (byte)TagType.LongPoint:
                    re.WriteLong(((LongPointData)value).X);
                    re.WriteLong(((LongPointData)value).Y);
                    break;
                case (byte)TagType.ULongPoint:
                    re.WriteLong((long)((ULongPointData)value).X);
                    re.WriteLong((long)((ULongPointData)value).Y);
                    break;
                case (byte)TagType.LongPoint3:
                    re.WriteLong(((LongPoint3Data)value).X);
                    re.WriteLong(((LongPoint3Data)value).Y);
                    re.WriteLong(((LongPoint3Data)value).Z);
                    break;
                case (byte)TagType.ULongPoint3:
                    re.WriteLong((long)((ULongPoint3Data)value).X);
                    re.WriteLong((long)((ULongPoint3Data)value).Y);
                    re.WriteLong((long)((ULongPoint3Data)value).Z);
                    break;
            }
            re.WriteLong(mtime.Ticks);
            re.WriteByte(qu);
            //Debug.Print(buffer.WriterIndex.ToString());
        }
        private Dictionary<string, IByteBuffer> buffers = new Dictionary<string, IByteBuffer>();

        private Dictionary<string, int> mDataCounts = new Dictionary<string, int>();

        /// <summary>
        /// 
        /// </summary>
        private void SendThreadPro()
        {
            while(!mIsClosed)
            {
                resetEvent.WaitOne();
                if (mIsClosed) return;
                resetEvent.Reset();
              
                HashSet<int> hval = new HashSet<int>();

                foreach (var cb in mCallBackRegistorIds)
                {
                    var buffer = BufferManager.Manager.Allocate(Api.ApiFunConst.RealDataPushFun, mChangedTags.Count * 64);
                    buffer.WriteInt(0);
                    buffers.Add(cb.Key, buffer);
                    mDataCounts.Add(cb.Key, 0);
                }

                foreach (var vv in mChangedTags.ToList())
                {
                    if (vv.Value)
                    {
                        foreach (var vvc in mCallBackRegistorIds)
                        {
                            if (vvc.Value.Item2 || vvc.Value.Item1.Contains(vv.Key))
                            {
                                ProcessTagPush(buffers[vvc.Key], vv.Key);
                                mDataCounts[vvc.Key]++;
                            }
                        }
                        mChangedTags[vv.Key] = false;
                    }
                   
                }

                foreach (var cb in buffers)
                {
                    cb.Value.MarkWriterIndex();
                    cb.Value.SetWriterIndex(1);
                    cb.Value.WriteInt(mDataCounts[cb.Key]);
                    cb.Value.ResetWriterIndex();

                    Parent.PushRealDatatoClient(cb.Key, cb.Value);
                }
                mDataCounts.Clear();
                buffers.Clear();
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

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
