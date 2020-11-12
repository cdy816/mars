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
using Cdy.Tag.Common;
using Cdy.Tag.Driver;
using DotNetty.Buffers;

namespace SpiderDriver
{
    /// <summary>
    /// 
    /// </summary>
    public class RealDataServerProcess : ServerProcessBase
    {

        #region ... Variables  ...
        

        private Dictionary<string, IdBuffer> mCallBackRegistorIds = new Dictionary<string, IdBuffer>();


        private Queue<KeyValuePair<int,object>> mChangedTags = new Queue<KeyValuePair<int, object>>(10);


        private Thread mScanThread;

        private ManualResetEvent resetEvent;

        private bool mIsClosed = false;

        private Dictionary<string, IByteBuffer> buffers = new Dictionary<string, IByteBuffer>();

        private Dictionary<string, int> mDataCounts = new Dictionary<string, int>();

        private ITagManager mTagManager;

        /// <summary>
        /// 
        /// </summary>
        public static HashSet<int> AllowTagIds = new HashSet<int>();


        private string mName;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public RealDataServerProcess()
        {
            mTagManager = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.ITagManager>();
        }

        #endregion ...Constructor...

        #region ... Properties ...

        public override byte FunId => APIConst.RealValueFun;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            mName = "SpiderDriver" + Guid.NewGuid().ToString();
            ServiceLocator.Locator.Resolve<IRealTagProduct>().SubscribeValueChangedForProducter(mName,
           new ProducterValueChangedNotifyProcesser.ValueChangedDelagete((vals) =>
           {
               lock (mChangedTags)
               {
                   foreach (var vv in vals)
                   {
                       mChangedTags.Enqueue(vv);
                   }
               }
               //mChangedTags.Enqueue(vals);
               resetEvent.Set();
           }), () => { return new List<int>() { -1 }; });
        }

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
            long id = data.ReadLong();
            if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(id))
            {
                switch (cmd)
                {
                    case APIConst.SetTagValueFun:
                        ProcessSetRealData(client, data);
                        break;
                    case APIConst.SetTagValueAndQualityFun:
                        ProcessSetRealDataAndQuality(client, data);
                        break;
                    case APIConst.RegistorTag:
                        ProcessValueChangeNotify(client, data);
                        break;
                    case APIConst.RemoveRegistorTag:
                        ProcessRemoveValueChangeNotify(client, data);
                        break;
                    case APIConst.ClearRegistorTag:
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
            var service = ServiceLocator.Locator.Resolve<IRealTagProduct>();
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
                if(AllowTagIds.Contains(id))
               service.SetTagValue(id, value);
            }
            service.SubmiteNotifyChanged();
            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.RealValueFun, (byte)1));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetRealDataAndQuality(string clientid, IByteBuffer block)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagProduct>();
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
                var qua = block.ReadByte();

                if (AllowTagIds.Contains(id))
                    service.SetTagValue(id, value,qua);
            }
            service.SubmiteNotifyChanged();
            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.RealValueFun, (byte)1));
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void ProcessRemoveValueChangeNotify(string clientId, IByteBuffer block)
        {
            try
            {
                if (mCallBackRegistorIds.ContainsKey(clientId))
                {
                    var itmp = mCallBackRegistorIds[clientId];
                    int minid = block.ReadInt();
                    for (int i = 0; i < minid; i++)
                    {
                        var ival = block.ReadInt();
                        itmp.ClearId(ival);
                    }
                }
                Parent.AsyncCallback(clientId, ToByteBuffer(APIConst.RealValueFun, (byte)1));
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
        private void ProcessValueChangeNotify(string clientId, IByteBuffer block)
        {
            try
            {
                int minid = block.ReadInt();
                
                for (int i = 0; i < minid; i++)
                {
                    var vv = block.ReadInt();
                    if (AllowTagIds.Contains(vv))
                    {
                        if (mCallBackRegistorIds.ContainsKey(clientId))
                        {
                            var itmp = mCallBackRegistorIds[clientId];
                            itmp.SetId(vv);
                        }
                        else
                        {
                            IdBuffer ids = new IdBuffer();
                            mCallBackRegistorIds.Add(clientId, ids);
                            ids.SetId(vv);
                        }
                    }
                }

                

                if (!mDataCounts.ContainsKey(clientId))
                {
                    mDataCounts.Add(clientId, 0);
                }

                Parent.AsyncCallback(clientId, ToByteBuffer(APIConst.RealValueFun, (byte)1));
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
            try
            {
                if (mCallBackRegistorIds.ContainsKey(clientId))
                {
                    mCallBackRegistorIds.Remove(clientId);
                }
                if (mDataCounts.ContainsKey(clientId))
                {
                    mDataCounts.Remove(clientId);
                }
            }
            catch
            {

            }
            Parent.AsyncCallback(clientId, ToByteBuffer(APIConst.RealValueFun, (byte)1));
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="id"></param>
        private void ProcessTagPush(IByteBuffer re,int id,byte type,object value)
        {
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
        }



        /// <summary>
        /// 
        /// </summary>
        private void SendThreadPro()
        {
            while(!mIsClosed)
            {
                try
                {
                    resetEvent.WaitOne();
                    if (mIsClosed) return;
                    resetEvent.Reset();

                    HashSet<int> hval = new HashSet<int>();

                    List<KeyValuePair<int, object>>  changtags = new List<KeyValuePair<int, object>>();

                    lock (mChangedTags)
                    {
                        changtags.AddRange(mChangedTags);
                        mChangedTags.Clear();
                    }

                    if (mCallBackRegistorIds.Count == 0) break;

                    var clients = mCallBackRegistorIds.ToArray();
                    foreach (var cb in clients)
                    {
                        var buffer = BufferManager.Manager.Allocate(APIConst.PushDataChangedFun, changtags.Count * 64 + 5);
                        buffer.WriteInt(0);
                        buffers.Add(cb.Key, buffer);
                        mDataCounts[cb.Key] = 0;
                    }

                    foreach (var vv in changtags)
                    {
                        foreach (var vvc in clients)
                        {
                            try
                            {
                                if (vvc.Value.CheckId(vv.Key))
                                {
                                    byte tp = (byte)mTagManager.GetTagById(vv.Key).Type;
                                    ProcessTagPush(buffers[vvc.Key], vv.Key, tp, vv.Value);
                                    mDataCounts[vvc.Key]++;
                                }
                            }
                            catch (Exception ex)
                            {
                                LoggerService.Service.Erro("SpiderDriver", "RealDataService " + ex.Message);
                            }
                        }
                    }

                    foreach (var cb in buffers)
                    {
                        if (cb.Value.WriterIndex > 6)
                        {
                            cb.Value.MarkWriterIndex();
                            cb.Value.SetWriterIndex(1);
                            cb.Value.WriteInt(mDataCounts[cb.Key]);
                            cb.Value.ResetWriterIndex();
                        }
                        Parent.PushRealDatatoClient(cb.Key, cb.Value);
                    }
                    buffers.Clear();
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public override void OnClientConnected(string id)
        {
            if(!mCallBackRegistorIds.ContainsKey(id))
            {
                mCallBackRegistorIds.Add(id,new IdBuffer());
            }
            base.OnClientConnected(id);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public override void OnClientDisconnected(string id)
        {
            if(mCallBackRegistorIds.ContainsKey(id))
            {
                mCallBackRegistorIds.Remove(id);
            }
            base.OnClientDisconnected(id);
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Dispose()
        {
            ServiceLocator.Locator.Resolve<IRealTagProduct>().UnSubscribeValueChangedForProducter(mName);
            this.mTagManager = null;
            base.Dispose();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
