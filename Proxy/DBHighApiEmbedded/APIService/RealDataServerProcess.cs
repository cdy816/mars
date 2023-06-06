﻿//==============================================================
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
using System.Reflection.PortableExecutable;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Cdy.Tag;
using Cheetah;
//using DotNetty.Buffers;
using Microsoft.VisualBasic;

namespace DBHighApi.Api
{
    /// <summary>
    /// 
    /// </summary>
    public class MonitorTag
    {
        /// <summary>
        /// 
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public byte Quality { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime Time { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public TagType Type { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsValueChanged { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void CheckValueChaned(object value,byte qu,DateTime time)
        {
            bool ischanged = this.Time != time || Quality != qu;

            if (value == null)
            {
                if (Value == null)
                {
                    return;
                }
                else
                {
                    Value = value;
                    IsValueChanged = true;
                }
            }
            else
            {
                if (!value.Equals(Value) || ischanged)
                {
                    Value = value;
                    IsValueChanged = true;
                }
            }
            Quality = qu;
            this.Time = time;
        }

        public void IncRef()
        {
            RefCount++;
        }

        public void DecRef()
        {
            RefCount--;
        }

        /// <summary>
        /// 
        /// </summary>
        public int RefCount { get; set; }
    }

    public class RealDataServerProcess : ServerProcessBase
    {

        #region ... Variables  ...

        /// <summary>
        /// 获取实时值
        /// </summary>
        public const byte RequestRealData = 0;

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

        /// <summary>
        /// 块改变通知
        /// </summary>
        public const byte BlockValueChangeNotify = 4;


        /// <summary>
        /// 
        /// </summary>
        public const byte RequestRealDataValue = 7;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestRealDataValueAndQuality = 8;


        /// <summary>
        /// 
        /// </summary>
        public const byte RequestRealData2 = 10;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestReal2DataValue = 17;

        /// <summary>
        /// 
        /// </summary>

        public const byte RequestReal2DataValueAndQuality = 18;

        /// <summary>
        /// 请求所有数据
        /// </summary>
        public const byte RealMemorySync = 13;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestComplexTagRealValue = 14;


        /// <summary>
        /// 
        /// </summary>
        public const byte ValueChangeNotify2 = 22;

        /// <summary>
        /// 按变量ID清空值改变通知2
        /// </summary>
        public const byte ResetValueChangeNotify2 = 23;

        /// <summary>
        /// 
        /// </summary>
        public const byte RealDataBlockPush = 1;

        public const byte RealDataPush = 0;

        public const byte SetRealDataToLastData = 120;

        public const byte SetTagValueCallBack = 30;


        /// <summary>
        /// 请求变量状态数据
        /// </summary>
        public const byte RequestStateData = 26;

        /// <summary>
        /// 请求变量扩展字段
        /// </summary>
        public const byte RequestExtendField2 = 27;

        /// <summary>
        /// 设置状态数据
        /// </summary>
        public const byte SetStateData = 28;

        /// <summary>
        /// 设置扩展数据
        /// </summary>
        public const byte SetExtendField2Data = 29;



        private Dictionary<string, HashSet<int>> mCallBackRegistorIds = new Dictionary<string, HashSet<int>>();

        private Dictionary<string, bool> mBlockCallBackRegistorIds = new Dictionary<string, bool>();

        private Dictionary<int, MonitorTag> mMonitors = new Dictionary<int, MonitorTag>();

        private Dictionary<int, MonitorTag> mtmp = new Dictionary<int, MonitorTag>();

        private Thread mScanThread;

        private ManualResetEvent resetEvent;

        private bool mIsClosed = false;
        //private ITagManager mTagManager;
        private IRealTagConsumer mTagConsumer;

        private Dictionary<string, ByteBuffer> buffers = new Dictionary<string, ByteBuffer>();

        private Dictionary<string, int> mDataCounts = new Dictionary<string, int>();


        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        public RealDataServerProcess()
        {
            //mTagManager = ServiceLocator.Locator.Resolve<ITagManager>();
            mTagConsumer = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            ServiceLocator.Locator.Resolve<IRealDataNotify>().SubscribeValueChangedForConsumer("HighRealDataServerProcess", new ValueChangedNotifyProcesser.ValueChangedDelegate((ids, len) => {
                //if (!mIsClosed)
                //    resetEvent.Set();
            }), new ValueChangedNotifyProcesser.BlockChangedDelegate((bids) => {

                if (!mIsClosed)
                {
                    resetEvent.Set();
                }

            }), null, RealDataNotifyType.Block);
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

            if (Parent != null)
            {
                if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(id))
                {
                    switch (cmd)
                    {
                        case RequestRealData:
                            ProcessGetRealData(client, data);
                            break;
                        case RequestComplexTagRealValue:
                            ProcessGetComplexTagRealValue(client, data);
                            break;
                        case RequestRealDataValue:
                            ProcessGetRealDataValue(client, data);
                            break;
                        case RequestRealDataValueAndQuality:
                            ProcessGetRealDataValueAndQuality(client, data);
                            break;
                        case RequestRealData2:
                            ProcessGetRealData2(client, data);
                            break;
                        case RequestReal2DataValue:
                            ProcessGetRealData2Value(client, data);
                            break;
                        case RequestReal2DataValueAndQuality:
                            ProcessGetRealData2ValueAndQuality(client, data);
                            break;
                        case RequestStateData:
                            ProcessGetStateData(client, data);
                            break;
                        case RequestExtendField2:
                            ProcessGetExtendField2Data(client, data);
                            break;
                        case SetDataValue:
                            ProcessSetRealData(client, data);
                            break;
                        case SetStateData:
                            ProcessSetStateData(client, data);
                            break;
                        case SetExtendField2Data:
                            ProcessSetExtendFiled2Data(client, data);
                            break;
                        case ValueChangeNotify:
                            ProcessValueChangeNotify(client, data);
                            break;
                        case ValueChangeNotify2:
                            ProcessValueChangeNotify2(client, data);
                            break;
                        case ResetValueChangeNotify:
                            ProcessResetValueChangedNotify(client, data);
                            break;
                        case ResetValueChangeNotify2:
                            ProcessResetValueChangedNotify2(client,data);
                            break;
                        case SetRealDataToLastData:
                            ProcessSetRealDataToLastData(client, data);
                            break;
                    }
                }
                else
                {
                    Parent.AsyncCallback(client, FunId, new byte[1], 0);
                }
            }
            base.ProcessSingleData(client, data);
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
            Parent.AsyncCallback(clientid, ToByteBuffer(ApiFunConst.SetTagValueCallBack, (byte)1));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetStateData(string clientid, ByteBuffer block)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            Dictionary<int, short> mvals = new Dictionary<int, short>();
            int count = block.ReadInt();
            bool res = true;
            for (int i = 0; i < count; i++)
            {
                var id = block.ReadInt();
                short typ = block.ReadShort();
                res &= service.SetTagState(id,typ);
            }
            Parent.AsyncCallback(clientid, ToByteBuffer(SetTagValueCallBack, res ? (byte)1 : (byte)0));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetExtendFiled2Data(string clientid, ByteBuffer block)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            int count = block.ReadInt();
            bool res = true;
            for (int i = 0; i < count; i++)
            {
                var id = block.ReadInt();
                var typ = block.ReadLong();
                 res &= service.SetTagExtend2(id,typ);
            }
            
            Parent.AsyncCallback(clientid, ToByteBuffer(SetTagValueCallBack, res ? (byte)1 : (byte)0));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cc"></param>
        /// <param name="re"></param>
        private void ProcessRealData(List<int> cc,ByteBuffer re)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            var sindex = re.WriteIndex;
            re.Write(cc.Count);
            int count = 0;
            foreach (var vv in cc)
            {
                byte qu, type;
                DateTime time;
                object value;
                value = service.GetTagValue(vv, out qu, out time, out type);
               
                if (value != null)
                {
                    count++;
                    re.Write(vv);
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
                            re.Write(Convert.ToInt32(value));
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
                            //re.Write(sval.Length);
                            re.Write(sval, Encoding.Unicode);
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
                        case (byte)TagType.Complex:
                            List<RealTagValueWithTimer> vals = new List<RealTagValueWithTimer>();
                            re.WriteByte((byte)TagType.Complex);

                            service.GetComplexTagValue(vv, vals);

                            re.Write(vals.Count);
                            foreach (var vtmp in vals)
                            {
                                re.Write(vtmp.Id);
                                re.Write((byte)vtmp.ValueType);
                                switch ((byte)vtmp.ValueType)
                                {
                                    case (byte)TagType.Bool:
                                        re.Write(Convert.ToByte(vtmp.Value));
                                        break;
                                    case (byte)TagType.Byte:
                                        re.Write(Convert.ToByte(vtmp.Value));
                                        break;
                                    case (byte)TagType.Short:
                                        re.Write(Convert.ToInt16(vtmp.Value));
                                        break;
                                    case (byte)TagType.UShort:
                                        re.Write(Convert.ToUInt16(vtmp.Value));
                                        break;
                                    case (byte)TagType.Int:
                                        re.Write(Convert.ToInt32(vtmp.Value));
                                        break;
                                    case (byte)TagType.UInt:
                                        re.Write(Convert.ToUInt32(vtmp.Value));
                                        break;
                                    case (byte)TagType.Long:
                                    case (byte)TagType.ULong:
                                        re.Write(Convert.ToInt64(vtmp.Value));
                                        break;
                                    case (byte)TagType.Float:
                                        re.Write(Convert.ToSingle(vtmp.Value));
                                        break;
                                    case (byte)TagType.Double:
                                        re.Write(Convert.ToDouble(vtmp.Value));
                                        break;
                                    case (byte)TagType.String:
                                        string ssval = vtmp.Value.ToString();
                                        re.Write(ssval);
                                        break;
                                    case (byte)TagType.DateTime:
                                        re.Write(((DateTime)vtmp.Value).Ticks);
                                        break;
                                    case (byte)TagType.IntPoint:
                                        re.Write(((IntPointData)vtmp.Value).X);
                                        re.Write(((IntPointData)vtmp.Value).Y);
                                        break;
                                    case (byte)TagType.UIntPoint:
                                        re.Write((int)((UIntPointData)vtmp.Value).X);
                                        re.Write((int)((UIntPointData)vtmp.Value).Y);
                                        break;
                                    case (byte)TagType.IntPoint3:
                                        re.Write(((IntPoint3Data)vtmp.Value).X);
                                        re.Write(((IntPoint3Data)vtmp.Value).Y);
                                        re.Write(((IntPoint3Data)vtmp.Value).Z);
                                        break;
                                    case (byte)TagType.UIntPoint3:
                                        re.Write((int)((UIntPoint3Data)vtmp.Value).X);
                                        re.Write((int)((UIntPoint3Data)vtmp.Value).Y);
                                        re.Write((int)((UIntPoint3Data)vtmp.Value).Z);
                                        break;
                                    case (byte)TagType.LongPoint:
                                        re.Write(((LongPointData)vtmp.Value).X);
                                        re.Write(((LongPointData)vtmp.Value).Y);
                                        break;
                                    case (byte)TagType.ULongPoint:
                                        re.Write((long)((ULongPointData)vtmp.Value).X);
                                        re.Write((long)((ULongPointData)vtmp.Value).Y);
                                        break;
                                    case (byte)TagType.LongPoint3:
                                        re.Write(((LongPoint3Data)vtmp.Value).X);
                                        re.Write(((LongPoint3Data)vtmp.Value).Y);
                                        re.Write(((LongPoint3Data)vtmp.Value).Z);
                                        break;
                                    case (byte)TagType.ULongPoint3:
                                        re.Write((long)((ULongPoint3Data)vtmp.Value).X);
                                        re.Write((long)((ULongPoint3Data)vtmp.Value).Y);
                                        re.Write((long)((ULongPoint3Data)vtmp.Value).Z);
                                        break;
                                }
                                re.Write(vtmp.Time.Ticks);
                                re.Write(vtmp.Quality);
                            }
                            break;
                    }

                    re.Write(time.Ticks);
                    re.WriteByte(qu);
                }
            }
            if (count != cc.Count)
            {
                long idtmp = re.WriteIndex;
                re.WriteIndex =(sindex);
                re.Write(count);
                re.WriteIndex=(idtmp);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void RelaseBlock(ByteBuffer block)
        {
            if (block != null)
            {
                while (block.RefCount > 0)
                {
                    block.DecRef();
                }
                block.UnlockAndReturn();
            }
        }

        private void ProcessRealDataValueAndQuality(List<int> cc, ByteBuffer re)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            var sindex = re.WriteIndex;
            re.Write(cc.Count);
            int count = 0;
            foreach (var vv in cc)
            {
                byte qu, type;
                DateTime time;
                object value;
                value = service.GetTagValue(vv, out qu, out time, out type);
                if (value != null)
                {
                    re.Write(vv);
                    count++;
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
                            re.Write((short)value);
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
                            string sval = value!=null?value.ToString():"";
                            //re.WriteInt(sval.Length);
                            re.Write(sval, Encoding.Unicode);
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
                        case (byte)TagType.Complex:
                            List<RealTagValueWithTimer> vals = new List<RealTagValueWithTimer>();
                            re.WriteByte((byte)TagType.Complex);

                            service.GetComplexTagValue(vv, vals);

                            re.Write(vals.Count);
                            foreach (var vtmp in vals)
                            {
                                re.Write(vtmp.Id);
                                re.Write((byte)vtmp.ValueType);
                                switch ((byte)vtmp.ValueType)
                                {
                                    case (byte)TagType.Bool:
                                        re.Write(Convert.ToByte(vtmp.Value));
                                        break;
                                    case (byte)TagType.Byte:
                                        re.Write(Convert.ToByte(vtmp.Value));
                                        break;
                                    case (byte)TagType.Short:
                                        re.Write(Convert.ToInt16(vtmp.Value));
                                        break;
                                    case (byte)TagType.UShort:
                                        re.Write(Convert.ToUInt16(vtmp.Value));
                                        break;
                                    case (byte)TagType.Int:
                                        re.Write(Convert.ToInt32(vtmp.Value));
                                        break;
                                    case (byte)TagType.UInt:
                                        re.Write(Convert.ToUInt32(vtmp.Value));
                                        break;
                                    case (byte)TagType.Long:
                                    case (byte)TagType.ULong:
                                        re.Write(Convert.ToInt64(vtmp.Value));
                                        break;
                                    case (byte)TagType.Float:
                                        re.Write(Convert.ToSingle(vtmp.Value));
                                        break;
                                    case (byte)TagType.Double:
                                        re.Write(Convert.ToDouble(vtmp.Value));
                                        break;
                                    case (byte)TagType.String:
                                        string ssval = vtmp.Value.ToString();
                                        re.Write(ssval);
                                        break;
                                    case (byte)TagType.DateTime:
                                        re.Write(((DateTime)vtmp.Value).Ticks);
                                        break;
                                    case (byte)TagType.IntPoint:
                                        re.Write(((IntPointData)vtmp.Value).X);
                                        re.Write(((IntPointData)vtmp.Value).Y);
                                        break;
                                    case (byte)TagType.UIntPoint:
                                        re.Write((int)((UIntPointData)vtmp.Value).X);
                                        re.Write((int)((UIntPointData)vtmp.Value).Y);
                                        break;
                                    case (byte)TagType.IntPoint3:
                                        re.Write(((IntPoint3Data)vtmp.Value).X);
                                        re.Write(((IntPoint3Data)vtmp.Value).Y);
                                        re.Write(((IntPoint3Data)vtmp.Value).Z);
                                        break;
                                    case (byte)TagType.UIntPoint3:
                                        re.Write((int)((UIntPoint3Data)vtmp.Value).X);
                                        re.Write((int)((UIntPoint3Data)vtmp.Value).Y);
                                        re.Write((int)((UIntPoint3Data)vtmp.Value).Z);
                                        break;
                                    case (byte)TagType.LongPoint:
                                        re.Write(((LongPointData)vtmp.Value).X);
                                        re.Write(((LongPointData)vtmp.Value).Y);
                                        break;
                                    case (byte)TagType.ULongPoint:
                                        re.Write((long)((ULongPointData)vtmp.Value).X);
                                        re.Write((long)((ULongPointData)vtmp.Value).Y);
                                        break;
                                    case (byte)TagType.LongPoint3:
                                        re.Write(((LongPoint3Data)vtmp.Value).X);
                                        re.Write(((LongPoint3Data)vtmp.Value).Y);
                                        re.Write(((LongPoint3Data)vtmp.Value).Z);
                                        break;
                                    case (byte)TagType.ULongPoint3:
                                        re.Write((long)((ULongPoint3Data)vtmp.Value).X);
                                        re.Write((long)((ULongPoint3Data)vtmp.Value).Y);
                                        re.Write((long)((ULongPoint3Data)vtmp.Value).Z);
                                        break;
                                }
                                re.Write(vtmp.Time.Ticks);
                                re.Write(vtmp.Quality);
                            }

                            break;
                    }
                    re.WriteByte(qu);

                }
            }

           
            if (count != cc.Count)
            {
                var idtmp = re.WriteIndex;
                re.WriteIndex=(sindex);
                re.Write(count);
                re.WriteIndex=(idtmp);
            }
        }

        private void ProcessRealDataValue(List<int> cc, ByteBuffer re)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            var sindex = re.WriteIndex;
            re.Write(cc.Count);
            int count = 0;
            //Debug.Print("ProcessRealDataValue:" + cc.Count);
            foreach (var vv in cc)
            {
                byte qu, type;
                DateTime time;
                object value;
                value = service.GetTagValue(vv, out qu, out time, out type);
             
                if (value != null)
                {
                    count++;
                    re.Write(vv);
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
                            string sval = value!=null?value.ToString():"";
                            re.Write(sval, Encoding.Unicode);
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
                        case (byte)TagType.Complex:
                            List<RealTagValueWithTimer> vals = new List<RealTagValueWithTimer>();
                            re.WriteByte((byte)TagType.Complex);

                            service.GetComplexTagValue(vv, vals);

                            re.Write(vals.Count);
                            foreach (var vtmp in vals)
                            {
                                re.Write(vtmp.Id);
                                re.Write((byte)vtmp.ValueType);
                                switch ((byte)vtmp.ValueType)
                                {
                                    case (byte)TagType.Bool:
                                        re.Write(Convert.ToByte(vtmp.Value));
                                        break;
                                    case (byte)TagType.Byte:
                                        re.Write(Convert.ToByte(vtmp.Value));
                                        break;
                                    case (byte)TagType.Short:
                                        re.Write(Convert.ToInt16(vtmp.Value));
                                        break;
                                    case (byte)TagType.UShort:
                                        re.Write(Convert.ToUInt16(vtmp.Value));
                                        break;
                                    case (byte)TagType.Int:
                                        re.Write(Convert.ToInt32(vtmp.Value));
                                        break;
                                    case (byte)TagType.UInt:
                                        re.Write(Convert.ToUInt32(vtmp.Value));
                                        break;
                                    case (byte)TagType.Long:
                                    case (byte)TagType.ULong:
                                        re.Write(Convert.ToInt64(vtmp.Value));
                                        break;
                                    case (byte)TagType.Float:
                                        re.Write(Convert.ToSingle(vtmp.Value));
                                        break;
                                    case (byte)TagType.Double:
                                        re.Write(Convert.ToDouble(vtmp.Value));
                                        break;
                                    case (byte)TagType.String:
                                        string ssval = vtmp.Value.ToString();
                                        re.Write(ssval);
                                        break;
                                    case (byte)TagType.DateTime:
                                        re.Write(((DateTime)vtmp.Value).Ticks);
                                        break;
                                    case (byte)TagType.IntPoint:
                                        re.Write(((IntPointData)vtmp.Value).X);
                                        re.Write(((IntPointData)vtmp.Value).Y);
                                        break;
                                    case (byte)TagType.UIntPoint:
                                        re.Write((int)((UIntPointData)vtmp.Value).X);
                                        re.Write((int)((UIntPointData)vtmp.Value).Y);
                                        break;
                                    case (byte)TagType.IntPoint3:
                                        re.Write(((IntPoint3Data)vtmp.Value).X);
                                        re.Write(((IntPoint3Data)vtmp.Value).Y);
                                        re.Write(((IntPoint3Data)vtmp.Value).Z);
                                        break;
                                    case (byte)TagType.UIntPoint3:
                                        re.Write((int)((UIntPoint3Data)vtmp.Value).X);
                                        re.Write((int)((UIntPoint3Data)vtmp.Value).Y);
                                        re.Write((int)((UIntPoint3Data)vtmp.Value).Z);
                                        break;
                                    case (byte)TagType.LongPoint:
                                        re.Write(((LongPointData)vtmp.Value).X);
                                        re.Write(((LongPointData)vtmp.Value).Y);
                                        break;
                                    case (byte)TagType.ULongPoint:
                                        re.Write((long)((ULongPointData)vtmp.Value).X);
                                        re.Write((long)((ULongPointData)vtmp.Value).Y);
                                        break;
                                    case (byte)TagType.LongPoint3:
                                        re.Write(((LongPoint3Data)vtmp.Value).X);
                                        re.Write(((LongPoint3Data)vtmp.Value).Y);
                                        re.Write(((LongPoint3Data)vtmp.Value).Z);
                                        break;
                                    case (byte)TagType.ULongPoint3:
                                        re.Write((long)((ULongPoint3Data)vtmp.Value).X);
                                        re.Write((long)((ULongPoint3Data)vtmp.Value).Y);
                                        re.Write((long)((ULongPoint3Data)vtmp.Value).Z);
                                        break;
                                }
                                re.Write(vtmp.Time.Ticks);
                                re.Write(vtmp.Quality);
                            }

                            break;
                    }
                }
            }
            if(count!=cc.Count)
            {
                var idtmp = re.WriteIndex;
                re.WriteIndex=(sindex);
                re.Write(count);
                re.WriteIndex=(idtmp);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void ProcessGetStateData(string clientId, ByteBuffer block)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            int count = block.ReadInt();
            List<int> cc = new List<int>(count);
            for (int i = 0; i < count; i++)
            {
                cc.Add(block.ReadInt());
            }
            if (Parent != null)
            {
                var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, count * 6 + 5);
                //var vss = service.GetTagState(cc);
                re.WriteByte(RequestStateData);
                re.Write(count);
                for (int i = 0; i < count; i++)
                {
                    var val = service.GetTagState(cc[i]);
                    if (val != null)
                    {
                        re.Write(cc[i]);
                        re.Write(val.Value);
                    }
                    else
                    {
                        re.Write(cc[i]);
                        re.Write((long)-1);
                    }
                }

                //if (vss != null)
                //{
                //    re.Write(vss.Count);
                //    for (int i = 0; i < vss.Count; i++)
                //    {
                //        re.Write(cc[i]);
                //        re.Write(vss[i]);
                //    }
                //}
                //else
                //{
                //    re.Write(0);
                //}
                Parent?.AsyncCallback(clientId, re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void ProcessGetExtendField2Data(string clientId, ByteBuffer block)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            int count = block.ReadInt();
            int[] cc = ArrayPool<int>.Shared.Rent(count);

            for (int i = 0; i < count; i++)
            {
                cc[i] = block.ReadInt();
            }

            var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, count * 12 + 8);
            re.WriteByte(RequestExtendField2);
            re.Write(count);
            for (int i = 0; i < count; i++)
            {
                var val = service.GetTagExtend2(cc[i]);
                if (val != null)
                {
                    re.Write(cc[i]);
                    re.Write(val.Value);
                }
                else
                {
                    re.Write(cc[i]);
                    re.Write((long)-1);
                }
            }
            Parent.AsyncCallback(clientId, re);
            ArrayPool<int>.Shared.Return(cc);

            //List<int> cc = new List<int>(count);
            //for (int i = 0; i < count; i++)
            //{
            //    cc.Add(block.ReadInt());
            //}
            //if (Parent != null)
            //{
            //    var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, count * 12 + 5);
            //    var vss = service.GetTagExtend2(cc);
            //    re.WriteByte(RequestExtendField2);
            //    re.Write(vss.Count);
            //    for (int i = 0; i < vss.Count; i++)
            //    {
            //        re.Write(cc[i]);
            //        re.Write(vss[i]);
            //    }
            //    Parent?.AsyncCallback(clientId, re);
            //}
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
            bool nocache = block.ReadByte() > 0 ? true : false;
            if (Parent != null)
            {
                var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, count * 34+1);
                re.WriteByte(RequestRealData);
                ProcessRealData(cc, re);
                Parent?.AsyncCallback(clientId, re);
            }
        }

        private void ProcessGetComplexTagRealValue(string clientId, ByteBuffer block)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            var vid = block.ReadInt();
            bool nocache = block.ReadByte() > 0 ? true : false;
            List<RealTagValueWithTimer> res = new List<RealTagValueWithTimer>();
            service.GetComplexTagValue(vid, res);
            if (res.Count > 0)
            {
                var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, res.Count * 34 + 8 + 1);
                re.WriteByte(RequestComplexTagRealValue);
                re.Write(res.Count);
                foreach (var vv in res)
                {
                    re.Write(vv.Id);
                    re.WriteByte(vv.ValueType);
                    switch (vv.ValueType)
                    {
                        case (byte)TagType.Bool:
                            re.Write(Convert.ToByte(vv.Value));
                            break;
                        case (byte)TagType.Byte:
                            re.Write(Convert.ToByte(vv.Value));
                            break;
                        case (byte)TagType.Short:
                            re.Write(Convert.ToInt16(vv.Value));
                            break;
                        case (byte)TagType.UShort:
                            re.Write(Convert.ToUInt16(vv.Value));
                            break;
                        case (byte)TagType.Int:
                            re.Write(Convert.ToInt32(vv.Value));
                            break;
                        case (byte)TagType.UInt:
                            re.Write(Convert.ToInt32(vv.Value));
                            break;
                        case (byte)TagType.Long:
                        case (byte)TagType.ULong:
                            re.Write(Convert.ToInt64(vv.Value));
                            break;
                        case (byte)TagType.Float:
                            re.Write(Convert.ToSingle(vv.Value));
                            break;
                        case (byte)TagType.Double:
                            re.Write(Convert.ToDouble(vv.Value));
                            break;
                        case (byte)TagType.String:
                            string sval = vv.Value.ToString();
                            //re.Write(sval.Length);
                            re.Write(sval, Encoding.Unicode);
                            break;
                        case (byte)TagType.DateTime:
                            re.Write(((DateTime)vv.Value).Ticks);
                            break;
                        case (byte)TagType.IntPoint:
                            re.Write(((IntPointData)vv.Value).X);
                            re.Write(((IntPointData)vv.Value).Y);
                            break;
                        case (byte)TagType.UIntPoint:
                            re.Write((int)((UIntPointData)vv.Value).X);
                            re.Write((int)((UIntPointData)vv.Value).Y);
                            break;
                        case (byte)TagType.IntPoint3:
                            re.Write(((IntPoint3Data)vv.Value).X);
                            re.Write(((IntPoint3Data)vv.Value).Y);
                            re.Write(((IntPoint3Data)vv.Value).Z);
                            break;
                        case (byte)TagType.UIntPoint3:
                            re.Write((int)((UIntPoint3Data)vv.Value).X);
                            re.Write((int)((UIntPoint3Data)vv.Value).Y);
                            re.Write((int)((UIntPoint3Data)vv.Value).Z);
                            break;
                        case (byte)TagType.LongPoint:
                            re.Write(((LongPointData)vv.Value).X);
                            re.Write(((LongPointData)vv.Value).Y);
                            break;
                        case (byte)TagType.ULongPoint:
                            re.Write((long)((ULongPointData)vv.Value).X);
                            re.Write((long)((ULongPointData)vv.Value).Y);
                            break;
                        case (byte)TagType.LongPoint3:
                            re.Write(((LongPoint3Data)vv.Value).X);
                            re.Write(((LongPoint3Data)vv.Value).Y);
                            re.Write(((LongPoint3Data)vv.Value).Z);
                            break;
                        case (byte)TagType.ULongPoint3:
                            re.Write((long)((ULongPoint3Data)vv.Value).X);
                            re.Write((long)((ULongPoint3Data)vv.Value).Y);
                            re.Write((long)((ULongPoint3Data)vv.Value).Z);
                            break;
                    }

                    re.Write(vv.Time.Ticks);
                    re.WriteByte(vv.Quality);
                }
                Parent?.AsyncCallback(clientId, re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void ProcessGetRealDataValue(string clientId, ByteBuffer block)
        {
            int count = block.ReadInt();
            List<int> cc = new List<int>(count);
            for (int i = 0; i < count; i++)
            {
                cc.Add(block.ReadInt());
            }
            bool nocache = block.ReadByte() > 0 ? true : false;
            if (Parent != null)
            {
                var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, count * 34+1);
                re.WriteByte(RequestRealDataValue);
                ProcessRealDataValue(cc, re);

                Parent?.AsyncCallback(clientId, re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void ProcessGetRealDataValueAndQuality(string clientId, ByteBuffer block)
        {
            int count = block.ReadInt();
            List<int> cc = new List<int>(count);
            for (int i = 0; i < count; i++)
            {
                cc.Add(block.ReadInt());
            }
            bool nocache = block.ReadByte() > 0 ? true : false;
            if (Parent != null)
            {
                var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, count * 34+1);
                re.WriteByte(RequestRealDataValueAndQuality);
                ProcessRealDataValueAndQuality(cc, re);
                Parent?.AsyncCallback(clientId, re);
            }
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
            bool nocache = block.ReadByte() > 0 ? true : false;

            if (Parent != null)
            {
                var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, cc.Count * 34+1);
                re.WriteByte(RequestRealData2);
                ProcessRealData(cc, re);

                Parent?.AsyncCallback(clientId, re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void ProcessGetRealData2Value(string clientId, ByteBuffer block)
        {
            int sid = block.ReadInt();
            int eid = block.ReadInt();
            List<int> cc = new List<int>(eid - sid);
            for (int i = sid; i <= eid; i++)
            {
                cc.Add(i);
            }
            bool nocache = block.ReadByte() > 0 ? true : false;
            if (Parent != null)
            {
                var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, cc.Count * 34+1);
                re.WriteByte(RequestReal2DataValue);
                ProcessRealDataValue(cc, re);
                Parent?.AsyncCallback(clientId, re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void ProcessGetRealData2ValueAndQuality(string clientId, ByteBuffer block)
        {
            int sid = block.ReadInt();
            int eid = block.ReadInt();
            List<int> cc = new List<int>(eid - sid);
            for (int i = sid; i <= eid; i++)
            {
                cc.Add(i);
            }
            bool nocache = block.ReadByte() > 0 ? true : false;
            if (Parent != null)
            {
                var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, cc.Count * 34+1);
                re.WriteByte(RequestReal2DataValueAndQuality);
                ProcessRealDataValueAndQuality(cc, re);
                //ProcessRealDataValueAndQuality(cc, re);
                Parent?.AsyncCallback(clientId, re);
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
                HashSet<int> ids;
                lock (mCallBackRegistorIds)
                {
                    if (mCallBackRegistorIds.ContainsKey(clientId))
                    {
                        ids = new HashSet<int>(mCallBackRegistorIds[clientId]);
                    }
                    else
                    {
                        ids = new HashSet<int>();
                    }
                }

                for (int i = minid; i <= maxid; i++)
                {
                    ids.Add(i);
                    lock (mMonitors)
                    {
                        if (!mMonitors.ContainsKey(i))
                        {
                            var vtag = ServiceLocator.Locator.Resolve<ITagManager>()?.GetTagById(i);
                            if(vtag!=null&&!mtmp.ContainsKey(i))
                            mtmp.Add(i, new MonitorTag() {  RefCount = 1, Type = vtag.Type });
                            else
                            {
                                LoggerService.Service.Info("ProcessValueChangeNotify", $" {clientId}  订购变量 Id {i} 不存在");
                            }
                        }
                        else
                        {
                            mMonitors[i].IncRef();
                        }
                    }
                }

                lock (mCallBackRegistorIds)
                {
                    if (mCallBackRegistorIds.ContainsKey(clientId))
                    {
                        mCallBackRegistorIds[clientId] = ids;
                    }
                    else
                    {
                        mCallBackRegistorIds.Add(clientId, ids);
                    }
                }

                lock(mDataCounts)
                if (!mDataCounts.ContainsKey(clientId))
                {
                    mDataCounts.Add(clientId, 0);
                }

                Parent?.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.RealDataRequestFun, 1));

                StringBuilder sb = new StringBuilder();
                foreach (var vv in ids)
                {
                    sb.Append(vv + ",");
                }
                LoggerService.Service.Info("ProcessValueChangeNotify", $"客户端 {clientId} 订购的变量列表:{sb.ToString()}");

                resetEvent?.Set();
            }
            catch(Exception ex)
            {
                Parent?.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.RealDataRequestFun, 0));
                LoggerService.Service.Warn("ProcessValueChangeNotify", ex.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void ProcessValueChangeNotify2(string clientId, ByteBuffer block)
        {
            try
            {
                int count = block.ReadInt();

                HashSet<int> ids;
                lock (mCallBackRegistorIds)
                {
                    if (mCallBackRegistorIds.ContainsKey(clientId))
                    {
                        ids = new HashSet<int>(mCallBackRegistorIds[clientId]);
                    }
                    else
                    {
                        ids = new HashSet<int>();
                    }
                }

                for (int i = 0; i < count; i++)
                {
                    int nid = block.ReadInt();
                    ids.Add(nid);
                    lock (mMonitors)
                    {
                        if (!mMonitors.ContainsKey(nid))
                        {
                            var vtag = ServiceLocator.Locator.Resolve<ITagManager>()?.GetTagById(nid);
                            if (vtag != null && !mtmp.ContainsKey(nid))
                                mtmp.Add(nid, new MonitorTag() { RefCount = 1, Type = vtag.Type }); 
                            else
                            {
                                LoggerService.Service.Info("ProcessValueChangeNotify2", $" {clientId}  订购变量 Id {nid} 不存在");
                            }
                        }
                        else
                        {
                            mMonitors[nid].IncRef();
                        }
                    }
                }
                lock (mCallBackRegistorIds)
                {
                    if (mCallBackRegistorIds.ContainsKey(clientId))
                    {
                        mCallBackRegistorIds[clientId] = ids;
                    }
                    else
                    {
                        mCallBackRegistorIds.Add(clientId, ids);
                    }
                }

                lock (mDataCounts)
                    if (!mDataCounts.ContainsKey(clientId))
                    {
                        mDataCounts.Add(clientId, 0);
                    }

                Parent?.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.RealDataRequestFun, 1));

                StringBuilder sb = new StringBuilder();
                foreach(var vv in ids)
                {
                    sb.Append(vv + ",");
                }
                LoggerService.Service.Info("ProcessValueChangeNotify2", $"客户端 {clientId} 订购的变量列表:{sb.ToString()}");

                resetEvent?.Set();
            }
            catch (Exception ex)
            {
                Parent?.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.RealDataRequestFun, 0));
                LoggerService.Service.Warn("ProcessValueChangeNotify2", ex.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void ProcessResetValueChangedNotify(string clientId, ByteBuffer block)
        {
            try
            {
                lock (mCallBackRegistorIds)
                {
                    if (mCallBackRegistorIds.ContainsKey(clientId))
                    {
                        var vtags = mCallBackRegistorIds[clientId];
                        foreach (var vv in vtags)
                        {
                            if (mMonitors.ContainsKey(vv))
                            {
                                mMonitors[vv].DecRef();
                            }
                        }
                        mCallBackRegistorIds.Remove(clientId);
                    }
                }
                lock (mDataCounts)
                {
                    if (mDataCounts.ContainsKey(clientId))
                    {
                        mDataCounts.Remove(clientId);
                    }
                }
                Parent?.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.RealDataRequestFun, 1));
            }
            catch
            {
                Parent?.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.RealDataRequestFun, 0));
            }
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void ProcessResetValueChangedNotify2(string clientId, ByteBuffer block)
        {
            try
            {
                int count = block.ReadInt();

               var  ids = new HashSet<int>();

                for (int i = 0; i < count; i++)
                {
                    int nid = block.ReadInt();
                    ids.Add(nid);
                }
                lock (mCallBackRegistorIds)
                {
                    if (mCallBackRegistorIds.ContainsKey(clientId))
                    {
                        var vtags = mCallBackRegistorIds[clientId];

                        foreach(var vv in ids)
                        {
                            if(vtags.Contains(vv))
                            {
                                vtags.Remove(vv);
                            }
                            if (mMonitors.ContainsKey(vv))
                            {
                                mMonitors[vv].DecRef();
                            }
                        }
                    }
                }


                Parent?.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.RealDataRequestFun, 1));

                StringBuilder sb = new StringBuilder();
                foreach (var vv in ids)
                {
                    sb.Append(vv + ",");
                }
                LoggerService.Service.Info("ProcessResetValueChangedNotify2", $"客户端 {clientId} 取消订购变量列表:{sb.ToString()}");

            }
            catch (Exception ex)
            {
                Parent?.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.RealDataRequestFun, 0));
                LoggerService.Service.Warn("ProcessResetValueChangedNotify2", ex.Message);
            }
        }

        private HisQueryResult<T> ProcessDataQuery<T>(int id, DateTime stime, DateTime etime)
        {
            return ServiceLocator.Locator.Resolve<IHisQuery>().ReadAllValue<T>(id, stime, etime);
        }

        private object GetLastValue(int id, TagType type, DateTime sTime, DateTime eTime, out byte quality)
        {
            switch (type)
            {
                case Cdy.Tag.TagType.Bool:
                    return ProcessDataQuery<bool>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.Byte:
                    return ProcessDataQuery<byte>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.DateTime:
                    return ProcessDataQuery<DateTime>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.Double:
                    return ProcessDataQuery<double>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.Float:
                    return ProcessDataQuery<float>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.Int:
                    return ProcessDataQuery<int>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.Long:
                    return ProcessDataQuery<long>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.Short:
                    return ProcessDataQuery<short>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.String:
                    return ProcessDataQuery<string>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.UInt:
                    return ProcessDataQuery<bool>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.ULong:
                    return ProcessDataQuery<ulong>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.UShort:
                    return ProcessDataQuery<ushort>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.IntPoint:
                    return ProcessDataQuery<IntPointData>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.UIntPoint:
                    return ProcessDataQuery<UIntPointData>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.IntPoint3:
                    return ProcessDataQuery<IntPoint3Data>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.UIntPoint3:
                    return ProcessDataQuery<UIntPoint3Data>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.LongPoint:
                    return ProcessDataQuery<LongPointData>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.ULongPoint:
                    return ProcessDataQuery<ULongPointData>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.LongPoint3:
                    return ProcessDataQuery<LongPoint3Data>(id, sTime, eTime).GetLastValue(out quality);
                case Cdy.Tag.TagType.ULongPoint3:
                    return ProcessDataQuery<ULongPoint3Data>(id, sTime, eTime).GetLastValue(out quality);
            }
            quality = (byte)QualityConst.Null;
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="type"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        private object GetLastAvaiableData(int id, TagType type, DateTime time, out byte quality)
        {
            object re = null;
            DateTime stime;
            DateTime etime = time;
            DateTime sttime = time.AddDays(-1);

            do
            {
                stime = etime.AddMinutes(-5);
                re = GetLastValue(id, type, stime, etime, out quality);
                if (re != null)
                {
                    return re;
                }
                etime = stime;
            }
            while (re == null && etime > sttime);

            quality = (byte)QualityConst.Null;
            return null;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        public void ProcessSetRealDataToLastData(string clientId, ByteBuffer block)
        {
            try
            {
                int id = block.ReadInt();
                var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);
                if (tags != null)
                {
                    var val = GetLastAvaiableData(id, tags.Type, DateTime.Now, out byte quality);
                    if (val == null) return;

                    switch (tags.Type)
                    {
                        case Cdy.Tag.TagType.Bool:
                            var bval = Convert.ToBoolean(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref bval, quality);
                            break;
                        case Cdy.Tag.TagType.Byte:
                            var bbval = Convert.ToByte(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref bbval, quality);
                            break;
                        case Cdy.Tag.TagType.DateTime:
                            var dval = Convert.ToDateTime(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref dval, quality);
                            break;
                        case Cdy.Tag.TagType.Double:
                            var ddval = Convert.ToDouble(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref ddval, quality);
                            break;
                        case Cdy.Tag.TagType.Float:
                            var fval = Convert.ToSingle(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref fval, quality);
                            break;
                        case Cdy.Tag.TagType.Int:
                            var ival = Convert.ToInt32(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref ival, quality);
                            break;
                        case Cdy.Tag.TagType.Long:
                            var lval = Convert.ToInt64(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref lval, quality);
                            break;
                        case Cdy.Tag.TagType.Short:
                            var sval = Convert.ToInt16(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref sval, quality);
                            break;
                        case Cdy.Tag.TagType.String:
                            var ssval = Convert.ToString(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ssval, quality);
                            break;
                        case Cdy.Tag.TagType.UInt:
                            var uival = Convert.ToUInt32(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref uival, quality);
                            break;
                        case Cdy.Tag.TagType.ULong:
                            var ultmp = Convert.ToUInt64(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref ultmp, quality);
                            break;
                        case Cdy.Tag.TagType.UShort:
                            var usval = Convert.ToUInt16(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref usval, quality);
                            break;
                        case Cdy.Tag.TagType.IntPoint:
                            var ipval = (IntPointData)(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref ipval, quality);
                            break;
                        case Cdy.Tag.TagType.UIntPoint:
                            var uipval = (UIntPointData)(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref uipval, quality);
                            break;
                        case Cdy.Tag.TagType.IntPoint3:
                            var ip3val = (IntPoint3Data)(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref ip3val, quality);
                            break;
                        case Cdy.Tag.TagType.UIntPoint3:
                            var uip3val = (UIntPoint3Data)(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref uip3val, quality);
                            break;
                        case Cdy.Tag.TagType.LongPoint:
                            var lpval = (LongPointData)(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref lpval, quality);
                            break;
                        case Cdy.Tag.TagType.ULongPoint:
                            var ulpval = (ULongPointData)(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref ulpval, quality);
                            break;
                        case Cdy.Tag.TagType.LongPoint3:
                            var lp3val = (LongPoint3Data)(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref lp3val, quality);
                            break;
                        case Cdy.Tag.TagType.ULongPoint3:
                            var ulp3val = (ULongPoint3Data)(val);
                            ServiceLocator.Locator.Resolve<Cdy.Tag.Driver.IRealTagProduct>().SetTagValue(id, ref ulp3val, quality);
                            break;
                    }

                    Parent?.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.SetTagValueCallBack, 1));
                }
                else
                {
                    Parent?.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.SetTagValueCallBack, 0));

                }
            }
            catch
            {
                Parent?.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.SetTagValueCallBack, 0));
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="id"></param>
        private void ProcessTagPush(ByteBuffer re,int id,byte type, object value,byte qu,DateTime time)
        {
            re.Write(id);
            re.Write(type);
            switch (type)
            {
                case (byte)TagType.Bool:
                    re.Write((byte)value);
                    break;
                case (byte)TagType.Byte:
                    re.Write((byte)value);
                    break;
                case (byte)TagType.Short:
                    re.Write((short)value);
                    break;
                case (byte)TagType.UShort:
                    re.Write((ushort)value);
                    break;
                case (byte)TagType.Int:
                    re.Write((int)value);
                    break;
                case (byte)TagType.UInt:
                    re.Write((int)value);
                    break;
                case (byte)TagType.Long:
                case (byte)TagType.ULong:
                    re.Write((long)value);
                    break;
                case (byte)TagType.Float:
                    re.Write((float)value);
                    break;
                case (byte)TagType.Double:
                    re.Write((double)value);
                    break;
                case (byte)TagType.String:
                    string sval = value.ToString();
                  //  re.WriteInt(sval.Length);
                    re.Write(sval, Encoding.Unicode);
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
            re.WriteByte(qu);
            re.Write(time.Ticks);
            //Debug.Print(buffer.WriterIndex.ToString());
        }


        /// <summary>
        /// 
        /// </summary>
        private void SendThreadPro()
        {
            while (!mIsClosed)
            {
                try
                {
                    mTagConsumer= ServiceLocator.Locator.Resolve<IRealTagConsumer>();

                    if (mTagConsumer == null)
                    {
                        Thread.Sleep(100);
                        continue;
                    }

                    resetEvent.WaitOne();
                    if (mIsClosed) return;
                    resetEvent.Reset();

                    //Stopwatch sw = new Stopwatch();
                    //sw.Start();

                    lock (mtmp)
                    {
                        if (mtmp.Count > 0)
                        {
                            foreach (var vv in mtmp)
                            {
                                if (!mMonitors.ContainsKey(vv.Key))
                                    mMonitors.Add(vv.Key, vv.Value);
                            }
                        }
                        mtmp.Clear();
                    }

                    foreach (var vv in mMonitors.Where(e => e.Value.RefCount <= 0).ToArray())
                    {
                        mMonitors.Remove(vv.Key);
                    }


                    var chgarry = ArrayPool<int>.Shared.Rent(mMonitors.Count());
                    Array.Clear(chgarry, 0, chgarry.Length);

                    int i = 0;
                    DateTime te;
                    byte qu;
                    byte vtype;
                    foreach (var vv in mMonitors)
                    {
                        var oval = mTagConsumer.GetTagValue(vv.Key, out qu, out te, out vtype);
                        vv.Value.CheckValueChaned(oval, qu, te);
                        if (vv.Value.IsValueChanged)
                        {
                            chgarry[i++] = vv.Key;
                            vv.Value.IsValueChanged = false;
                        }
                    }


                    if (i > 0)
                    {

                        KeyValuePair<string, HashSet<int>>[] clients;
                        lock (mCallBackRegistorIds)
                            clients = mCallBackRegistorIds.ToArray();

                        foreach (var cb in clients)
                        {
                            var buffer = Parent.Allocate(Api.ApiFunConst.RealDataPushFun, i * (64 + 8) + 4);
                            buffer.Write(0);
                            buffers.Add(cb.Key, buffer);
                            lock (mDataCounts)
                            {
                                if (mDataCounts.ContainsKey(cb.Key))
                                    mDataCounts[cb.Key] = 0;
                            }
                        }

                        //StringBuilder sb = new StringBuilder();

                        for (int j = 0; j < i; j++)
                        {
                            var vid = chgarry[j];
                            MonitorTag tag = mMonitors[vid];
                            if (tag.Value != null)
                            {
                                foreach (var vvc in clients)
                                {
                                    lock (mCallBackRegistorIds)
                                    {
                                        if (vvc.Value.Contains(vid))
                                        {
                                            ProcessTagPush(buffers[vvc.Key], vid, (byte)tag.Type, tag.Value, tag.Quality, tag.Time);
                                            lock (mDataCounts)
                                            {
                                                if (mDataCounts.ContainsKey(vvc.Key)) mDataCounts[vvc.Key]++;
                                            }
                                            //sb.Append(vid + ":" + tag.Value + "  ");
                                        }
                                    }
                                }
                            }
                        }

                        foreach (var cb in buffers)
                        {
                            //  cb.Value.MarkWriterIndex();
                            var vindex = cb.Value.WriteIndex;
                            cb.Value.WriteIndex = 1;
                            lock (mDataCounts)
                            {
                                if (mDataCounts.ContainsKey(cb.Key))
                                {
                                    cb.Value.Write(mDataCounts[cb.Key]);
                                    cb.Value.WriteIndex = vindex;
                                    // cb.Value.ResetWriterIndex();
                                    Parent.PushRealDatatoClient(cb.Key, cb.Value);
                                    //LoggerService.Service.Info("RealDataServerProcess", $"推送到客户端{cb.Key} 数据: {sb.ToString()}");
                                }
                            }
                        }
                        buffers.Clear();
                    }

                    //sw.Stop();
                    //LoggerService.Service.Info("RealDataServerProcess", "推送数据耗时" + sw.ElapsedMilliseconds + " 个数大小:" + i);

                }
                catch (Exception ex)
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
            resetEvent = null;
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
                ProcessResetValueChangedNotify(id, null);
                mCallBackRegistorIds.Remove(id);
            }
            base.OnClientDisconnected(id);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}