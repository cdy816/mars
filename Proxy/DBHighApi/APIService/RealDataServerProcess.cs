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
using DBRuntime.Proxy;
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
        public TagType Type { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsValueChanged { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void CheckValueChaned(object value,byte qu)
        {
            if(value!=Value)
            {
                Value = value;
                IsValueChanged = true;
            }
            Quality = qu;
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


        public const byte RequestReal2DataValueAndQuality = 18;

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
        /// 
        /// </summary>
        public const byte ValueChangeNotify2 = 22;

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

        private Dictionary<string, HashSet<int>> mCallBackRegistorIds = new Dictionary<string, HashSet<int>>();

        private Dictionary<string, bool> mBlockCallBackRegistorIds = new Dictionary<string, bool>();

        //private Memory<int> mChangedTags = new Memory<int>(10);

        private Dictionary<int, MonitorTag> mMonitors = new Dictionary<int, MonitorTag>();

        private Dictionary<int, MonitorTag> mtmp = new Dictionary<int, MonitorTag>();

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
            DatabaseRunner.Manager.ValueUpdateEvent += Manager_ValueUpdateEvent;
        }

        #endregion ...Constructor...

        #region ... Properties ...

        public override byte FunId => ApiFunConst.RealDataRequestFun;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private  void Manager_ValueUpdateEvent(object sender, EventArgs e)
        {
            resetEvent?.Set();
        }

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
                    case SetDataValue:
                        ProcessSetRealData(client, data);
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
            Parent.AsyncCallback(clientid, ToByteBuffer(ApiFunConst.RealDataRequestFun, (byte)1));
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

        private void ProcessRealDataNocache(List<int> cc, ByteBuffer re)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            var sindex = re.WriteIndex;
            re.Write(cc.Count);
            int count = 0;

            var vdata = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.GetRealData(cc);
            if (vdata != null)
            {
                var valuecount = vdata.ReadInt();
                for (int i = 0; i < valuecount; i++)
                {
                    var vid = vdata.ReadInt();
                    var type = vdata.ReadByte();

                    re.Write(vid);
                    re.Write(type);

                    switch (type)
                    {
                        case (byte)TagType.Bool:
                            re.Write((byte)vdata.ReadByte());
                            break;
                        case (byte)TagType.Byte:
                            re.Write((byte)vdata.ReadByte());
                            break;
                        case (byte)TagType.Short:
                            re.Write(vdata.ReadShort());
                            break;
                        case (byte)TagType.UShort:
                            re.Write(vdata.ReadUShort());
                            break;
                        case (byte)TagType.Int:
                            re.Write(vdata.ReadInt());
                            break;
                        case (byte)TagType.UInt:
                            re.Write(vdata.ReadUInt());
                            break;
                        case (byte)TagType.Long:
                        case (byte)TagType.ULong:
                            re.Write(vdata.ReadULong());
                            break;
                        case (byte)TagType.Float:
                            re.Write(vdata.ReadFloat());
                            break;
                        case (byte)TagType.Double:
                            re.Write(vdata.ReadDouble());
                            break;
                        case (byte)TagType.String:
                            re.Write(vdata.ReadString(), Encoding.Unicode);
                            break;
                        case (byte)TagType.DateTime:
                            re.Write(vdata.ReadLong());
                            break;
                        case (byte)TagType.IntPoint:
                        case (byte)TagType.UIntPoint:
                            re.Write(vdata.ReadInt());
                            re.Write(vdata.ReadInt());
                            break;
                        case (byte)TagType.IntPoint3:
                        case (byte)TagType.UIntPoint3:
                            re.Write(vdata.ReadInt());
                            re.Write(vdata.ReadInt());
                            re.Write(vdata.ReadInt());
                            break;
                        case (byte)TagType.LongPoint:
                        case (byte)TagType.ULongPoint:
                            re.Write(vdata.ReadLong());
                            re.Write(vdata.ReadLong());
                            break;
                        case (byte)TagType.LongPoint3:
                        case (byte)TagType.ULongPoint3:
                            re.Write(vdata.ReadLong());
                            re.Write(vdata.ReadLong());
                            re.Write(vdata.ReadLong());
                            break;
                    }

                    re.Write(vdata.ReadLong());
                    re.Write(vdata.ReadByte());

                    count++;
                }

            }


            if (count != cc.Count)
            {
                var idtmp = re.WriteIndex;
                re.WriteIndex = (sindex);
                re.Write(count);
                re.WriteIndex = (idtmp);
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

        private void ProcessRealDataValueAndQualityNoCache(List<int> cc, ByteBuffer re)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            var sindex = re.WriteIndex;
            re.Write(cc.Count);
            int count = 0;
            //Debug.Print("ProcessRealDataValue:" + cc.Count);

            var vdata = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.GetRealData(cc);
            if (vdata != null)
            {
                var valuecount = vdata.ReadInt();
                for (int i = 0; i < valuecount; i++)
                {
                    var vid = vdata.ReadInt();
                    var type = vdata.ReadByte();

                    re.Write(vid);
                    re.Write(type);

                    switch (type)
                    {
                        case (byte)TagType.Bool:
                            re.Write((byte)vdata.ReadByte());
                            break;
                        case (byte)TagType.Byte:
                            re.Write((byte)vdata.ReadByte());
                            break;
                        case (byte)TagType.Short:
                            re.Write(vdata.ReadShort());
                            break;
                        case (byte)TagType.UShort:
                            re.Write(vdata.ReadUShort());
                            break;
                        case (byte)TagType.Int:
                            re.Write(vdata.ReadInt());
                            break;
                        case (byte)TagType.UInt:
                            re.Write(vdata.ReadUInt());
                            break;
                        case (byte)TagType.Long:
                        case (byte)TagType.ULong:
                            re.Write(vdata.ReadULong());
                            break;
                        case (byte)TagType.Float:
                            re.Write(vdata.ReadFloat());
                            break;
                        case (byte)TagType.Double:
                            re.Write(vdata.ReadDouble());
                            break;
                        case (byte)TagType.String:
                            re.Write(vdata.ReadString(), Encoding.Unicode);
                            break;
                        case (byte)TagType.DateTime:
                            re.Write(vdata.ReadLong());
                            break;
                        case (byte)TagType.IntPoint:
                        case (byte)TagType.UIntPoint:
                            re.Write(vdata.ReadInt());
                            re.Write(vdata.ReadInt());
                            break;
                        case (byte)TagType.IntPoint3:
                        case (byte)TagType.UIntPoint3:
                            re.Write(vdata.ReadInt());
                            re.Write(vdata.ReadInt());
                            re.Write(vdata.ReadInt());
                            break;
                        case (byte)TagType.LongPoint:
                        case (byte)TagType.ULongPoint:
                            re.Write(vdata.ReadLong());
                            re.Write(vdata.ReadLong());
                            break;
                        case (byte)TagType.LongPoint3:
                        case (byte)TagType.ULongPoint3:
                            re.Write(vdata.ReadLong());
                            re.Write(vdata.ReadLong());
                            re.Write(vdata.ReadLong());
                            break;
                    }

                    vdata.ReadLong();
                    re.Write(vdata.ReadByte());

                    count++;
                }

            }


            if (count != cc.Count)
            {
                var idtmp = re.WriteIndex;
                re.WriteIndex = (sindex);
                re.Write(count);
                re.WriteIndex = (idtmp);
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
                           // re.Write(sval.Length);
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

        private void ProcessRealDataValueNoCache(List<int> cc, ByteBuffer re)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            var sindex = re.WriteIndex;
            re.Write(cc.Count);
            int count = 0;
            //Debug.Print("ProcessRealDataValue:" + cc.Count);

            var vdata = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.GetRealData(cc);
            if (vdata!=null)
            {
                var valuecount = vdata.ReadInt();
                for(int i=0;i<valuecount;i++)
                {
                    var vid = vdata.ReadInt();
                    var type = vdata.ReadByte();

                    re.Write(vid);
                    re.Write(type);

                    switch (type)
                    {
                        case (byte)TagType.Bool:
                            re.Write((byte)vdata.ReadByte());
                            break;
                        case (byte)TagType.Byte:
                            re.Write((byte)vdata.ReadByte());
                            break;
                        case (byte)TagType.Short:
                            re.Write(vdata.ReadShort());
                            break;
                        case (byte)TagType.UShort:
                            re.Write(vdata.ReadUShort());
                            break;
                        case (byte)TagType.Int:
                            re.Write(vdata.ReadInt());
                            break;
                        case (byte)TagType.UInt:
                            re.Write(vdata.ReadUInt());
                            break;
                        case (byte)TagType.Long:
                        case (byte)TagType.ULong:
                            re.Write(vdata.ReadULong());
                            break;
                        case (byte)TagType.Float:
                            re.Write(vdata.ReadFloat());
                            break;
                        case (byte)TagType.Double:
                            re.Write(vdata.ReadDouble());
                            break;
                        case (byte)TagType.String:
                            re.Write(vdata.ReadString(), Encoding.Unicode);
                            break;
                        case (byte)TagType.DateTime:
                            re.Write(vdata.ReadLong());
                            break;
                        case (byte)TagType.IntPoint:
                        case (byte)TagType.UIntPoint:
                            re.Write(vdata.ReadInt());
                            re.Write(vdata.ReadInt());
                            break;
                        case (byte)TagType.IntPoint3:
                        case (byte)TagType.UIntPoint3:
                            re.Write(vdata.ReadInt());
                            re.Write(vdata.ReadInt());
                            re.Write(vdata.ReadInt());
                            break;
                        case (byte)TagType.LongPoint:
                        case (byte)TagType.ULongPoint:
                            re.Write(vdata.ReadLong());
                            re.Write(vdata.ReadLong());
                            break;
                        case (byte)TagType.LongPoint3:
                        case (byte)TagType.ULongPoint3:
                            re.Write(vdata.ReadLong());
                            re.Write(vdata.ReadLong());
                            re.Write(vdata.ReadLong());
                            break;
                    }

                    vdata.ReadLong();
                    vdata.ReadByte();

                    count++;
                }

            }


            if (count != cc.Count)
            {
                var idtmp = re.WriteIndex;
                re.WriteIndex = (sindex);
                re.Write(count);
                re.WriteIndex = (idtmp);
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
            bool nocache = block.ReadByte() > 0 ? true : false;

            var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, count * 34);
            if(!nocache)
            {
                ProcessRealData(cc, re);
            }
            else
            {
                ProcessRealDataNocache(cc,re);
            }
            Parent.AsyncCallback(clientId, re);
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
            var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, count * 34);
            if (nocache)
            {
                ProcessRealDataValueNoCache(cc, re);
            }
            else
            {
               
                ProcessRealDataValue(cc, re);
            }

            Parent.AsyncCallback(clientId, re);
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
            var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, count * 34);
            
            if (nocache)
            {
                ProcessRealDataValueAndQualityNoCache(cc, re);
            }
            else
            {
                ProcessRealDataValueAndQuality(cc, re);
            }
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
            bool nocache = block.ReadByte() > 0 ? true : false;

            var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, cc.Count * 34);
            if (nocache)
            {
                ProcessRealDataNocache(cc,re);
            }
            else
            {
                ProcessRealData(cc, re);
            }

            Parent.AsyncCallback(clientId, re);
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

            var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, cc.Count * 34);
            if (nocache)
            {
                ProcessRealDataValueNoCache(cc, re);
            }
            else
            {
                ProcessRealDataValue(cc, re);
            }
            Parent.AsyncCallback(clientId, re);
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

            var re = Parent.Allocate(ApiFunConst.RealDataRequestFun, cc.Count * 34);
            if (nocache)
            {
                ProcessRealDataValueAndQualityNoCache(cc, re);
            }
            else
            {
                ProcessRealDataValueAndQuality(cc, re);
            }
            //ProcessRealDataValueAndQuality(cc, re);
            Parent.AsyncCallback(clientId, re);
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
                HashSet<int> ids = new HashSet<int>();
                for (int i = minid; i <= maxid; i++)
                {
                    ids.Add(i);
                    lock (mMonitors)
                    {
                        if (!mMonitors.ContainsKey(i))
                        {
                            var vtag = mTagManager.GetTagById(i);
                            if(vtag!=null)
                            mtmp.Add(i, new MonitorTag() { Value = 0, RefCount = 1, Type = vtag.Type });
                        }
                        else
                        {
                            mMonitors[i].IncRef();
                        }
                    }
                }

                if (mCallBackRegistorIds.ContainsKey(clientId))
                {
                    mCallBackRegistorIds[clientId] = ids;
                }
                else
                {
                    mCallBackRegistorIds.Add(clientId, ids);
                }

                lock(mDataCounts)
                if (!mDataCounts.ContainsKey(clientId))
                {
                    mDataCounts.Add(clientId, 0);
                }

                Parent.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.RealDataRequestFun, 1));
            }
            catch(Exception ex)
            {
                Parent.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.RealDataRequestFun, 0));
                Debug.Print(ex.Message);
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

                HashSet<int> ids = new HashSet<int>();
                for (int i = 0; i <= count; i++)
                {
                    int nid = block.ReadInt();
                    ids.Add(nid);
                    lock (mMonitors)
                    {
                        if (!mMonitors.ContainsKey(nid))
                        {
                            var vtag = mTagManager.GetTagById(nid);
                            if (vtag != null)
                                mtmp.Add(i, new MonitorTag() { Value = 0, RefCount = 1, Type = vtag.Type });
                        }
                        else
                        {
                            mMonitors[nid].IncRef();
                        }
                    }
                }

                if (mCallBackRegistorIds.ContainsKey(clientId))
                {
                    mCallBackRegistorIds[clientId] = ids;
                }
                else
                {
                    mCallBackRegistorIds.Add(clientId, ids);
                }

                lock (mDataCounts)
                    if (!mDataCounts.ContainsKey(clientId))
                    {
                        mDataCounts.Add(clientId, 0);
                    }

                Parent.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.RealDataRequestFun, 1));
            }
            catch (Exception ex)
            {
                Parent.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.RealDataRequestFun, 0));
                Debug.Print(ex.Message);
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
                
                if (mCallBackRegistorIds.ContainsKey(clientId))
                {
                    var vtags = mCallBackRegistorIds[clientId];
                    foreach(var vv in vtags)
                    {
                        if(mMonitors.ContainsKey(vv))
                        {
                            mMonitors[vv].DecRef();
                        }
                    }
                    mCallBackRegistorIds.Remove(clientId);
                }
                lock (mDataCounts)
                {
                    if (mDataCounts.ContainsKey(clientId))
                    {
                        mDataCounts.Remove(clientId);
                    }
                }
                Parent.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.RealDataRequestFun, 1));
            }
            catch
            {
                Parent.AsyncCallback(clientId, ToByteBuffer(ApiFunConst.RealDataRequestFun, 0));
            }
            
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="id"></param>
        private void ProcessTagPush(ByteBuffer re,int id,byte type, object value,byte qu)
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
            //Debug.Print(buffer.WriterIndex.ToString());
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

                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    lock (mtmp)
                    {
                        if (mtmp.Count > 0)
                        {
                            foreach (var vv in mtmp)
                            {
                                if(!mMonitors.ContainsKey(vv.Key))
                                mMonitors.Add(vv.Key, vv.Value);
                            }
                        }
                        mtmp.Clear();
                    }

                    foreach(var vv in mMonitors.Where(e=>e.Value.RefCount<=0).ToArray())
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
                        var oval = mTagConsumer.GetTagValue(vv.Key,out qu,out te,out vtype);
                        vv.Value.CheckValueChaned(oval,qu);
                        if(vv.Value.IsValueChanged)
                        {
                            chgarry[i++] = vv.Key;
                            vv.Value.IsValueChanged = false;
                        }
                    }

                    if(i>0)
                    {
                        var clients = mCallBackRegistorIds.ToArray();

                        foreach (var cb in clients)
                        {
                            var buffer = Parent.Allocate(Api.ApiFunConst.RealDataPushFun, i * 64 + 4);
                            buffer.Write(0);
                            buffers.Add(cb.Key, buffer);
                            lock (mDataCounts)
                            {
                                if(mDataCounts.ContainsKey(cb.Key))
                                mDataCounts[cb.Key] = 0;
                            }
                        }

                        for(int j=0;j<i;j++)
                        {
                            var vid = chgarry[j];
                            MonitorTag tag = mMonitors[vid];
                            foreach (var vvc in clients)
                            {
                                if (vvc.Value.Contains(vid))
                                {
                                    ProcessTagPush(buffers[vvc.Key], vid, (byte)tag.Type,tag.Value,tag.Quality);
                                    lock (mDataCounts)
                                    {
                                        if (mDataCounts.ContainsKey(vvc.Key)) mDataCounts[vvc.Key]++;
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
                                }
                            }
                        }
                        buffers.Clear();
                    }

                    sw.Stop();
                    LoggerService.Service.Info("RealDataServerProcess", "推送数据耗时" + sw.ElapsedMilliseconds + " 个数大小:" + i);

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
