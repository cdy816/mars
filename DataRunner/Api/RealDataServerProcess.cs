//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/14 11:00:38.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
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

        private Dictionary<string, List<int>> mCallBackRegistorIds = new Dictionary<string, List<int>>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

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
        protected unsafe override void ProcessSingleData(string client, IByteBuffer data)
        {
            byte cmd = data.ReadByte();
            string id = ReadString(data);
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
                Parent.AsyncCallback(client, FunId, new byte[1], 1);
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
            string loginId = ReadString(block);
            var service = Cdy.Tag.ServiceLocator.Locator.Resolve<IRealTagComsumer>();
            if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
            {
                int count = block.ReadInt();
                for(int i=0;i<count;i++)
                {
                    var id = block.ReadInt();
                    byte typ = block.ReadByte();
                    object value=null;
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
                    service.SetTagValueForConsumer(id, value);
                }
                Parent.AsyncCallback(clientid, ToByteBuffer(ApiFunConst.RealDataRequestFun, (byte)1));
            }
            else
            {
                Parent.AsyncCallback(clientid, ToByteBuffer(ApiFunConst.RealDataRequestFun,(byte)0));
            }
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
                            re.WriteString(sval, Encoding.UTF8);
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
            string loginId = ReadString(block);
            if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
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
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void ProcessGetRealData2(string clientId, IByteBuffer block)
        {
            string loginId = ReadString(block);
            if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
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
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void ProcessValueChangeNotify(string clientId, IByteBuffer block)
        {
            string loginId = ReadString(block);
            if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
            {
                int minid = block.ReadInt();
                int maxid = block.ReadInt();
                List<int> ids = new List<int>();
                if (minid < 0)
                {
                    ids.Add(-1);
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
                    mCallBackRegistorIds[clientId] = ids;
                }
                else
                {
                    mCallBackRegistorIds.Add(clientId, ids);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void ProcessResetValueChangedNotify(string clientId, IByteBuffer block)
        {
            string loginId = ReadString(block);
            if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
            {
                if (mCallBackRegistorIds.ContainsKey(clientId))
                {
                    mCallBackRegistorIds.Remove(clientId);
                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
