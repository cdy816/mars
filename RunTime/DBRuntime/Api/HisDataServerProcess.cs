//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/14 11:00:38.
//  Version 1.0
//  种道洋
//==============================================================

using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Cdy.Tag;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace DBRuntime.Api
{
    public class HisDataServerProcess : ServerProcessBase
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDatasByTimePoint = 0;
        
        /// <summary>
        /// 
        /// </summary>
        public const byte RequestAllHisData = 1;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDataByTimeSpan = 2;

        /// <summary>
        /// 读取数据的统计值
        /// </summary>
        public const byte RequestNumberStatistics = 3;

        /// <summary>
        /// 读取某个时间点的统计值
        /// </summary>
        public const byte RequestNumberStatisticsByTimePoint = 4;


        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        public override byte FunId => ApiFunConst.HisDataRequestFun;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        protected override void ProcessSingleData(string client, IByteBuffer data)
        {
            if (data.ReferenceCount == 0)
            {
                Debug.Print("invailed data buffer in HisDataServerProcess");
                return;
            }
            byte cmd = data.ReadByte();
            long id = data.ReadLong();
            if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(id))
            {
                switch (cmd)
                {
                    case RequestAllHisData:
                       Task.Run(()=> { ProcessRequestAllHisDataByMemory(client, data);
                           data.ReleaseBuffer();
                       });
                        break;
                    case RequestHisDatasByTimePoint:
                        Task.Run(()=> { ProcessRequestHisDatasByTimePointByMemory(client, data); data.ReleaseBuffer(); });
                        break;
                    case RequestHisDataByTimeSpan:
                        Task.Run(() => { ProcessRequestHisDataByTimeSpanByMemory(client, data); data.ReleaseBuffer(); });
                        break;
                    case RequestNumberStatistics:
                        Task.Run(() => { ProcessRequestNumberStatistics(client, data); data.ReleaseBuffer(); });
                        break;
                    case RequestNumberStatisticsByTimePoint:
                        Task.Run(() => { ProcessRequestStatisitcsDatasByTimePoint(client, data); data.ReleaseBuffer(); });
                        break;
                    default:
                        data.ReleaseBuffer();
                        break;
                }
            }
            else
            {
                Parent.AsyncCallback(client, FunId, new byte[1], 0);
                base.ProcessSingleData(client, data);
            }
            
        }

        #region Serise to IByteBuffer

        //private unsafe IByteBuffer WriteDataToBuffer(byte type,HisQueryResult<bool> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId,5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for(int i=0;i<resb.Count;i++)
        //    {
        //        var value = resb.GetValue(i, out time,out qu);
        //        re.WriteBoolean(value);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<byte> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteByte(value);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<short> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteShort(value);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<ushort> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteUnsignedShort(value);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}


        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<int> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteInt(value);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<uint> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteInt((int)value);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<long> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteLong((long)value);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}


        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<ulong> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteLong((long)value);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<double> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteDouble((double)value);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<float> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteFloat(value);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<DateTime> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteLong(value.Ticks);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<string> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteString(value);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="type"></param>
        ///// <param name="resb"></param>
        ///// <returns></returns>
        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<IntPointData> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteInt(value.X);
        //        re.WriteInt(value.Y);

        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<IntPoint3Data> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteInt(value.X);
        //        re.WriteInt(value.Y);
        //        re.WriteInt(value.Z);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<UIntPointData> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteInt((int)value.X);
        //        re.WriteInt((int)value.Y);

        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<UIntPoint3Data> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteInt((int)value.X);
        //        re.WriteInt((int)value.Y);
        //        re.WriteInt((int)value.Z);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="type"></param>
        ///// <param name="resb"></param>
        ///// <returns></returns>
        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<LongPointData> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteLong(value.X);
        //        re.WriteLong(value.Y);

        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="type"></param>
        ///// <param name="resb"></param>
        ///// <returns></returns>
        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<ULongPointData> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteLong((long)value.X);
        //        re.WriteLong((long)value.Y);

        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<LongPoint3Data> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteLong(value.X);
        //        re.WriteLong(value.Y);
        //        re.WriteLong(value.Z);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}

        //private unsafe IByteBuffer WriteDataToBuffer(byte type, HisQueryResult<ULongPoint3Data> resb)
        //{
        //    var re = BufferManager.Manager.Allocate(FunId, 5 + resb.Position);
        //    re.WriteByte(type);
        //    re.WriteInt(resb.Count);
        //    DateTime time;
        //    byte qu;
        //    for (int i = 0; i < resb.Count; i++)
        //    {
        //        var value = resb.GetValue(i, out time, out qu);
        //        re.WriteLong((long)value.X);
        //        re.WriteLong((long)value.Y);
        //        re.WriteLong((long)value.Z);
        //        re.WriteLong(time.Ticks);
        //        re.WriteByte(qu);
        //    }
        //    return re;
        //}
        #endregion

        private unsafe IByteBuffer WriteDataToBufferByMemory<T>(byte type, HisQueryResult<T> resb)
        {
            var vdata = resb.Contracts();
            var re = BufferManager.Manager.Allocate(FunId, 5 + vdata.Size);
            re.WriteByte(type);
            re.WriteInt(resb.Count);

            Marshal.Copy(vdata.Address, re.Array, re.ArrayOffset + re.WriterIndex, vdata.Size);
            re.SetWriterIndex(re.WriterIndex + vdata.Size);

            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type"></param>
        /// <param name="resb"></param>
        /// <returns></returns>
        private unsafe IByteBuffer WriteDataToBufferByMemory(byte type, NumberStatisticsQueryResult resb)
        {
            var vdata = resb;
            var re = BufferManager.Manager.Allocate(FunId, 5 + vdata.Position);
            re.WriteByte(type);
            re.WriteInt(resb.Count);
         
            Marshal.Copy(vdata.MemoryHandle, re.Array, re.ArrayOffset+ re.WriterIndex, vdata.Position);
            re.SetWriterIndex(re.WriterIndex + vdata.Position);
            
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private unsafe void ProcessRequestNumberStatistics(string clientId, IByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime sTime = new DateTime(data.ReadLong());
            DateTime eTime = new DateTime(data.ReadLong());
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            IByteBuffer re = null;

            if (tags != null)
            {
                re = WriteDataToBufferByMemory((byte)tags.Type, ProcessStatisticsDataQuery(id, sTime, eTime));
                Parent.AsyncCallback(clientId, re);
            }
            else
            {
                Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
            }
        }

        private unsafe void ProcessRequestAllHisDataByMemory(string clientId, IByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime sTime = new DateTime(data.ReadLong());
            DateTime eTime = new DateTime(data.ReadLong());
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            IByteBuffer re = null;

            if (tags != null)
            {
                switch (tags.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<bool>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<byte>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<DateTime>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<double>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<float>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<int>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<long>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<short>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<string>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<bool>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<ulong>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<ushort>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<IntPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<UIntPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<LongPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<ULongPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, sTime, eTime));
                        break;
                }
                Parent.AsyncCallback(clientId, re);
            }
            else
            {
                Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private void ProcessRequestStatisitcsDatasByTimePoint(string clientId, IByteBuffer data)
        {
            int id = data.ReadInt();
            int count = data.ReadInt();
            List<DateTime> times = new List<DateTime>();
            for (int i = 0; i < count; i++)
            {
                times.Add(new DateTime(data.ReadLong()));
            }
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            if (tags != null)
            {
               var  re = WriteDataToBufferByMemory((byte)tags.Type, ProcessStatisticsDataQuery(id, times));
                Parent.AsyncCallback(clientId, re);
            }
            else
            {
                Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private void ProcessRequestHisDatasByTimePointByMemory(string clientId, IByteBuffer data)
        {
            int id = data.ReadInt();
            Cdy.Tag.QueryValueMatchType type = (QueryValueMatchType)data.ReadByte();
            int count = data.ReadInt();
            List<DateTime> times = new List<DateTime>();
            for(int i=0;i<count;i++)
            {
                times.Add(new DateTime(data.ReadLong()));
            }
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            IByteBuffer re = null;

            if (tags != null)
            {
                switch (tags.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<byte>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<DateTime>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<double>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<float>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<int>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<long>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<short>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<string>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<ulong>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<ushort>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<IntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<UIntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<LongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<ULongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, times, type));
                        break;
                }
                Parent.AsyncCallback(clientId, re);
            }
            else
            {
                Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private void ProcessRequestHisDataByTimeSpanByMemory(string clientId, IByteBuffer data)
        {
            int id = data.ReadInt();
            Cdy.Tag.QueryValueMatchType type = (QueryValueMatchType)data.ReadByte();
            DateTime stime = new DateTime(data.ReadLong());
            DateTime etime = new DateTime(data.ReadLong());
            TimeSpan ts = new TimeSpan(data.ReadLong());
            List<DateTime> times = new List<DateTime>();
            DateTime tmp = stime;
            while (tmp <= etime)
            {
                times.Add(tmp);
                tmp += ts;
            }
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            IByteBuffer re = null;

            if (tags != null)
            {
                switch (tags.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<byte>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<DateTime>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<double>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<float>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<int>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<long>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<short>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<string>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<ulong>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<ushort>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<IntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<UIntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<LongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<ULongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBufferByMemory((byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, times, type));
                        break;
                }
                Parent.AsyncCallback(clientId, re);
            }
            else
            {
                Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
            }
        }



        private HisQueryResult<T> ProcessDataQuery<T>(int id, DateTime stime, DateTime etime)
        {
            return ServiceLocator.Locator.Resolve<IHisQuery>().ReadAllValue<T>(id, stime, etime);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="stime"></param>
        /// <param name="etime"></param>
        /// <returns></returns>
        private NumberStatisticsQueryResult ProcessStatisticsDataQuery(int id, DateTime stime, DateTime etime)
        {
            return ServiceLocator.Locator.Resolve<IHisQuery>().ReadNumberStatistics(id, stime, etime);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <returns></returns>
        private NumberStatisticsQueryResult ProcessStatisticsDataQuery(int id, IEnumerable<DateTime> times)
        {
            return ServiceLocator.Locator.Resolve<IHisQuery>().ReadNumberStatistics(id, times);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        private HisQueryResult<T> ProcessDataQuery<T>(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType type)
        {
            return ServiceLocator.Locator.Resolve<IHisQuery>().ReadValue<T>(id, times, type);
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="data"></param>
        //private unsafe void ProcessRequestAllHisData(string clientId, IByteBuffer data)
        //{
        //    int id = data.ReadInt();
        //    DateTime sTime = new DateTime(data.ReadLong());
        //    DateTime eTime = new DateTime(data.ReadLong());
        //    var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

        //    IByteBuffer re = null;

        //    if (tags != null)
        //    {
        //        switch (tags.Type)
        //        {
        //            case Cdy.Tag.TagType.Bool:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<bool>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.Byte:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<byte>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.DateTime:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<DateTime>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.Double:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<double>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.Float:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<float>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.Int:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<int>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.Long:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<long>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.Short:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<short>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.String:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<string>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.UInt:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<bool>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.ULong:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<ulong>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.UShort:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<ushort>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.IntPoint:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<IntPointData>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.UIntPoint:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<UIntPointData>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.IntPoint3:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.UIntPoint3:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.LongPoint:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<LongPointData>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.ULongPoint:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<ULongPointData>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.LongPoint3:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, sTime, eTime));
        //                break;
        //            case Cdy.Tag.TagType.ULongPoint3:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, sTime, eTime));
        //                break;
        //        }
        //        Parent.AsyncCallback(clientId, re);
        //    }
        //    else
        //    {
        //        Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="data"></param>
        //private void ProcessRequestHisDatasByTimePoint(string clientId, IByteBuffer data)
        //{
        //    int id = data.ReadInt();
        //    Cdy.Tag.QueryValueMatchType type = (QueryValueMatchType)data.ReadByte();
        //    int count = data.ReadInt();
        //    List<DateTime> times = new List<DateTime>();
        //    for(int i=0;i<count;i++)
        //    {
        //        times.Add(new DateTime(data.ReadLong()));
        //    }
        //    var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

        //    IByteBuffer re = null;

        //    if (tags != null)
        //    {
        //        switch (tags.Type)
        //        {
        //            case Cdy.Tag.TagType.Bool:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.Byte:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<byte>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.DateTime:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<DateTime>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.Double:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<double>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.Float:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<float>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.Int:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<int>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.Long:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<long>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.Short:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<short>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.String:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<string>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.UInt:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.ULong:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<ulong>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.UShort:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<ushort>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.IntPoint:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<IntPointData>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.UIntPoint:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<UIntPointData>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.IntPoint3:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.UIntPoint3:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.LongPoint:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<LongPointData>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.ULongPoint:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<ULongPointData>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.LongPoint3:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.ULongPoint3:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, times, type));
        //                break;
        //        }
        //        Parent.AsyncCallback(clientId, re);
        //    }
        //    else
        //    {
        //        Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="data"></param>
        //private void ProcessRequestHisDataByTimeSpan(string clientId, IByteBuffer data)
        //{
        //    int id = data.ReadInt();
        //    Cdy.Tag.QueryValueMatchType type = (QueryValueMatchType)data.ReadByte();
        //    DateTime stime = new DateTime(data.ReadLong());
        //    DateTime etime = new DateTime(data.ReadLong());
        //    TimeSpan ts = new TimeSpan(data.ReadLong());
        //    List<DateTime> times = new List<DateTime>();
        //    DateTime tmp = stime;
        //    while(tmp<=etime)
        //    {
        //        times.Add(tmp);
        //        tmp += ts;
        //    }
        //    var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

        //    IByteBuffer re = null;

        //    if (tags != null)
        //    {
        //        switch (tags.Type)
        //        {
        //            case Cdy.Tag.TagType.Bool:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.Byte:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<byte>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.DateTime:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<DateTime>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.Double:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<double>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.Float:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<float>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.Int:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<int>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.Long:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<long>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.Short:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<short>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.String:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<string>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.UInt:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.ULong:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<ulong>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.UShort:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<ushort>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.IntPoint:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<IntPointData>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.UIntPoint:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<UIntPointData>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.IntPoint3:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.UIntPoint3:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.LongPoint:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<LongPointData>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.ULongPoint:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<ULongPointData>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.LongPoint3:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, times, type));
        //                break;
        //            case Cdy.Tag.TagType.ULongPoint3:
        //                re = WriteDataToBuffer((byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, times, type));
        //                break;
        //        }
        //        Parent.AsyncCallback(clientId, re);
        //    }
        //    else
        //    {
        //        Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
        //    }
        //}

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
