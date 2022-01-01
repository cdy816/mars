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
using System.Diagnostics;
using System.Text;
using Cdy.Tag;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Cheetah;
using System.Linq;

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

        /// <summary>
        /// 统计、查找某个时间段内的值
        /// </summary>
        public const byte RequestValueStatistics = 5;

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
        protected override void ProcessSingleData(string client, ByteBuffer data)
        {
            if (data.RefCount == 0)
            {
                Debug.Print("invailed data buffer in HisDataServerProcess");
                return;
            }
            byte cmd = data.ReadByte();
            long id = data.ReadLong();
            try
            {
                if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(id))
                {
                    switch (cmd)
                    {
                        case RequestAllHisData:
                            Task.Run(() =>
                            {
                                ProcessRequestAllHisDataByMemory(client, data);
                                data.UnlockAndReturn();
                            });
                            break;
                        case RequestHisDatasByTimePoint:
                            Task.Run(() => { ProcessRequestHisDatasByTimePointByMemory(client, data); data.UnlockAndReturn(); });
                            break;
                        case RequestHisDataByTimeSpan:
                            Task.Run(() => { ProcessRequestHisDataByTimeSpanByMemory(client, data); data.UnlockAndReturn(); });
                            break;
                        case RequestNumberStatistics:
                            Task.Run(() => { ProcessRequestNumberStatistics(client, data); data.UnlockAndReturn(); });
                            break;
                        case RequestNumberStatisticsByTimePoint:
                            Task.Run(() => { ProcessRequestStatisitcsDatasByTimePoint(client, data); data.UnlockAndReturn(); });
                            break;
                        case RequestValueStatistics:
                            Task.Run(() => { ProcessRequestValueStatistics(client, data); data.UnlockAndReturn(); });
                            break;
                        default:
                            data.UnlockAndReturn();
                            break;
                    }
                }
                else
                {
                    Parent.AsyncCallback(client, FunId, new byte[1], 0);
                    base.ProcessSingleData(client, data);
                }
            }
            catch(Exception ex)
            {
                LoggerService.Service.Warn("HisDataServerProcess", $"{ex.Message} {ex.StackTrace}");
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

        private unsafe ByteBuffer WriteDataToBufferByMemory<T>(int cid,byte type, HisQueryResult<T> resb)
        {
            var vdata = resb.Contracts();
            var re = Parent.Allocate(FunId, 5 + vdata.Size+4);
            re.Write(cid);
            re.Write(type);
            re.Write(resb.Count);
            re.Write(vdata.Address, vdata.Size);
            //Marshal.Copy(vdata.Address, re.Array, re.ArrayOffset + re.WriterIndex, vdata.Size);
            //re.SetWriterIndex(re.WriterIndex + vdata.Size);

            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type"></param>
        /// <param name="resb"></param>
        /// <returns></returns>
        private unsafe ByteBuffer WriteDataToBufferByMemory(int cid, byte type, NumberStatisticsQueryResult resb)
        {
            var vdata = resb;
            var re = Parent.Allocate(FunId, 5 + vdata.Position+4);
            re.Write(cid);
            re.Write(type);
            re.Write(resb.Count);
            re.Write(vdata.MemoryHandle, vdata.Position);
            //Marshal.Copy(vdata.MemoryHandle, re.Array, re.ArrayOffset+ re.WriterIndex, vdata.Position);
            //re.SetWriterIndex(re.WriterIndex + vdata.Position);

            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private unsafe void ProcessRequestNumberStatistics(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime sTime = new DateTime(data.ReadLong());
            DateTime eTime = new DateTime(data.ReadLong());
            int cid = data.ReadInt();
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            ByteBuffer re = null;

            if (tags != null)
            {
                re = WriteDataToBufferByMemory(cid,(byte)tags.Type, ProcessStatisticsDataQuery(id, sTime, eTime));
                Parent.AsyncCallback(clientId, re);
            }
            else
            {
                Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
            }
        }

        private unsafe void ProcessRequestAllHisDataByMemory(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime sTime = new DateTime(data.ReadLong());
            DateTime eTime = new DateTime(data.ReadLong());
            int cid = data.ReadInt();
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            ByteBuffer re = null;

            if (tags != null)
            {
                switch (tags.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        re = WriteDataToBufferByMemory(cid,(byte)tags.Type, ProcessDataQuery<bool>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<byte>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<DateTime>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<double>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<float>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<int>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<long>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<short>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<string>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<bool>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ulong>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ushort>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<IntPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<UIntPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<LongPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ULongPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, sTime, eTime));
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
        private void ProcessRequestStatisitcsDatasByTimePoint(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            int count = data.ReadInt();
            List<DateTime> times = new List<DateTime>();
            for (int i = 0; i < count; i++)
            {
                times.Add(new DateTime(data.ReadLong()));
            }
            int cid = data.ReadInt();
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            if (tags != null)
            {
               var  re = WriteDataToBufferByMemory(cid,(byte)tags.Type, ProcessStatisticsDataQuery(id, times));
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
        private void ProcessRequestHisDatasByTimePointByMemory(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            Cdy.Tag.QueryValueMatchType type = (QueryValueMatchType)data.ReadByte();
            int count = data.ReadInt();
            List<DateTime> times = new List<DateTime>();
            for(int i=0;i<count;i++)
            {
                times.Add(new DateTime(data.ReadLong()));
            }
            int cid = data.ReadInt();
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            ByteBuffer re = null;

            if (tags != null)
            {
                switch (tags.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        re = WriteDataToBufferByMemory(cid,(byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<byte>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<DateTime>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<double>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<float>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<int>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<long>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<short>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<string>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ulong>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ushort>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<IntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<UIntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<LongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ULongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, times, type));
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
        private void ProcessRequestHisDataByTimeSpanByMemory(string clientId, ByteBuffer data)
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
            int cid = data.ReadInt();
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            ByteBuffer re = null;

            if (tags != null)
            {
                switch (tags.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        re = WriteDataToBufferByMemory(cid,(byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<byte>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<DateTime>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<double>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<float>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<int>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<long>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<short>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<string>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ulong>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ushort>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<IntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<UIntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<LongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ULongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, times, type));
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


        private void ProcessRequestValueStatistics(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime stime = new DateTime(data.ReadLong());
            DateTime etime = new DateTime(data.ReadLong());
            int cid = data.ReadInt();
            byte typ = data.ReadByte();//类型
            

            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);
            ByteBuffer re=null;
            switch (typ)
            {
                case 0:
                    //FindNumberTagValue
                    string[] sdata = data.ReadString().Split("|");
                   
                    Tuple< DateTime,object> dt=null;
                    switch (tags.Type)
                    {
                        case Cdy.Tag.TagType.Byte:
                            dt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValue<byte>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Double:
                            dt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValue<double>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Float:
                            dt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValue<float>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Int:
                            dt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValue<int>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Long:
                            dt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValue<long>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Short:
                            dt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValue<short>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.UInt:
                            dt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValue<uint>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.ULong:
                            dt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValue<ulong>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.UShort:
                            dt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValue<ushort>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                    }
                    re = Parent.Allocate(FunId, 1 + 8);
                    re.Write(cid);
                    re.Write(dt.Item1);
                    re.Write(Convert.ToDouble(dt.Item2));
                    break;
                case 1:
                    //FindNumberTagValues
                    sdata = data.ReadString().Split("|");
                    Dictionary<DateTime,object> dts=null;
                    switch (tags.Type)
                    {
                        case Cdy.Tag.TagType.Byte:
                            dts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValues<byte>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Double:
                            dts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValues<double>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Float:
                            dts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValues<float>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Int:
                            dts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValues<int>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Long:
                            dts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValues<long>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Short:
                            dts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValues<short>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.UInt:
                            dts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValues<uint>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.ULong:
                            dts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValues<ulong>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.UShort:
                            dts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValues<ushort>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;

                    }
                    if (dts != null)
                    {
                        re = Parent.Allocate(FunId, 1 + dts.Count*8+4);
                        re.Write(cid);
                        re.Write(dts.Count);
                        foreach(var vv in dts)
                        {
                            re.Write(vv.Key);
                            re.Write(Convert.ToDouble(vv.Value));
                        }
                    }
                    break;
                case 2:
                    //FindNumberTagValueDuration
                    sdata = data.ReadString().Split("|");

                    double dtmp=0;
                    switch (tags.Type)
                    {
                        case Cdy.Tag.TagType.Byte:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValueDuration<byte>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Double:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValueDuration<double>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Float:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValueDuration<float>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Int:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValueDuration<int>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Long:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValueDuration<long>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.Short:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValueDuration<short>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.UInt:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValueDuration<uint>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.ULong:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValueDuration<ulong>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.UShort:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValueDuration<ushort>(id, stime, etime, Convert.ToDouble(sdata[1]), Convert.ToDouble(sdata[2]), (NumberStatisticsType)Convert.ToByte(sdata[0]));
                            break;

                    }
                    re = Parent.Allocate(FunId, 1 + 8);
                    re.Write(cid);
                    re.Write(dtmp);
                    break;
                case 3:
                    //FindNumberTagMaxMinValue
                    sdata = data.ReadString().Split("|");

                    dtmp = 0;
                    IEnumerable<DateTime> times=null;
                    switch (tags.Type)
                    {
                        case Cdy.Tag.TagType.Byte:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagMaxMinValue<byte>(id, stime, etime, (NumberStatisticsType)Convert.ToByte(sdata[0]), out times);
                            break;
                        case Cdy.Tag.TagType.Double:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagMaxMinValue<double>(id, stime, etime, (NumberStatisticsType)Convert.ToByte(sdata[0]), out times);
                            break;
                        case Cdy.Tag.TagType.Float:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagMaxMinValue<float>(id, stime, etime, (NumberStatisticsType)Convert.ToByte(sdata[0]), out times);
                            break;
                        case Cdy.Tag.TagType.Int:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagMaxMinValue<int>(id, stime, etime, (NumberStatisticsType)Convert.ToByte(sdata[0]), out times);
                            break;
                        case Cdy.Tag.TagType.Long:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagMaxMinValue<long>(id, stime, etime, (NumberStatisticsType)Convert.ToByte(sdata[0]), out times);
                            break;
                        case Cdy.Tag.TagType.Short:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagMaxMinValue<short>(id, stime, etime, (NumberStatisticsType)Convert.ToByte(sdata[0]), out times);
                            break;
                        case Cdy.Tag.TagType.UInt:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagMaxMinValue<uint>(id, stime, etime, (NumberStatisticsType)Convert.ToByte(sdata[0]), out times);
                            break;
                        case Cdy.Tag.TagType.ULong:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagMaxMinValue<ulong>(id, stime, etime, (NumberStatisticsType)Convert.ToByte(sdata[0]), out times);
                            break;
                        case Cdy.Tag.TagType.UShort:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagMaxMinValue<ushort>(id, stime, etime, (NumberStatisticsType)Convert.ToByte(sdata[0]), out times);
                            break;

                    }
                    if (times != null)
                    {
                        re = Parent.Allocate(FunId, 1 + 8 + times.Count() * 8 + 4);
                        re.Write(cid);
                        re.Write(dtmp);
                        re.Write(times.Count());
                        foreach(var vv in times)
                        {
                            re.Write(vv);
                        }
                    }
                    break;
                case 4:
                    //FindNumberTagAvgValue
                    sdata = data.ReadString().Split("|");

                    dtmp = 0;
                    switch (tags.Type)
                    {
                        case Cdy.Tag.TagType.Byte:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagAvgValue<byte>(id, stime, etime);
                            break;
                        case Cdy.Tag.TagType.Double:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagAvgValue<double>(id, stime, etime);
                            break;
                        case Cdy.Tag.TagType.Float:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagAvgValue<float>(id, stime, etime);
                            break;
                        case Cdy.Tag.TagType.Int:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagAvgValue<int>(id, stime, etime);
                            break;
                        case Cdy.Tag.TagType.Long:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagAvgValue<long>(id, stime, etime);
                            break;
                        case Cdy.Tag.TagType.Short:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagAvgValue<short>(id, stime, etime);
                            break;
                        case Cdy.Tag.TagType.UInt:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagAvgValue<uint>(id, stime, etime);
                            break;
                        case Cdy.Tag.TagType.ULong:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagAvgValue<ulong>(id, stime, etime);
                            break;
                        case Cdy.Tag.TagType.UShort:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagAvgValue<ushort>(id, stime, etime);
                            break;

                    }
                    re = Parent.Allocate(FunId, 1 + 8);
                    re.Write(cid);
                    re.Write(dtmp);
                    break;

                case 10:
                    //FindNoNumberTagValue
                   DateTime  dtt = DateTime.MinValue;
                    sdata = data.ReadString().Split("|");
                    switch (tags.Type)
                    {
                        case Cdy.Tag.TagType.Bool:
                            bool btmp =bool.Parse(sdata[0]);
                            dtt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValue<bool>(id, stime, etime,btmp);
                            break;
                        case Cdy.Tag.TagType.DateTime:
                            DateTime dttmp = DateTime.FromBinary(long.Parse(sdata[0]));
                            dtt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValue<DateTime>(id, stime, etime, dttmp);
                            break;
                        case Cdy.Tag.TagType.String:
                            dtt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValue<string>(id, stime, etime, sdata[0]);
                            break;
                        case Cdy.Tag.TagType.IntPoint:
                            dtt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValue<IntPointData>(id, stime, etime,IntPointData.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.UIntPoint:
                            dtt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValue<UIntPointData>(id, stime, etime, UIntPointData.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.IntPoint3:
                            dtt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValue<IntPoint3Data>(id, stime, etime, IntPoint3Data.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.UIntPoint3:
                            dtt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValue<UIntPoint3Data>(id, stime, etime, UIntPoint3Data.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.LongPoint:
                            dtt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValue<LongPointData>(id, stime, etime, LongPointData.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.ULongPoint:
                            dtt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValue<ULongPointData>(id, stime, etime, ULongPointData.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.LongPoint3:
                            dtt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValue<LongPoint3Data>(id, stime, etime, LongPoint3Data.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.ULongPoint3:
                            dtt = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValue<ULongPoint3Data>(id, stime, etime, ULongPoint3Data.FromString(sdata[0]));
                            break;

                    }
                    re = Parent.Allocate(FunId, 1 + 8);
                    re.Write(cid);
                    re.Write(dtt);
                    break;
                case 11:
                    //FindNoNumberTagValues
                    List<DateTime> dtts = null;
                    sdata = data.ReadString().Split("|");
                    switch (tags.Type)
                    {
                        case Cdy.Tag.TagType.Bool:
                            bool btmp = bool.Parse(sdata[0]);
                            dtts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValues<bool>(id, stime, etime, btmp);
                            break;
                        case Cdy.Tag.TagType.DateTime:
                            DateTime dttmp = DateTime.FromBinary(long.Parse(sdata[0]));
                            dtts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValues<DateTime>(id, stime, etime, dttmp);
                            break;
                        case Cdy.Tag.TagType.String:
                            dtts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValues<string>(id, stime, etime, sdata[0]);
                            break;
                        case Cdy.Tag.TagType.IntPoint:
                            dtts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValues<IntPointData>(id, stime, etime, IntPointData.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.UIntPoint:
                            dtts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValues<UIntPointData>(id, stime, etime, UIntPointData.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.IntPoint3:
                            dtts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValues<IntPoint3Data>(id, stime, etime, IntPoint3Data.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.UIntPoint3:
                            dtts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValues<UIntPoint3Data>(id, stime, etime, UIntPoint3Data.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.LongPoint:
                            dtts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValues<LongPointData>(id, stime, etime, LongPointData.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.ULongPoint:
                            dtts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValues<ULongPointData>(id, stime, etime, ULongPointData.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.LongPoint3:
                            dtts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValues<LongPoint3Data>(id, stime, etime, LongPoint3Data.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.ULongPoint3:
                            dtts = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValues<ULongPoint3Data>(id, stime, etime, ULongPoint3Data.FromString(sdata[0]));
                            break;

                    }

                    if (dtts != null)
                    {
                        re = Parent.Allocate(FunId, 1 + dtts.Count * 8 + 4);
                        re.Write(cid);
                        re.Write(dtts.Count);
                        foreach (var vv in dtts)
                        {
                            re.Write(vv);
                        }
                    }
                    break;
                case 12:
                    //FindNoNumberTagValueDuration
                    dtmp = 0;
                    sdata = data.ReadString().Split("|");
                    switch (tags.Type)
                    {
                        case Cdy.Tag.TagType.Bool:
                            bool btmp = bool.Parse(sdata[0]);
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValueDuration<bool>(id, stime, etime, btmp);
                            break;
                        case Cdy.Tag.TagType.DateTime:
                            DateTime dttmp = DateTime.FromBinary(long.Parse(sdata[0]));
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValueDuration<DateTime>(id, stime, etime, dttmp);
                            break;
                        case Cdy.Tag.TagType.String:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValueDuration<string>(id, stime, etime, sdata[0]);
                            break;
                        case Cdy.Tag.TagType.IntPoint:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValueDuration<IntPointData>(id, stime, etime, IntPointData.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.UIntPoint:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValueDuration<UIntPointData>(id, stime, etime, UIntPointData.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.IntPoint3:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValueDuration<IntPoint3Data>(id, stime, etime, IntPoint3Data.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.UIntPoint3:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValueDuration<UIntPoint3Data>(id, stime, etime, UIntPoint3Data.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.LongPoint:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValueDuration<LongPointData>(id, stime, etime, LongPointData.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.ULongPoint:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValueDuration<ULongPointData>(id, stime, etime, ULongPointData.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.LongPoint3:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValueDuration<LongPoint3Data>(id, stime, etime, LongPoint3Data.FromString(sdata[0]));
                            break;
                        case Cdy.Tag.TagType.ULongPoint3:
                            dtmp = ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValueDuration<ULongPoint3Data>(id, stime, etime, ULongPoint3Data.FromString(sdata[0]));
                            break;

                    }
                    re = Parent.Allocate(FunId, 1 + 8);
                    re.Write(cid);
                    re.Write(dtmp);
                    break;
            }

            if(re!=null)
            {
                Parent.AsyncCallback(clientId, re);
            }
            else
            {
                Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
            }
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
