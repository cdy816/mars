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
using Cdy.Tag.Driver;

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

        /// <summary>
        /// 修改历史数据
        /// </summary>
        public const byte ModifyHisData= 6;

        /// <summary>
        /// 删除历史数据
        /// </summary>
        public const byte DeleteHisData = 7;


        /// <summary>
        /// 根据时间间隔查询数据的历史记录，忽略系统退出的影响
        /// </summary>
        public const byte RequestHisDataByTimeSpanByIgnorClosedQuality = 11;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDatasByTimePointByIgnorClosedQuality = 10;

        /// <summary>
        /// 通过SQL查询历史
        /// </summary>
        public const byte ReadHisValueBySQL = 140;

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
                        case ReadHisValueBySQL:
                            Task.Run(() =>
                            {
                                ProcessQueryBySQL(client, data);
                                data.UnlockAndReturn();
                            });
                            break;
                        case RequestHisDatasByTimePoint:
                            Task.Run(() => { ProcessRequestHisDatasByTimePoint(client, data); data.UnlockAndReturn(); });
                            break;
                        case RequestHisDatasByTimePointByIgnorClosedQuality:
                            Task.Run(() => { ProcessRequestHisDatasByTimePointByIgnoreClosedQualtiy(client, data); data.UnlockAndReturn(); });
                            break;
                        case RequestHisDataByTimeSpan:
                            Task.Run(() => { ProcessRequestHisDataByTimeSpanByMemory(client, data); data.UnlockAndReturn(); });
                            break;
                        case RequestHisDataByTimeSpanByIgnorClosedQuality:
                            Task.Run(() => { ProcessRequestHisDataByTimeSpanByMemoryByIgnorClosedQuality(client, data); data.UnlockAndReturn(); });
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
                        case ModifyHisData:
                            Task.Run(() => { ProcessModifyHisData(client, data); data.UnlockAndReturn(); });
                            break;
                        case DeleteHisData:
                            Task.Run(() => { ProcessDeleteHisData(client, data); data.UnlockAndReturn(); });
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
            //var vdata = resb;
            var re = Parent.Allocate(FunId, 5 + vdata.Size+4);
            re.Write(cid);
            re.Write(type);
            re.Write(resb.Count);
            re.Write(vdata.Address, vdata.Size);

            resb.Dispose();
            vdata.Dispose();
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

            resb.Dispose();

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
                re = Parent.Allocate(FunId, 5 + 4);
                re.Write(cid);
                re.Write(0);
                re.Write(0);
                Parent.AsyncCallback(clientId, re);
                //Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
            }
        }

        private SqlExpress ParseSql(string sql, out List<int> selecttag, out Dictionary<int, byte> tagtps)
        {
            var sqlexp = new SqlExpress().FromString(sql);
            List<string> ls = new List<string>();
            var serice = ServiceLocator.Locator.Resolve<ITagManager>();

            if (sqlexp.Select.Selects.Count > 0 && sqlexp.Select.Selects[0].TagName == "*" && !string.IsNullOrEmpty(sqlexp.From))
            {
                var tags = serice.GetTagByArea(sqlexp.From);
                if (tags != null && tags.Count > 0)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach (var vv in tags.Select(e => e.FullName))
                    {
                        sb.Append(vv.ToString() + ",");
                    }
                    sb.Length = sb.Length > 0 ? sb.Length - 1 : sb.Length;
                    sql = sql.Replace("*", sb.ToString());

                    sqlexp = new SqlExpress().FromString(sql);
                }
                else
                {
                    string sgroup=sqlexp.From;
                    if(sgroup=="_root")
                    {
                        sgroup = "";
                    }
                    tags = serice.GetTagsByGroup(sgroup);
                    if(tags != null && tags.Count > 0)
                    {
                        StringBuilder sb = new StringBuilder();
                        foreach (var vv in tags.Select(e => e.FullName))
                        {
                            sb.Append(vv.ToString() + ",");
                        }
                        sb.Length = sb.Length > 0 ? sb.Length - 1 : sb.Length;
                        sql = sql.Replace("*", sb.ToString());

                        sqlexp = new SqlExpress().FromString(sql);
                    }
                }
            }
            Dictionary<int, byte> tps = new Dictionary<int, byte>();
            List<int> selids = new List<int>();
            if (sqlexp.Select != null)
            {
                foreach (var vv in sqlexp.Select.Selects)
                {
                    var tag = serice.GetTagByName(vv.TagName);
                    if (!tps.ContainsKey(tag.Id))
                    {
                        tps.Add(tag.Id, (byte)tag.Type);
                    }
                    selids.Add(tag.Id);
                }
            }

            if (sqlexp.Where != null)
            {
                FillTagIds(sqlexp.Where, tps);
            }
            selecttag = selids;
            tagtps = tps;
            return sqlexp;
        }

        private void FillTagIds(ExpressFilter filter, Dictionary<int, byte> tps)
        {
            var serice = ServiceLocator.Locator.Resolve<ITagManager>();
            foreach (var vv in filter.Filters)
            {
                if (vv is ExpressFilter)
                {
                    FillTagIds(vv as ExpressFilter, tps);
                }
                else
                {
                    var fa = (vv as FilterAction);
                    if (fa.TagName.ToLower() == "time")
                    {
                        continue;
                    }
                    var tag = serice.GetTagByName(fa.TagName);
                    if (tag != null)
                    {
                        fa.TagId = tag.Id;
                        if (!tps.ContainsKey(tag.Id))
                        {
                            tps.Add(tag.Id, (byte)tag.Type);
                        }
                    }
                    else
                    {
                        throw new Exception($"tag {fa.TagName} 不存在!");
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cc"></param>
        /// <param name="re"></param>
        private void ProcessRealData(List<int> cc, int len, ByteBuffer re)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            re.Write(len);
            DateTime tmp = DateTime.UtcNow;
            //foreach (var vv in cc)
            for (int i = 0; i < len; i++)
            {
                var vv = cc[i];

                re.Write(vv);

                byte qu, type;
                DateTime time;
                object value;

                if (service.IsComplexTag(vv))
                {
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
                                string sval = vtmp.Value.ToString();
                                re.Write(sval);
                                //re.Write(sval.Length);
                                //re.Write(sval, Encoding.Unicode);
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

                    re.Write(tmp.Ticks);
                    re.WriteByte((byte)QualityConst.Null);
                }
                else
                {
                    value = service.GetTagValue(vv, out qu, out time, out type);

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
                            case (byte)TagType.Complex:
                                break;
                        }

                        re.Write(time.Ticks);
                        re.WriteByte(qu);
                    }
                    else
                    {
                        re.WriteByte((byte)TagType.Byte);
                        re.WriteByte(0);
                        re.Write(tmp.Ticks);
                        re.WriteByte((byte)QualityConst.Null);
                    }
                }
            }
        }
        /// <summary>
        /// 查询实时值
        /// </summary>
        /// <param name="selids"></param>
        /// <param name="id"></param>
        private ByteBuffer ProcessQueryRealValueBySql(List<int> selids, int id)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagProduct>();
            var re = Parent.Allocate(FunId, 10 + 8 * selids.Count);
            re.Write(id);
            re.Write((byte)ReadHisValueBySQL);
            re.Write((byte)2);
            ProcessRealData(selids, selids.Count, re);
            return re;
        }
        private void ProcessQueryBySQL(string clientid, ByteBuffer block)
        {
            try
            {
                string sql = block.ReadString();
                int id = block.ReadInt();
                if (!string.IsNullOrEmpty(sql))
                {
                    var sqlexp = ParseSql(sql, out List<int> selids, out Dictionary<int, byte> tps);
                    if (sqlexp.Where == null || (sqlexp.Where.LowerTime == null && sqlexp.Where.UpperTime == null))
                    {
                        var re = ProcessQueryRealValueBySql(selids, id);
                        Parent.AsyncCallback(clientid, re);
                        return;
                    }
                    else if (sqlexp.Where.UpperTime == null)
                    {
                        sqlexp.Where.UpperTime = new LowerEqualAction() { IgnorFit = true, Target = DateTime.Now.ToString() };
                    }
                    else if (sqlexp.Where.LowerTime == null)
                    {
                        LoggerService.Service.Warn("DirectAccess", $"Sql 语句格式不支持.");
                        return;
                    }
                    var qq = ServiceLocator.Locator.Resolve<IHisQuery>().ReadAllValueAndFilter(selids, DateTime.Parse(sqlexp.Where.LowerTime.Target.ToString()), DateTime.Parse(sqlexp.Where.UpperTime.Target.ToString()), sqlexp.Where, tps);
                    if (qq != null)
                    {
                        if (sqlexp.Select.IsAllNone())
                        {
                            var smetas = qq.SeriseMeta();
                            var re = Parent.Allocate(FunId, 12 + qq.AvaiableSize + smetas.Length * 2);
                            re.Write(id);
                            re.Write((byte)ReadHisValueBySQL);
                            re.Write((byte)0);
                            re.Write(smetas);
                            re.Write(qq.AvaiableSize);
                            re.Write(qq.Address, qq.AvaiableSize);
                            Parent.AsyncCallback(clientid, re);
                            //直接返回表格内容
                        }
                        else
                        {
                            //做二次计算值
                            List<object> vals = new List<object>();
                            foreach (var vv in sqlexp.Select.Selects)
                            {
                                vals.Add(vv.Cal(qq));
                            }

                            var re = Parent.Allocate(FunId, 10 + 8 * vals.Count);
                            re.Write(id);
                            re.Write((byte)ReadHisValueBySQL);
                            re.Write((byte)1);
                            re.Write(vals.Count);
                            foreach (var vv in vals)
                            {
                                re.Write(Convert.ToDouble(vv));
                            }
                            Parent.AsyncCallback(clientid, re);
                        }
                        qq.Dispose();
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("DirectAccess", ex.Message);
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
        private void ProcessRequestHisDatasByTimePoint(string clientId, ByteBuffer data)
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
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<uint>(id, times, type));
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
                //Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
                re = Parent.Allocate(FunId, 5 + 4);
                re.Write(cid);
                re.Write(0);
                re.Write(0);
                Parent.AsyncCallback(clientId, re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private void ProcessRequestHisDatasByTimePointByIgnoreClosedQualtiy(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            Cdy.Tag.QueryValueMatchType type = (QueryValueMatchType)data.ReadByte();
            int count = data.ReadInt();
            List<DateTime> times = new List<DateTime>();
            for (int i = 0; i < count; i++)
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
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<byte>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<DateTime>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<double>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<float>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<int>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<long>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<short>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<string>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<uint>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ulong>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ushort>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<IntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<UIntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<IntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<UIntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<LongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ULongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<LongPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ULongPoint3Data>(id, times, type));
                        break;
                }
                Parent.AsyncCallback(clientId, re);
            }
            else
            {
                //Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
                re = Parent.Allocate(FunId, 5 + 4);
                re.Write(cid);
                re.Write(0);
                re.Write(0);
                Parent.AsyncCallback(clientId, re);
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
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<uint>(id, times, type));
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
                re = Parent.Allocate(FunId, 5 + 4);
                re.Write(cid);
                re.Write(0);
                re.Write(0);
                Parent.AsyncCallback(clientId, re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private void ProcessRequestHisDataByTimeSpanByMemoryByIgnorClosedQuality(string clientId, ByteBuffer data)
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
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<byte>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<DateTime>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<double>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<float>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<int>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<long>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<short>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<string>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<uint>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ulong>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ushort>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<IntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<UIntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<IntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<UIntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<LongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ULongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<LongPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ULongPoint3Data>(id, times, type));
                        break;
                }
                Parent.AsyncCallback(clientId, re);
            }
            else
            {
                re = Parent.Allocate(FunId, 5 + 4);
                re.Write(cid);
                re.Write(0);
                re.Write(0);
                Parent.AsyncCallback(clientId, re);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private void ProcessModifyHisData(string clientId,ByteBuffer data)
        {
            //Id,User,Msg
            int id = data.ReadInt();
            string user =data.ReadString();
            string msg = data.ReadString();
            TagType tp = (TagType)data.ReadByte();

            int count = data.ReadInt();
            int cid = data.ReadInt();
            var hp = ServiceLocator.Locator.Resolve<IHisDataPatch>();

            var re = Parent.Allocate(FunId, 5);
            re.Write(cid);
         
            

            if (hp != null)
            {
                var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);
                if (tags != null && tags.Type == tp)
                {
                    switch (tp)
                    {
                        case TagType.Bool:
                            HisQueryResult<bool> datas = new HisQueryResult<bool>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = data.ReadByte() > 0;
                                var qua = data.ReadByte();

                                datas.Add(bval,vtime,qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<bool>(id,datas,user,msg);
                            data.Dispose();
                            break;
                        case TagType.Byte:
                            HisQueryResult<byte> bdatas = new HisQueryResult<byte>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = data.ReadByte();
                                var qua = data.ReadByte();

                                bdatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<byte>(id, bdatas, user, msg);
                            data.Dispose();
                            break;
                        case TagType.UShort:
                            HisQueryResult<ushort> usdatas = new HisQueryResult<ushort>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = data.ReadUShort();
                                var qua = data.ReadByte();

                                usdatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<ushort>(id, usdatas, user, msg);
                            data.Dispose();
                            break;
                        case TagType.Short:
                            HisQueryResult<short> sdatas = new HisQueryResult<short>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = data.ReadShort();
                                var qua = data.ReadByte();

                                sdatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<short>(id, sdatas, user, msg);
                            data.Dispose();
                            break;
                        case TagType.Int:
                            HisQueryResult<int> idatas = new HisQueryResult<int>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = data.ReadInt();
                                var qua = data.ReadByte();

                                idatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<int>(id, idatas, user, msg);
                            data.Dispose();

                            break;
                        case TagType.UInt:
                            HisQueryResult<uint> uidatas = new HisQueryResult<uint>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = data.ReadUInt();
                                var qua = data.ReadByte();

                                uidatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<uint>(id, uidatas, user, msg);
                            data.Dispose();
                            break;
                        case TagType.Long:
                            HisQueryResult<long> ldatas = new HisQueryResult<long>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = data.ReadLong();
                                var qua = data.ReadByte();

                                ldatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<long>(id, ldatas, user, msg);
                            data.Dispose();
                            break;
                        case TagType.ULong:
                            HisQueryResult<ulong> uldatas = new HisQueryResult<ulong>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = data.ReadULong();
                                var qua = data.ReadByte();

                                uldatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<ulong>(id, uldatas, user, msg);
                            data.Dispose();
                            break;
                        case TagType.String:
                            HisQueryResult<string> ssdatas = new HisQueryResult<string>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = data.ReadString();
                                var qua = data.ReadByte();

                                ssdatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<string>(id, ssdatas, user, msg);
                            data.Dispose();
                            break;
                        case TagType.DateTime:
                            HisQueryResult<DateTime> ddatas = new HisQueryResult<DateTime>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = data.ReadDateTime();
                                var qua = data.ReadByte();

                                ddatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<DateTime>(id, ddatas, user, msg);
                            data.Dispose();
                            break;
                        case TagType.Double:
                            HisQueryResult<double> dddatas = new HisQueryResult<double>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = data.ReadDouble();
                                var qua = data.ReadByte();

                                dddatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<double>(id, dddatas, user, msg);
                            data.Dispose();
                            break;
                        case TagType.Float:
                            HisQueryResult<float> fdatas = new HisQueryResult<float>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = data.ReadFloat();
                                var qua = data.ReadByte();

                                fdatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<float>(id, fdatas, user, msg);
                            data.Dispose();
                            break;
                        case TagType.IntPoint:
                            HisQueryResult<IntPointData> ipdatas = new HisQueryResult<IntPointData>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = new IntPointData(data.ReadInt(), data.ReadInt());
                                var qua = data.ReadByte();

                                ipdatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<IntPointData>(id, ipdatas, user, msg);
                            data.Dispose();
                            break;
                        case TagType.UIntPoint:
                            HisQueryResult<UIntPointData> uipdatas = new HisQueryResult<UIntPointData>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = new UIntPointData(data.ReadUInt(), data.ReadUInt());
                                var qua = data.ReadByte();

                                uipdatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<UIntPointData>(id, uipdatas, user, msg);
                            data.Dispose();

                            break;
                        case TagType.IntPoint3:
                            HisQueryResult<IntPoint3Data> ip3datas = new HisQueryResult<IntPoint3Data>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = new IntPoint3Data(data.ReadInt(), data.ReadInt(), data.ReadInt());
                                var qua = data.ReadByte();

                                ip3datas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<IntPoint3Data>(id, ip3datas, user, msg);
                            data.Dispose();

                            break;
                        case TagType.UIntPoint3:
                            HisQueryResult<UIntPoint3Data> uip3datas = new HisQueryResult<UIntPoint3Data>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = new UIntPoint3Data(data.ReadUInt(), data.ReadUInt(), data.ReadUInt());
                                var qua = data.ReadByte();

                                uip3datas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<UIntPoint3Data>(id, uip3datas, user, msg);
                            data.Dispose();

                            break;
                        case TagType.LongPoint:
                            HisQueryResult<LongPointData> lipdatas = new HisQueryResult<LongPointData>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = new LongPointData(data.ReadLong(), data.ReadLong());
                                var qua = data.ReadByte();

                                lipdatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<LongPointData>(id, lipdatas, user, msg);
                            data.Dispose();

                            break;
                        case TagType.LongPoint3:
                            HisQueryResult<LongPoint3Data> lipdatas3 = new HisQueryResult<LongPoint3Data>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = new LongPoint3Data(data.ReadLong(), data.ReadLong(), data.ReadLong());
                                var qua = data.ReadByte();

                                lipdatas3.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<LongPoint3Data>(id, lipdatas3, user, msg);
                            data.Dispose();

                            break;
                        case TagType.ULongPoint:
                            HisQueryResult<ULongPointData> ulipdatas = new HisQueryResult<ULongPointData>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = new ULongPointData(data.ReadULong(), data.ReadULong());
                                var qua = data.ReadByte();

                                ulipdatas.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<ULongPointData>(id, ulipdatas, user, msg);
                            data.Dispose();
                            break;
                        case TagType.ULongPoint3:
                            HisQueryResult<ULongPoint3Data> ulipdatas3 = new HisQueryResult<ULongPoint3Data>(count);
                            for (int i = 0; i < count; i++)
                            {
                                var vtime = data.ReadDateTime().ToUniversalTime();
                                var bval = new ULongPoint3Data(data.ReadULong(), data.ReadULong(), data.ReadULong());
                                var qua = data.ReadByte();

                                ulipdatas3.Add(bval, vtime, qua);
                            }
                            ServiceLocator.Locator.Resolve<IHisQuery>().ModifyHisData<ULongPoint3Data>(id, ulipdatas3, user, msg);
                            data.Dispose();
                            break;
                    }


                    re.Write((byte)1);
                    Parent.AsyncCallback(clientId, re);
                }
                else
                {
                    re.Write((byte)0);
                    Parent.AsyncCallback(clientId, re);
                }
            }
            else
            {
                re.Write((byte)0);
                Parent.AsyncCallback(clientId, re);
            }

            
        }

        private object GetLastValue(int id,TagType type,DateTime sTime,DateTime eTime,out byte quality)
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
                    return ProcessDataQuery<uint>(id, sTime, eTime).GetLastValue(out quality);
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
        private object GetLastAvaiableData(int id,TagType type,DateTime time,out byte quality)
        {
            object re = null;
            DateTime stime;
            DateTime etime=time;
            DateTime sttime = time.AddDays(-1);

            do
            {
                stime = etime.AddMinutes(-5);
                 re = GetLastValue(id,type,stime,etime,out  quality);
                if(re!=null)
                {
                    return re;
                }
                etime = stime;
            }
            while(re== null&& etime> sttime);

            quality = (byte)QualityConst.Null;
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private void ProcessDeleteHisData(string clientId,ByteBuffer data)
        {
            int id = data.ReadInt();
            string user = data.ReadString();
            string msg = data.ReadString();
            DateTime stime = data.ReadDateTime().ToUniversalTime();
            DateTime etime = data.ReadDateTime().ToUniversalTime();

            int cid = data.ReadInt();
            var re = Parent.Allocate(FunId, 5);
            re.Write(cid);

            var hp = ServiceLocator.Locator.Resolve<IHisDataPatch>();
            if(hp != null)
            {
                var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);
                if(tags != null)
                {
                    switch (tags.Type)
                    {
                        case Cdy.Tag.TagType.Bool:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<bool>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.Byte:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<byte>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.DateTime:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<DateTime>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.Double:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<double>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.Float:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<float>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.Int:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<int>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.Long:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<long>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.Short:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<short>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.String:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<string>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.UInt:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<uint>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.ULong:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<ulong>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.UShort:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<ushort>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.IntPoint:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<IntPointData>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.UIntPoint:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<UIntPointData>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.IntPoint3:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<IntPoint3Data>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.UIntPoint3:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<UIntPoint3Data>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.LongPoint:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<LongPointData>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.ULongPoint:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<ULongPointData>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.LongPoint3:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<LongPoint3Data>(id, stime, etime, user, msg);
                            break;
                        case Cdy.Tag.TagType.ULongPoint3:
                            ServiceLocator.Locator.Resolve<IHisQuery>().DeleteHisData<ULongPoint3Data>(id, stime, etime, user, msg);
                            break;
                    }
                    

                    re.Write((byte)1);
                    Parent.AsyncCallback(clientId, re);
                }
                else
                {
                    re.Write((byte)0);
                    Parent.AsyncCallback(clientId, re);
                }
            }
            else
            {
                re.Write((byte)0);
                Parent.AsyncCallback(clientId, re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="stime"></param>
        /// <param name="etime"></param>
        /// <returns></returns>
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

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        private HisQueryResult<T> ProcessDataQueryByIgnorClosedQuality<T>(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType type)
        {
            return ServiceLocator.Locator.Resolve<IHisQuery>().ReadValueIgnorClosedQuality<T>(id, times, type);
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
