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
        public override void ProcessData(string client, IByteBuffer data)
        {
            if (data.ReferenceCount == 0)
            {
                Debug.Print("invailed data buffer in HisDataServerProcess");
                return;
            }
            byte cmd = data.ReadByte();
            string id = data.ReadString();
            if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(id))
            {
                switch(cmd)
                {
                    case RequestAllHisData:
                        ProcessRequestAllHisData(client,data);
                        break;
                    case RequestHisDatasByTimePoint:
                        ProcessRequestHisDatasByTimePoint(client, data);
                        break;
                    case RequestHisDataByTimeSpan:
                        ProcessRequestHisDataByTimeSpan(client, data);
                        break;
                }
            }
            else
            {
                Parent.AsyncCallback(client, FunId, new byte[1], 0);
            }
            base.ProcessData(client, data);
        }

        private unsafe IByteBuffer WriteDataToBuffer<T>(byte id,HisQueryResult<T> resb)
        {
            var re = BufferManager.Manager.Allocate(FunId,1 + resb.Position);
            re.WriteByte(id);
            Buffer.MemoryCopy((void*)(re.AddressOfPinnedMemory() + 2), (void*)resb.Address, resb.Position, resb.Position);
            re.SetWriterIndex(2 + resb.Position);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        private unsafe void ProcessRequestAllHisData(string clientId,IByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime sTime = new DateTime(data.ReadLong());
            DateTime eTime = new DateTime(data.ReadLong());
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            IByteBuffer re = null;

            if(tags!=null)
            {
                switch (tags.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        re = WriteDataToBuffer<bool>((byte)tags.Type, ProcessDataQuery<bool>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBuffer<byte>((byte)tags.Type, ProcessDataQuery<byte>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBuffer<DateTime>((byte)tags.Type, ProcessDataQuery<DateTime>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBuffer<double>((byte)tags.Type, ProcessDataQuery<double>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBuffer<float>((byte)tags.Type, ProcessDataQuery<float>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBuffer<int>((byte)tags.Type, ProcessDataQuery<int>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBuffer<long>((byte)tags.Type, ProcessDataQuery<long>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBuffer<short>((byte)tags.Type, ProcessDataQuery<short>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBuffer<string>((byte)tags.Type, ProcessDataQuery<string>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBuffer<bool>((byte)tags.Type, ProcessDataQuery<bool>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBuffer<ulong>((byte)tags.Type, ProcessDataQuery<ulong>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBuffer<ushort>((byte)tags.Type, ProcessDataQuery<ushort>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBuffer<IntPointData>((byte)tags.Type, ProcessDataQuery<IntPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBuffer<UIntPointData>((byte)tags.Type, ProcessDataQuery<UIntPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBuffer<IntPoint3Data>((byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBuffer<UIntPoint3Data>((byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBuffer<LongPointData>((byte)tags.Type, ProcessDataQuery<LongPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBuffer<ULongPointTag>((byte)tags.Type, ProcessDataQuery<ULongPointTag>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBuffer<LongPoint3Data>((byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBuffer<ULongPoint3Data>((byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, sTime, eTime));
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
        /// <param name="data"></param>
        private void ProcessRequestHisDatasByTimePoint(string clientId, IByteBuffer data)
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
                        re = WriteDataToBuffer<bool>((byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBuffer<byte>((byte)tags.Type, ProcessDataQuery<byte>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBuffer<DateTime>((byte)tags.Type, ProcessDataQuery<DateTime>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBuffer<double>((byte)tags.Type, ProcessDataQuery<double>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBuffer<float>((byte)tags.Type, ProcessDataQuery<float>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBuffer<int>((byte)tags.Type, ProcessDataQuery<int>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBuffer<long>((byte)tags.Type, ProcessDataQuery<long>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBuffer<short>((byte)tags.Type, ProcessDataQuery<short>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBuffer<string>((byte)tags.Type, ProcessDataQuery<string>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBuffer<bool>((byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBuffer<ulong>((byte)tags.Type, ProcessDataQuery<ulong>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBuffer<ushort>((byte)tags.Type, ProcessDataQuery<ushort>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBuffer<IntPointData>((byte)tags.Type, ProcessDataQuery<IntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBuffer<UIntPointData>((byte)tags.Type, ProcessDataQuery<UIntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBuffer<IntPoint3Data>((byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBuffer<UIntPoint3Data>((byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBuffer<LongPointData>((byte)tags.Type, ProcessDataQuery<LongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBuffer<ULongPointTag>((byte)tags.Type, ProcessDataQuery<ULongPointTag>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBuffer<LongPoint3Data>((byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBuffer<ULongPoint3Data>((byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, times, type));
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
        /// <param name="data"></param>
        private void ProcessRequestHisDataByTimeSpan(string clientId, IByteBuffer data)
        {
            int id = data.ReadInt();
            Cdy.Tag.QueryValueMatchType type = (QueryValueMatchType)data.ReadByte();
            DateTime stime = new DateTime(data.ReadLong());
            DateTime etime = new DateTime(data.ReadLong());
            TimeSpan ts = new TimeSpan(data.ReadLong());
            List<DateTime> times = new List<DateTime>();
            DateTime tmp = stime;
            while(tmp<=etime)
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
                        re = WriteDataToBuffer<bool>((byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBuffer<byte>((byte)tags.Type, ProcessDataQuery<byte>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBuffer<DateTime>((byte)tags.Type, ProcessDataQuery<DateTime>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBuffer<double>((byte)tags.Type, ProcessDataQuery<double>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBuffer<float>((byte)tags.Type, ProcessDataQuery<float>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBuffer<int>((byte)tags.Type, ProcessDataQuery<int>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBuffer<long>((byte)tags.Type, ProcessDataQuery<long>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBuffer<short>((byte)tags.Type, ProcessDataQuery<short>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBuffer<string>((byte)tags.Type, ProcessDataQuery<string>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBuffer<bool>((byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBuffer<ulong>((byte)tags.Type, ProcessDataQuery<ulong>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBuffer<ushort>((byte)tags.Type, ProcessDataQuery<ushort>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBuffer<IntPointData>((byte)tags.Type, ProcessDataQuery<IntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBuffer<UIntPointData>((byte)tags.Type, ProcessDataQuery<UIntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBuffer<IntPoint3Data>((byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBuffer<UIntPoint3Data>((byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBuffer<LongPointData>((byte)tags.Type, ProcessDataQuery<LongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBuffer<ULongPointTag>((byte)tags.Type, ProcessDataQuery<ULongPointTag>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBuffer<LongPoint3Data>((byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBuffer<ULongPoint3Data>((byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, times, type));
                        break;
                }
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
