//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/14 11:00:38.
//  Version 1.0
//  种道洋
//==============================================================

//using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Cdy.Tag;
using System.Runtime.InteropServices;
using Cheetah;

namespace DBHighApi.Api
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
        /// 
        /// </summary>
        public const byte RequestStatisticData = 3;

        /// <summary>
        /// 
        /// </summary>

        public const byte RequestStatisticDataByTimeSpan = 4;

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
        public override void ProcessData(string client, ByteBuffer data)
        {
            if (data.RefCount == 0)
            {
               LoggerService.Service.Warn("ProcessData","invailed data buffer in HisDataServerProcess");
                return;
            }
            byte cmd = data.ReadByte();
            long id = data.ReadLong();
            if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(id))
            {
                switch(cmd)
                {
                    case RequestAllHisData:
                        ProcessRequestAllHisDataByMemory(client,data);
                        break;
                    case RequestHisDatasByTimePoint:
                        ProcessRequestHisDatasByTimePointByMemory(client, data);
                        break;
                    case RequestHisDataByTimeSpan:
                        ProcessRequestHisDataByTimeSpanByMemory(client, data);
                        break;
                    case RequestStatisticData:
                        ProcessRequestStatisticsDataByMemory(client, data);
                        break;
                    case RequestStatisticDataByTimeSpan:
                        ProcessRequestStatisticsDataByTimePointByMemory(client, data);
                        break;
                }
            }
            else
            {
                Parent.AsyncCallback(client, FunId, new byte[1], 0);
            }
            base.ProcessData(client, data);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type"></param>
        /// <param name="resb"></param>
        /// <returns></returns>
        private unsafe ByteBuffer WriteDataToBufferByMemory<T>(byte type, HisQueryResult<T> resb)
        {
            var vdata = resb.Contracts();
            var re = Parent.Allocate(FunId, 5 + vdata.Size);
            re.WriteByte(type);
            re.Write(resb.Count);
            re.Write(vdata.Address, vdata.Size);

            //Marshal.Copy(vdata.Address, re.Array, re.ArrayOffset+ 6, vdata.Size);
            //re.SetWriterIndex(re.WriterIndex + vdata.Size);

            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private unsafe void ProcessRequestAllHisDataByMemory(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime sTime = new DateTime(data.ReadLong());
            DateTime eTime = new DateTime(data.ReadLong());

            ByteBuffer re  = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisValue(id, sTime, eTime);
            re.UnLock();
            Parent.AsyncCallback(clientId, re);
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private unsafe void ProcessRequestStatisticsDataByMemory(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime sTime = new DateTime(data.ReadLong());
            DateTime eTime = new DateTime(data.ReadLong());

            ByteBuffer re = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryStatisticsHisValueByMemory(id, sTime, eTime);
            re.UnLock();
            Parent.AsyncCallback(clientId, re);
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
            ByteBuffer re = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData(id, times, type);
            re.UnLock();
            Parent.AsyncCallback(clientId, re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private void ProcessRequestStatisticsDataByTimePointByMemory(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            int count = data.ReadInt();
            List<DateTime> times = new List<DateTime>();
            for (int i = 0; i < count; i++)
            {
                times.Add(new DateTime(data.ReadLong()));
            }
            ByteBuffer re = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryStatisticsHisDataByMemory(id, times);
            re.UnLock();
            Parent.AsyncCallback(clientId, re);
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

            ByteBuffer re = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData(id, stime,etime,ts, type);
            re.UnLock();
            Parent.AsyncCallback(clientId, re);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
