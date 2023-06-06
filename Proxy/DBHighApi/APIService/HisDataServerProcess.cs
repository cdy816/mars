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
using System.Linq;
using System.Threading.Tasks;

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

        //
        public const byte RequestFindTagValue = 5;

        //
        public const byte RequestFindTagValues = 6;

        //
        public const byte RequestCalTagValueKeepTime = 7;

        //
        public const byte RequestCalNumberTagAvgValue = 8;

        //
        public const byte RequestFindNumberTagMaxValue = 9;

        //
        public const byte RequestFindNumberTagMinValue = 10;

        /// <summary>
        /// 修改历史数据
        /// </summary>
        public const byte ModifyHisData = 16;

        /// <summary>
        /// 删除历史数据
        /// </summary>
        public const byte DeleteHisData = 17;


        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDatasByTimePointIgnorSystemExit = 20;

        /// <summary>
        /// 
        /// </summary>

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDataByTimeSpanIgnorSystemExit = 22;

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
                    case RequestHisDatasByTimePointIgnorSystemExit:
                        ProcessRequestHisDatasByTimePointByIgnorSystemExit(client, data);
                        break;
                    case RequestHisDataByTimeSpan:
                        ProcessRequestHisDataByTimeSpanByMemory(client, data);
                        break;
                    case RequestHisDataByTimeSpanIgnorSystemExit:
                        ProcessRequestHisDataByTimeSpanByIgnorSystemExit(client, data);
                        break;
                    case RequestStatisticData:
                        ProcessRequestStatisticsDataByMemory(client, data);
                        break;
                    case RequestStatisticDataByTimeSpan:
                        ProcessRequestStatisticsDataByTimePointByMemory(client, data);
                        break;
                    case RequestFindTagValue:
                        ProcessFindTagValue(client, data);
                        break;
                    case RequestFindTagValues:
                        ProcessFindTagValues(client, data);
                        break;
                    case RequestFindNumberTagMaxValue:
                        ProcessFindNumberTagMaxValue(client, data);
                        break;
                    case RequestFindNumberTagMinValue:
                        ProcessFindNumberTagMinValue(client, data);
                        break;
                    case RequestCalNumberTagAvgValue:
                        ProcessCalNumberTagAvgValue(client, data);
                        break;
                    case RequestCalTagValueKeepTime:
                        ProcessCalTagValueKeepTime(client, data);
                        break;
                    case ModifyHisData:
                         ProcessModifyHisData(client, data);
                        break;
                    case DeleteHisData:
                        ProcessDeleteHisData(client, data);
                        break;
                    case ReadHisValueBySQL:
                        ProcessRequestBySql(client,data);
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

        private void ProcessRequestBySql(string clientId, ByteBuffer data)
        {
            string sql = data.ReadString();
            int id = data.ReadInt();
            var res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisValueBySql(sql);
            if(res is ByteBuffer)
            {
                var byt = res as ByteBuffer;
                var re = Parent.Allocate(FunId, (int)(10 + byt.WriteIndex));
                re.Write(id);
                re.Write((byte)ReadHisValueBySQL);
                re.Write((byte)2);
                re.Write(new IntPtr(byt.Handles[0].ToInt64() + byt.ReadIndex), (int)byt.ReadableCount);
                Parent.AsyncCallback(clientId, re);
            }
            else if(res is HisQueryTableResult)
            {
                var qq = res as HisQueryTableResult;
                var smetas = qq.SeriseMeta();
                var re = Parent.Allocate(FunId, 12 + qq.AvaiableSize + smetas.Length * 2);
                re.Write(id);
                re.Write((byte)ReadHisValueBySQL);
                re.Write((byte)0);
                re.Write(smetas);
                re.Write(qq.AvaiableSize);
                re.Write(qq.Address, qq.AvaiableSize);
                Parent.AsyncCallback(clientId, re);
            }
            else if(res is List<double>)
            {
                var byt = res as List<double>;
                var re = Parent.Allocate(FunId, (int)(10 + byt.Count*8));
                re.Write(id);
                re.Write((byte)ReadHisValueBySQL);
                re.Write((byte)1);
                for(int i=0;i<byt.Count;i++)
                {
                    re.Write(byt[i]);
                }
                Parent.AsyncCallback(clientId, re);
            }
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

            if (re != null)
            {
                var ree = Parent.Allocate(ApiFunConst.HisDataRequestFun, (int)(re.WriteIndex));
                int icount = (int)(re.WriteIndex - re.ReadIndex);
                re.CopyTo(ree, re.ReadIndex, 1, icount);
                re.UnlockAndReturn();
                ree.WriteIndex += icount;
                Parent.AsyncCallback(clientId, ree);
            }
            else
            {
                var ree = Parent.Allocate(ApiFunConst.HisDataRequestFun, 1);
                Parent.AsyncCallback(clientId, ree);
            }
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
            //re.UnLock();
            //Parent.AsyncCallback(clientId, re);

            if (re != null)
            {
                var ree = Parent.Allocate(ApiFunConst.HisDataRequestFun, (int)(re.WriteIndex));
                int icount = (int)(re.WriteIndex - re.ReadIndex);
                re.CopyTo(ree, re.ReadIndex, 1, icount);
                re.UnlockAndReturn();
                ree.WriteIndex += icount;
                Parent.AsyncCallback(clientId, ree);
            }
            else
            {
                var ree = Parent.Allocate(ApiFunConst.HisDataRequestFun, 1);
                Parent.AsyncCallback(clientId, ree);
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
            ByteBuffer re = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData(id, times, type);

            if (re != null)
            {
                var ree = Parent.Allocate(ApiFunConst.HisDataRequestFun, (int)(re.WriteIndex));
                int icount = (int)(re.WriteIndex - re.ReadIndex);
                re.CopyTo(ree, re.ReadIndex, 1, icount);
                re.UnlockAndReturn();
                ree.WriteIndex += icount;
                Parent.AsyncCallback(clientId, ree);
            }
            else
            {
                var ree = Parent.Allocate(ApiFunConst.HisDataRequestFun, 1);
                Parent.AsyncCallback(clientId, ree);
            }
            //re.UnLock();
            //Parent.AsyncCallback(clientId, re);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private void ProcessRequestHisDatasByTimePointByIgnorSystemExit(string clientId, ByteBuffer data)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            int id = data.ReadInt();
            Cdy.Tag.QueryValueMatchType type = (QueryValueMatchType)data.ReadByte();
            int count = data.ReadInt();
            List<DateTime> times = new List<DateTime>();
            for (int i = 0; i < count; i++)
            {
                times.Add(new DateTime(data.ReadLong()));
            }
            ByteBuffer re = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit(id, times, type);

            if (re != null)
            {
                var ree = Parent.Allocate(ApiFunConst.HisDataRequestFun, (int)(re.WriteIndex));
                int icount = (int)(re.WriteIndex - re.ReadIndex);
                re.CopyTo(ree, re.ReadIndex, 1, icount);
                re.UnlockAndReturn();
                ree.WriteIndex += icount;
                Parent.AsyncCallback(clientId, ree);
            }
            else
            {
                var ree = Parent.Allocate(ApiFunConst.HisDataRequestFun, 1);
                Parent.AsyncCallback(clientId, ree);
            }
            sw.Stop();
            LoggerService.Service.Info("ProcessRequestHisDatasByTimePointByIgnorSystemExit", $" 查询耗时{sw.ElapsedMilliseconds}");
            //re.UnLock();
            //Parent.AsyncCallback(clientId, re);
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

            if (re != null)
            {
                var ree = Parent.Allocate(ApiFunConst.HisDataRequestFun, (int)(re.WriteIndex));
                int icount = (int)(re.WriteIndex - re.ReadIndex);
                re.CopyTo(ree, re.ReadIndex, 1, icount);
                re.UnlockAndReturn();
                ree.WriteIndex += icount;
                Parent.AsyncCallback(clientId, ree);
            }
            else
            {
                var ree = Parent.Allocate(ApiFunConst.HisDataRequestFun, 1);
                Parent.AsyncCallback(clientId, ree);
            }
            //re.UnLock();
            //Parent.AsyncCallback(clientId, re);
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
            //re.UnLock();
            //Parent.AsyncCallback(clientId, re);
            if (re != null)
            {
                var ree = Parent.Allocate(ApiFunConst.HisDataRequestFun, (int)(re.WriteIndex));
                int icount = (int)(re.WriteIndex - re.ReadIndex);
                re.CopyTo(ree, re.ReadIndex, 1, icount);
                re.UnlockAndReturn();
                ree.WriteIndex += icount;
                Parent.AsyncCallback(clientId, ree);
            }
            else
            {
                var ree = Parent.Allocate(ApiFunConst.HisDataRequestFun, 1);
                Parent.AsyncCallback(clientId, ree);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private void ProcessRequestHisDataByTimeSpanByIgnorSystemExit(string clientId, ByteBuffer data)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            int id = data.ReadInt();
            Cdy.Tag.QueryValueMatchType type = (QueryValueMatchType)data.ReadByte();
            DateTime stime = new DateTime(data.ReadLong());
            DateTime etime = new DateTime(data.ReadLong());
            TimeSpan ts = new TimeSpan(data.ReadLong());

            ByteBuffer re = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit(id, stime, etime, ts, type);
            //re.UnLock();
            //Parent.AsyncCallback(clientId, re);
            if (re != null)
            {
                var ree = Parent.Allocate(ApiFunConst.HisDataRequestFun, (int)(re.WriteIndex));
                int icount = (int)(re.WriteIndex - re.ReadIndex);
                re.CopyTo(ree, re.ReadIndex, 1, icount);
                re.UnlockAndReturn();
                ree.WriteIndex += icount;
                Parent.AsyncCallback(clientId, ree);
            }
            else
            {
                var ree = Parent.Allocate(ApiFunConst.HisDataRequestFun, 1);
                Parent.AsyncCallback(clientId, ree);
            }
            sw.Stop();
            LoggerService.Service.Info("ProcessRequestHisDataByTimeSpanByIgnorSystemExit", $" 查询耗时{sw.ElapsedMilliseconds}");
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private unsafe void ProcessFindTagValue(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime sTime = new DateTime(data.ReadLong());
            DateTime eTime = new DateTime(data.ReadLong());
            byte type = data.ReadByte();
            ByteBuffer re = Parent.Allocate(ApiFunConst.HisDataRequestFun, 20);
            re.Write((byte)RequestFindTagValue);

            var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);
            if(tag!=null)
            {
                DateTime? dres = null;
                Tuple<DateTime, object> res = null;
                re.Write((byte)tag.Type);
                switch (tag.Type)
                {
                    case Cdy.Tag.TagType.DateTime:
                        dres = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValue(tag.Id, sTime, eTime, DateTime.Parse(data.ReadString()));
                        if (dres != null)
                        {
                            re.Write((byte)1);
                            re.Write(dres.Value);
                        }
                        else
                        {
                            re.Write((byte)0);
                            re.Write(DateTime.MinValue);
                        }
                        re.Write((double)0);
                        break;
                    case Cdy.Tag.TagType.Bool:
                        dres = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValue(tag.Id, sTime, eTime,Convert.ToByte(bool.Parse(data.ReadString())));
                        if (dres != null)
                        {
                            re.Write((byte)1);
                            re.Write(dres.Value);
                        }
                        else
                        {
                            re.Write((byte)0);
                            re.Write(DateTime.MinValue);
                        }
                        re.Write((double)0);
                        break;
                    case Cdy.Tag.TagType.String:
                    case Cdy.Tag.TagType.IntPoint:
                    case Cdy.Tag.TagType.UIntPoint:
                    case Cdy.Tag.TagType.IntPoint3:
                    case Cdy.Tag.TagType.UIntPoint3:
                    case Cdy.Tag.TagType.LongPoint:
                    case Cdy.Tag.TagType.ULongPoint:
                    case Cdy.Tag.TagType.LongPoint3:
                    case Cdy.Tag.TagType.ULongPoint3:
                        dres = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValue(tag.Id, sTime, eTime, data.ReadString());
                        if (dres != null)
                        {
                            re.Write((byte)1);
                            re.Write(dres.Value);
                        }
                        else
                        {
                            re.Write((byte)0);
                            re.Write(DateTime.MinValue);
                        }
                        re.Write((double)0);
                        break;
                    case Cdy.Tag.TagType.Double:
                    case Cdy.Tag.TagType.Float:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValue(tag.Id, sTime, eTime, (NumberStatisticsType)type, double.Parse( data.ReadString()), double.Parse(data.ReadString()));
                        if (res != null)
                        {
                            re.Write((byte)1);
                            re.Write(res.Item1);
                            re.Write(Convert.ToDouble(res.Item2));
                        }
                        else
                        {
                            re.Write((byte)0);
                           
                        }
                        break;
                    case Cdy.Tag.TagType.Byte:
                    case Cdy.Tag.TagType.Int:
                    case Cdy.Tag.TagType.Long:
                    case Cdy.Tag.TagType.UInt:
                    case Cdy.Tag.TagType.Short:
                    case Cdy.Tag.TagType.ULong:
                    case Cdy.Tag.TagType.UShort:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValue(tag.Id, sTime, eTime, (NumberStatisticsType)type, long.Parse(data.ReadString()), int.Parse(data.ReadString()));
                        if (res != null)
                        {
                            re.Write((byte)1);
                            re.Write(res.Item1);
                            re.Write(Convert.ToDouble(res.Item2));
                        }
                        else
                        {
                            re.Write((byte)0);
                        }
                        break;

                }
            }
            Parent.AsyncCallback(clientId, re);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private unsafe void ProcessFindTagValues(string clientId,ByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime sTime = new DateTime(data.ReadLong());
            DateTime eTime = new DateTime(data.ReadLong());
            byte type = data.ReadByte();
            ByteBuffer re = Parent.Allocate(ApiFunConst.HisDataRequestFun, 20);
            re.Write((byte)RequestFindTagValues);
            var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            if (tag != null)
            {
                IEnumerable<DateTime> dres = null;
                Dictionary<DateTime, object> res = null;
                re.Write((byte)tag.Type);
                switch (tag.Type)
                {
                    case Cdy.Tag.TagType.DateTime:
                        dres = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValues(tag.Id, sTime, eTime,DateTime.Parse(data.ReadString()));
                        if (dres != null)
                        {
                            re.Write((byte)1);

                            re.CheckAndResize(20 +4 + dres.Count() * 8);
                            re.Write(dres.Count());
                            foreach(var vv in dres)
                            {
                                re.Write(vv);
                            }
                        }
                        else
                        {
                            re.Write((byte)0);
                          
                        }
                        break;
                    case Cdy.Tag.TagType.Bool:
                        dres = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValues(tag.Id, sTime, eTime,Convert.ToByte(bool.Parse( data.ReadString())));
                        if (dres != null)
                        {
                            re.Write((byte)1);

                            re.CheckAndResize(20 + 4 + dres.Count() * 8);
                            re.Write(dres.Count());
                            foreach (var vv in dres)
                            {
                                re.Write(vv);
                            }
                        }
                        else
                        {
                            re.Write((byte)0);

                        }
                        break;
                    case Cdy.Tag.TagType.String:
                    case Cdy.Tag.TagType.IntPoint:
                    case Cdy.Tag.TagType.UIntPoint:
                    case Cdy.Tag.TagType.IntPoint3:
                    case Cdy.Tag.TagType.UIntPoint3:
                    case Cdy.Tag.TagType.LongPoint:
                    case Cdy.Tag.TagType.ULongPoint:
                    case Cdy.Tag.TagType.LongPoint3:
                    case Cdy.Tag.TagType.ULongPoint3:
                        dres = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValues(tag.Id, sTime, eTime, data.ReadString());
                        if (dres != null)
                        {
                            re.Write((byte)1);

                            re.CheckAndResize(20 + 4 + dres.Count() * 8);
                            re.Write(dres.Count());
                            foreach (var vv in dres)
                            {
                                re.Write(vv);
                            }
                        }
                        else
                        {
                            re.Write((byte)0);

                        }
                        break;
                    case Cdy.Tag.TagType.Double:
                    case Cdy.Tag.TagType.Float:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValues(tag.Id, sTime, eTime, (NumberStatisticsType)type, double.Parse( data.ReadString()),double.Parse( data.ReadString()));
                        if (res != null)
                        {
                            re.Write((byte)1);
                            re.CheckAndResize(20 + 4 + res.Count() * 16);
                            re.Write(res.Count());
                            foreach (var vv in res)
                            {
                                re.Write(vv.Key);
                                re.Write(Convert.ToDouble(vv.Value));
                            }
                        }
                        else
                        {
                            re.Write((byte)0);

                        }
                        break;
                    case Cdy.Tag.TagType.Byte:
                    case Cdy.Tag.TagType.Int:
                    case Cdy.Tag.TagType.Long:
                    case Cdy.Tag.TagType.UInt:
                    case Cdy.Tag.TagType.Short:
                    case Cdy.Tag.TagType.ULong:
                    case Cdy.Tag.TagType.UShort:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValues(tag.Id, sTime, eTime, (NumberStatisticsType)type, long.Parse(data.ReadString()), int.Parse(data.ReadString()));
                        if (res != null)
                        {
                            re.Write((byte)1);
                            re.CheckAndResize(20 + 4 + res.Count() * 16);
                            re.Write(res.Count());
                            foreach (var vv in res)
                            {
                                re.Write(vv.Key);
                                re.Write(Convert.ToDouble(vv.Value));
                            }
                        }
                        else
                        {
                            re.Write((byte)0);

                        }
                        break;

                }
            }
            Parent.AsyncCallback(clientId, re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private unsafe void ProcessCalTagValueKeepTime(string clientId,ByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime sTime = new DateTime(data.ReadLong());
            DateTime eTime = new DateTime(data.ReadLong());
            byte type = data.ReadByte();
            ByteBuffer re = Parent.Allocate(ApiFunConst.HisDataRequestFun, 20);
            re.Write((byte)RequestCalTagValueKeepTime);
            var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);
            if (tag != null)
            {
                double? res = null;
                re.Write((byte)tag.Type);
                switch (tag.Type)
                {
                    case Cdy.Tag.TagType.DateTime:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValueDuration(tag.Id, sTime, eTime, DateTime.Parse(data.ReadString()));
                        break;
                    case Cdy.Tag.TagType.Bool:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValueDuration(tag.Id, sTime, eTime, Convert.ToByte(bool.Parse(data.ReadString())));
                        break;
                    case Cdy.Tag.TagType.String:
                    case Cdy.Tag.TagType.IntPoint:
                    case Cdy.Tag.TagType.UIntPoint:
                    case Cdy.Tag.TagType.IntPoint3:
                    case Cdy.Tag.TagType.UIntPoint3:
                    case Cdy.Tag.TagType.LongPoint:
                    case Cdy.Tag.TagType.ULongPoint:
                    case Cdy.Tag.TagType.LongPoint3:
                    case Cdy.Tag.TagType.ULongPoint3:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValueDuration(tag.Id, sTime, eTime, data.ReadString());
                        break;
                    case Cdy.Tag.TagType.Double:
                    case Cdy.Tag.TagType.Float:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValueDuration(tag.Id, sTime, eTime, (NumberStatisticsType)type, double.Parse(data.ReadString()), double.Parse(data.ReadString()));
                        break;
                    case Cdy.Tag.TagType.Byte:
                    case Cdy.Tag.TagType.Int:
                    case Cdy.Tag.TagType.Long:
                    case Cdy.Tag.TagType.UInt:
                    case Cdy.Tag.TagType.Short:
                    case Cdy.Tag.TagType.ULong:
                    case Cdy.Tag.TagType.UShort:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValueDuration(tag.Id, sTime, eTime, (NumberStatisticsType)type, long.Parse(data.ReadString()), int.Parse(data.ReadString()));
                        break;

                }
                if(res!=null)
                {
                    re.Write((byte)1);
                    re.Write(res.Value);
                }
                else
                {
                    re.Write((byte)0);
                }
            }
            Parent.AsyncCallback(clientId, re);
        }

        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private unsafe void ProcessCalNumberTagAvgValue(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime sTime = new DateTime(data.ReadLong());
            DateTime eTime = new DateTime(data.ReadLong());
            ByteBuffer re = Parent.Allocate(ApiFunConst.HisDataRequestFun, 20);
            re.Write((byte)RequestCalNumberTagAvgValue);
            var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);
            if (tag != null)
            {
                double? res = null;
                re.Write((byte)tag.Type);
                switch (tag.Type)
                {
                    case Cdy.Tag.TagType.Double:
                    case Cdy.Tag.TagType.Float:
                    case Cdy.Tag.TagType.Byte:
                    case Cdy.Tag.TagType.Int:
                    case Cdy.Tag.TagType.Long:
                    case Cdy.Tag.TagType.UInt:
                    case Cdy.Tag.TagType.Short:
                    case Cdy.Tag.TagType.ULong:
                    case Cdy.Tag.TagType.UShort:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.StatisticsTagAvgValue(tag.Id, sTime,eTime);
                        break;

                }
                if (res != null)
                {
                    re.Write((byte)1);
                    re.Write(res.Value);
                }
                else
                {
                    re.Write((byte)0);
                }
            }
            Parent.AsyncCallback(clientId, re);
        }


        private unsafe void ProcessFindNumberTagMaxValue(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime sTime = new DateTime(data.ReadLong());
            DateTime eTime = new DateTime(data.ReadLong());
            ByteBuffer re = Parent.Allocate(ApiFunConst.HisDataRequestFun, 20);
            re.Write((byte)RequestFindNumberTagMaxValue);
            var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);
            if (tag != null)
            {
                Tuple<double, List<DateTime>> res = null;
                re.Write((byte)tag.Type);
                switch (tag.Type)
                {
                    case Cdy.Tag.TagType.Double:
                    case Cdy.Tag.TagType.Float:
                    case Cdy.Tag.TagType.Byte:
                    case Cdy.Tag.TagType.Int:
                    case Cdy.Tag.TagType.Long:
                    case Cdy.Tag.TagType.UInt:
                    case Cdy.Tag.TagType.Short:
                    case Cdy.Tag.TagType.ULong:
                    case Cdy.Tag.TagType.UShort:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.StatisticsTagMaxMinValue(tag.Id, sTime, eTime, NumberStatisticsType.Max);
                        break;

                }

                if (res != null)
                {
                    re.CheckAndResize(20 + 8 + 4 + res.Item2.Count * 8);
                    re.Write((byte)1);
                    re.Write(res.Item1);
                    re.Write(res.Item2.Count);
                    foreach(var vv in res.Item2)
                    {
                        re.Write(vv);
                    }
                }
                else
                {
                    re.Write((byte)0);
                }
            }
            Parent.AsyncCallback(clientId, re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private unsafe void ProcessFindNumberTagMinValue(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime sTime = new DateTime(data.ReadLong());
            DateTime eTime = new DateTime(data.ReadLong());
            ByteBuffer re = Parent.Allocate(ApiFunConst.HisDataRequestFun, 20);
            re.Write((byte)RequestFindNumberTagMinValue);
            var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);
            if (tag != null)
            {
                Tuple<double, List<DateTime>> res = null;
                re.Write((byte)tag.Type);
                switch (tag.Type)
                {
                    case Cdy.Tag.TagType.Double:
                    case Cdy.Tag.TagType.Float:
                    case Cdy.Tag.TagType.Byte:
                    case Cdy.Tag.TagType.Int:
                    case Cdy.Tag.TagType.Long:
                    case Cdy.Tag.TagType.UInt:
                    case Cdy.Tag.TagType.Short:
                    case Cdy.Tag.TagType.ULong:
                    case Cdy.Tag.TagType.UShort:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.StatisticsTagMaxMinValue(tag.Id, sTime, eTime, NumberStatisticsType.Min);
                        break;

                }

                if (res != null)
                {
                    re.CheckAndResize(20 + 8 + 4 + res.Item2.Count * 8);
                    re.Write((byte)1);
                    re.Write(res.Item1);
                    re.Write(res.Item2.Count);
                    foreach (var vv in res.Item2)
                    {
                        re.Write(vv);
                    }
                }
                else
                {
                    re.Write((byte)0);
                }
            }
            Parent.AsyncCallback(clientId, re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        public unsafe void ProcessModifyHisData(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            string user = data.ReadString();
            string msg = data.ReadString();
            TagType tp = (TagType)data.ReadByte();
            int count = data.ReadInt();
            List<TagHisValue<object>> values = new List<TagHisValue<object>>();

            switch (tp)
            {
                case TagType.Bool:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add( new TagHisValue<object>() { Time = data.ReadDateTime(), Value = data.ReadByte() > 0, Quality = data.ReadByte() });
                    }
                    break;
                case TagType.Byte:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = data.ReadByte(), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.UShort:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = data.ReadUShort(), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.Short:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = data.ReadShort(), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.Int:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = data.ReadInt(), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.UInt:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = data.ReadUInt(), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.Long:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = data.ReadLong(), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.ULong:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = data.ReadULong(), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.Double:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = data.ReadDouble(), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.Float:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = data.ReadFloat(), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.String:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = data.ReadString(), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.DateTime:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = data.ReadDateTime(), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.IntPoint:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = new IntPointData(data.ReadInt(), data.ReadInt()), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.UIntPoint:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = new UIntPointData(data.ReadUInt(), data.ReadUInt()), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.IntPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = new IntPoint3Data(data.ReadInt(), data.ReadInt(), data.ReadInt()), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.UIntPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = new UIntPoint3Data(data.ReadUInt(), data.ReadUInt(), data.ReadUInt()), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.LongPoint:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = new LongPointData(data.ReadLong(), data.ReadLong()), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.LongPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = new LongPoint3Data(data.ReadLong(), data.ReadLong(), data.ReadLong()), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.ULongPoint:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = new ULongPointData(data.ReadULong(), data.ReadULong()), Quality = data.ReadByte() });
                    }
                    break;
                case TagType.ULongPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        values.Add(new TagHisValue<object>() { Time = data.ReadDateTime(), Value = new ULongPoint3Data(data.ReadULong(), data.ReadULong(), data.ReadULong()), Quality = data.ReadByte() });
                        // hp.AppendPatchValue(data.ReadDateTime(), new ULongPoint3Data(data.ReadULong(), data.ReadULong(), data.ReadULong()), data.ReadByte());
                    }
                    break;
            }


            bool bval = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.ModifyHisValue(id,tp,user,msg,values);
            ByteBuffer re = Parent.Allocate(ApiFunConst.HisDataRequestFun, 20);
            re.Write((byte)ModifyHisData);
            re.WriteByte((bval) ? (byte)1 : (byte)0);

            Parent.AsyncCallback(clientId, re);

        }


        public unsafe void ProcessDeleteHisData(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            string user = data.ReadString();
            string msg = data.ReadString();
            DateTime stime =data.ReadDateTime();
            DateTime etime =data.ReadDateTime();
            bool bval = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.DeleteHisValue(id,  user, msg, stime,etime);
            ByteBuffer re = Parent.Allocate(ApiFunConst.HisDataRequestFun, 20);
            re.Write((byte)DeleteHisData);
            re.WriteByte((bval) ? (byte)1 : (byte)0);
            Parent.AsyncCallback(clientId, re);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
