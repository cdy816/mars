using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cdy.Tag;
using Cheetah;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace DBGrpcApi
{
    public class HisDataService : HislData.HislDataBase
    {
        private readonly ILogger<HisDataService> _logger;
        public HisDataService(ILogger<HisDataService> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        private string GetGroupName(string tag)
        {
            if (tag.LastIndexOf(".") > 0)
            {
                return tag.Substring(0, tag.LastIndexOf("."));
            }
            return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<HisDataCollectionReplay> GetHisValue(HisDataRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                HisDataCollectionReplay re = new HisDataCollectionReplay() { Result = true };
                foreach (var vv in request.Tags)
                {
                    string sname = GetGroupName(vv);
                    if(SecurityManager.Manager.CheckReaderPermission(request.Token,sname))
                    {
                        if (Startup.IsRunningEmbed)
                        {
                            ReadTagHisValueLocal(vv, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Duration, request.QueryType, re);
                        }
                        else
                        {
                            ReadTagHisValue(vv, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Duration, request.QueryType, re);
                        }
                    }
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new HisDataCollectionReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<HisDataCollectionReplay> GetHisValueIgnorSystemExit(HisDataRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                HisDataCollectionReplay re = new HisDataCollectionReplay() { Result = true };
                foreach (var vv in request.Tags)
                {
                    string sname = GetGroupName(vv);
                    if (SecurityManager.Manager.CheckReaderPermission(request.Token, sname))
                    {
                        if (Startup.IsRunningEmbed)
                        {
                            ReadTagHisValueByIgnorSystemExitLocal(vv, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Duration, request.QueryType, re);
                        }
                        else
                        {
                            ReadTagHisValueByIgnorSystemExit(vv, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Duration, request.QueryType, re);
                        }
                    }
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new HisDataCollectionReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<HisDataCollectionReplay> GetAllHisValue(AllHisDataRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                HisDataCollectionReplay re = new HisDataCollectionReplay() { Result = true };
                foreach (var vv in request.Tags)
                {
                    string sname = GetGroupName(vv);
                    if (SecurityManager.Manager.CheckReaderPermission(request.Token, sname))
                    {
                        if (Startup.IsRunningEmbed)
                        {
                            ReadTagAllHisValueLocal(vv, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), re);
                        }
                        else
                        {
                            ReadTagAllHisValue(vv, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), re);
                        }
                    }
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new HisDataCollectionReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<StatisticsDataCollectionReplay> GetNumberValueStatisticsData(NumberValueStatisticsDataRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                StatisticsDataCollectionReplay re = new StatisticsDataCollectionReplay() { Result = true };
                foreach (var vv in request.Tags)
                {
                    string sname = GetGroupName(vv);
                    if (SecurityManager.Manager.CheckReaderPermission(request.Token, sname))
                    {
                        if (Startup.IsRunningEmbed)
                        {
                            ReadTagStatisticsValueLocal(vv, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), re);
                        }
                        else
                        {
                            ReadTagStatisticsValue(vv, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), re);
                        }
                    }
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new StatisticsDataCollectionReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<StatisticsDataCollectionReplay> GetNumberValueStatisticsDataAtTimePoint(NumberValueStatisticsDataAtTimePointRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                StatisticsDataCollectionReplay re = new StatisticsDataCollectionReplay() { Result = true };
                foreach (var vv in request.Tags)
                {
                    string sname = GetGroupName(vv);
                    if (SecurityManager.Manager.CheckReaderPermission(request.Token, sname))
                    {
                        List<DateTime> ltmp = new List<DateTime>();
                        DateTime dtime = DateTime.FromBinary(request.StartTime);
                        DateTime etime = DateTime.FromBinary(request.EndTime);
                        while(dtime<=etime)
                        {
                            ltmp.Add(dtime);
                            dtime = dtime.AddSeconds(request.Duration);
                        }
                        if (Startup.IsRunningEmbed)
                        {
                            ReadTagStatisticsValueLocal(vv, ltmp, re);
                        }
                        else
                        {
                            ReadTagStatisticsValue(vv, ltmp, re);
                        }
                    }
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new StatisticsDataCollectionReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="result"></param>
        /// <param name="valueType"></param>
        private void ProcessResult<T> (string tag, object value,HisDataCollectionReplay result,int valueType)
        {
            HisDataPointCollection hdp = new HisDataPointCollection() { Tag = tag,ValueType= valueType };
            var vdata = value as HisQueryResult<T>;
            if (vdata != null)
            {
                for (int i = 0; i < vdata.Count; i++)
                {
                    byte qu;
                    DateTime time;
                    var val = vdata.GetValue(i, out time, out qu);
                    hdp.Values.Add(new HisDataPoint() {  Time = time.ToBinary(), Value = val.ToString(),Quality=qu });
                }
            }
            result.Values.Add(hdp);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="result"></param>
        /// <param name="valueType"></param>
        /// <param name="times"></param>
        private void ProcessResult<T>(string tag, object value, HisDataCollectionReplay result, int valueType, List<DateTime> times)
        {
            HisDataPointCollection hdp = new HisDataPointCollection() { Tag = tag, ValueType = valueType };
            var vdata = value as HisQueryResult<T>;
            if (vdata != null)
            {
                SortedDictionary<DateTime, HisDataPoint> rtmp = new SortedDictionary<DateTime, HisDataPoint>();
                for (int i = 0; i < vdata.Count; i++)
                {
                    byte qu;
                    DateTime time;
                    var val = vdata.GetValue(i, out time, out qu);
                    rtmp.Add(time, new HisDataPoint() { Time = time.ToBinary(), Value = val.ToString(),Quality=qu });
                    //hdp.Values.Add(new HisDataPoint() { Time = time.ToBinary(), Value = val.ToString() });
                }

                foreach (var vv in times)
                {
                    if (!rtmp.ContainsKey(vv))
                    {
                        rtmp.Add(vv, new HisDataPoint() { Time = vv.ToBinary(), Value = default(T).ToString(),Quality=(byte)QualityConst.Null });
                    }
                }

                foreach(var vv in rtmp)
                {
                    hdp.Values.Add(vv.Value);
                }
            }
            result.Values.Add(hdp);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <param name="timespan"></param>
        /// <returns></returns>
        private List<DateTime> GetTimes(DateTime starttime, DateTime endtime, TimeSpan timespan)
        {
            List<DateTime> re = new List<DateTime>();
            DateTime dt = starttime;
            while (dt < endtime)
            {
                re.Add(dt);
                dt = dt.Add(timespan);
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="duration"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private void ReadTagHisValue(string tag,DateTime startTime,DateTime endTime,int duration,int type, HisDataCollectionReplay result)
        {
            var tgs = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(tag);
            if (tgs == null) return;
            object res;
            var times = GetTimes(startTime, endTime, TimeSpan.FromMilliseconds(duration));
            switch (tgs.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<bool>(tgs.Id,startTime,endTime,TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<bool>(tag, res,result,(int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<byte>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<byte>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<DateTime>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<DateTime>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<double>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<double>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<float>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<float>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<int>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<int>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<long>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<long>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<short>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<short>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<string>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<string>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<uint>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<uint>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ulong>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<ulong>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ushort>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<ushort>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPointData>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<IntPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPointData>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<UIntPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPoint3Data>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<IntPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPoint3Data>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<UIntPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPointData>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<LongPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPointData>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<ULongPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPoint3Data>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<LongPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPoint3Data>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<ULongPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="duration"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private void ReadTagHisValueLocal(string tag, DateTime startTime, DateTime endTime, int duration, int type, HisDataCollectionReplay result)
        {
            var hisserver = ServiceLocator.Locator.Resolve<IHisQuery>();
            var tgs = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(tag);
            if (tgs == null) return;

            object res;
            var times = GetTimes(startTime, endTime, TimeSpan.FromMilliseconds(duration));
            switch (tgs.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = hisserver.ReadValue<bool>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<bool>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = hisserver.ReadValue<byte>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<byte>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = hisserver.ReadValue<DateTime>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<DateTime>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = hisserver.ReadValue<double>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<double>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = hisserver.ReadValue<float>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<float>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = hisserver.ReadValue<int>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<int>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = hisserver.ReadValue<long>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<long>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = hisserver.ReadValue<short>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<short>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res = hisserver.ReadValue<string>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<string>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = hisserver.ReadValue<uint>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<uint>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = hisserver.ReadValue<ulong>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<ulong>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = hisserver.ReadValue<ushort>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<ushort>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = hisserver.ReadValue<IntPointData>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<IntPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = hisserver.ReadValue<UIntPointData>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<UIntPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = hisserver.ReadValue<IntPoint3Data>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<IntPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = hisserver.ReadValue<UIntPoint3Data>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<UIntPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = hisserver.ReadValue<LongPointData>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<LongPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = hisserver.ReadValue<ULongPointData>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<ULongPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = hisserver.ReadValue<LongPoint3Data>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<LongPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = hisserver.ReadValue<ULongPoint3Data>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<ULongPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="duration"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private void ReadTagHisValueByIgnorSystemExit(string tag, DateTime startTime, DateTime endTime, int duration, int type, HisDataCollectionReplay result)
        {
            var tgs = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(tag);
            if (tgs == null) return;
            object res;
            var times = GetTimes(startTime, endTime, TimeSpan.FromMilliseconds(duration));
            switch (tgs.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<bool>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<bool>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<byte>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<byte>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<DateTime>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<DateTime>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<double>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<double>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<float>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<float>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<int>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<int>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<long>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<long>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<short>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<short>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<string>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<string>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<uint>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<uint>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ulong>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<ulong>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ushort>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<ushort>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<IntPointData>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<IntPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<UIntPointData>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<UIntPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<IntPoint3Data>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<IntPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<UIntPoint3Data>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<UIntPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<LongPointData>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<LongPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ULongPointData>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<ULongPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<LongPoint3Data>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<LongPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ULongPoint3Data>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<ULongPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="duration"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private void ReadTagHisValueByIgnorSystemExitLocal(string tag, DateTime startTime, DateTime endTime, int duration, int type, HisDataCollectionReplay result)
        {
            var hisserver = ServiceLocator.Locator.Resolve<IHisQuery>();
            var tgs = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(tag);
            if (tgs == null) return;
            object res;
            var times = GetTimes(startTime, endTime, TimeSpan.FromMilliseconds(duration));
            switch (tgs.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = hisserver.ReadValueIgnorClosedQuality<bool>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<bool>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = hisserver.ReadValueIgnorClosedQuality<byte>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<byte>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = hisserver.ReadValueIgnorClosedQuality<DateTime>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<DateTime>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = hisserver.ReadValueIgnorClosedQuality<double>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<double>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = hisserver.ReadValueIgnorClosedQuality<float>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<float>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = hisserver.ReadValueIgnorClosedQuality<int>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<int>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = hisserver.ReadValueIgnorClosedQuality<long>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<long>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = hisserver.ReadValueIgnorClosedQuality<short>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<short>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res = hisserver.ReadValueIgnorClosedQuality<string>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<string>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = hisserver.ReadValueIgnorClosedQuality<uint>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<uint>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = hisserver.ReadValueIgnorClosedQuality<ulong>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<ulong>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = hisserver.ReadValueIgnorClosedQuality<ushort>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<ushort>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<IntPointData>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<IntPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<UIntPointData>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<UIntPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<IntPoint3Data>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<IntPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<UIntPoint3Data>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<UIntPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<LongPointData>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<LongPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<ULongPointData>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<ULongPointData>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<LongPoint3Data>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<LongPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<ULongPoint3Data>(tgs.Id, times, (QueryValueMatchType)(type));
                    ProcessResult<ULongPoint3Data>(tag, res, result, (int)tgs.Type, times);
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        private void ReadTagAllHisValue(string tag, DateTime startTime, DateTime endTime,  HisDataCollectionReplay result)
        {
            var tgs = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(tag);
            if (tgs == null) return;

            object res;
            switch (tgs.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<bool>(tgs.Id, startTime, endTime);
                    ProcessResult<bool>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<byte>(tgs.Id, startTime, endTime);
                    ProcessResult<byte>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<DateTime>(tgs.Id, startTime, endTime);
                    ProcessResult<DateTime>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<double>(tgs.Id, startTime, endTime);
                    ProcessResult<double>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<float>(tgs.Id, startTime, endTime);
                    ProcessResult<float>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<int>(tgs.Id, startTime, endTime);
                    ProcessResult<int>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<long>(tgs.Id, startTime, endTime);
                    ProcessResult<long>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<short>(tgs.Id, startTime, endTime);
                    ProcessResult<short>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<string>(tgs.Id, startTime, endTime);
                    ProcessResult<string>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<uint>(tgs.Id, startTime, endTime);
                    ProcessResult<uint>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ulong>(tgs.Id, startTime, endTime);
                    ProcessResult<ulong>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ushort>(tgs.Id, startTime, endTime);
                    ProcessResult<ushort>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<IntPointData>(tgs.Id, startTime, endTime);
                    ProcessResult<IntPointData>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<UIntPointData>(tgs.Id, startTime, endTime);
                    ProcessResult<UIntPointData>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<IntPoint3Data>(tgs.Id, startTime, endTime);
                    ProcessResult<IntPoint3Data>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<UIntPoint3Data>(tgs.Id, startTime, endTime);
                    ProcessResult<UIntPoint3Data>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<LongPointData>(tgs.Id, startTime, endTime);
                    ProcessResult<LongPointData>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ULongPointData>(tgs.Id, startTime, endTime);
                    ProcessResult<ULongPointData>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<LongPoint3Data>(tgs.Id, startTime, endTime);
                    ProcessResult<LongPoint3Data>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ULongPoint3Data>(tgs.Id, startTime, endTime);
                    ProcessResult<ULongPoint3Data>(tag, res, result, (int)tgs.Type);
                    break;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        private void ReadTagAllHisValueLocal(string tag, DateTime startTime, DateTime endTime, HisDataCollectionReplay result)
        {
            var hisserver = ServiceLocator.Locator.Resolve<IHisQuery>();
            var tgs = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(tag);
            if (tgs == null) return;

            object res;
            switch (tgs.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = hisserver.ReadAllValue<bool>(tgs.Id, startTime, endTime);
                    ProcessResult<bool>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = hisserver.ReadAllValue<byte>(tgs.Id, startTime, endTime);
                    ProcessResult<byte>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = hisserver.ReadAllValue<DateTime>(tgs.Id, startTime, endTime);
                    ProcessResult<DateTime>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = hisserver.ReadAllValue<double>(tgs.Id, startTime, endTime);
                    ProcessResult<double>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = hisserver.ReadAllValue<float>(tgs.Id, startTime, endTime);
                    ProcessResult<float>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = hisserver.ReadAllValue<int>(tgs.Id, startTime, endTime);
                    ProcessResult<int>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = hisserver.ReadAllValue<long>(tgs.Id, startTime, endTime);
                    ProcessResult<long>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = hisserver.ReadAllValue<short>(tgs.Id, startTime, endTime);
                    ProcessResult<short>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.String:
                    res = hisserver.ReadAllValue<string>(tgs.Id, startTime, endTime);
                    ProcessResult<string>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = hisserver.ReadAllValue<uint>(tgs.Id, startTime, endTime);
                    ProcessResult<uint>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = hisserver.ReadAllValue<ulong>(tgs.Id, startTime, endTime);
                    ProcessResult<ulong>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = hisserver.ReadAllValue<ushort>(tgs.Id, startTime, endTime);
                    ProcessResult<ushort>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = hisserver.ReadAllValue<IntPointData>(tgs.Id, startTime, endTime);
                    ProcessResult<IntPointData>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = hisserver.ReadAllValue<UIntPointData>(tgs.Id, startTime, endTime);
                    ProcessResult<UIntPointData>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = hisserver.ReadAllValue<IntPoint3Data>(tgs.Id, startTime, endTime);
                    ProcessResult<IntPoint3Data>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = hisserver.ReadAllValue<UIntPoint3Data>(tgs.Id, startTime, endTime);
                    ProcessResult<UIntPoint3Data>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = hisserver.ReadAllValue<LongPointData>(tgs.Id, startTime, endTime);
                    ProcessResult<LongPointData>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = hisserver.ReadAllValue<ULongPointData>(tgs.Id, startTime, endTime);
                    ProcessResult<ULongPointData>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = hisserver.ReadAllValue<LongPoint3Data>(tgs.Id, startTime, endTime);
                    ProcessResult<LongPoint3Data>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = hisserver.ReadAllValue<ULongPoint3Data>(tgs.Id, startTime, endTime);
                    ProcessResult<ULongPoint3Data>(tag, res, result, (int)tgs.Type);
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        private void ReadTagStatisticsValue(string tag, DateTime startTime, DateTime endTime, StatisticsDataCollectionReplay result)
        {
            var tgs = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(tag);
            if (tgs == null) return;

           var  res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryStatisticsHisData(tgs.Id, startTime, endTime);
            ProcessStatisticsDataResult(tag, res, result, (int)tgs.Type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        private void ReadTagStatisticsValueLocal(string tag, DateTime startTime, DateTime endTime, StatisticsDataCollectionReplay result)
        {
            var tgs = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(tag);
            if (tgs == null) return;

            var res = ServiceLocator.Locator.Resolve<IHisQuery>().ReadNumberStatistics(tgs.Id, startTime, endTime);
            ProcessStatisticsDataResult(tag, res, result, (int)tgs.Type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="times"></param>
        /// <param name="result"></param>
        private void ReadTagStatisticsValue(string tag,List<DateTime> times, StatisticsDataCollectionReplay result)
        {
            var tgs = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(tag);
            if (tgs == null) return;

            var res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryStatisticsHisData(tgs.Id, times);
            ProcessStatisticsDataResult(tag, res, result, (int)tgs.Type);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="times"></param>
        /// <param name="result"></param>
        private void ReadTagStatisticsValueLocal(string tag, List<DateTime> times, StatisticsDataCollectionReplay result)
        {
            var tgs = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(tag);
            if (tgs == null) return;

            var res = ServiceLocator.Locator.Resolve<IHisQuery>().ReadNumberStatistics(tgs.Id, times);
            ProcessStatisticsDataResult(tag, res, result, (int)tgs.Type);
        }


        private void ProcessStatisticsDataResult(string tag, NumberStatisticsQueryResult value, StatisticsDataCollectionReplay result, int valueType)
        {
            StatisticsDataPointCollection hdp = new StatisticsDataPointCollection() { Tag = tag };
            DateTime time,maxvalueTime,minvalueTime;
            double avgvalue, minvalue, maxvalue;
            if (value != null)
            {
                for (int i = 0; i < value.Count; i++)
                {
                    var val = value.ReadValue(i, out time, out avgvalue, out maxvalue,out maxvalueTime, out minvalue, out minvalueTime);
                    hdp.Values.Add(new StatisticsDataPoint() { Time = time.ToBinary(), AvgValue = avgvalue,MaxTime=maxvalueTime.ToBinary(),MaxValue=maxvalue,MinTime=minvalueTime.ToBinary(),MinValue=minvalue });
                }
            }
            result.Values.Add(hdp);
        }

        private DateTime? FindNoNumberTagValue<T>(int id,DateTime startTime,DateTime endTime,object value)
        {
            if(Startup.IsRunningEmbed)
            {
                return ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValue<T>(id, startTime, endTime, value);
            }
            else
            {
                return DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValue(id,startTime, endTime, value);
            }
        }

        private Tuple<DateTime, object> FindNumberTagValue<T>(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value, object interval)
        {
            if (Startup.IsRunningEmbed)
            {
                return ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValue<T>(id, startTime, endTime, Convert.ToDouble(value), Convert.ToDouble(interval), type);
            }
            else
            {
                return DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValue(id,startTime,endTime,type,value,interval);
            }
        }

        private IEnumerable<DateTime> FindNoNumberTagValues<T>(int id, DateTime startTime, DateTime endTime, object value)
        {
            if (Startup.IsRunningEmbed)
            {
                return ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValues<T>(id, startTime, endTime, value);
            }
            else
            {
                return DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValues(id, startTime, endTime, value);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="value"></param>
        /// <param name="interval"></param>
        /// <returns></returns>
        private Dictionary<DateTime, object> FindNumberTagValues<T>(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value, object interval)
        {
            if (Startup.IsRunningEmbed)
            {
                return ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValues<T>(id, startTime, endTime, Convert.ToDouble(value), Convert.ToDouble(interval), type);
            }
            else
            {
                return DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValues(id, startTime, endTime, type, value, interval);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public double? FindNoNumberTagValueDuration<T>(int id, DateTime startTime, DateTime endTime,object value)
        {
            if (Startup.IsRunningEmbed)
            {
                return ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValueDuration<T>(id, startTime, endTime, value);
            }
            else
            {
                return DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValueDuration(id, startTime, endTime, value);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="value"></param>
        /// <param name="interval"></param>
        /// <returns></returns>
        public double? FindNumberTagValueDuration<T>(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value, object interval)
        {
            if (Startup.IsRunningEmbed)
            {
                return ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValueDuration<T>(id, startTime, endTime, Convert.ToDouble(value), Convert.ToDouble(interval), type);
            }
            else
            {
                return DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValueDuration(id, startTime, endTime, type,value,interval);
            }
        }

        public double? StatisticsTagAvgValue<T>(int id, DateTime startTime, DateTime endTime)
        {
            if (Startup.IsRunningEmbed)
            {
                return ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagAvgValue<T>(id, startTime, endTime);
            }
            else
            {
                return DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.StatisticsTagAvgValue(id, startTime, endTime);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public Tuple<double, List<DateTime>> StatisticsTagMaxMinValue<T>(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type)
        {
            if (Startup.IsRunningEmbed)
            {
                var re = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagMaxMinValue<T>(id, startTime, endTime,type,out IEnumerable<DateTime> times);
                return new Tuple<double, List<DateTime>>(re,times.ToList());
            }
            else
            {
                return DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.StatisticsTagMaxMinValue(id, startTime, endTime,type);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<FindTagValueDateTimeReplay> FindTagValue(FindTagValueRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                FindTagValueDateTimeReplay re = new FindTagValueDateTimeReplay() { Result = true };
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.Tag);
                if(tag != null)
                {
                    DateTime? dres = null;
                    Tuple<DateTime, object> res=null;
                    switch (tag.Type)
                    {
                        case Cdy.Tag.TagType.DateTime:
                            dres = FindNoNumberTagValue<DateTime>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToDateTime(request.Value));
                            if (dres != null)
                            {
                                re.Time.Add(dres.Value.Ticks);
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.Bool:
                            dres = FindNoNumberTagValue<bool>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToBoolean(request.Value));
                            if (dres != null)
                            {
                                re.Time.Add(dres.Value.Ticks);
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.String:
                            dres = FindNoNumberTagValue<string>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (dres != null)
                            {
                                re.Time.Add(dres.Value.Ticks);
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.IntPoint:
                            dres = FindNoNumberTagValue<IntPointData>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (dres != null)
                            {
                                re.Time.Add(dres.Value.Ticks);
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.UIntPoint:
                            dres = FindNoNumberTagValue<UIntPointData>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (dres != null)
                            {
                                re.Time.Add(dres.Value.Ticks);
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.IntPoint3:
                            dres = FindNoNumberTagValue<IntPoint3Data>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (dres != null)
                            {
                                re.Time.Add(dres.Value.Ticks);
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.UIntPoint3:
                            dres = FindNoNumberTagValue<UIntPoint3Data>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (dres != null)
                            {
                                re.Time.Add(dres.Value.Ticks);
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.LongPoint:
                            dres = FindNoNumberTagValue<LongPointData>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (dres != null)
                            {
                                re.Time.Add(dres.Value.Ticks);
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.ULongPoint:
                            dres = FindNoNumberTagValue<ULongPointData>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (dres != null)
                            {
                                re.Time.Add(dres.Value.Ticks);
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.LongPoint3:
                            dres = FindNoNumberTagValue<LongPoint3Data>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (dres != null)
                            {
                                re.Time.Add(dres.Value.Ticks);
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.ULongPoint3:
                            dres =FindNoNumberTagValue<ULongPoint3Data>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (dres != null)
                            {
                                re.Time.Add(dres.Value.Ticks);
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.Double:
                            res =FindNumberTagValue<double>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToDouble(request.Value),request.Interval);
                            if (res != null)
                            {
                                re.Time.Add(res.Item1.Ticks);
                                re.Value.Add(Convert.ToDouble(res.Item2));
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.Float:
                            res = FindNumberTagValue<float>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToSingle(request.Value), request.Interval);
                            if (res != null)
                            {
                                re.Time.Add(res.Item1.Ticks);
                                re.Value.Add(Convert.ToDouble(res.Item2));
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.Byte:
                            res = FindNumberTagValue<byte>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            if (res != null)
                            {
                                re.Time.Add(res.Item1.Ticks);
                                re.Value.Add(Convert.ToDouble(res.Item2));
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.Int:
                            res = FindNumberTagValue<int>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            if (res != null)
                            {
                                re.Time.Add(res.Item1.Ticks);
                                re.Value.Add(Convert.ToDouble(res.Item2));
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.Long:
                            res = FindNumberTagValue<long>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            if (res != null)
                            {
                                re.Time.Add(res.Item1.Ticks);
                                re.Value.Add(Convert.ToDouble(res.Item2));
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.UInt:
                            res = FindNumberTagValue<uint>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            if (res != null)
                            {
                                re.Time.Add(res.Item1.Ticks);
                                re.Value.Add(Convert.ToDouble(res.Item2));
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.Short:
                            res = FindNumberTagValue<short>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            if (res != null)
                            {
                                re.Time.Add(res.Item1.Ticks);
                                re.Value.Add(Convert.ToDouble(res.Item2));
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.ULong:
                            res = FindNumberTagValue<ulong>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            if (res != null)
                            {
                                re.Time.Add(res.Item1.Ticks);
                                re.Value.Add(Convert.ToDouble(res.Item2));
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;
                        case Cdy.Tag.TagType.UShort:
                            res = FindNumberTagValue<ushort>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            if (res != null)
                            {
                                re.Time.Add(res.Item1.Ticks);
                                re.Value.Add(Convert.ToDouble(res.Item2));
                            }
                            else
                            {
                                re.Result = false;
                            }
                            break;

                    }
                    
                }
                else
                {
                    re.Result = false;
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new FindTagValueDateTimeReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<FindTagValueDateTimeReplay> FindTagValues(FindTagValueRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                FindTagValueDateTimeReplay re = new FindTagValueDateTimeReplay() { Result = true };
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.Tag);
                if (tag != null)
                {
                    Dictionary<DateTime,object> res = null;
                    IEnumerable<DateTime> dres;
                    switch (tag.Type)
                    {
                        case Cdy.Tag.TagType.DateTime:
                            dres = FindNoNumberTagValues<DateTime>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToDateTime(request.Value));
                            if (res != null)
                            {
                                foreach (var vv in dres)
                                {
                                    re.Time.Add(vv.Ticks);
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.Bool:
                            dres = FindNoNumberTagValues<bool>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
                            if (res != null)
                            {
                                foreach (var vv in dres)
                                {
                                    re.Time.Add(vv.Ticks);
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.String:
                            dres = FindNoNumberTagValues<string>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (res != null)
                            {
                                foreach (var vv in dres)
                                {
                                    re.Time.Add(vv.Ticks);
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.IntPoint:
                            dres = FindNoNumberTagValues<IntPointData>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (res != null)
                            {
                                foreach (var vv in dres)
                                {
                                    re.Time.Add(vv.Ticks);
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.UIntPoint:
                            dres = FindNoNumberTagValues<UIntPointData>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (res != null)
                            {
                                foreach (var vv in dres)
                                {
                                    re.Time.Add(vv.Ticks);
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.IntPoint3:
                            dres = FindNoNumberTagValues<IntPoint3Data>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (res != null)
                            {
                                foreach (var vv in dres)
                                {
                                    re.Time.Add(vv.Ticks);
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.UIntPoint3:
                            dres = FindNoNumberTagValues<UIntPoint3Data>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (res != null)
                            {
                                foreach (var vv in dres)
                                {
                                    re.Time.Add(vv.Ticks);
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.LongPoint:
                            dres = FindNoNumberTagValues<LongPointData>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (res != null)
                            {
                                foreach (var vv in dres)
                                {
                                    re.Time.Add(vv.Ticks);
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.ULongPoint:
                            dres = FindNoNumberTagValues<ULongPointData>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (res != null)
                            {
                                foreach (var vv in dres)
                                {
                                    re.Time.Add(vv.Ticks);
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.LongPoint3:
                            dres = FindNoNumberTagValues<LongPoint3Data>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (res != null)
                            {
                                foreach (var vv in dres)
                                {
                                    re.Time.Add(vv.Ticks);
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.ULongPoint3:
                            dres = FindNoNumberTagValues<ULongPoint3Data>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Value);
                            if (res != null)
                            {
                                foreach (var vv in dres)
                                {
                                    re.Time.Add(vv.Ticks);
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.Double:
                            res = FindNumberTagValues<double>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToDouble(request.Value), request.Interval);
                            if (res != null)
                            {
                                foreach (var vv in res)
                                {
                                    re.Time.Add(vv.Key.Ticks);
                                    re.Value.Add(Convert.ToDouble(vv.Value));
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.Float:
                            res = FindNumberTagValues<float>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToSingle(request.Value), request.Interval);
                            if (res != null)
                            {
                                foreach (var vv in res)
                                {
                                    re.Time.Add(vv.Key.Ticks);
                                    re.Value.Add(Convert.ToDouble(vv.Value));
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.Byte:
                            res = FindNumberTagValues<byte>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            if (res != null)
                            {
                                foreach (var vv in res)
                                {
                                    re.Time.Add(vv.Key.Ticks);
                                    re.Value.Add(Convert.ToDouble(vv.Value));
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.Int:
                            res = FindNumberTagValues<int>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            if (res != null)
                            {
                                foreach (var vv in res)
                                {
                                    re.Time.Add(vv.Key.Ticks);
                                    re.Value.Add(Convert.ToDouble(vv.Value));
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.Long:
                            res = FindNumberTagValues<long>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            if (res != null)
                            {
                                foreach (var vv in res)
                                {
                                    re.Time.Add(vv.Key.Ticks);
                                    re.Value.Add(Convert.ToDouble(vv.Value));
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.UInt:
                            res = FindNumberTagValues<uint>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            if (res != null)
                            {
                                foreach (var vv in res)
                                {
                                    re.Time.Add(vv.Key.Ticks);
                                    re.Value.Add(Convert.ToDouble(vv.Value));
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.Short:
                            res = FindNumberTagValues<short>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            if (res != null)
                            {
                                foreach (var vv in res)
                                {
                                    re.Time.Add(vv.Key.Ticks);
                                    re.Value.Add(Convert.ToDouble(vv.Value));
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.ULong:
                            res = FindNumberTagValues<ulong>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            if (res != null)
                            {
                                foreach (var vv in res)
                                {
                                    re.Time.Add(vv.Key.Ticks);
                                    re.Value.Add(Convert.ToDouble(vv.Value));
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.UShort:
                            res = FindNumberTagValues<ushort>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            if (res != null)
                            {
                                foreach (var vv in res)
                                {
                                    re.Time.Add(vv.Key.Ticks);
                                    re.Value.Add(Convert.ToDouble(vv.Value));
                                }
                            }
                            break;

                    }
                   
                }
                else
                {
                    re.Result = false;
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new FindTagValueDateTimeReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<FindTagValueDoubleReplay> CalTagValueKeepTime(FindTagValueRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                FindTagValueDoubleReplay re = new FindTagValueDoubleReplay() { Result = true };
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.Tag);
                if (tag != null)
                {
                    double? res = null;
                    switch (tag.Type)
                    {
                        case Cdy.Tag.TagType.DateTime:
                            res = FindNoNumberTagValueDuration<DateTime>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
                            break;
                        case Cdy.Tag.TagType.Bool:
                            res = FindNoNumberTagValueDuration<bool>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
                            break;
                        case Cdy.Tag.TagType.String:
                            res = FindNoNumberTagValueDuration<string>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
                            break;
                        case Cdy.Tag.TagType.IntPoint:
                            res = FindNoNumberTagValueDuration<IntPointData>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
                            break;
                        case Cdy.Tag.TagType.UIntPoint:
                            res = FindNoNumberTagValueDuration<UIntPointData>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
                            break;
                        case Cdy.Tag.TagType.IntPoint3:
                            res = FindNoNumberTagValueDuration<IntPoint3Data>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
                            break;
                        case Cdy.Tag.TagType.UIntPoint3:
                            res = FindNoNumberTagValueDuration<UIntPoint3Data>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
                            break;
                        case Cdy.Tag.TagType.LongPoint:
                            res = FindNoNumberTagValueDuration<LongPointData>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
                            break;
                        case Cdy.Tag.TagType.ULongPoint:
                            res = FindNoNumberTagValueDuration<ULongPointData>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
                            break;
                        case Cdy.Tag.TagType.LongPoint3:
                            res = FindNoNumberTagValueDuration<LongPoint3Data>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
                            break;
                        case Cdy.Tag.TagType.ULongPoint3:
                            res = FindNoNumberTagValueDuration<ULongPoint3Data>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
                            break;
                        case Cdy.Tag.TagType.Double:
                            res = FindNumberTagValueDuration<double>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToDouble(request.Value),request.Interval);
                            break;
                        case Cdy.Tag.TagType.Float:
                            res = FindNumberTagValueDuration<float>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToSingle(request.Value), request.Interval);
                            break;
                        case Cdy.Tag.TagType.Byte:
                            res = FindNumberTagValueDuration<byte>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            break;
                        case Cdy.Tag.TagType.Int:
                            res = FindNumberTagValueDuration<int>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            break;
                        case Cdy.Tag.TagType.Long:
                            res = FindNumberTagValueDuration<long>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            break;
                        case Cdy.Tag.TagType.UInt:
                            res = FindNumberTagValueDuration<uint>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            break;
                        case Cdy.Tag.TagType.Short:
                            res = FindNumberTagValueDuration<short>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            break;
                        case Cdy.Tag.TagType.ULong:
                            res = FindNumberTagValueDuration<ulong>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            break;
                        case Cdy.Tag.TagType.UShort:
                            res = FindNumberTagValueDuration<ushort>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
                            break;

                    }
                    if (res!=null)
                    {
                        re.Values = res.Value;
                    }
                    else
                    {
                        re.Result = false;
                    }
                }
                else
                {
                    re.Result = false;
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new FindTagValueDoubleReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<FindTagValueDoubleReplay> CalNumberTagAvgValue(FindTagValueRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                FindTagValueDoubleReplay re = new FindTagValueDoubleReplay() { Result = true };
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.Tag);
                if (tag != null)
                {
                    double? res = null;
                    switch (tag.Type)
                    {
                        case Cdy.Tag.TagType.Double:
                            res = StatisticsTagAvgValue<double>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime));
                            break;
                        case Cdy.Tag.TagType.Float:
                            res = StatisticsTagAvgValue<float>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime));
                            break;
                        case Cdy.Tag.TagType.Byte:
                            res = StatisticsTagAvgValue<byte>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime));
                            break;
                        case Cdy.Tag.TagType.Int:
                            res = StatisticsTagAvgValue<int>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime));
                            break;
                        case Cdy.Tag.TagType.Long:
                            res = StatisticsTagAvgValue<long>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime));
                            break;
                        case Cdy.Tag.TagType.UInt:
                            res = StatisticsTagAvgValue<uint>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime));
                            break;
                        case Cdy.Tag.TagType.Short:
                            res = StatisticsTagAvgValue<short>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime));
                            break;
                        case Cdy.Tag.TagType.ULong:
                            res = StatisticsTagAvgValue<ulong>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime));
                            break;
                        case Cdy.Tag.TagType.UShort:
                            res = StatisticsTagAvgValue<ushort>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime));
                            break;

                    }
                    if (res != null)
                    {
                        re.Values = res.Value;
                    }
                    else
                    {
                        re.Result = false;
                    }
                }
                else
                {
                    re.Result = false;
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new FindTagValueDoubleReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<FindTagValueDoubleDateTimeReplay> FindNumberTagMaxValue(FindTagValueRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                FindTagValueDoubleDateTimeReplay re = new FindTagValueDoubleDateTimeReplay() { Result = true };
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.Tag);
                if (tag != null)
                {
                    Tuple<double,List<DateTime>> res = null;
                    switch (tag.Type)
                    {
                        case Cdy.Tag.TagType.Double:
                            res = StatisticsTagMaxMinValue<double>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Max);
                            break;
                        case Cdy.Tag.TagType.Float:
                            res = StatisticsTagMaxMinValue<float>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Max);
                            break;
                        case Cdy.Tag.TagType.Byte:
                            res = StatisticsTagMaxMinValue<byte>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Max);
                            break;
                        case Cdy.Tag.TagType.Int:
                            res = StatisticsTagMaxMinValue<int>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Max);
                            break;
                        case Cdy.Tag.TagType.Long:
                            res = StatisticsTagMaxMinValue<long>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Max);
                            break;
                        case Cdy.Tag.TagType.UInt:
                            res = StatisticsTagMaxMinValue<uint>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Max);
                            break;
                        case Cdy.Tag.TagType.Short:
                            res = StatisticsTagMaxMinValue<short>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Max);
                            break;
                        case Cdy.Tag.TagType.ULong:
                            res = StatisticsTagMaxMinValue<ulong>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Max);
                            break;
                        case Cdy.Tag.TagType.UShort:
                            res = StatisticsTagMaxMinValue<ushort>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime),NumberStatisticsType.Max);
                            break;

                    }
                    if (res != null)
                    {
                        re.Values = res.Item1;
                        re.Times.Add(res.Item2.Select(e=>e.Ticks));
                    }
                    else
                    {
                        re.Result = false;
                    }
                }
                else
                {
                    re.Result = false;
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new FindTagValueDoubleDateTimeReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<FindTagValueDoubleDateTimeReplay> FindNumberTagMinValue(FindTagValueRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                FindTagValueDoubleDateTimeReplay re = new FindTagValueDoubleDateTimeReplay() { Result = true };
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.Tag);
                if (tag != null)
                {
                    Tuple<double, List<DateTime>> res = null;
                    switch (tag.Type)
                    {
                        case Cdy.Tag.TagType.Double:
                            res = StatisticsTagMaxMinValue<double>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Min);
                            break;
                        case Cdy.Tag.TagType.Float:
                            res = StatisticsTagMaxMinValue<float>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Min);
                            break;
                        case Cdy.Tag.TagType.Byte:
                            res = StatisticsTagMaxMinValue<byte>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Min);
                            break;
                        case Cdy.Tag.TagType.Int:
                            res = StatisticsTagMaxMinValue<int>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Min);
                            break;
                        case Cdy.Tag.TagType.Long:
                            res = StatisticsTagMaxMinValue<long>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Min);
                            break;
                        case Cdy.Tag.TagType.UInt:
                            res = StatisticsTagMaxMinValue<uint>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Min);
                            break;
                        case Cdy.Tag.TagType.Short:
                            res = StatisticsTagMaxMinValue<short>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Min);
                            break;
                        case Cdy.Tag.TagType.ULong:
                            res = StatisticsTagMaxMinValue<ulong>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Min);
                            break;
                        case Cdy.Tag.TagType.UShort:
                            res =StatisticsTagMaxMinValue<ushort>(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Min);
                            break;

                    }
                    if (res != null)
                    {
                        re.Values = res.Item1;
                        re.Times.Add(res.Item2.Select(e => e.Ticks));
                    }
                    else
                    {
                        re.Result = false;
                    }
                }
                else
                {
                    re.Result = false;
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new FindTagValueDoubleDateTimeReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<QueryBySqlReplay> QueryDataBySql(QueryBySqlRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                return Task.FromResult(ExecuteSqlQuery(request.Sql));
            }
            else
            {
                return Task.FromResult(new QueryBySqlReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        private QueryBySqlReplay ExecuteSqlQuery(string sql)
        {
            if (Startup.IsRunningEmbed)
            {
                return ExecuteByLocal(sql);
            }
            else
            {
                return ExecuteFromRemote(sql);
            }
        }

        private QueryBySqlReplay ExecuteFromRemote(string sql)
        {
            var tagservice = ServiceLocator.Locator.Resolve<ITagManager>();
            QueryBySqlReplay reqs = new QueryBySqlReplay() { Result = true };
            reqs.Value = new StringTable();
            var res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisValueBySql(sql);
            if (res is ByteBuffer)
            {
                var re = res as ByteBuffer;
                object val = null;
                List<RealTagValueWithTimer> rre = new List<RealTagValueWithTimer>();
                var valuecount = re.ReadInt();
                for (int i = 0; i < valuecount; i++)
                {
                    var vvid = re.ReadInt();
                    var type = re.ReadByte();

                    switch (type)
                    {
                        case (byte)TagType.Bool:
                            val = ((byte)re.ReadByte());
                            break;
                        case (byte)TagType.Byte:
                            val = ((byte)re.ReadByte());
                            break;
                        case (byte)TagType.Short:
                            val = (re.ReadShort());
                            break;
                        case (byte)TagType.UShort:
                            val = (re.ReadUShort());
                            break;
                        case (byte)TagType.Int:
                            val = (re.ReadInt());
                            break;
                        case (byte)TagType.UInt:
                            val = (re.ReadUInt());
                            break;
                        case (byte)TagType.Long:
                        case (byte)TagType.ULong:
                            val = (re.ReadULong());
                            break;
                        case (byte)TagType.Float:
                            val = (re.ReadFloat());
                            break;
                        case (byte)TagType.Double:
                            val = (re.ReadDouble());
                            break;
                        case (byte)TagType.String:
                            val = re.ReadString();
                            break;
                        case (byte)TagType.DateTime:
                            val = DateTime.FromBinary(re.ReadLong());
                            break;
                        case (byte)TagType.IntPoint:
                            val = new IntPointData(re.ReadInt(), re.ReadInt());
                            break;
                        case (byte)TagType.UIntPoint:
                            val = new UIntPointData(re.ReadInt(), re.ReadInt());
                            break;
                        case (byte)TagType.IntPoint3:
                            val = new IntPoint3Data(re.ReadInt(), re.ReadInt(), re.ReadInt());
                            break;
                        case (byte)TagType.UIntPoint3:
                            val = new UIntPoint3Data(re.ReadInt(), re.ReadInt(), re.ReadInt());
                            break;
                        case (byte)TagType.LongPoint:
                            val = new LongPointData(re.ReadLong(), re.ReadLong());
                            break;
                        case (byte)TagType.ULongPoint:
                            val = new ULongPointData(re.ReadLong(), re.ReadLong());
                            break;
                        case (byte)TagType.LongPoint3:
                            val = new LongPoint3Data(re.ReadLong(), re.ReadLong(), re.ReadLong());
                            break;
                        case (byte)TagType.ULongPoint3:
                            val = new ULongPoint3Data(re.ReadLong(), re.ReadLong(), re.ReadLong());
                            break;
                        case (byte)TagType.Complex:
                            int count = re.ReadInt();
                            for (int j = 0; j < count; j++)
                            {
                                var cvid = re.ReadInt();
                                var vtp = re.ReadByte();
                                object cval = null;
                                switch (vtp)
                                {
                                    case (byte)TagType.Bool:
                                        cval = ((byte)re.ReadByte());
                                        break;
                                    case (byte)TagType.Byte:
                                        cval = ((byte)re.ReadByte());
                                        break;
                                    case (byte)TagType.Short:
                                        cval = (re.ReadShort());
                                        break;
                                    case (byte)TagType.UShort:
                                        cval = (re.ReadUShort());
                                        break;
                                    case (byte)TagType.Int:
                                        cval = (re.ReadInt());
                                        break;
                                    case (byte)TagType.UInt:
                                        cval = (re.ReadUInt());
                                        break;
                                    case (byte)TagType.Long:
                                    case (byte)TagType.ULong:
                                        cval = (re.ReadULong());
                                        break;
                                    case (byte)TagType.Float:
                                        cval = (re.ReadFloat());
                                        break;
                                    case (byte)TagType.Double:
                                        cval = (re.ReadDouble());
                                        break;
                                    case (byte)TagType.String:
                                        cval = re.ReadString();
                                        break;
                                    case (byte)TagType.DateTime:
                                        cval = DateTime.FromBinary(re.ReadLong());
                                        break;
                                    case (byte)TagType.IntPoint:
                                        cval = new IntPointData(re.ReadInt(), re.ReadInt());
                                        break;
                                    case (byte)TagType.UIntPoint:
                                        cval = new UIntPointData(re.ReadInt(), re.ReadInt());
                                        break;
                                    case (byte)TagType.IntPoint3:
                                        cval = new IntPoint3Data(re.ReadInt(), re.ReadInt(), re.ReadInt());
                                        break;
                                    case (byte)TagType.UIntPoint3:
                                        cval = new UIntPoint3Data(re.ReadInt(), re.ReadInt(), re.ReadInt());
                                        break;
                                    case (byte)TagType.LongPoint:
                                        cval = new LongPointData(re.ReadLong(), re.ReadLong());
                                        break;
                                    case (byte)TagType.ULongPoint:
                                        cval = new ULongPointData(re.ReadLong(), re.ReadLong());
                                        break;
                                    case (byte)TagType.LongPoint3:
                                        cval = new LongPoint3Data(re.ReadLong(), re.ReadLong(), re.ReadLong());
                                        break;
                                    case (byte)TagType.ULongPoint3:
                                        cval = new ULongPoint3Data(re.ReadLong(), re.ReadLong(), re.ReadLong());
                                        break;
                                }
                                var ctime = DateTime.FromBinary(re.ReadLong()).ToLocalTime();
                                var cqua = re.ReadByte();

                                 var isrow = new StringTableItem();
                                isrow.Columns.Add(cvid.ToString());
                                isrow.Columns.Add(vtp.ToString());
                                isrow.Columns.Add(cval.ToString());
                                isrow.Columns.Add(cqua.ToString());
                                isrow.Columns.Add(ctime.ToString());
                                reqs.Value.Rows.Add(isrow);
                            }
                            val = null;
                            break;
                    }
                    var vtime = DateTime.FromBinary(re.ReadLong()).ToLocalTime();
                    var qua = re.ReadByte();
                    if (val != null)
                    {
                        var srow = new StringTableItem();
                        srow.Columns.Add(vvid.ToString());
                        srow.Columns.Add(val.ToString());
                        srow.Columns.Add(qua.ToString());
                        srow.Columns.Add(vtime.ToString());
                        reqs.Value.Rows.Add(srow);
                    }
                }
                reqs.ValueType = 2;
                re.UnlockAndReturn();
            }
            else
            {
                if (res is HisQueryTableResult)
                {
                  
                    var htr = (res as HisQueryTableResult);
                    if (htr != null)
                    {
                        var srow = new StringTableItem();
                        reqs.Value.Rows.Add(srow);
                        srow.Columns.Add("time");
                        foreach(var vv in htr.Columns.Keys)
                        {
                            try
                            {
                                var vtag = tagservice.GetTagById(int.Parse(vv));
                                if (vtag != null)
                                {
                                    srow.Columns.Add(vtag.FullName);
                                }
                                else
                                {
                                    srow.Columns.Add(vv);
                                }
                            }
                            catch
                            {
                                srow.Columns.Add(vv);
                            }
                        }
                        //srow.Columns.AddRange(htr.Columns.Keys);
                        foreach (var vv in htr.ReadRows())
                        {
                            srow = new StringTableItem();
                            reqs.Value.Rows.Add(srow);
                            srow.Columns.Add(vv.Item1.ToString());
                            srow.Columns.AddRange(vv.Item2.Select(e => e.ToString()));
                        }
                    }
                    reqs.ValueType = 0;
                }
                else if (res is List<double>)
                {
                    var srow = new StringTableItem();
                    reqs.Value.Rows.Add(srow);
                    srow.Columns.AddRange((res as List<double>).Select(e => e.ToString()));
                    reqs.ValueType = 1;
                }
                else
                {
                    reqs.Result=false;
                }
            }
            return reqs;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        private QueryBySqlReplay ExecuteByLocal(string sql)
        {
            var tagservice = ServiceLocator.Locator.Resolve<ITagManager>();
            QueryBySqlReplay reqs = new QueryBySqlReplay() { Result = true };
            try
            {
                if (!string.IsNullOrEmpty(sql))
                {
                    var sqlexp = ParseSql(sql, out List<int> selids, out Dictionary<int, byte> tps);
                    if (sqlexp.Where == null || (sqlexp.Where.LowerTime == null && sqlexp.Where.UpperTime == null))
                    {
                        var res = ProcessRealData(selids);
                        if(res != null)
                        {
                            foreach(var vvres in res)
                            {
                                var srow = new StringTableItem();
                                reqs.Value.Rows.Add(srow);
                                srow.Columns.Add(vvres.Id.ToString());
                                srow.Columns.Add(vvres.Value.ToString());
                                srow.Columns.Add(vvres.Quality.ToString());
                                srow.Columns.Add(vvres.Time.ToString());
                            }
                        }
                        reqs.ValueType = 2;
                    }
                    else if (sqlexp.Where.UpperTime == null)
                    {
                        sqlexp.Where.UpperTime = new LowerEqualAction() { IgnorFit = true, Target = DateTime.Now.ToString() };
                    }
                    else if (sqlexp.Where.LowerTime == null)
                    {
                        LoggerService.Service.Warn("HisDataController", $"Sql 语句格式不支持.");
                        return null;
                    }
                    var qq = ServiceLocator.Locator.Resolve<IHisQuery>().ReadAllValueAndFilter(selids, DateTime.Parse(sqlexp.Where.LowerTime.Target.ToString()), DateTime.Parse(sqlexp.Where.UpperTime.Target.ToString()), sqlexp.Where, tps);
                    if (qq != null)
                    {
                        if (sqlexp.Select.IsAllNone())
                        {
                            var srow = new StringTableItem();
                            reqs.Value.Rows.Add(srow);
                            foreach (var vv in qq.Columns.Keys)
                            {
                                try
                                {
                                    var vtag = tagservice.GetTagById(int.Parse(vv));
                                    if (vtag != null)
                                    {
                                        srow.Columns.Add(vtag.FullName);
                                    }
                                    else
                                    {
                                        srow.Columns.Add(vv);
                                    }
                                }
                                catch
                                {
                                    srow.Columns.Add(vv);
                                }
                            }
                            //srow.Columns.AddRange(qq.Columns.Keys);

                            foreach (var vv in qq.ReadRows())
                            {
                                srow = new StringTableItem();
                                reqs.Value.Rows.Add(srow);
                                srow.Columns.Add(vv.Item1.ToString());
                                srow.Columns.AddRange(vv.Item2.Select(e => e.ToString()));
                            }
                            qq.Dispose();
                            reqs.ValueType = 0;
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
                            qq.Dispose();
                            var srow = new StringTableItem();
                            reqs.Value.Rows.Add(srow);
                            srow.Columns.AddRange(vals.Select(e => e.ToString()));
                            reqs.ValueType = 1;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("HisDataController", ex.Message);
            }
            return reqs;
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
        private List<RealTagValueWithTimer> ProcessRealData(List<int> cc)
        {
            List<RealTagValueWithTimer> revals = new List<RealTagValueWithTimer>();
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            for (int i = 0; i < cc.Count; i++)
            {
                var vv = cc[i];
                byte qu, type;
                DateTime time;
                object value;

                if (service.IsComplexTag(vv))
                {
                    List<RealTagValueWithTimer> vals = new List<RealTagValueWithTimer>();
                    service.GetComplexTagValue(vv, vals);
                    revals.AddRange(vals);
                }
                else
                {
                    value = service.GetTagValue(vv, out qu, out time, out type);
                    revals.Add(new RealTagValueWithTimer() { Id = vv, Value = value, Quality = qu, Time = time, ValueType = type });
                }
            }
            return revals;
        }

    }
}
