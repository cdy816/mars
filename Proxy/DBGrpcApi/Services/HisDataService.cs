using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cdy.Tag;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace DBGrpcApi
{
    public class HisDataService : HislData.HislDataBase
    {
        private readonly ILogger<RealDataService> _logger;
        public HisDataService(ILogger<RealDataService> logger)
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
                        ReadTagHisValue(vv, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Duration, request.QueryType, re);
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
                        ReadTagAllHisValue(vv, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), re);
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
                        ReadTagStatisticsValue(vv, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), re);
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
                        ReadTagStatisticsValue(vv, ltmp, re);
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
                    hdp.Values.Add(new HisDataPoint() {  Time = time.ToBinary(), Value = val.ToString() });
                }
            }
            result.Values.Add(hdp);
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
            switch (tgs.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<bool>(tgs.Id,startTime,endTime,TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<bool>(tag, res,result,(int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<byte>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<byte>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<DateTime>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<DateTime>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<double>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<double>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<float>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<float>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<int>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<int>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<long>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<long>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<short>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<short>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<string>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<string>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<uint>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<uint>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ulong>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<ulong>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ushort>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<ushort>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPointData>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<IntPointData>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPointData>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<UIntPointData>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPoint3Data>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<IntPoint3Data>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPoint3Data>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<UIntPoint3Data>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPointData>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<LongPointData>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPointData>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<ULongPointData>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPoint3Data>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                     ProcessResult<LongPoint3Data>(tag, res, result, (int)tgs.Type);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPoint3Data>(tgs.Id, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
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
        /// <param name="times"></param>
        /// <param name="result"></param>
        private void ReadTagStatisticsValue(string tag,List<DateTime> times, StatisticsDataCollectionReplay result)
        {
            var tgs = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(tag);
            if (tgs == null) return;

            var res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryStatisticsHisData(tgs.Id, times);
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
                        case Cdy.Tag.TagType.Bool:
                        case Cdy.Tag.TagType.String:
                        case Cdy.Tag.TagType.IntPoint:
                        case Cdy.Tag.TagType.UIntPoint:
                        case Cdy.Tag.TagType.IntPoint3:
                        case Cdy.Tag.TagType.UIntPoint3:
                        case Cdy.Tag.TagType.LongPoint:
                        case Cdy.Tag.TagType.ULongPoint:
                        case Cdy.Tag.TagType.LongPoint3:
                        case Cdy.Tag.TagType.ULongPoint3:
                            dres = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValue(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
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
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValue(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToDouble(request.Value),request.Interval);
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
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValue(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToSingle(request.Value), request.Interval);
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
                        case Cdy.Tag.TagType.Int:
                        case Cdy.Tag.TagType.Long:
                        case Cdy.Tag.TagType.UInt:
                        case Cdy.Tag.TagType.Short:
                        case Cdy.Tag.TagType.ULong:
                        case Cdy.Tag.TagType.UShort:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValue(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
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
                        case Cdy.Tag.TagType.Bool:
                        case Cdy.Tag.TagType.String:
                        case Cdy.Tag.TagType.IntPoint:
                        case Cdy.Tag.TagType.UIntPoint:
                        case Cdy.Tag.TagType.IntPoint3:
                        case Cdy.Tag.TagType.UIntPoint3:
                        case Cdy.Tag.TagType.LongPoint:
                        case Cdy.Tag.TagType.ULongPoint:
                        case Cdy.Tag.TagType.LongPoint3:
                        case Cdy.Tag.TagType.ULongPoint3:
                            dres = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValues(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
                            if (res != null)
                            {
                                foreach (var vv in dres)
                                {
                                    re.Time.Add(vv.Ticks);
                                }
                            }
                            break;
                        case Cdy.Tag.TagType.Double:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValues(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToDouble(request.Value), request.Interval);
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
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValues(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToSingle(request.Value), request.Interval);
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
                        case Cdy.Tag.TagType.Int:
                        case Cdy.Tag.TagType.Long:
                        case Cdy.Tag.TagType.UInt:
                        case Cdy.Tag.TagType.Short:
                        case Cdy.Tag.TagType.ULong:
                        case Cdy.Tag.TagType.UShort:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValues(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
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
                        case Cdy.Tag.TagType.Bool:
                        case Cdy.Tag.TagType.String:
                        case Cdy.Tag.TagType.IntPoint:
                        case Cdy.Tag.TagType.UIntPoint:
                        case Cdy.Tag.TagType.IntPoint3:
                        case Cdy.Tag.TagType.UIntPoint3:
                        case Cdy.Tag.TagType.LongPoint:
                        case Cdy.Tag.TagType.ULongPoint:
                        case Cdy.Tag.TagType.LongPoint3:
                        case Cdy.Tag.TagType.ULongPoint3:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValueDuration(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), Convert.ToByte(request.Value));
                            break;
                        case Cdy.Tag.TagType.Double:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValueDuration(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToDouble(request.Value),request.Interval);
                            break;
                        case Cdy.Tag.TagType.Float:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValueDuration(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToSingle(request.Value), request.Interval);
                            break;
                        case Cdy.Tag.TagType.Byte:
                        case Cdy.Tag.TagType.Int:
                        case Cdy.Tag.TagType.Long:
                        case Cdy.Tag.TagType.UInt:
                        case Cdy.Tag.TagType.Short:
                        case Cdy.Tag.TagType.ULong:
                        case Cdy.Tag.TagType.UShort:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValueDuration(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), (NumberStatisticsType)(byte)request.CompareType, Convert.ToInt64(request.Value), request.Interval);
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
                        case Cdy.Tag.TagType.Float:
                        case Cdy.Tag.TagType.Byte:
                        case Cdy.Tag.TagType.Int:
                        case Cdy.Tag.TagType.Long:
                        case Cdy.Tag.TagType.UInt:
                        case Cdy.Tag.TagType.Short:
                        case Cdy.Tag.TagType.ULong:
                        case Cdy.Tag.TagType.UShort:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.StatisticsTagAvgValue(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime));
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
                        case Cdy.Tag.TagType.Float:
                        case Cdy.Tag.TagType.Byte:
                        case Cdy.Tag.TagType.Int:
                        case Cdy.Tag.TagType.Long:
                        case Cdy.Tag.TagType.UInt:
                        case Cdy.Tag.TagType.Short:
                        case Cdy.Tag.TagType.ULong:
                        case Cdy.Tag.TagType.UShort:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.StatisticsTagMaxMinValue(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime),NumberStatisticsType.Max);
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
                        case Cdy.Tag.TagType.Float:
                        case Cdy.Tag.TagType.Byte:
                        case Cdy.Tag.TagType.Int:
                        case Cdy.Tag.TagType.Long:
                        case Cdy.Tag.TagType.UInt:
                        case Cdy.Tag.TagType.Short:
                        case Cdy.Tag.TagType.ULong:
                        case Cdy.Tag.TagType.UShort:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.StatisticsTagMaxMinValue(tag.Id, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), NumberStatisticsType.Min);
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

    }
}
