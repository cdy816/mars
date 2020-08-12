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
                return tag.Substring(0, tag.LastIndexOf(".") - 1);
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

    }
}
