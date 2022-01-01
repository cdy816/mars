using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cdy.Tag;
using DbInRunWebApi.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NSwag.Annotations;

namespace DbInRunWebApi.Controllers
{
    /// <summary>
    /// 
    /// </summary>
    [ApiController]
    [Route("[controller]")]
    [OpenApiTag("变量历史值服务", Description = "变量历史值服务")]
    public class HisDataController : ControllerBase
    {
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tagname"></param>
        /// <param name="datas"></param>
        /// <returns></returns>
        private HisValue ProcessResult<T>(string tagname, object datas)
        {
            if (datas != null)
            {
                HisValue re = new HisValue() { tagName = tagname, Result = true };
                List<ValueItem> values = new List<ValueItem>();
                var vdata = datas as HisQueryResult<T>;
                if (vdata != null)
                {
                    for (int i = 0; i < vdata.Count; i++)
                    {
                        byte qu;
                        DateTime time;
                        var val = vdata.GetValue(i, out time, out qu);
                        values.Add(new ValueItem() { Time = time, Quality = qu, Value = val.ToString() });
                    }
                    re.Values = values;

                    vdata.Dispose();
                }
                else
                {
                    if(datas!=null && datas is IDisposable)
                    {
                        (datas as IDisposable).Dispose();
                    }
                    re.Result = false;
                    re.ErroMessage = "no result";
                }
                return re;
            }
            return null;
        }


        //private HisValue ProcessResult<T>(string tagname, Dictionary<DateTime, Tuple<object, byte>> datas)
        //{
        //    HisValue re = new HisValue() { tagName = tagname, Result = true };
        //    List<ValueItem> values = new List<ValueItem>();
        //    if (datas != null)
        //    {
        //        foreach (var vv in datas)
        //        {
        //            values.Add(new ValueItem() { Time = vv.Key, Value = vv.Value.Item1.ToString(), Quality = vv.Value.Item2 });
        //        }
        //    }
        //    re.Values = values;
        //    return re;
        //}

        private List<DateTime> ConvertToTimes(List<string> time)
        {
            return time.Select(e => DateTime.Parse(e)).ToList();
        }
        /// <summary>
        /// 获取变量指定时间点上的历史值
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet()]
        public HisValue Get([FromBody] HisDataRequest request)
        {
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new HisValue() { Result = false, ErroMessage = "not login" };
            }
            try
            {
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
                if (tag == null) return null;
                object res;
                HisValue revals = null;
                switch (tag.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<bool>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<bool>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Byte:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<byte>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<byte>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<DateTime>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<DateTime>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Double:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<double>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<double>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Float:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<float>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<float>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Int:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<int>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<int>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Long:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<long>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<long>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Short:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<short>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<short>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.String:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<string>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<string>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.UInt:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<uint>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<uint>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.ULong:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ulong>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<ulong>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.UShort:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ushort>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<ushort>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<IntPointData>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<UIntPointData>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<IntPoint3Data>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<UIntPoint3Data>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<LongPointData>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPointTag>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<ULongPointData>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<LongPoint3Data>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                        revals = ProcessResult<ULongPoint3Data>(request.TagName, res);
                        break;
                }

                if (revals != null)
                {
                    return revals;
                }
                else
                {
                    return new HisValue() { Result = false, ErroMessage = "No result", tagName = request.TagName };
                }
            }
            catch(Exception ex)
            {
                return new HisValue() { Result = false, ErroMessage = ex.Message, tagName = request.TagName };
            }
        }

        /// <summary>
        /// 获取多个变量指定时间点上的历史值
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetMutiTagHisValueAtTimes")]
        public List<HisValue> GetMutiTagHisValueAtTimes([FromBody] MutiTagHisDataRequest request)
        {
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new List<HisValue>() { new HisValue() { Result = false, ErroMessage = "not login" } };
            }
            try
            {
                List<HisValue> re = new List<HisValue>();
                foreach (var vv in request.TagNames)
                {
                    var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(vv);
                    if (tag == null) return null;
                    object res;
                    HisValue revals = null;
                    switch (tag.Type)
                    {
                        case Cdy.Tag.TagType.Bool:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<bool>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<bool>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Byte:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<byte>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<byte>(vv, res);
                            break;
                        case Cdy.Tag.TagType.DateTime:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<DateTime>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<DateTime>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Double:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<double>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<double>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Float:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<float>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<float>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Int:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<int>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<int>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Long:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<long>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<long>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Short:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<short>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<short>(vv, res);
                            break;
                        case Cdy.Tag.TagType.String:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<string>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<string>(vv, res);
                            break;
                        case Cdy.Tag.TagType.UInt:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<uint>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<uint>(vv, res);
                            break;
                        case Cdy.Tag.TagType.ULong:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ulong>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<ulong>(vv, res);
                            break;
                        case Cdy.Tag.TagType.UShort:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ushort>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<ushort>(vv, res);
                            break;
                        case Cdy.Tag.TagType.IntPoint:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<IntPointData>(vv, res);
                            break;
                        case Cdy.Tag.TagType.UIntPoint:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<UIntPointData>(vv, res);
                            break;
                        case Cdy.Tag.TagType.IntPoint3:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<IntPoint3Data>(vv, res);
                            break;
                        case Cdy.Tag.TagType.UIntPoint3:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<UIntPoint3Data>(vv, res);
                            break;
                        case Cdy.Tag.TagType.LongPoint:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<LongPointData>(vv, res);
                            break;
                        case Cdy.Tag.TagType.ULongPoint:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPointTag>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<ULongPointData>(vv, res);
                            break;
                        case Cdy.Tag.TagType.LongPoint3:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<LongPoint3Data>(vv, res);
                            break;
                        case Cdy.Tag.TagType.ULongPoint3:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                            revals = ProcessResult<ULongPoint3Data>(vv, res);
                            break;
                    }

                    if (revals != null)
                    {
                        re.Add(revals);
                    }
                }
                return re;
            }
            catch (Exception ex)
            {
                return new List<HisValue>() { new HisValue() { Result = false, ErroMessage = ex.Message } };
            }
        }

        /// <summary>
        /// 获取一个时间段内,指定时间间隔上的变量的历史值
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetByTimeSpan")]
        public HisValue GetByTimeSpan([FromBody] HisDataRequest2 request)
        {
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new HisValue() { Result = false, ErroMessage = "not login" };
            }
            try
            {
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
                if (tag == null) return null;
                object res;
                HisValue revals = null;
                switch (tag.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<bool>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<bool>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Byte:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<byte>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<byte>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<DateTime>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<DateTime>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Double:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<double>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<double>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Float:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<float>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<float>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Int:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<int>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Long:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<long>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<long>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Short:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<short>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<short>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.String:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<string>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<string>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.UInt:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<uint>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.ULong:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ulong>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<ulong>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.UShort:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ushort>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<ushort>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<IntPointData>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<UIntPointData>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<IntPoint3Data>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<UIntPoint3Data>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<LongPointData>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPointTag>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<ULongPointData>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<LongPoint3Data>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                        revals = ProcessResult<ULongPoint3Data>(request.TagName, res);
                        break;
                }

                if (revals != null)
                    return revals;
                else
                {
                    return new HisValue() { Result = false, ErroMessage = "No result", tagName = request.TagName };
                }
            }
            catch(Exception ex)
            {
                return new HisValue() { Result = false, ErroMessage = ex.Message, tagName = request.TagName };
            }

        }



        /// <summary>
        /// 获取多个变量一个时间段内,指定时间间隔上的历史值
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetMutiTagHisDataByTimeSpan")]
        public List<HisValue> GetMutiTagHisDataByTimeSpan([FromBody] MutiTagHisDataRequest2 request)
        {
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new List< HisValue >{new HisValue() { Result = false, ErroMessage = "not login" }};
            }
            try
            {
                List<HisValue> re = new List<HisValue>();
                foreach (var vv in request.TagNames)
                {
                    var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(vv);
                    if (tag == null) return null;
                    object res;
                    HisValue revals = null;
                    switch (tag.Type)
                    {
                        case Cdy.Tag.TagType.Bool:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<bool>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<bool>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Byte:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<byte>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<byte>(vv, res);
                            break;
                        case Cdy.Tag.TagType.DateTime:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<DateTime>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<DateTime>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Double:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<double>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<double>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Float:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<float>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<float>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Int:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<int>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Long:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<long>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<long>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Short:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<short>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<short>(vv, res);
                            break;
                        case Cdy.Tag.TagType.String:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<string>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<string>(vv, res);
                            break;
                        case Cdy.Tag.TagType.UInt:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<uint>(vv, res);
                            break;
                        case Cdy.Tag.TagType.ULong:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ulong>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<ulong>(vv, res);
                            break;
                        case Cdy.Tag.TagType.UShort:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ushort>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<ushort>(vv, res);
                            break;
                        case Cdy.Tag.TagType.IntPoint:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<IntPointData>(vv, res);
                            break;
                        case Cdy.Tag.TagType.UIntPoint:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<UIntPointData>(vv, res);
                            break;
                        case Cdy.Tag.TagType.IntPoint3:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<IntPoint3Data>(vv, res);
                            break;
                        case Cdy.Tag.TagType.UIntPoint3:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<UIntPoint3Data>(vv, res);
                            break;
                        case Cdy.Tag.TagType.LongPoint:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<LongPointData>(vv, res);
                            break;
                        case Cdy.Tag.TagType.ULongPoint:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPointTag>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<ULongPointData>(vv, res);
                            break;
                        case Cdy.Tag.TagType.LongPoint3:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<LongPoint3Data>(vv, res);
                            break;
                        case Cdy.Tag.TagType.ULongPoint3:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                            revals = ProcessResult<ULongPoint3Data>(vv, res);
                            break;
                    }

                    if (revals != null)
                        re.Add(revals);
                    
                }

                return re;
            }
            catch (Exception ex)
            {
                return new List<HisValue> { new HisValue() { Result = false, ErroMessage = ex.Message } };
            }
        }

        private DateTime ConvertToDateTime(string time)
        {
            return DateTime.Parse(time);
        }

        private TimeSpan ConvertToTimeSpan(string time)
        {
            return TimeSpan.FromSeconds(int.Parse(time));
        }

        /// <summary>
        /// 获取一个时间段内,变量记录的所有历史值
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetAllValue")]
        public HisValue GetAllValue([FromBody] AllHisDataRequest request)
        {
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new HisValue() { Result = false, ErroMessage = "not login" };
            }
            try
            {
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
                if (tag == null) return new HisValue() { Result = false, ErroMessage = "tag not exist" } ;
                object res;
                HisValue revals = null;
                switch (tag.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<bool>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<bool>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Byte:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<byte>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<byte>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<DateTime>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<DateTime>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Double:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<double>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<double>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Float:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<float>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<float>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Int:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<int>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Long:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<long>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<long>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.Short:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<short>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<short>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.String:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<string>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<string>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.UInt:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<uint>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.ULong:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ulong>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<ulong>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.UShort:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ushort>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<ushort>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<IntPointData>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<UIntPointData>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<IntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<IntPoint3Data>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<UIntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<UIntPoint3Data>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<LongPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<LongPointData>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ULongPointTag>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<ULongPointData>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<LongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<LongPoint3Data>(request.TagName, res);
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ULongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        revals = ProcessResult<ULongPoint3Data>(request.TagName, res);
                        break;
                }

                if (revals != null)
                    return revals;
                else
                {
                    return new HisValue() { Result = false, ErroMessage = "No result", tagName = request.TagName };
                }
            }
            catch(Exception ex)
            {
                return new HisValue() { Result = false, ErroMessage = ex.Message, tagName = request.TagName };
            }
        }


        /// <summary>
        /// 获取多个变量时间段内,记录的所有历史值
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetMutiTagAllValue")]
        public List<HisValue> GetMutiTagAllValue([FromBody] AllMutiTagHisDataRequest request)
        {
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new List<HisValue>(){ new HisValue() { Result = false, ErroMessage = "not login" }};
            }

            List<HisValue> re = new List<HisValue>();

            foreach(var vv in request.TagNames)
            {
                try
                {
                    var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(vv);
                    if (tag == null) return null;
                    object res;
                    HisValue revals = null;
                    switch (tag.Type)
                    {
                        case Cdy.Tag.TagType.Bool:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<bool>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<bool>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Byte:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<byte>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<byte>(vv, res);
                            break;
                        case Cdy.Tag.TagType.DateTime:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<DateTime>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<DateTime>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Double:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<double>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<double>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Float:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<float>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<float>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Int:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<int>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Long:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<long>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<long>(vv, res);
                            break;
                        case Cdy.Tag.TagType.Short:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<short>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<short>(vv, res);
                            break;
                        case Cdy.Tag.TagType.String:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<string>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<string>(vv, res);
                            break;
                        case Cdy.Tag.TagType.UInt:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<uint>(vv, res);
                            break;
                        case Cdy.Tag.TagType.ULong:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ulong>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<ulong>(vv, res);
                            break;
                        case Cdy.Tag.TagType.UShort:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ushort>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<ushort>(vv, res);
                            break;
                        case Cdy.Tag.TagType.IntPoint:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<IntPointData>(vv, res);
                            break;
                        case Cdy.Tag.TagType.UIntPoint:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<UIntPointData>(vv, res);
                            break;
                        case Cdy.Tag.TagType.IntPoint3:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<IntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<IntPoint3Data>(vv, res);
                            break;
                        case Cdy.Tag.TagType.UIntPoint3:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<UIntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<UIntPoint3Data>(vv, res);
                            break;
                        case Cdy.Tag.TagType.LongPoint:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<LongPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<LongPointData>(vv, res);
                            break;
                        case Cdy.Tag.TagType.ULongPoint:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ULongPointTag>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<ULongPointData>(vv, res);
                            break;
                        case Cdy.Tag.TagType.LongPoint3:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<LongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<LongPoint3Data>(vv, res);
                            break;
                        case Cdy.Tag.TagType.ULongPoint3:
                            res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ULongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                            revals = ProcessResult<ULongPoint3Data>(vv, res);
                            break;
                    }

                    if (revals != null)
                        re.Add(revals);
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                
            }

            return re;

        }

        /// <summary>
        /// 获取统计信息
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetStatisticsValue")]
        public StatisticsValue GetStatisticsValue([FromBody] AllHisDataRequest request)
        {
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new StatisticsValue() { Result = false, ErroMessage = "not login" };
            }
            try
            {
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
                if (tag == null) return null;
                StatisticsValue revals = new StatisticsValue() { tagName = request.TagName };
                var res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryStatisticsHisData(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));

                if (res != null)
                {
                    double avgvalue, maxvalue, minvalue;
                    DateTime time, maxtime, mintime;
                    for (int i = 0; i < res.Count; i++)
                    {
                        res.ReadValue(i, out time, out avgvalue, out maxvalue, out maxtime, out minvalue, out mintime);
                        revals.Values.Add(new StatisticsValueItem() { Time = time, AvgValue = avgvalue, MaxValue = maxvalue, MinValue = minvalue, MaxValueTime = maxtime, MinValueTime = mintime });
                    }
                    res.Dispose();
                }
                if (revals != null)
                {
                    return revals;
                }
                else
                {
                    return new StatisticsValue() { Result = false, ErroMessage = "no value", tagName = request.TagName };
                }
            }
            catch(Exception ex)
            {
                return new StatisticsValue() { Result = false, ErroMessage = ex.Message, tagName = request.TagName };
            }
        }

        /// <summary>
        /// 获取指定时间点上的变量的统计信息
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetStatisticsValueByTimeSpan")]
        public StatisticsValue GetStatisticsValueByTimeSpan([FromBody] StatisticsDataRequest request)
        {
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new StatisticsValue() { Result = false, ErroMessage = "not login" };
            }

            try
            {
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
                if (tag == null) return null;
                StatisticsValue revals = new StatisticsValue() { tagName = request.TagName };

                List<DateTime> ltmp = new List<DateTime>();
                DateTime dtime = ConvertToDateTime(request.StartTime);
                DateTime etime = ConvertToDateTime(request.EndTime);
                while (dtime <= etime)
                {
                    ltmp.Add(dtime);
                    dtime = dtime.Add(ConvertToTimeSpan(request.Duration));
                }

                var res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryStatisticsHisData(tag.Id, ltmp);

                if (res != null)
                {
                    double avgvalue, maxvalue, minvalue;
                    DateTime time, maxtime, mintime;
                    for (int i = 0; i < res.Count; i++)
                    {
                        res.ReadValue(i, out time, out avgvalue, out maxvalue, out maxtime, out minvalue, out mintime);
                        revals.Values.Add(new StatisticsValueItem() { Time = time, AvgValue = avgvalue, MaxValue = maxvalue, MinValue = minvalue, MaxValueTime = maxtime, MinValueTime = mintime });
                    }
                    res.Dispose();
                }

                if (revals != null)
                {
                    return revals;
                }
                else
                {
                    return new StatisticsValue() { Result = false, ErroMessage = "no value", tagName = request.TagName };
                }
            }
            catch(Exception ex)
            {
                return new StatisticsValue() { Result = false, ErroMessage = ex.Message, tagName = request.TagName };
            }
        }

        /// <summary>
        /// 查找在某个时间段内是否包括指定的值
        /// </summary>
        /// <param name="request"></param>
        /// <returns>返回找到的第一个值的时间，未找到返回 空</returns>
        [HttpGet("FindTagValue")]
        public FindTagValueResult FindTagValue([FromBody] FindTagValueEqualRequest request)
        {
            FindTagValueResult re = new FindTagValueResult() { Result = true, TagName = request.TagName };
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new FindTagValueResult() { Result = false, ErroMessage = "not login",TagName=request.TagName };
            }
            try
            {
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
                if (tag == null) return new FindTagValueResult() { Result = false, ErroMessage = "tag not exist", TagName = request.TagName };
                DateTime? dres=null;
                Tuple<DateTime, object> res = null;
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
                        dres = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValue(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime),Convert.ToByte(request.Value));
                        re.Value = dres.HasValue ? dres.Value : "";
                        break;
                    case Cdy.Tag.TagType.Double:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValue(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToDouble(request.Value), request.Interval);
                        re.Value = new { time = res.Item1.ToString("yyyy-MM-dd HH:mm:ss.fff"), value = res.Item2 };
                        break;
                    case Cdy.Tag.TagType.Float:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValue(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToSingle(request.Value), request.Interval);
                        re.Value = new { time = res.Item1.ToString("yyyy-MM-dd HH:mm:ss.fff"), value = res.Item2 };
                        break;
                    case Cdy.Tag.TagType.Byte:
                    case Cdy.Tag.TagType.Int:
                    case Cdy.Tag.TagType.Long:
                    case Cdy.Tag.TagType.UInt:
                    case Cdy.Tag.TagType.Short:
                    case Cdy.Tag.TagType.ULong:
                    case Cdy.Tag.TagType.UShort:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValue(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        re.Value = new { time = res.Item1.ToString("yyyy-MM-dd HH:mm:ss.fff"), value = res.Item2 };
                        break;                   

                }
                
            }
            catch(Exception ex)
            {
                re.Result = false;
                re.Value = ex.Message;
            }
            return re;
        }

        /// <summary>
        /// 查找在某个时间段内是否包括指定的值
        /// </summary>
        /// <param name="request"></param>
        /// <returns>返回所有满足条件的时间的集合</returns>
        [HttpGet("FindTagValues")]
        public FindTagValueResult FindTagValues([FromBody] FindTagValueEqualRequest request)
        {
            FindTagValueResult re = new FindTagValueResult() { Result = true, TagName = request.TagName };
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new FindTagValueResult() { Result = false, ErroMessage = "not login", TagName = request.TagName };
            }
            try
            {
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
                if (tag == null) return new FindTagValueResult() { Result = false, ErroMessage = "tag not exist", TagName = request.TagName };
                Dictionary<DateTime,object> res = null;
                IEnumerable<DateTime> nres = null;
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
                        nres = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValues(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), Convert.ToByte(request.Value));
                        re.Value = nres;
                        break;
                    case Cdy.Tag.TagType.Double:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValues(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToDouble(request.Value), request.Interval);
                        re.Value = res.Select(e => new { time = e.Key.ToString("yyyy-MM-dd HH:mm:ss.fff"), value = e.Value });
                        break;
                    case Cdy.Tag.TagType.Float:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValues(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToSingle(request.Value), request.Interval);
                        re.Value =res.Select(e=> new { time = e.Key.ToString("yyyy-MM-dd HH:mm:ss.fff"), value = e.Value });
                        break;
                    case Cdy.Tag.TagType.Byte:
                    case Cdy.Tag.TagType.Int:
                    case Cdy.Tag.TagType.Long:
                    case Cdy.Tag.TagType.UInt:
                    case Cdy.Tag.TagType.Short:
                    case Cdy.Tag.TagType.ULong:
                    case Cdy.Tag.TagType.UShort:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValues(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        re.Value = res.Select(e => new { time = e.Key.ToString("yyyy-MM-dd HH:mm:ss.fff"), value = e.Value });
                        break;

                }
                
            }
            catch (Exception ex)
            {
                re.Result = false;
                re.Value = ex.Message;
            }
            return re;
        }


        /// <summary>
        /// 计算在某个时间段内满足指定的值条件的持续时间
        /// </summary>
        /// <param name="request"></param>
        /// <returns>累计时长</returns>
        [HttpGet("CalTagValueKeepTime")]
        public FindTagValueResult CalTagValueKeepTime([FromBody] FindTagValueEqualRequest request)
        {
            FindTagValueResult re = new FindTagValueResult() { Result = true,TagName=request.TagName };
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new FindTagValueResult() { Result = false, ErroMessage = "not login", TagName = request.TagName };
            }
            try
            {
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
                if (tag == null) return new FindTagValueResult() { Result = false, ErroMessage = "tag not exist", TagName = request.TagName };
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
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValueDuration(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), Convert.ToByte(request.Value));
                        break;
                    case Cdy.Tag.TagType.Double:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValueDuration(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToDouble(request.Value),request.Interval);
                        break;
                    case Cdy.Tag.TagType.Float:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValueDuration(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToSingle(request.Value), request.Interval);
                        break;
                    case Cdy.Tag.TagType.Byte:
                    case Cdy.Tag.TagType.Int:
                    case Cdy.Tag.TagType.Long:
                    case Cdy.Tag.TagType.UInt:
                    case Cdy.Tag.TagType.Short:
                    case Cdy.Tag.TagType.ULong:
                    case Cdy.Tag.TagType.UShort:
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValueDuration(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        break;

                }
                re.Value = res;
            }
            catch (Exception ex)
            {
                re.Result = false;
                re.Value = ex.Message;
            }
            return re;
        }


        /// <summary>
        /// 计算在某个时间段内数值型变量的平均值
        /// </summary>
        /// <param name="request"></param>
        /// <returns>平均值</returns>
        [HttpGet("CalNumberTagAvgValue")]
        public FindTagValueResult CalNumberTagAvgValue([FromBody] FindTagValueEqualRequest request)
        {
            FindTagValueResult re = new FindTagValueResult() { Result = true,TagName=request.TagName };
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new FindTagValueResult() { Result = false, ErroMessage = "not login", TagName = request.TagName };
            }
            try
            {
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
                if (tag == null) return new FindTagValueResult() { Result = false, ErroMessage = "tag not exist", TagName = request.TagName };
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
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.StatisticsTagAvgValue(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                        break;
                }
                re.Value = res;
            }
            catch (Exception ex)
            {
                re.Result = false;
                re.Value = ex.Message;
            }
            return re;
        }


        /// <summary>
        /// 查找在某个时间段内数值型变量的最大值
        /// </summary>
        /// <param name="request"></param>
        /// <returns>最大值和等于最大值的时间的集合</returns>
        [HttpGet("FindNumberTagMaxValue")]
        public FindTagValueResult FindNumberTagMaxValue([FromBody] FindTagValueEqualRequest request)
        {
            FindTagValueResult re = new FindTagValueResult() { Result = true,TagName=request.TagName };
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new FindTagValueResult() { Result = false, ErroMessage = "not login", TagName = request.TagName };
            }
            try
            {
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
                if (tag == null) return new FindTagValueResult() { Result = false, ErroMessage = "tag not exist", TagName = request.TagName };
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
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.StatisticsTagMaxMinValue(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime),NumberStatisticsType.Max);
                        break;
                }
                re.Value = new { Value = res.Item1, Times = res.Item2 };
            }
            catch (Exception ex)
            {
                re.Result = false;
                re.Value = ex.Message;
            }
            return re;
        }


        /// <summary>
        /// 查找在某个时间段内数值型变量的最小值
        /// </summary>
        /// <param name="request"></param>
        /// <returns>最小值和等于最小值的时间的集合</returns>
        [HttpGet("FindNumberTagMinValue")]
        public FindTagValueResult FindNumberTagMinValue([FromBody] FindTagValueEqualRequest request)
        {
            FindTagValueResult re = new FindTagValueResult() { Result = true, TagName = request.TagName };
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new FindTagValueResult() { Result = false, ErroMessage = "not login", TagName = request.TagName };
            }
            try
            {
                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
                if (tag == null) return new FindTagValueResult() { Result = false, ErroMessage = "tag not exist", TagName = request.TagName };
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
                        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.StatisticsTagMaxMinValue(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), NumberStatisticsType.Min);
                        break;
                }
                re.Value = new { Value = res.Item1, Times = res.Item2 };
            }
            catch (Exception ex)
            {
                re.Result = false;
                re.Value = ex.Message;
            }
            return re;
        }
    }
}
