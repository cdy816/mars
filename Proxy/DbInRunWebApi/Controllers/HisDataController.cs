using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Cdy.Tag;
using Cheetah;
using DbInRunWebApi.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NSwag.Annotations;
using YamlDotNet.Core.Tokens;

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

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tagname"></param>
        /// <param name="datas"></param>
        /// <param name="times"></param>
        /// <returns></returns>
        private HisValue ProcessResult<T>(string tagname, object datas, List<DateTime> times)
        {
            if (datas != null)
            {
                HisValue re = new HisValue() { tagName = tagname, Result = true };
                //List<ValueItem> values = new List<ValueItem>();
                var vdata = datas as HisQueryResult<T>;
                if (vdata != null)
                {
                    SortedDictionary<DateTime, ValueItem> rtmp = new SortedDictionary<DateTime, ValueItem>();
                    for (int i = 0; i < vdata.Count; i++)
                    {
                        byte qu;
                        DateTime time;
                        var val = vdata.GetValue(i, out time, out qu);
                        if(!rtmp.ContainsKey(time))
                        rtmp.Add(time,new ValueItem() { Time = time, Quality = qu, Value = val.ToString() });
                    }

                    foreach(var vv in times)
                    {
                        if(!rtmp.ContainsKey(vv))
                        {
                            rtmp.Add(vv, new ValueItem() { Time = vv, Quality = (byte)QualityConst.Null, Value = default(T).ToString() });
                        }
                    }

                    re.Values = rtmp.Values.ToList();

                    vdata.Dispose();
                }
                else
                {
                    if (datas != null && datas is IDisposable)
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


        private List<DateTime> ConvertToTimes(List<string> time)
        {
            return time.Select(e => DateTime.Parse(e)).ToList();
        }

        private HisValue GetHisValue(Tagbase tag, HisDataRequest request)
        {
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
            return revals;
        }

        private HisValue GetHisValueLocal(Tagbase tag, HisDataRequest request)
        {
            object res;
            HisValue revals = null;
            var hisserver = ServiceLocator.Locator.Resolve<IHisQuery>();
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res =  hisserver.ReadValue<bool>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<bool>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res =  hisserver.ReadValue<byte>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<byte>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res =  hisserver.ReadValue<DateTime>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<DateTime>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Double:
                    res =  hisserver.ReadValue<double>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<double>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Float:
                    res =  hisserver.ReadValue<float>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<float>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Int:
                    res =  hisserver.ReadValue<int>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<int>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Long:
                    res =  hisserver.ReadValue<long>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<long>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Short:
                    res =  hisserver.ReadValue<short>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<short>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.String:
                    res =  hisserver.ReadValue<string>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<string>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res =  hisserver.ReadValue<uint>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<uint>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res =  hisserver.ReadValue<ulong>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ulong>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res =  hisserver.ReadValue<ushort>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ushort>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res =  hisserver.ReadValue<IntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<IntPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res =  hisserver.ReadValue<UIntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<UIntPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res =  hisserver.ReadValue<IntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res =  hisserver.ReadValue<UIntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res =  hisserver.ReadValue<LongPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<LongPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res =  hisserver.ReadValue<ULongPointTag>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ULongPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res =  hisserver.ReadValue<LongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res =  hisserver.ReadValue<ULongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(request.TagName, res);
                    break;
            }
            return revals;
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
                if(WebAPIStartup.IsRunningEmbed)
                {
                    revals = GetHisValueLocal(tag,request);
                }
                else
                {
                    revals = GetHisValue(tag,request);
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

        private HisValue QueryHisDataByIgnorSystemExit(Tagbase tag,HisDataRequest request)
        {
            object res;
            HisValue revals = null;
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<bool>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<bool>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<byte>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<byte>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<DateTime>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<DateTime>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<double>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<double>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<float>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<float>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<int>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<int>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<long>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<long>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<short>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<short>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<string>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<string>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<uint>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<uint>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ulong>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ulong>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ushort>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ushort>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<IntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<IntPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<UIntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<UIntPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<IntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<UIntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<LongPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<LongPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ULongPointTag>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ULongPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<LongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ULongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(request.TagName, res);
                    break;
            }
            return revals;
        }

        private HisValue QueryHisDataByIgnorSystemExitLocal(Tagbase tag, HisDataRequest request)
        {
            object res;
            HisValue revals = null;
            var hisserver = ServiceLocator.Locator.Resolve<IHisQuery>();
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = hisserver.ReadValueIgnorClosedQuality<bool>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<bool>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = hisserver.ReadValueIgnorClosedQuality<byte>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<byte>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = hisserver.ReadValueIgnorClosedQuality<DateTime>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<DateTime>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = hisserver.ReadValueIgnorClosedQuality<double>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<double>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = hisserver.ReadValueIgnorClosedQuality<float>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<float>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = hisserver.ReadValueIgnorClosedQuality<int>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<int>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = hisserver.ReadValueIgnorClosedQuality<long>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<long>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = hisserver.ReadValueIgnorClosedQuality<short>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<short>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.String:
                    res = hisserver.ReadValueIgnorClosedQuality<string>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<string>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = hisserver.ReadValueIgnorClosedQuality<uint>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<uint>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = hisserver.ReadValueIgnorClosedQuality<ulong>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ulong>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = hisserver.ReadValueIgnorClosedQuality<ushort>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ushort>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<IntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<IntPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<UIntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<UIntPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<IntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<UIntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<LongPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<LongPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<ULongPointTag>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ULongPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<LongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<ULongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(request.TagName, res);
                    break;
            }
            return revals;
        }


        /// <summary>
        /// 获取变量指定时间点上的历史值,在数据拟合时忽略系统退出的影响
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetAtTimesByIgnorSystemExit")]
        public HisValue GetAtTimesByIgnorSystemExit([FromBody] HisDataRequest request)
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
                if (WebAPIStartup.IsRunningEmbed)
                {
                    revals = QueryHisDataByIgnorSystemExitLocal(tag, request);
                }
                else
                {
                    revals = QueryHisDataByIgnorSystemExit(tag, request);
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
            catch (Exception ex)
            {
                return new HisValue() { Result = false, ErroMessage = ex.Message, tagName = request.TagName };
            }
        }

        private HisValue GetMultiTagHisValue(Tagbase tag, MutiTagHisDataRequest request)
        {
            object res;
            HisValue revals = null;
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<bool>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<bool>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<byte>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<byte>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<DateTime>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<DateTime>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<double>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<double>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<float>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<float>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<int>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<int>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<long>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<long>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<short>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<short>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<string>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<string>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<uint>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<uint>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ulong>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ulong>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ushort>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ushort>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<IntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<IntPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<UIntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<UIntPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<IntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<UIntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<LongPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<LongPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ULongPointTag>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ULongPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<LongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ULongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(tag.Name, res);
                    break;
            }
            return revals;
        }

        private HisValue GetMultiTagHisValueLocal(Tagbase tag, MutiTagHisDataRequest request)
        {
            object res;
            HisValue revals = null;
            var hisserver = ServiceLocator.Locator.Resolve<IHisQuery>();
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = hisserver.ReadValueIgnorClosedQuality<bool>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<bool>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = hisserver.ReadValueIgnorClosedQuality<byte>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<byte>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = hisserver.ReadValueIgnorClosedQuality<DateTime>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<DateTime>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = hisserver.ReadValueIgnorClosedQuality<double>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<double>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = hisserver.ReadValueIgnorClosedQuality<float>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<float>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = hisserver.ReadValueIgnorClosedQuality<int>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<int>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = hisserver.ReadValueIgnorClosedQuality<long>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<long>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = hisserver.ReadValueIgnorClosedQuality<short>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<short>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.String:
                    res = hisserver.ReadValueIgnorClosedQuality<string>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<string>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = hisserver.ReadValueIgnorClosedQuality<uint>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<uint>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = hisserver.ReadValueIgnorClosedQuality<ulong>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ulong>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = hisserver.ReadValueIgnorClosedQuality<ushort>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ushort>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<IntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<IntPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<UIntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<UIntPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<IntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<UIntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<LongPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<LongPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<ULongPointTag>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ULongPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<LongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<ULongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(tag.Name, res);
                    break;
            }
            return revals;
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
                    //object res;
                    HisValue revals = null;
                    if (WebAPIStartup.IsRunningEmbed)
                    {
                        revals = GetMultiTagHisValueLocal(tag, request);
                    }
                    else
                    {
                        revals = GetMultiTagHisValue(tag, request);
                    }
                    //var times = ConvertToTimes(request.Times);
                    //switch (tag.Type)
                    //{
                    //    case Cdy.Tag.TagType.Bool:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<bool>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<bool>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.Byte:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<byte>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<byte>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.DateTime:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<DateTime>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<DateTime>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.Double:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<double>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<double>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.Float:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<float>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<float>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.Int:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<int>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<int>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.Long:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<long>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<long>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.Short:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<short>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<short>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.String:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<string>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<string>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.UInt:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<uint>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<uint>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.ULong:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ulong>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<ulong>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.UShort:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ushort>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<ushort>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.IntPoint:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<IntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<IntPointData>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.UIntPoint:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<UIntPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<UIntPointData>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.IntPoint3:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<IntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<IntPoint3Data>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.UIntPoint3:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<UIntPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<UIntPoint3Data>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.LongPoint:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<LongPointData>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<LongPointData>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.ULongPoint:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ULongPointTag>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<ULongPointData>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.LongPoint3:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<LongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<LongPoint3Data>(vv, res, times);
                    //        break;
                    //    case Cdy.Tag.TagType.ULongPoint3:
                    //        res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ULongPoint3Data>(tag.Id, ConvertToTimes(request.Times), request.MatchType);
                    //        revals = ProcessResult<ULongPoint3Data>(vv, res, times);
                    //        break;
                    //}

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

        private List<DateTime> GetTimes(DateTime starttime,DateTime endtime,TimeSpan timespan)
        {
            List<DateTime> re = new List<DateTime>();
            DateTime dt = starttime;
            while(dt<endtime)
            {
                re.Add(dt);
                dt = dt.Add(timespan);
            }
            return re;
        }

        private HisValue GetValueByTimeSpan(Tagbase tag,HisDataRequest2 request)
        {
            object res;
            HisValue revals = null;
            var times = GetTimes(ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration));
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<bool>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<bool>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<byte>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<byte>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<DateTime>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<DateTime>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<double>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<double>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<float>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<float>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<int>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<long>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<long>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<short>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<short>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<string>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<string>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<uint>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ulong>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ulong>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ushort>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ushort>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<IntPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<UIntPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<LongPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPointTag>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ULongPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(request.TagName, res, times);
                    break;
            }
            return revals;
        }

        private HisValue GetValueByTimeSpanLocal(Tagbase tag,HisDataRequest2 request)
        {
            object res;
            HisValue revals = null;
            var hisserver = ServiceLocator.Locator.Resolve<IHisQuery>();
            var times = GetTimes(ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration));
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = hisserver.ReadValue<bool>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<bool>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = hisserver.ReadValue<byte>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<byte>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = hisserver.ReadValue<DateTime>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<DateTime>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = hisserver.ReadValue<double>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<double>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = hisserver.ReadValue<float>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<float>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = hisserver.ReadValue<int>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<int>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = hisserver.ReadValue<long>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<long>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = hisserver.ReadValue<short>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<short>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res = hisserver.ReadValue<string>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<string>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = hisserver.ReadValue<uint>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<uint>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = hisserver.ReadValue<ulong>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<ulong>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = hisserver.ReadValue<ushort>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<ushort>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = hisserver.ReadValue<IntPointData>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<IntPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = hisserver.ReadValue<UIntPointData>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<UIntPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = hisserver.ReadValue<IntPoint3Data>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = hisserver.ReadValue<UIntPoint3Data>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = hisserver.ReadValue<LongPointData>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<LongPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = hisserver.ReadValue<ULongPointTag>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<ULongPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = hisserver.ReadValue<LongPoint3Data>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = hisserver.ReadValue<ULongPoint3Data>(tag.Id,times, request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(request.TagName, res, times);
                    break;
            }
            return revals;
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
                if (WebAPIStartup.IsRunningEmbed)
                {
                    revals = GetValueByTimeSpanLocal(tag, request);
                }
                else
                {
                    revals = GetValueByTimeSpan(tag, request);
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

        private HisValue GetValueByTimeSpanIgnorSystemExit(Tagbase tag, HisDataRequest2 request)
        {
            object res;
            HisValue revals = null;
            var times = GetTimes(ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration));
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<bool>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<bool>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<byte>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<byte>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<DateTime>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<DateTime>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<double>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<double>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<float>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<float>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<int>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<long>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<long>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<short>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<short>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<string>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<string>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<uint>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ulong>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ulong>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ushort>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ushort>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<IntPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<IntPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<UIntPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<UIntPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<IntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<UIntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<LongPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<LongPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ULongPointTag>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ULongPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<LongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ULongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(request.TagName, res, times);
                    break;
            }
            return revals;
        }

        private HisValue GetValueByTimeSpanIgnorSystemExitLocal(Tagbase tag, HisDataRequest2 request)
        {
            object res;
            HisValue revals = null;
            var hisserver = ServiceLocator.Locator.Resolve<IHisQuery>();
            var times = GetTimes(ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration));
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = hisserver.ReadValueIgnorClosedQuality<bool>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<bool>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = hisserver.ReadValueIgnorClosedQuality<byte>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<byte>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = hisserver.ReadValueIgnorClosedQuality<DateTime>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<DateTime>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = hisserver.ReadValueIgnorClosedQuality<double>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<double>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = hisserver.ReadValueIgnorClosedQuality<float>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<float>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = hisserver.ReadValueIgnorClosedQuality<int>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<int>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = hisserver.ReadValueIgnorClosedQuality<long>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<long>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = hisserver.ReadValueIgnorClosedQuality<short>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<short>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res = hisserver.ReadValueIgnorClosedQuality<string>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<string>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = hisserver.ReadValueIgnorClosedQuality<uint>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<uint>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = hisserver.ReadValueIgnorClosedQuality<ulong>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<ulong>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = hisserver.ReadValueIgnorClosedQuality<ushort>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<ushort>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<IntPointData>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<IntPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<UIntPointData>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<UIntPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<IntPoint3Data>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<UIntPoint3Data>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<LongPointData>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<LongPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<ULongPointTag>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<ULongPointData>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<LongPoint3Data>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(request.TagName, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<ULongPoint3Data>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(request.TagName, res, times);
                    break;
            }
            return revals;
        }

        /// <summary>
        /// 获取一个时间段内,指定时间间隔上的变量的历史值,数据拟合过程中忽略系统退出的影响
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetByTimeSpanIgnorSystemExit")]
        public HisValue GetByTimeSpanIgnorSystemExit([FromBody] HisDataRequest2 request)
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
                if (WebAPIStartup.IsRunningEmbed)
                {
                    revals = GetValueByTimeSpanIgnorSystemExitLocal(tag, request);
                }
                else
                {
                    revals = GetValueByTimeSpanIgnorSystemExit(tag, request);
                }

                if (revals != null)
                    return revals;
                else
                {
                    return new HisValue() { Result = false, ErroMessage = "No result", tagName = request.TagName };
                }
            }
            catch (Exception ex)
            {
                return new HisValue() { Result = false, ErroMessage = ex.Message, tagName = request.TagName };
            }

        }

        private HisValue GetMultiValueByTimeSpan(Tagbase tag, MutiTagHisDataRequest2 request)
        {
            object res;
            HisValue revals = null;
            var times = GetTimes(ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration));
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<bool>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<bool>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<byte>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<byte>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<DateTime>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<DateTime>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<double>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<double>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<float>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<float>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<int>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<long>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<long>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<short>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<short>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<string>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<string>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<uint>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ulong>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ulong>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ushort>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ushort>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<IntPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<UIntPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<LongPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPointTag>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ULongPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(tag.Name, res, times);
                    break;
            }
            return revals;
        }

        private HisValue GetMultiValueByTimeSpanLocal(Tagbase tag, MutiTagHisDataRequest2 request)
        {
            object res;
            HisValue revals = null;
            var hisserver = ServiceLocator.Locator.Resolve<IHisQuery>();
            var times = GetTimes(ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration));
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = hisserver.ReadValue<bool>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<bool>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = hisserver.ReadValue<byte>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<byte>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = hisserver.ReadValue<DateTime>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<DateTime>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = hisserver.ReadValue<double>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<double>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = hisserver.ReadValue<float>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<float>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = hisserver.ReadValue<int>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<int>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = hisserver.ReadValue<long>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<long>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = hisserver.ReadValue<short>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<short>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res = hisserver.ReadValue<string>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<string>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = hisserver.ReadValue<uint>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<uint>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = hisserver.ReadValue<ulong>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<ulong>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = hisserver.ReadValue<ushort>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<ushort>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = hisserver.ReadValue<IntPointData>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<IntPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = hisserver.ReadValue<UIntPointData>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<UIntPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = hisserver.ReadValue<IntPoint3Data>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = hisserver.ReadValue<UIntPoint3Data>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = hisserver.ReadValue<LongPointData>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<LongPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = hisserver.ReadValue<ULongPointTag>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<ULongPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = hisserver.ReadValue<LongPoint3Data>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = hisserver.ReadValue<ULongPoint3Data>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(tag.Name, res, times);
                    break;
            }
            return revals;
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
                    if (WebAPIStartup.IsRunningEmbed)
                    {
                        revals = GetMultiValueByTimeSpanLocal(tag, request);
                    }
                    else
                    {
                        revals = GetMultiValueByTimeSpan(tag, request);
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


        private HisValue GetMultiValueByTimeSpanIgnorSystemExit(Tagbase tag, MutiTagHisDataRequest2 request)
        {
            object res;
            HisValue revals = null;
            var times = GetTimes(ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration));
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<bool>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<bool>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<byte>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<byte>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<DateTime>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<DateTime>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<double>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<double>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<float>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<float>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<int>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<long>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<long>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<short>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<short>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<string>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<string>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<uint>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ulong>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ulong>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ushort>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ushort>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<IntPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<IntPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<UIntPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<UIntPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<IntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<UIntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<LongPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<LongPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ULongPointTag>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ULongPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<LongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisDataByIgnorSystemExit<ULongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(tag.Name, res, times);
                    break;
            }
            return revals;
        }

        private HisValue GetMultiValueByTimeSpanIgnorSystemExitLocal(Tagbase tag, MutiTagHisDataRequest2 request)
        {
            object res;
            HisValue revals = null;
            var hisserver = ServiceLocator.Locator.Resolve<IHisQuery>();
            var times = GetTimes(ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime), ConvertToTimeSpan(request.Duration));
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = hisserver.ReadValueIgnorClosedQuality<bool>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<bool>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = hisserver.ReadValueIgnorClosedQuality<byte>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<byte>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = hisserver.ReadValueIgnorClosedQuality<DateTime>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<DateTime>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = hisserver.ReadValueIgnorClosedQuality<double>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<double>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = hisserver.ReadValueIgnorClosedQuality<float>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<float>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = hisserver.ReadValueIgnorClosedQuality<int>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<int>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = hisserver.ReadValueIgnorClosedQuality<long>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<long>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = hisserver.ReadValueIgnorClosedQuality<short>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<short>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res = hisserver.ReadValueIgnorClosedQuality<string>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<string>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = hisserver.ReadValueIgnorClosedQuality<uint>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<uint>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = hisserver.ReadValueIgnorClosedQuality<ulong>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<ulong>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = hisserver.ReadValueIgnorClosedQuality<ushort>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<ushort>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<IntPointData>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<IntPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<UIntPointData>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<UIntPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<IntPoint3Data>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<UIntPoint3Data>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<LongPointData>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<LongPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = hisserver.ReadValueIgnorClosedQuality<ULongPointTag>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<ULongPointData>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<LongPoint3Data>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(tag.Name, res, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = hisserver.ReadValueIgnorClosedQuality<ULongPoint3Data>(tag.Id, times, request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(tag.Name, res, times);
                    break;
            }
            return revals;
        }

        /// <summary>
        /// 获取多个变量一个时间段内,指定时间间隔上的历史值
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetMutiTagHisDataByTimeSpanIgnorSystemExit")]
        public List<HisValue> GetMutiTagHisDataByTimeSpanIgnorSystemExit([FromBody] MutiTagHisDataRequest2 request)
        {
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new List<HisValue> { new HisValue() { Result = false, ErroMessage = "not login" } };
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
                    if (WebAPIStartup.IsRunningEmbed)
                    {
                        revals = GetMultiValueByTimeSpanIgnorSystemExitLocal(tag, request);
                    }
                    else
                    {
                        revals = GetMultiValueByTimeSpanIgnorSystemExit(tag, request);
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


        private HisValue ReadAllValue(Tagbase tag, AllHisDataRequest request)
        {
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
            return revals;
        }

        private HisValue ReadAllValueLocal(Tagbase tag, AllHisDataRequest request)
        {
            object res;
            HisValue revals = null;
            var hisserver = ServiceLocator.Locator.Resolve<IHisQuery>();
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = hisserver.ReadAllValue<bool>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<bool>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = hisserver.ReadAllValue<byte>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<byte>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = hisserver.ReadAllValue<DateTime>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<DateTime>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = hisserver.ReadAllValue<double>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<double>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = hisserver.ReadAllValue<float>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<float>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = hisserver.ReadAllValue<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<int>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = hisserver.ReadAllValue<long>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<long>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = hisserver.ReadAllValue<short>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<short>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.String:
                    res = hisserver.ReadAllValue<string>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<string>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = hisserver.ReadAllValue<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<uint>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = hisserver.ReadAllValue<ulong>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ulong>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = hisserver.ReadAllValue<ushort>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ushort>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = hisserver.ReadAllValue<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<IntPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = hisserver.ReadAllValue<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<UIntPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = hisserver.ReadAllValue<IntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<IntPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = hisserver.ReadAllValue<UIntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<UIntPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = hisserver.ReadAllValue<LongPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<LongPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = hisserver.ReadAllValue<ULongPointTag>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ULongPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = hisserver.ReadAllValue<LongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<LongPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = hisserver.ReadAllValue<ULongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ULongPoint3Data>(request.TagName, res);
                    break;
            }
            return revals;
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
                HisValue revals = null;
                if(WebAPIStartup.IsRunningEmbed)
                {
                    revals = ReadAllValue(tag, request);
                }
                else
                {
                    revals = ReadAllValueLocal(tag, request);
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

        [HttpGet("GetValueBySql")]
        public object GetValueBySql([FromBody] SqlDataRequest request)
        {
            if (!DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                return new ResponseBase() { Result = false, ErroMessage = "not login" };
            }
            try
            {
                object res=null;
                if (WebAPIStartup.IsRunningEmbed)
                {
                    res = ProcessQueryBySQLLocal(request.Sql);
                }
                else
                {
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisValueBySql(request.Sql);
                    if(res is ByteBuffer)
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
                                    ComplexRealValue cv = new ComplexRealValue();
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
                                        cv.Add(new ComplexRealValueItem() { Id = cvid, ValueType = vtp, Value = cval, Quality = cqua, Time = ctime });
                                    }
                                    val = cv;
                                    break;
                            }
                            var vtime = DateTime.FromBinary(re.ReadLong()).ToLocalTime();
                            var qua = re.ReadByte();

                            rre.Add(new RealTagValueWithTimer() { ValueType = type, Id = vvid, Value = val, Quality = qua, Time = vtime });
                        }
                        return new SqlQueryReponse() { Result = true, Value = rre,ValueType=2 };


                    }
                }
                if(res is HisQueryTableResult)
                {
                    TableValue tval = new TableValue();
                    var htr = (res as HisQueryTableResult);
                    if (htr != null)
                    {
                        tval.Columns = htr.Columns.Keys.ToArray();
                        tval.Datas = new List<IEnumerable<object>>();
                        foreach (var vv in htr.ReadRows())
                        {
                            List<object> ltmp = new List<object>();
                            ltmp.Add(vv.Item1);
                            ltmp.AddRange(vv.Item2);
                            tval.Datas.Add(ltmp.ToArray());
                        }
                    }
                    return new SqlQueryReponse() { Result = true, Value = tval, ValueType = 0 };
                }
                else if(res is List<double>)
                {
                    CollectionValue cv = new CollectionValue();
                    cv.Datas = (res as List<double>).Select(e=>(object)e).ToList();
                    return new SqlQueryReponse() { Result = true, Value = cv, ValueType = 1 };
                }
                else if(res is List<RealTagValueWithTimer>)
                {
                    return new SqlQueryReponse() { Result = true, Value = res, ValueType = 2 };
                }
                else
                {
                    return new ResponseBase() { Result = false, ErroMessage = "查到无法识别数据！" };
                }
            }
            catch (Exception ex)
            {
                return new ResponseBase() { Result = false, ErroMessage = ex.Message };
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
                    revals.Add(new RealTagValueWithTimer() { Id=vv,Value = value,Quality=qu,Time=time,ValueType = type});
                }
            }
            return revals;
        }

        private object ProcessQueryBySQLLocal(string sql)
        {
            try
            {
                if (!string.IsNullOrEmpty(sql))
                {
                    var sqlexp = ParseSql(sql, out List<int> selids, out Dictionary<int, byte> tps);
                    if (sqlexp.Where == null || (sqlexp.Where.LowerTime == null && sqlexp.Where.UpperTime == null))
                    {
                        return ProcessRealData(selids);
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
                            return qq;
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
                            return vals;
                            
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("HisDataController", ex.Message);
            }
            return null;
        }

        private HisValue ReadMultiAllValue(Tagbase tag, AllMutiTagHisDataRequest request)
        {
            object res;
            HisValue revals = null;
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<bool>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<bool>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<byte>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<byte>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<DateTime>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<DateTime>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<double>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<double>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<float>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<float>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<int>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<long>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<long>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<short>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<short>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<string>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<string>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<uint>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ulong>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ulong>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ushort>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ushort>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<IntPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<UIntPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<IntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<IntPoint3Data>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<UIntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<UIntPoint3Data>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<LongPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<LongPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ULongPointTag>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ULongPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<LongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<LongPoint3Data>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ULongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ULongPoint3Data>(tag.Name, res);
                    break;
            }
            return revals;
        }

        private HisValue ReadMultiAllValueLocal(Tagbase tag, AllMutiTagHisDataRequest request)
        {
            object res;
            HisValue revals = null;
            var hisserver = ServiceLocator.Locator.Resolve<IHisQuery>();
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = hisserver.ReadAllValue<bool>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<bool>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = hisserver.ReadAllValue<byte>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<byte>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = hisserver.ReadAllValue<DateTime>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<DateTime>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = hisserver.ReadAllValue<double>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<double>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = hisserver.ReadAllValue<float>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<float>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = hisserver.ReadAllValue<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<int>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = hisserver.ReadAllValue<long>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<long>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = hisserver.ReadAllValue<short>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<short>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.String:
                    res = hisserver.ReadAllValue<string>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<string>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = hisserver.ReadAllValue<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<uint>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = hisserver.ReadAllValue<ulong>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ulong>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = hisserver.ReadAllValue<ushort>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ushort>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = hisserver.ReadAllValue<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<IntPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = hisserver.ReadAllValue<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<UIntPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = hisserver.ReadAllValue<IntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<IntPoint3Data>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = hisserver.ReadAllValue<UIntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<UIntPoint3Data>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = hisserver.ReadAllValue<LongPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<LongPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = hisserver.ReadAllValue<ULongPointTag>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ULongPointData>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = hisserver.ReadAllValue<LongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<LongPoint3Data>(tag.Name, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = hisserver.ReadAllValue<ULongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ULongPoint3Data>(tag.Name, res);
                    break;
            }
            return revals;
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
                    if (WebAPIStartup.IsRunningEmbed)
                    {
                        revals = ReadMultiAllValue(tag, request);
                    }
                    else
                    {
                        revals = ReadMultiAllValueLocal(tag, request);
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
                var hisserver = ServiceLocator.Locator.Resolve<IHisQuery>();

                var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
                if (tag == null) return null;
                StatisticsValue revals = new StatisticsValue() { tagName = request.TagName };
                NumberStatisticsQueryResult res;
                if (WebAPIStartup.IsRunningEmbed)
                {
                    res = hisserver.ReadNumberStatistics(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                }
                else
                {
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryStatisticsHisData(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                }

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

                var hisserver = ServiceLocator.Locator.Resolve<IHisQuery>();
                NumberStatisticsQueryResult res;
                if (WebAPIStartup.IsRunningEmbed)
                {
                    res = hisserver.ReadNumberStatistics(tag.Id, ltmp);
                }
                else
                {
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryStatisticsHisData(tag.Id, ltmp);
                }

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
                DateTime? dres = null;
                Tuple<DateTime, object> res = null;
                switch (tag.Type)
                {
                    case Cdy.Tag.TagType.DateTime:
                        dres = FindNoNumberTagValue<DateTime>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToDateTime(request.Value));
                        if (dres != null)
                        {
                            re.Value = dres.Value;
                            // re.Value = dres.Value;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.Bool:
                        dres = FindNoNumberTagValue<bool>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToBoolean(request.Value));
                        if (dres != null)
                        {
                             re.Value = dres.Value;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.String:
                        dres = FindNoNumberTagValue<string>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (dres != null)
                        {
                             re.Value = dres.Value;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        dres = FindNoNumberTagValue<IntPointData>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (dres != null)
                        {
                             re.Value = dres.Value;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        dres = FindNoNumberTagValue<UIntPointData>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (dres != null)
                        {
                             re.Value = dres.Value;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        dres = FindNoNumberTagValue<IntPoint3Data>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (dres != null)
                        {
                             re.Value = dres.Value;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        dres = FindNoNumberTagValue<UIntPoint3Data>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (dres != null)
                        {
                             re.Value = dres.Value;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        dres = FindNoNumberTagValue<LongPointData>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (dres != null)
                        {
                             re.Value = dres.Value;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        dres = FindNoNumberTagValue<ULongPointData>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (dres != null)
                        {
                             re.Value = dres.Value;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        dres = FindNoNumberTagValue<LongPoint3Data>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (dres != null)
                        {
                             re.Value = dres.Value;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        dres = FindNoNumberTagValue<ULongPoint3Data>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (dres != null)
                        {
                             re.Value = dres.Value;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.Double:
                        res = FindNumberTagValue<double>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToDouble(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res.Item1;
                            //re.Time.Add(res.Item1.Ticks);
                            //re.Value.Add(Convert.ToDouble(res.Item2));
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.Float:
                        res = FindNumberTagValue<float>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToSingle(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res.Item1;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.Byte:
                        res = FindNumberTagValue<byte>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res.Item1;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.Int:
                        res = FindNumberTagValue<int>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res.Item1;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.Long:
                        res = FindNumberTagValue<long>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res.Item1;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.UInt:
                        res = FindNumberTagValue<uint>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res.Item1;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.Short:
                        res = FindNumberTagValue<short>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res.Item1;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.ULong:
                        res = FindNumberTagValue<ulong>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res.Item1;
                        }
                        else
                        {
                            re.Result = false;
                        }
                        break;
                    case Cdy.Tag.TagType.UShort:
                        res = FindNumberTagValue<ushort>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res.Item1;
                        }
                        else
                        {
                            re.Result = false;
                        }
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

        private DateTime? FindNoNumberTagValue<T>(int id, DateTime startTime, DateTime endTime, object value)
        {
            if (WebAPIStartup.IsRunningEmbed)
            {
                return ServiceLocator.Locator.Resolve<IHisQuery>().FindNoNumberTagValue<T>(id, startTime, endTime, value);
            }
            else
            {
                return DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNoNumberTagValue(id, startTime, endTime, value);
            }
        }

        private Tuple<DateTime, object> FindNumberTagValue<T>(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value, object interval)
        {
            if (WebAPIStartup.IsRunningEmbed)
            {
                return ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValue<T>(id, startTime, endTime, Convert.ToDouble(value), Convert.ToDouble(interval), type);
            }
            else
            {
                return DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValue(id, startTime, endTime, type, value, interval);
            }
        }

        private IEnumerable<DateTime> FindNoNumberTagValues<T>(int id, DateTime startTime, DateTime endTime, object value)
        {
            if (WebAPIStartup.IsRunningEmbed)
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
            if (WebAPIStartup.IsRunningEmbed)
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
        public double? FindNoNumberTagValueDuration<T>(int id, DateTime startTime, DateTime endTime, object value)
        {
            if (WebAPIStartup.IsRunningEmbed)
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
            if (WebAPIStartup.IsRunningEmbed)
            {
                return ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagValueDuration<T>(id, startTime, endTime, Convert.ToDouble(value), Convert.ToDouble(interval), type);
            }
            else
            {
                return DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.FindNumberTagValueDuration(id, startTime, endTime, type, value, interval);
            }
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
                Dictionary<DateTime, object> res = null;
                IEnumerable<DateTime> dres;
                switch (tag.Type)
                {
                    case Cdy.Tag.TagType.DateTime:
                        dres = FindNoNumberTagValues<DateTime>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToDateTime(request.Value));
                        if (res != null)
                        {
                            re.Value = dres;
                            //foreach (var vv in dres)
                            //{
                            //    re.Value.Add(vv.Ticks);
                            //}
                        }
                        break;
                    case Cdy.Tag.TagType.Bool:
                        dres = FindNoNumberTagValues<bool>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToByte(request.Value));
                        if (res != null)
                        {
                            re.Value = dres;
                        }
                        break;
                    case Cdy.Tag.TagType.String:
                        dres = FindNoNumberTagValues<string>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (res != null)
                        {
                            re.Value = dres;
                        }
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        dres = FindNoNumberTagValues<IntPointData>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (res != null)
                        {
                            re.Value = dres;
                        }
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        dres = FindNoNumberTagValues<UIntPointData>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (res != null)
                        {
                            re.Value = dres;
                        }
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        dres = FindNoNumberTagValues<IntPoint3Data>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (res != null)
                        {
                            re.Value = dres;
                        }
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        dres = FindNoNumberTagValues<UIntPoint3Data>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (res != null)
                        {
                            re.Value = dres;
                        }
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        dres = FindNoNumberTagValues<LongPointData>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (res != null)
                        {
                            re.Value = dres;
                        }
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        dres = FindNoNumberTagValues<ULongPointData>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (res != null)
                        {
                            re.Value = dres;
                        }
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        dres = FindNoNumberTagValues<LongPoint3Data>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (res != null)
                        {
                            re.Value = dres;
                        }
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        dres = FindNoNumberTagValues<ULongPoint3Data>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), request.Value);
                        if (res != null)
                        {
                            re.Value = dres;
                        }
                        break;
                    case Cdy.Tag.TagType.Double:
                        res = FindNumberTagValues<double>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToDouble(request.Value), request.Interval);
                        if (res != null)
                        {
                            //foreach (var vv in res)
                            //{
                            //    re.Time.Add(vv.Key.Ticks);
                            //    re.Value.Add(Convert.ToDouble(vv.Value));
                            //}
                            re.Value = res;
                        }
                        break;
                    case Cdy.Tag.TagType.Float:
                        res = FindNumberTagValues<float>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToSingle(request.Value), request.Interval);
                        if (res != null)
                        {
                            //foreach (var vv in res)
                            //{
                            //    re.Time.Add(vv.Key.Ticks);
                            //    re.Value.Add(Convert.ToDouble(vv.Value));
                            //}
                            re.Value = res;
                        }
                        break;
                    case Cdy.Tag.TagType.Byte:
                        res = FindNumberTagValues<byte>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        if (res != null)
                        {
                            //foreach (var vv in res)
                            //{
                            //    re.Time.Add(vv.Key.Ticks);
                            //    re.Value.Add(Convert.ToDouble(vv.Value));
                            //}
                            re.Value = res;
                        }
                        break;
                    case Cdy.Tag.TagType.Int:
                        res = FindNumberTagValues<int>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res;
                        }
                        break;
                    case Cdy.Tag.TagType.Long:
                        res = FindNumberTagValues<long>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res;
                        }
                        break;
                    case Cdy.Tag.TagType.UInt:
                        res = FindNumberTagValues<uint>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res;
                        }
                        break;
                    case Cdy.Tag.TagType.Short:
                        res = FindNumberTagValues<short>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res;
                        }
                        break;
                    case Cdy.Tag.TagType.ULong:
                        res = FindNumberTagValues<ulong>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res;
                        }
                        break;
                    case Cdy.Tag.TagType.UShort:
                        res = FindNumberTagValues<ushort>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        if (res != null)
                        {
                            re.Value = res;
                        }
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
                        res = FindNoNumberTagValueDuration<DateTime>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToByte(request.Value));
                        break;
                    case Cdy.Tag.TagType.Bool:
                        res = FindNoNumberTagValueDuration<bool>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToByte(request.Value));
                        break;
                    case Cdy.Tag.TagType.String:
                        res = FindNoNumberTagValueDuration<string>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToByte(request.Value));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        res = FindNoNumberTagValueDuration<IntPointData>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToByte(request.Value));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        res = FindNoNumberTagValueDuration<UIntPointData>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToByte(request.Value));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        res = FindNoNumberTagValueDuration<IntPoint3Data>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToByte(request.Value));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        res = FindNoNumberTagValueDuration<UIntPoint3Data>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToByte(request.Value));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        res = FindNoNumberTagValueDuration<LongPointData>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToByte(request.Value));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        res = FindNoNumberTagValueDuration<ULongPointData>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToByte(request.Value));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        res = FindNoNumberTagValueDuration<LongPoint3Data>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToByte(request.Value));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        res = FindNoNumberTagValueDuration<ULongPoint3Data>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), Convert.ToByte(request.Value));
                        break;
                    case Cdy.Tag.TagType.Double:
                        res = FindNumberTagValueDuration<double>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToDouble(request.Value), request.Interval);
                        break;
                    case Cdy.Tag.TagType.Float:
                        res = FindNumberTagValueDuration<float>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToSingle(request.Value), request.Interval);
                        break;
                    case Cdy.Tag.TagType.Byte:
                        res = FindNumberTagValueDuration<byte>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        break;
                    case Cdy.Tag.TagType.Int:
                        res = FindNumberTagValueDuration<int>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        break;
                    case Cdy.Tag.TagType.Long:
                        res = FindNumberTagValueDuration<long>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        break;
                    case Cdy.Tag.TagType.UInt:
                        res = FindNumberTagValueDuration<uint>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        break;
                    case Cdy.Tag.TagType.Short:
                        res = FindNumberTagValueDuration<short>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        break;
                    case Cdy.Tag.TagType.ULong:
                        res = FindNumberTagValueDuration<ulong>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
                        break;
                    case Cdy.Tag.TagType.UShort:
                        res = FindNumberTagValueDuration<ushort>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), (NumberStatisticsType)(byte)request.ValueCompareType, Convert.ToInt64(request.Value), request.Interval);
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

        public double? StatisticsTagAvgValue<T>(int id, DateTime startTime, DateTime endTime)
        {
            if (WebAPIStartup.IsRunningEmbed)
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
            if (WebAPIStartup.IsRunningEmbed)
            {
                var re = ServiceLocator.Locator.Resolve<IHisQuery>().FindNumberTagMaxMinValue<T>(id, startTime, endTime, type, out IEnumerable<DateTime> times);
                return new Tuple<double, List<DateTime>>(re, times.ToList());
            }
            else
            {
                return DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.StatisticsTagMaxMinValue(id, startTime, endTime, type);
            }
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
                        res = StatisticsTagAvgValue<double>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime));
                        break;
                    case Cdy.Tag.TagType.Float:
                        res = StatisticsTagAvgValue<float>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        res = StatisticsTagAvgValue<byte>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime));
                        break;
                    case Cdy.Tag.TagType.Int:
                        res = StatisticsTagAvgValue<int>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime));
                        break;
                    case Cdy.Tag.TagType.Long:
                        res = StatisticsTagAvgValue<long>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        res = StatisticsTagAvgValue<uint>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime));
                        break;
                    case Cdy.Tag.TagType.Short:
                        res = StatisticsTagAvgValue<short>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        res = StatisticsTagAvgValue<ulong>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        res = StatisticsTagAvgValue<ushort>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime));
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
                        res = StatisticsTagMaxMinValue<double>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Max);
                        break;
                    case Cdy.Tag.TagType.Float:
                        res = StatisticsTagMaxMinValue<float>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Max);
                        break;
                    case Cdy.Tag.TagType.Byte:
                        res = StatisticsTagMaxMinValue<byte>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Max);
                        break;
                    case Cdy.Tag.TagType.Int:
                        res = StatisticsTagMaxMinValue<int>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Max);
                        break;
                    case Cdy.Tag.TagType.Long:
                        res = StatisticsTagMaxMinValue<long>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Max);
                        break;
                    case Cdy.Tag.TagType.UInt:
                        res = StatisticsTagMaxMinValue<uint>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Max);
                        break;
                    case Cdy.Tag.TagType.Short:
                        res = StatisticsTagMaxMinValue<short>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Max);
                        break;
                    case Cdy.Tag.TagType.ULong:
                        res = StatisticsTagMaxMinValue<ulong>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Max);
                        break;
                    case Cdy.Tag.TagType.UShort:
                        res = StatisticsTagMaxMinValue<ushort>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Max);
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
                        res = StatisticsTagMaxMinValue<double>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Min);
                        break;
                    case Cdy.Tag.TagType.Float:
                        res = StatisticsTagMaxMinValue<float>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Min);
                        break;
                    case Cdy.Tag.TagType.Byte:
                        res = StatisticsTagMaxMinValue<byte>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Min);
                        break;
                    case Cdy.Tag.TagType.Int:
                        res = StatisticsTagMaxMinValue<int>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Min);
                        break;
                    case Cdy.Tag.TagType.Long:
                        res = StatisticsTagMaxMinValue<long>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Min);
                        break;
                    case Cdy.Tag.TagType.UInt:
                        res = StatisticsTagMaxMinValue<uint>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Min);
                        break;
                    case Cdy.Tag.TagType.Short:
                        res = StatisticsTagMaxMinValue<short>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Min);
                        break;
                    case Cdy.Tag.TagType.ULong:
                        res = StatisticsTagMaxMinValue<ulong>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Min);
                        break;
                    case Cdy.Tag.TagType.UShort:
                        res = StatisticsTagMaxMinValue<ushort>(tag.Id, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime), NumberStatisticsType.Min);
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
