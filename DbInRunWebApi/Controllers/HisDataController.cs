using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cdy.Tag;
using DbInRunWebApi.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace DbInRunWebApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class HisDataController : ControllerBase
    {
        private List<HisValue> ProcessResult<T>(HisQueryResult<T> datas)
        {
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet()]
        public HisValueQueryResponse Get([FromBody] HisDataRequest request)
        {
            var tag  = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
            object res;
            List<HisValue> revals = new List<HisValue>(request.Times.Count);
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                     res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<bool>(tag.Id,request.Times,request.MatchType);
                    
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<byte>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<DateTime>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<double>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<float>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<int>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<long>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<short>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<string>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<uint>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ulong>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ushort>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<int>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<uint>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPoint3Data>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPoint3Data>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPointData>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPointTag>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPoint3Data>(tag.Id,request.Times,request.MatchType);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPoint3Data>(tag.Id,request.Times,request.MatchType);
                    break;
            }

            
            return new HisValueQueryResponse() { Result = false };
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetByTimeSpan")]
        public HisValueQueryResponse GetByTimeSpan([FromBody] HisDataRequest2 request)
        {
            return new HisValueQueryResponse() { Result = false };
        }

        [HttpGet("GetAllValue")]
        public HisValueQueryResponse GetAllValue([FromBody] AllHisDataRequest request)
        {
            return new HisValueQueryResponse() { Result = false };
        }

    }
}
