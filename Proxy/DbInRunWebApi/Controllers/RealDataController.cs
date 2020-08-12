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
    public class RealDataController : ControllerBase
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet()]
        public RealValueQueryResponse Get([FromBody] RealDataRequest request)
        {
            if(DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token)&&DbInRunWebApi.SecurityManager.Manager.CheckReaderPermission(request.Token,request.Group))
            {
                RealValueQueryResponse response = new RealValueQueryResponse() { Result = true, Datas = new List<RealValue>(request.TagNames.Count) };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                var ids = service.GetTagIdByName(request.TagNames.Select(e=>string.IsNullOrEmpty(request.Group)?e:request.Group+"."+e).ToList());
                for (int i = 0; i < request.TagNames.Count; i++)
                {
                    if (ids[i].HasValue)
                    {
                        byte quality;
                        DateTime time;
                        byte tagtype = 0;
                        var val = service.GetTagValue(ids[i].Value, out quality, out time, out tagtype);
                        response.Datas.Add(new RealValue() { Quality = quality, Time = time, Value = val });
                    }
                }
                return response;
            }
            //ServiceLocator.Locator.Resolve<IRealTagComsumer>().GetTagValue()
            return new RealValueQueryResponse() { Result = false };
        }

        [HttpGet("Value")]
        public RealValueOnlyQueryResponse GetValueOnly([FromBody] RealDataRequest request)
        {
            if (DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token) && DbInRunWebApi.SecurityManager.Manager.CheckReaderPermission(request.Token, request.Group))
            {
                RealValueOnlyQueryResponse response = new RealValueOnlyQueryResponse() { Result = true, Datas = new List<object>() };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                var ids = service.GetTagIdByName(request.TagNames.Select(e => string.IsNullOrEmpty(request.Group) ? e : request.Group + "." + e).ToList());
                for (int i = 0; i < request.TagNames.Count; i++)
                {
                    if (ids[i].HasValue)
                    {
                        byte quality;
                        DateTime time;
                        byte tagtype = 0;
                        var val = service.GetTagValue(ids[i].Value, out quality, out time, out tagtype);
                        response.Datas.Add(val);
                    }
                }
                return response;
            }
            //ServiceLocator.Locator.Resolve<IRealTagComsumer>().GetTagValue()
            return new RealValueOnlyQueryResponse() { Result = false };
        }


        [HttpGet("ValueAndQuality")]
        public RealValueAndQualityQueryResponse GetValueAndQuality([FromBody] RealDataRequest request)
        {
            if (DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token) && DbInRunWebApi.SecurityManager.Manager.CheckReaderPermission(request.Token, request.Group))
            {
                RealValueAndQualityQueryResponse response = new RealValueAndQualityQueryResponse() { Result = true, Datas = new List<RealValueAndQuality>() };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                var ids = service.GetTagIdByName(request.TagNames.Select(e => string.IsNullOrEmpty(request.Group) ? e : request.Group + "." + e).ToList());
                for (int i = 0; i < request.TagNames.Count; i++)
                {
                    if (ids[i].HasValue)
                    {
                        byte quality;
                        DateTime time;
                        byte tagtype = 0;
                        var val = service.GetTagValue(ids[i].Value, out quality, out time, out tagtype);
                        response.Datas.Add(new RealValueAndQuality() { Quality = quality, Value = val });
                    }
                }
                return response;
            }
            //ServiceLocator.Locator.Resolve<IRealTagComsumer>().GetTagValue()
            return new RealValueAndQualityQueryResponse() { Result = false };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        private string GetGroupName(string tag)
        {
            if(tag.LastIndexOf(".")>0)
            {
                return tag.Substring(0, tag.LastIndexOf(".") - 1);
            }
            return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public RealDataSetResponse Post([FromBody] RealDataSetRequest request)
        {
            HashSet<string> grps = new HashSet<string>();
            foreach(var vv in request.Values.Keys)
            {
                var str = GetGroupName(vv);
                if (!string.IsNullOrEmpty(str))
                    grps.Add(str);
                else
                    grps.Add("");
            }

            if (DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                bool re = true;
                foreach(var vv in grps)
                {
                    re &= DbInRunWebApi.SecurityManager.Manager.CheckWritePermission(request.Token, vv);
                }
                if(!re) return new RealDataSetResponse() { Result = false };

                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();

                re = true;
                foreach (var vv in request.Values)
                    re &= service.SetTagValueForConsumer(vv.Key, vv.Value);

                return new RealDataSetResponse() { Result = re };
            }
            return new RealDataSetResponse() { Result = false };
        }
    }
}
