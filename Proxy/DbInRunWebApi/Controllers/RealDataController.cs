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
    /// Real data service
    /// </summary>
    [ApiController]
    [Route("[controller]")]
    [OpenApiTag("实时数据服务", Description = "实时数据服务")]
    public class RealDataController : ControllerBase
    {
        /// <summary>
        /// 获取变量的实时值(值、时间、质量戳)
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
                        response.Datas.Add(new RealValue() { Quality = quality, Time = time.ToLocalTime(), Value = val });
                    }
                }
                return response;
            }
            //ServiceLocator.Locator.Resolve<IRealTagComsumer>().GetTagValue()
            return new RealValueQueryResponse() { Result = false };
        }

        /// <summary>
        /// 只获取变量的实时值
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
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

        /// <summary>
        /// 获取变量的实时值、质量戳
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
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
        /// 更新变量的值
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public RealDataSetResponse Post([FromBody] RealDataSetRequest request)
        {
            HashSet<string> grps = new HashSet<string>();
            foreach(var vv in request.Values)
            {
                var str = GetGroupName(vv.Key);
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
                if(!re) return new RealDataSetResponse() { Result = false,ErroMessage= "permission denied" };

                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();

                re = true;
                Dictionary<string, bool> res = new Dictionary<string, bool>();
                foreach (var vv in request.Values)
                {
                    var rr = service.SetTagValueForConsumer(vv.Key, vv.Value);
                    re &= rr;
                    res.Add(vv.Key, rr);
                }

                return new RealDataSetResponse() { Result = re,SetResults=res };
            }
            return new RealDataSetResponse() { Result = false,ErroMessage="not login" };
        }
    }
}
