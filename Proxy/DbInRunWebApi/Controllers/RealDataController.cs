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
                RealValueQueryResponse response = new RealValueQueryResponse() { Result = true, Datas = new List<object>(request.TagNames.Count) };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                var ids = service.GetTagIdByName(request.TagNames.Select(e=>string.IsNullOrEmpty(request.Group)?e:request.Group+"."+e).ToList());
                var tagservice = ServiceLocator.Locator.Resolve<ITagManager>();
                for (int i = 0; i < request.TagNames.Count; i++)
                {
                    if (ids[i].HasValue)
                    {
                        byte quality;
                        DateTime time;
                        byte tagtype = 0;
                        if (service.IsComplexTag(ids[i].Value))
                        {
                            List<RealTagValueWithTimer> res = new List<RealTagValueWithTimer>();
                            service.GetComplexTagValue(ids[i].Value, res);
                            RealValueCollection rvc = new RealValueCollection() { SubValues=new List<RealValue>(), Name =  request.TagNames[i] };

                            foreach (var vv in res)
                            {
                                var vtag = tagservice.GetTagById(vv.Id);
                                rvc.SubValues.Add(new RealValue() {Name =vtag!=null?vtag.FullName: vv.Id.ToString(), Quality = vv.Quality, Value = vv.Value,Time=vv.Time });
                            }

                            response.Datas.Add(rvc);
                        }
                        else
                        {
                            var val = service.GetTagValue(ids[i].Value, out quality, out time, out tagtype);
                            response.Datas.Add(new RealValue() { Quality = quality, Time = time.ToLocalTime(), Value = val, Name = request.TagNames[i] });
                        }
                    }
                    else
                    {
                        response.Datas.Add(new RealValue() { Quality = (byte)QualityConst.Null, Name = request.TagNames[i] });
                    }
                }
                return response;
            }
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
                var tagservice = ServiceLocator.Locator.Resolve<ITagManager>();
                for (int i = 0; i < request.TagNames.Count; i++)
                {
                    if (ids[i].HasValue)
                    {
                        byte quality;
                        DateTime time;
                        byte tagtype = 0;

                        if (service.IsComplexTag(ids[i].Value))
                        {
                            List<RealTagValueWithTimer> res = new List<RealTagValueWithTimer>();
                            service.GetComplexTagValue(ids[i].Value, res);
                            RealValueCollection rvc = new RealValueCollection() { SubValues = new List<RealValue>(), Name = request.TagNames[i] };

                            foreach (var vv in res)
                            {
                                var vtag = tagservice.GetTagById(vv.Id);
                                rvc.SubValues.Add(new RealValue() { Name = vtag != null ? vtag.FullName : vv.Id.ToString(), Quality = vv.Quality, Value = vv.Value, Time = vv.Time });
                            }

                            response.Datas.Add(rvc);
                        }
                        else
                        {
                            var val = service.GetTagValue(ids[i].Value, out quality, out time, out tagtype);
                            response.Datas.Add(val);
                        }
                    }
                    else
                    {
                        response.Datas.Add(new RealValue() { Quality = (byte)QualityConst.Null });
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
                RealValueAndQualityQueryResponse response = new RealValueAndQualityQueryResponse() { Result = true, Datas = new List<object>() };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                var ids = service.GetTagIdByName(request.TagNames.Select(e => string.IsNullOrEmpty(request.Group) ? e : request.Group + "." + e).ToList());
                var tagservice = ServiceLocator.Locator.Resolve<ITagManager>();
                for (int i = 0; i < request.TagNames.Count; i++)
                {
                    if (ids[i].HasValue)
                    {
                        byte quality;
                        DateTime time;
                        byte tagtype = 0;

                        if (service.IsComplexTag(ids[i].Value))
                        {
                            List<RealTagValueWithTimer> res = new List<RealTagValueWithTimer>();
                            service.GetComplexTagValue(ids[i].Value,res);
                            RealValueAndQualityCollection rvc = new RealValueAndQualityCollection() { SubValues = new List<RealValue>(), Name = request.TagNames[i] };

                            foreach (var vv in res)
                            {
                                var vtag = tagservice.GetTagById(vv.Id);
                                rvc.SubValues.Add(new RealValue() { Name = vtag != null ? vtag.FullName : vv.Id.ToString(), Quality = vv.Quality, Value = vv.Value, Time = vv.Time });
                            }

                            response.Datas.Add(rvc);
                        }
                        else
                        {
                            var val = service.GetTagValue(ids[i].Value, out quality, out time, out tagtype);
                            response.Datas.Add(new RealValueAndQuality() { Quality = quality, Value = val, Name = request.TagNames[i] });
                        }
                       
                    }
                    else
                    {
                        response.Datas.Add(new RealValueAndQuality() { Quality = (byte)QualityConst.Null, Name = request.TagNames[i] });
                    }
                }
                return response;
            }
            return new RealValueAndQualityQueryResponse() { Result = false };
        }

        /// <summary>
        /// 只获取变量的实时值
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("State")]
        public RealValueOnlyQueryResponse GetState([FromBody] RealDataRequest request)
        {
            if (DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token) && DbInRunWebApi.SecurityManager.Manager.CheckReaderPermission(request.Token, request.Group))
            {
                RealValueOnlyQueryResponse response = new RealValueOnlyQueryResponse() { Result = true, Datas = new List<object>() };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                var ids = service.GetTagIdByName(request.TagNames.Select(e => string.IsNullOrEmpty(request.Group) ? e : request.Group + "." + e).ToList());
                var tagservice = ServiceLocator.Locator.Resolve<ITagManager>();
                Dictionary<int, string> tags = new Dictionary<int, string>();
                for (int i = 0; i < request.TagNames.Count; i++)
                {
                    if (ids[i].HasValue)
                    {

                        if (service.IsComplexTag(ids[i].Value))
                        {
                            service.ListComplexTagChildTagId(ids[i].Value, tags);
                            //List<RealTagValueWithTimer> res = new List<RealTagValueWithTimer>();
                            //service.GetComplexTagValue(ids[i].Value, res);

                            //PropertyValueCollection<short> rvc = new PropertyValueCollection<short>() { SubValues = new List<short>(), Name = request.TagNames[i] };
                            //var ses = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.GetTagState(res.Select(e => e.Id).ToList());
                            //rvc.SubValues.AddRange(ses);
                            //response.Datas.Add(rvc);
                        }
                        else
                        {
                            tags.Add(ids[i].Value, request.TagNames[i]);
                        }
                    }
                    else
                    {
                        response.Datas.Add(new PropetyValue<short>() { Name = request.TagNames[i], Value = -1 });
                    }
                }

                if (tags.Count > 0)
                {
                    List<short> states = null;
                    if (WebAPIStartup.IsRunningEmbed)
                    {
                        states = new List<short>();
                        foreach (var vv in tags)
                        {
                            var vval = service.GetTagState(vv.Key);
                            if (vval != null)
                                states.Add(vval.Value);
                            else
                            {
                                states.Add(-1);
                            }

                        }
                    }
                    else
                    {
                        states = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.GetTagState(tags.Keys.ToList());
                    }

                    if (states != null && states.Count > 0)
                    {
                        int i = 0;
                        foreach (var vv in tags)
                        {
                            response.Datas.Add(new PropetyValue<short>() { Name = vv.Value, Value = states[i] });
                            i++;
                        }
                    }
                }

                return response;
            }
            else
            {
                return new RealValueOnlyQueryResponse() { Result = false };
            }
        }

        /// <summary>
        /// 获取扩展属性
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("ExtendField2")]
        public RealValueOnlyQueryResponse GetExtendField2([FromBody] RealDataRequest request)
        {
            if (DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token) && DbInRunWebApi.SecurityManager.Manager.CheckReaderPermission(request.Token, request.Group))
            {
                RealValueOnlyQueryResponse response = new RealValueOnlyQueryResponse() { Result = true, Datas = new List<object>() };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                var ids = service.GetTagIdByName(request.TagNames.Select(e => string.IsNullOrEmpty(request.Group) ? e : request.Group + "." + e).ToList());
                var tagservice = ServiceLocator.Locator.Resolve<ITagManager>();
                Dictionary<int, string> tags = new Dictionary<int, string>();
                for (int i = 0; i < request.TagNames.Count; i++)
                {
                    if (ids[i].HasValue)
                    {

                        if (service.IsComplexTag(ids[i].Value))
                        {
                            service.ListComplexTagChildTagId(ids[i].Value, tags);
                            //List<RealTagValueWithTimer> res = new List<RealTagValueWithTimer>();
                            //service.GetComplexTagValue(ids[i].Value, res);

                            //PropertyValueCollection<long> rvc = new PropertyValueCollection<long>() { SubValues = new List<long>(), Name = request.TagNames[i] };
                            //var ses = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.GetTagExtendField2(res.Select(e => e.Id).ToList());
                            //rvc.SubValues.AddRange(ses);
                            //response.Datas.Add(rvc);
                        }
                        else
                        {
                            tags.Add(ids[i].Value, request.TagNames[i]);
                        }
                    }
                    else
                    {
                        response.Datas.Add(new PropetyValue<long>() { Name = request.TagNames[i], Value = -1 });
                    }
                }

                if (tags.Count > 0)
                {
                    List<long> states = null;
                    if (WebAPIStartup.IsRunningEmbed)
                    {
                        states = new List<long>();
                        foreach (var vv in tags)
                        {
                            var vval = service.GetTagExtend2(vv.Key);
                            if (vval != null)
                                states.Add(vval.Value);
                            else
                            {
                                states.Add(-1);
                            }

                        }
                    }
                    else
                        states = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.GetTagExtendField2(tags.Keys.ToList());
                    if (states != null && states.Count > 0)
                    {
                        int i = 0;
                        foreach (var vv in tags)
                        {
                            response.Datas.Add(new PropetyValue<long>() { Name = vv.Value, Value = states[i] });
                            i++;
                        }
                    }
                }

                return response;
            }
            else
            {
                return new RealValueOnlyQueryResponse() { Result = false };
            }
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

        /// <summary>
        /// 更新变量的状态值
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost("SetState")]
        public RealDataSetResponse SetState([FromBody] RealDataSetRequest request)
        {
            HashSet<string> grps = new HashSet<string>();
            foreach (var vv in request.Values)
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
                foreach (var vv in grps)
                {
                    re &= DbInRunWebApi.SecurityManager.Manager.CheckWritePermission(request.Token, vv);
                }
                if (!re) return new RealDataSetResponse() { Result = false, ErroMessage = "permission denied" };

                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();

                re = true;
                Dictionary<int, short> res = new Dictionary<int, short>();
                var tnames = request.Values.Keys.ToList();
                var vid = service.GetTagIdByName(tnames);

                if(vid!=null)
                {
                    int i = 0;
                    foreach(var vv in vid)
                    {

                        if(vv!=null)
                        {
                            res.Add(vv.Value, short.Parse(request.Values[tnames[i]]));
                        }
                        i++;
                    }
                }
                if (WebAPIStartup.IsRunningEmbed)
                {
                    foreach (var vv in res)
                    {
                       re&= service.SetTagState(vv.Key, vv.Value);
                    }
                }
                else
                {
                    re = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.SetTagState(res);
                }
                //re = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.SetTagState(res);

                return new RealDataSetResponse() { Result = re};
            }
            return new RealDataSetResponse() { Result = false, ErroMessage = "not login" };
        }


        /// <summary>
        /// 更新变量的扩展属性2
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost("SetExtendField2")]
        public RealDataSetResponse SetExtendField2([FromBody] RealDataSetRequest request)
        {
            HashSet<string> grps = new HashSet<string>();
            foreach (var vv in request.Values)
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
                foreach (var vv in grps)
                {
                    re &= DbInRunWebApi.SecurityManager.Manager.CheckWritePermission(request.Token, vv);
                }
                if (!re) return new RealDataSetResponse() { Result = false, ErroMessage = "permission denied" };

                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();

                re = true;
                Dictionary<int, long> res = new Dictionary<int, long>();
                var tnames = request.Values.Keys.ToList();
                var vid = service.GetTagIdByName(tnames);

                if (vid != null)
                {
                    int i = 0;
                    foreach (var vv in vid)
                    {

                        if (vv != null)
                        {
                            res.Add(vv.Value, long.Parse(request.Values[tnames[i]]));
                        }
                        i++;
                    }
                }

                if (WebAPIStartup.IsRunningEmbed)
                {
                    foreach (var vv in res)
                    {
                        service.SetTagExtend2(vv.Key, vv.Value);
                    }
                }
                else
                {
                    re = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.SetTagExtendField2(res);
                }
                //re = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.SetTagExtendField2(res);

                return new RealDataSetResponse() { Result = re };
            }
            return new RealDataSetResponse() { Result = false, ErroMessage = "not login" };
        }
    }
}
