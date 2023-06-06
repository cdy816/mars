using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cdy.Tag;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace DBGrpcApi
{
    public class RealDataService : RealData.RealDataBase
    {

        private readonly ILogger<RealDataService> _logger;
        /// <summary>
        /// 
        /// </summary>
        /// <param name="logger"></param>
        public RealDataService(ILogger<RealDataService> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetTagIdByNameReplay> GetTagIdByName(GetTagIdByNameRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token) && SecurityManager.Manager.CheckReaderPermission(request.Token, request.Group))
            {
                GetTagIdByNameReplay response = new GetTagIdByNameReplay() { Result = true };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                var ids = service.GetTagIdByName(request.TagNames.Select(e => string.IsNullOrEmpty(request.Group) ? e : request.Group + "." + e).ToList());
                for (int i = 0; i < request.TagNames.Count; i++)
                {
                    if(ids.Count > i && ids[i].HasValue)
                    {
                        response.Ids.Add(ids[i].Value);
                    }
                    else
                    {
                        response.Ids.Add(-1);
                    }
                }
                return Task.FromResult(response);
            }
            else
            {
                return Task.FromResult(new GetTagIdByNameReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetRealValueReply> GetRealValue(GetRealValueRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token) && SecurityManager.Manager.CheckReaderPermission(request.Token, request.Group))
            {
                GetRealValueReply response = new GetRealValueReply() { Result = true };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                var ids = service.GetTagIdByName(request.TagNames.Select(e => string.IsNullOrEmpty(request.Group) ? e : request.Group + "." + e).ToList());
                for (int i = 0; i < request.TagNames.Count; i++)
                {
                    if (ids.Count>i && ids[i].HasValue)
                    {
                        byte quality;
                        DateTime time;
                        byte tagtype = 0;
                        if (service.IsComplexTag(ids[i].Value))
                        {
                            List<RealTagValueWithTimer> res = new List<RealTagValueWithTimer>();
                            service.GetComplexTagValue(ids[i].Value, res);
                            response.Values.Add(new ValueQualityTime() { Id = i, ValueType = (byte)TagType.Complex, Quality = 0, Value = FormateToString(res) });
                        }
                        else
                        {
                            var val = service.GetTagValue(ids[i].Value, out quality, out time, out tagtype);
                            if (val != null)
                                response.Values.Add(new ValueQualityTime() { Id = i, Quality = quality, Value = val.ToString(), ValueType = tagtype, Time = time.ToBinary() });
                            else
                                response.Values.Add(new ValueQualityTime() { Id = i, Quality = (byte)QualityConst.Null, Value = "", ValueType = tagtype, Time = 0 });
                        }
                    }
                }
                return Task.FromResult(response);
            }
            else
            {
                return Task.FromResult(new GetRealValueReply() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetTagStateReply> GetTagState(GetTagStateRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token) && SecurityManager.Manager.CheckReaderPermission(request.Token, request.Group))
            {
                GetTagStateReply response = new GetTagStateReply() { Result = true };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                var ids = service.GetTagIdByName(request.TagNames.Select(e => string.IsNullOrEmpty(request.Group) ? e : request.Group + "." + e).ToList());
                Dictionary<int, string> tags = new Dictionary<int, string>();
                for (int i = 0; i < request.TagNames.Count; i++)
                {
                    if (ids.Count > i && ids[i].HasValue)
                    {
                        if (service.IsComplexTag(ids[i].Value))
                        {
                            service.ListComplexTagChildTagId(ids[i].Value, tags);
                        }
                        else
                        {
                            tags.Add(ids[i].Value, request.TagNames[i]);
                        }
                    }
                    else
                    {
                        response.Values.Add(new IntValueItem() { TagName = request.TagNames[i], Value = -1 });
                    }
                }
                if (tags.Count > 0)
                {
                    List<short> states = null;
                    if (Startup.IsRunningEmbed)
                    {
                        states= new List<short>();
                        foreach(var vv in tags)
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
                            response.Values.Add(new IntValueItem() { TagName = vv.Value, Value = states[i] });
                            i++;
                        }
                    }
                }
                return Task.FromResult(response);
            }
            else
            {
                return Task.FromResult(new GetTagStateReply() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetTagExtendField2Reply> GetTagExtendField2(GetTagStateRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token) && SecurityManager.Manager.CheckReaderPermission(request.Token, request.Group))
            {
                GetTagExtendField2Reply response = new GetTagExtendField2Reply() { Result = true };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                var ids = service.GetTagIdByName(request.TagNames.Select(e => string.IsNullOrEmpty(request.Group) ? e : request.Group + "." + e).ToList());
                Dictionary<int, string> tags = new Dictionary<int, string>();
                for (int i = 0; i < request.TagNames.Count; i++)
                {
                    if (ids.Count > i && ids[i].HasValue)
                    {
                        if (service.IsComplexTag(ids[i].Value))
                        {
                            service.ListComplexTagChildTagId(ids[i].Value, tags);
                        }
                        else
                        {
                            tags.Add(ids[i].Value, request.TagNames[i]);
                        }
                    }
                    else
                    {
                        response.Values.Add(new Int64ValueItem() { TagName = request.TagNames[i], Value = -1 });
                    }
                }
                if (tags.Count > 0)
                {
                    List<long> states=null;
                    if (Startup.IsRunningEmbed)
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
                            response.Values.Add(new Int64ValueItem() { TagName = vv.Value, Value = states[i] });
                            i++;
                        }
                    }
                }
                return Task.FromResult(response);
            }
            else
            {
                return Task.FromResult(new GetTagExtendField2Reply() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetRealValueReply> GetRealValueById(GetRealValueByIdRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token) && SecurityManager.Manager.CheckReaderPermission(request.Token, request.Group))
            {
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                GetRealValueReply response = new GetRealValueReply() { Result = true };
                for (int i = 0; i < request.Ids.Count; i++)
                {
                    byte quality;
                    DateTime time;
                    byte tagtype = 0;

                    if (service.IsComplexTag(request.Ids[i]))
                    {
                        List<RealTagValueWithTimer> res = new List<RealTagValueWithTimer>();
                        service.GetComplexTagValue(request.Ids[i], res);
                        response.Values.Add(new ValueQualityTime() { Id = request.Ids[i], ValueType = (byte)TagType.Complex, Quality = 0, Value = FormateToString(res) });
                    }
                    else
                    {
                        var val = service.GetTagValue(request.Ids[i], out quality, out time, out tagtype);
                        if (val != null)
                            response.Values.Add(new ValueQualityTime() { Id = request.Ids[i], Quality = quality, Value = val.ToString(), ValueType = tagtype, Time = time.ToBinary() });
                        else
                            response.Values.Add(new ValueQualityTime() { Id = request.Ids[i], Quality = (byte)QualityConst.Null, Value = "", ValueType = tagtype, Time = 0 });
                    }
                }
                return Task.FromResult(response);
            }
            else
            {
                return Task.FromResult(new GetRealValueReply() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetRealValueAndQualityReply> GetRealValueAndQuality(GetRealValueRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token) && SecurityManager.Manager.CheckReaderPermission(request.Token, request.Group))
            {
                GetRealValueAndQualityReply response = new GetRealValueAndQualityReply() { Result = true };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                var ids = service.GetTagIdByName(request.TagNames.Select(e => string.IsNullOrEmpty(request.Group) ? e : request.Group + "." + e).ToList());
                for (int i = 0; i < request.TagNames.Count; i++)
                {
                    if (ids.Count > i && ids[i].HasValue)
                    {
                        byte quality;
                        DateTime time;
                        byte tagtype = 0;
                        if (service.IsComplexTag(ids[i].Value))
                        {
                            List<RealTagValueWithTimer> res = new List<RealTagValueWithTimer>();
                            service.GetComplexTagValue(ids[i].Value, res);
                            response.Values.Add(new ValueAndQuality() { Id = i, ValueType = (byte)TagType.Complex, Quality = 0, Value = FormateToString(res) });
                        }
                        else
                        {

                            var val = service.GetTagValue(ids[i].Value, out quality, out time, out tagtype);
                            if (val != null)
                                response.Values.Add(new ValueAndQuality() { Id = i, Quality = quality, Value = val.ToString(), ValueType = tagtype });
                            else
                                response.Values.Add(new ValueAndQuality() { Id = i, Quality = (int)QualityConst.Null, Value = "", ValueType = tagtype });
                        }
                    }
                }
                return Task.FromResult(response);
            }
            else
            {
                return Task.FromResult(new GetRealValueAndQualityReply() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="vals"></param>
        /// <returns></returns>
        private string FormateToString(List<RealTagValueWithTimer> vals)
        {
            StringBuilder sb = new StringBuilder();
            foreach (RealTagValueWithTimer val in vals)
            {
                sb.Append($"{val.Id}:{val.ValueType}:{val.Value}:{val.Quality}:{val.Time.ToBinary()};");
            }
            return sb.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetRealValueAndQualityReply> GetRealValueAndQualityById(GetRealValueByIdRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token) && SecurityManager.Manager.CheckReaderPermission(request.Token, request.Group))
            {
                GetRealValueAndQualityReply response = new GetRealValueAndQualityReply() { Result = true };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                for (int i = 0; i < request.Ids.Count; i++)
                {
                    byte quality;
                    DateTime time;
                    byte tagtype = 0;
                    var vid = request.Ids[i];
                    if (service.IsComplexTag(vid))
                    {
                        List<RealTagValueWithTimer> res = new List<RealTagValueWithTimer>();
                        service.GetComplexTagValue(vid, res);
                        response.Values.Add(new ValueAndQuality() { Id = vid, ValueType = (byte)TagType.Complex, Quality = 0, Value = FormateToString(res) });
                    }
                    else
                    {
                        var val = service.GetTagValue(vid, out quality, out time, out tagtype);
                        if (val != null)
                            response.Values.Add(new ValueAndQuality() { Id = vid, Quality = quality, Value = val.ToString(), ValueType = tagtype });
                        else
                            response.Values.Add(new ValueAndQuality() { Id = vid, Quality = (int)QualityConst.Null, Value = "", ValueType = tagtype });
                    }
                }
                return Task.FromResult(response);
            }
            else
            {
                return Task.FromResult(new GetRealValueAndQualityReply() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetRealValueOnlyReply> GetRealValueOnly(GetRealValueRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token) && SecurityManager.Manager.CheckReaderPermission(request.Token, request.Group))
            {
                GetRealValueOnlyReply response = new GetRealValueOnlyReply() { Result = true };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                var ids = service.GetTagIdByName(request.TagNames.Select(e => string.IsNullOrEmpty(request.Group) ? e : request.Group + "." + e).ToList());
                for (int i = 0; i < request.TagNames.Count; i++)
                {
                    if (ids.Count>i && ids[i].HasValue)
                    {
                        byte quality;
                        DateTime time;
                        byte tagtype = 0;
                        if (service.IsComplexTag(ids[i].Value))
                        {
                            List<RealTagValueWithTimer> res = new List<RealTagValueWithTimer>();
                            service.GetComplexTagValue(ids[i].Value, res);
                            response.Values.Add(new ValueOnly() { Id = i, ValueType = (byte)TagType.Complex,Value = FormateToString(res) });
                        }
                        else
                        {
                            var val = service.GetTagValue(ids[i].Value, out quality, out time, out tagtype);
                            if (val != null)
                                response.Values.Add(new ValueOnly() { Id = i, Value = val.ToString(), ValueType = tagtype });
                            else
                                response.Values.Add(new ValueOnly() { Id = i, Value = "", ValueType = tagtype });
                        }
                    }
                }
                return Task.FromResult(response);
            }
            else
            {
                return Task.FromResult(new GetRealValueOnlyReply() { Result = false });
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetRealValueOnlyReply> GetRealValueOnlyById(GetRealValueByIdRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token) && SecurityManager.Manager.CheckReaderPermission(request.Token, request.Group))
            {
                GetRealValueOnlyReply response = new GetRealValueOnlyReply() { Result = true };
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                for (int i = 0; i < request.Ids.Count; i++)
                {
                    byte quality;
                    DateTime time;
                    byte tagtype = 0;
                    var vid = request.Ids[i];
                    if (service.IsComplexTag(vid))
                    {
                        List<RealTagValueWithTimer> res = new List<RealTagValueWithTimer>();
                        service.GetComplexTagValue(vid, res);
                        response.Values.Add(new ValueOnly() { Id = vid, ValueType = (byte)TagType.Complex, Value = FormateToString(res) });
                    }
                    else
                    {
                        var val = service.GetTagValue(vid, out quality, out time, out tagtype);
                        if (val != null)
                            response.Values.Add(new ValueOnly() { Id = vid, Value = val.ToString(), ValueType = tagtype });
                        else
                            response.Values.Add(new ValueOnly() { Id = vid, Value = "", ValueType = tagtype });
                    }
                }
                return Task.FromResult(response);
            }
            else
            {
                return Task.FromResult(new GetRealValueOnlyReply() { Result = false });
            }
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
        /// 设置实时值
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReply> SetRealValue(SetRealValueRequest request, ServerCallContext context)
        {
            bool re = true;
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                foreach (var vv in request.Values)
                {
                    string sname = GetGroupName(vv.TagName);
                    re &= SecurityManager.Manager.CheckWritePermission(request.Token, sname);
                }

                if (re)
                {
                    foreach (var vv in request.Values)
                    {
                        service.SetTagValueForConsumer(vv.TagName, vv.Value);
                    }
                }

                return Task.FromResult(new BoolResultReply { Result = re});
            }
            return Task.FromResult(new BoolResultReply { Result = false });

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReply> SetTagState(SetTagStateRequest request, ServerCallContext context)
        {
            bool re = true;
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                foreach (var vv in request.Values)
                {
                    string sname = GetGroupName(vv.TagName);
                    re &= SecurityManager.Manager.CheckWritePermission(request.Token, sname);
                }

                if (re)
                {
                    Dictionary<int, short> vals = new Dictionary<int, short>();
                    foreach (var vv in request.Values)
                    {
                        var vid = service.GetTagIdByName(new List<string>() { vv.TagName });
                        if(vid!=null&&vid.Count>0 && vid[0].HasValue && vid[0].Value>-1)
                        {
                            vals.Add(vid[0].Value,(short)vv.Value);
                        }
                    }
                    if (Startup.IsRunningEmbed)
                    {
                        foreach(var vv in vals)
                        {
                           re&= service.SetTagState(vv.Key,vv.Value);
                        }
                    }
                    else
                    {
                        re = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.SetTagState(vals);
                    }
                }

                return Task.FromResult(new BoolResultReply { Result = re });
            }
            return Task.FromResult(new BoolResultReply { Result = false });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReply> SetTagExtendField2(SetTagExtendField2Request request, ServerCallContext context)
        {
            bool re = true;
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                foreach (var vv in request.Values)
                {
                    string sname = GetGroupName(vv.TagName);
                    re &= SecurityManager.Manager.CheckWritePermission(request.Token, sname);
                }

                if (re)
                {
                    Dictionary<int, long> vals = new Dictionary<int, long>();
                    foreach (var vv in request.Values)
                    {
                        var vid = service.GetTagIdByName(new List<string>() { vv.TagName });
                        if (vid != null && vid.Count > 0 && vid[0].HasValue && vid[0].Value > -1)
                        {
                            vals.Add(vid[0].Value, vv.Value);
                        }
                    }
                    if (Startup.IsRunningEmbed)
                    {
                        foreach (var vv in vals)
                        {
                            service.SetTagExtend2(vv.Key, vv.Value);
                        }
                    }
                    else
                    {
                        re = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.SetTagExtendField2(vals);
                    }
                }
                return Task.FromResult(new BoolResultReply { Result = re });
            }
            return Task.FromResult(new BoolResultReply { Result = false });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReply> SetRealValueById(SetRealValueByIdRequest request, ServerCallContext context)
        {
            bool re = true;
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                var smm = ServiceLocator.Locator.Resolve<ITagManager>();
                foreach (var vv in request.Values)
                {
                    string sname = smm.GetTagById(vv.TagId).Group;
                    re &= SecurityManager.Manager.CheckWritePermission(request.Token, sname);
                }

                if (re)
                {
                    foreach (var vv in request.Values)
                    {
                        service.SetTagValueForConsumer(vv.TagId, vv.Value);
                    }
                }

                return Task.FromResult(new BoolResultReply { Result = re });
            }
            return Task.FromResult(new BoolResultReply { Result = false });
        }

    }
}
