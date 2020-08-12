using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cdy.Tag;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace DBGrpcApi
{
    public class RealDataService : RealData.RealDataBase
    {
        private readonly ILogger<RealDataService> _logger;
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
        public override Task<GetRealValueReply> GetRealValue(GetRealValueRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token) && SecurityManager.Manager.CheckReaderPermission(request.Token, request.Group))
            {
                GetRealValueReply response = new GetRealValueReply() { Result = true };
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
                        response.Values.Add(new ValueQualityTime() { Id = i, Quality = quality, Value = val.ToString(), ValueType = tagtype,Time=time.ToBinary()});
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
                    if (ids[i].HasValue)
                    {
                        byte quality;
                        DateTime time;
                        byte tagtype = 0;
                        var val = service.GetTagValue(ids[i].Value, out quality, out time, out tagtype);
                        response.Values.Add(new ValueAndQuality() { Id = i, Quality = quality, Value = val.ToString(), ValueType = tagtype });
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
                    if (ids[i].HasValue)
                    {
                        byte quality;
                        DateTime time;
                        byte tagtype = 0;
                        var val = service.GetTagValue(ids[i].Value, out quality, out time, out tagtype);
                        response.Values.Add(new ValueOnly() { Id = i, Value = val.ToString(), ValueType = tagtype });
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

    }
}
