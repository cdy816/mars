using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace DBDevelopService
{
    public class DevelopServerService : DevelopServer.DevelopServerBase
    {
        //private readonly ILogger<DevelopServerService> _logger;
        //public DevelopServerService(ILogger<DevelopServerService> logger)
        //{
        //    _logger = logger;
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<LoginReply> Login(LoginRequest request, ServerCallContext context)
        {
            return base.Login(request, context);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetHistTagMessageReply> GetHisAllTag(GetRequest request, ServerCallContext context)
        {
            return base.GetHisAllTag(request, context);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetRealTagMessageReply> GetRealAllTag(GetRequest request, ServerCallContext context)
        {
            return base.GetRealAllTag(request, context);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetHistTagMessageReply> QueryHisTag(QueryMessage request, ServerCallContext context)
        {
            return base.QueryHisTag(request, context);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetRealTagMessageReply> QueryRealTag(QueryMessage request, ServerCallContext context)
        {
            return base.QueryRealTag(request, context);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> RemoveTag(RemoveTagMessage request, ServerCallContext context)
        {
            return base.RemoveTag(request, context);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> UpdateHisTag(UpdateHisTagRequestMessage request, ServerCallContext context)
        {
            return base.UpdateHisTag(request, context);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> UpdateRealTag(UpdateRealTagRequestMessage request, ServerCallContext context)
        {
            return base.UpdateRealTag(request, context);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<AddTagReplyMessage> AddTag(AddTagRequestMessage request, ServerCallContext context)
        {
            return base.AddTag(request, context);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> Save(GetRequest request, ServerCallContext context)
        {
            return base.Save(request, context);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> Cancel(GetRequest request, ServerCallContext context)
        {
            return base.Cancel(request, context);
        }

        
    }
}
