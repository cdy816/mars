using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace DBGrpcApi
{
    public class SecurityService : Security.SecurityBase
    {
        private readonly ILogger<RealDataService> _logger;
        public SecurityService(ILogger<RealDataService> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<LoginReply> Login(LoginRequest request, ServerCallContext context)
        {
            string Token = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().Login(request.Name, request.Password);
            return Task.FromResult(new LoginReply() { Token = Token, Time = DateTime.UtcNow.ToBinary(), Timeout = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().TimeOut });
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<LogoutReply> Logout(LogoutRequest request, ServerCallContext context)
        {
            Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().Logout(request.Token);
            return Task.FromResult(new LogoutReply() { Result=true });
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<HartReply> Hart(HartRequest request, ServerCallContext context)
        {

            var dt = DateTime.FromBinary(request.Time);
            if ((DateTime.UtcNow - dt).TotalSeconds > Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().TimeOut)
            {
                return Task.FromResult(new HartReply() { Result = false });
            }

            Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().FreshUserId(request.Token);
            return Task.FromResult(new HartReply() { Result = true });
        }

    }
}
