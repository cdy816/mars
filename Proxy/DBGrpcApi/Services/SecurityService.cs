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
        private readonly ILogger<SecurityService> _logger;
        public SecurityService(ILogger<SecurityService> logger)
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
            string sip = context.Peer.Split(":")[1];
            if (Cdy.Tag.Common.ClientAuthorization.Instance.CheckIp(sip) && Cdy.Tag.Common.ClientAuthorization.Instance.CheckApplication(request.ApplicationCode))
            {
                var ss = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>();
                if (ss != null)
                {
                    string Token = ss.Login(request.Name, request.Password);
                    return Task.FromResult(new LoginReply() { Token = Token, Time = DateTime.UtcNow.ToBinary(), Timeout = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().TimeOut });
                }
                else
                {
                    return Task.FromResult(new LoginReply() { Token = "", Time = DateTime.UtcNow.ToBinary(), Timeout = 0 });
                }
            }
            else
            {
                return Task.FromResult(new LoginReply() { Token = "", Time = DateTime.UtcNow.ToBinary(), Timeout = 0 });
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<LogoutReply> Logout(LogoutRequest request, ServerCallContext context)
        {
            var ss = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>();
            if (ss != null)
            {
                ss.Logout(request.Token);
                return Task.FromResult(new LogoutReply() { Result = true });
            }
            else
            {
                return Task.FromResult(new LogoutReply() { Result = false });
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<HeartReply> Heart(HeartRequest request, ServerCallContext context)
        {
            try
            {
                var dt = DateTime.FromBinary(request.Time);
                if ((DateTime.UtcNow - dt).TotalMinutes > Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().TimeOut)
                {
                    return Task.FromResult(new HeartReply() { Result = false });
                }

                Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().FreshUserId(request.Token);
                return Task.FromResult(new HeartReply() { Result = true });
            }
            catch
            {
                return Task.FromResult(new HeartReply() { Result = false });
            }
        }

    }
}
