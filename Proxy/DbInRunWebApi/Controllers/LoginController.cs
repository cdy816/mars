using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DbInRunWebApi.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NSwag.Annotations;

namespace DbInRunWebApi.Controllers
{
    /// <summary>
    /// 
    /// </summary>
    [Route("[controller]")]
    [ApiController]
    [OpenApiTag("登录服务", Description = "登录服务")]
    public class LoginController : ControllerBase
    {
        /// <summary>
        /// 登录
        /// </summary>
        /// <param name="user"></param>
        /// <returns></returns>
        [HttpPost("TryLogin")]
        public LoginResponse Login([FromBody] LoginUser user)
        {
            var service = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>();
            if (service != null)
            {
                string Token = service.Login(user.UserName, user.Password);
                return new LoginResponse() { Token = Token, Result = !string.IsNullOrEmpty(Token), LoginTime = DateTime.Now.ToString(), TimeOut = service.TimeOut};
            }
            else
            {
                return new LoginResponse() { Result = false };
            }
        }

        /// <summary>
        /// 心跳，维持用户在线
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        [HttpPost("Hart")]
        public ResponseBase Hart([FromBody] Requestbase token)
        {
            //try
            //{
            //    if (string.IsNullOrEmpty(token.Time))
            //    {
            //        return false;
            //    }
            //    //long ltmp = long.Parse(token.Time);
            //    DateTime dt = DateTime.Parse(token.Time);

            //    if ((DateTime.Now - dt).TotalSeconds > Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().TimeOut)
            //    {
            //        return false;
            //    }
            //}
            //catch
            //{

            //}

            return new ResponseBase() { Result = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().FreshUserId(token.Token) };
        }

        /// <summary>
        /// 登出
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        [HttpPost("Logout")]
        public ResponseBase Logout([FromBody] Requestbase token)
        {
            return new ResponseBase() { Result = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().Logout(token.Token) };
        }

    }
}
