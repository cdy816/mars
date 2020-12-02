using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DbInRunWebApi.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace DbInRunWebApi.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class LoginController : ControllerBase
    {
        // POST: Login
        [HttpPost("TryLogin")]
        public LoginResponse Login([FromBody] LoginUser user)
        {
            var service = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>();
            if (service != null)
            {
                string Token = service.Login(user.UserName, user.Password);
                return new LoginResponse() { Token = Token, Result = !string.IsNullOrEmpty(Token), LoginTime = DateTime.UtcNow.ToBinary(), TimeOut = service.TimeOut};
            }
            else
            {
                return new LoginResponse() { Result = false };
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        [HttpPost("Hart")]
        public bool Hart([FromBody] Requestbase token)
        {
            if (string.IsNullOrEmpty(token.Time))
            {
                return false;
            }
            long ltmp = long.Parse(token.Time);

            if ((DateTime.UtcNow - DateTime.FromBinary(ltmp)).TotalSeconds > Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().TimeOut)
            {
                return false;
            }

            return Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().FreshUserId(token.Token);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <returns></returns>
        [HttpPost("Logout")]
        public bool Logout([FromBody] Requestbase token)
        {
            return Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().Logout(token.Token);
        }

    }
}
