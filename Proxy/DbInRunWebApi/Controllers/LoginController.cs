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
            string  Token = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().Login(user.UserName, user.Password);
            //return null;
            return new LoginResponse() { Token = Token,Result = !string.IsNullOrEmpty(Token), Time=DateTime.Now };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        [HttpPost("Hart")]
        public bool Hart([FromBody] Requestbase token)
        {
           // return true;
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
            //return true;
            return Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().Logout(token.Token);
        }

    }
}
