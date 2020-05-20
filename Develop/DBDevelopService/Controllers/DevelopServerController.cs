using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace DBDevelopService.Controllers
{
    [Route("api/[controller]/[action]")]
    public class DevelopServerController : Controller
    {
        // POST api/<controller>
        [HttpPost]
        public string Login([FromBody]LoginMessage value)
        {
            return SecurityManager.Manager.Login(value.UserName, value.Password);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mLoginId"></param>
        /// <returns></returns>
        [HttpPost]
        public bool Logout([FromBody]RequestBase request)
        {
            SecurityManager.Manager.Logout(request.Id);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private bool CheckLoginId(string id, string permission = "")
        {
            return SecurityManager.Manager.CheckKeyAvaiable(id) && SecurityManager.Manager.CheckPermission(id, permission);
        }

        #region database
        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        [HttpPost]
        public object QueryDatabase([FromBody]RequestBase request)
        {
            if (!CheckLoginId(request.Id))
            {
                return new Erro() { ErroMsg = "权限不足" };
            }
            List<Database> re = new List<Database>();
            foreach (var vv in DbManager.Instance.ListDatabase())
            {
                re.Add(new Database(){Name = vv, Desc = DbManager.Instance.GetDatabase(vv).Desc });
            }
            return re;
        }
        #endregion
    }
}
