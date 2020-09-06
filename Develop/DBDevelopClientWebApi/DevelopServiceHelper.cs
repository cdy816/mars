using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Net;
using System.Text;

namespace DBDevelopClientWebApi
{
    /// <summary>
    /// 
    /// </summary>
    public class DevelopServiceHelper
    {

        #region ... Variables  ...

        WebClient mClient;
        private string mLoginId;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public string Server { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string LastMessage { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sval"></param>
        /// <returns></returns>
        private string Post(string fun,string sval)
        {
            if(mClient==null)
            mClient = new WebClient();
            mClient.Headers[HttpRequestHeader.ContentType] = "application/json";
            mClient.Encoding = Encoding.UTF8;
            return mClient.UploadString(Server + "/DevelopServer/" + fun, sval);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public bool Login(string username, string password)
        {
            LoginMessage login = new LoginMessage() { UserName = username, Password = password };
            string sval = Post("Login",JsonConvert.SerializeObject(login));

            var result = JsonConvert.DeserializeObject<ResultResponse>(sval);
            if(result.HasErro)
            {
                LastMessage = result.ErroMsg;
                return false;
            }
            else
            {
                mLoginId = result.Result.ToString();
                return true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Logout()
        {
            RequestBase request = new RequestBase() { Id = mLoginId };
            Post("Logout", JsonConvert.SerializeObject(request));
            return true;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
