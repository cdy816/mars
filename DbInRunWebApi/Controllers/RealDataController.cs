using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cdy.Tag;
using DbInRunWebApi.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace DbInRunWebApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class RealDataController : ControllerBase
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet()]
        public RealValueQueryResponse Get([FromBody] RealDataRequest request)
        {
            if(DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token)&&DbInRunWebApi.SecurityManager.Manager.CheckReaderPermission(request.Token,request.Group))
            {
                RealValueQueryResponse response = new RealValueQueryResponse() { Result = true, Datas = new List<RealValue>(request.TagNames.Count) };
                var service = ServiceLocator.Locator.Resolve<IRealTagComsumer>();
                var ids = service.GetTagIdByName(request.TagNames);
                for(int i=0;i<request.TagNames.Count;i++)
                {
                    if (ids[i].HasValue)
                    {
                        byte quality;
                        DateTime time;
                        var val = service.GetTagValue(ids[i].Value, out quality, out time);
                        response.Datas.Add(new RealValue() { Quality = quality, Time = time, Value = val });
                    }
                }
                return response;
            }
            //ServiceLocator.Locator.Resolve<IRealTagComsumer>().GetTagValue()
            return new RealValueQueryResponse() { Result = false };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public RealDataSetResponse Post([FromBody] RealDataSetRequest request)
        {
            return new RealDataSetResponse() { Result = false };
        }
    }
}
