using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DbInRunWebApi.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace DbInRunWebApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class HisDataController : ControllerBase
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet()]
        public HisValueQueryResponse Get([FromBody] HisDataRequest request)
        {
            return new HisValueQueryResponse() { Result = false };
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetByTimeSpan")]
        public HisValueQueryResponse GetByTimeSpan([FromBody] HisDataRequest2 request)
        {
            return new HisValueQueryResponse() { Result = false };
        }

    }
}
