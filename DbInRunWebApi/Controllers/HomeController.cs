using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace DbWebApiProxy.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class HomeController : ControllerBase
    {
        // GET: api/Home
        [HttpGet]
        public string Index()
        {
            return "welcome to mar realtime datase web api.";
        }

        
    }
}
