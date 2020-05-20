using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace DbWebApiProxy.Controllers
{
    
    public class HomeController : Controller
    {
        // GET: api/Home
        [HttpGet]
        public string Index()
        {
            return Res.Get("WelcomeMsg");
        }
    }
}
