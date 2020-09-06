﻿using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DBDevelopService.Controllers
{
    public class RequestBase
    {
        public string Id { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiDatabaseRequest : RequestBase
    {
        public string Database { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiTagRequest : WebApiDatabaseRequest
    {
        public WebApiTag Tag { get; set; }
    }


    /// <summary>
    /// 
    /// </summary>
    public class WebApiRemoveTagRequest : WebApiDatabaseRequest
    {
        /// <summary>
        /// 
        /// </summary>
        public List<int> TagIds { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiAddTagRequest : WebApiDatabaseRequest
    {
        /// <summary>
        /// 
        /// </summary>
        public List<WebApiTag> Tags { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiImportTagRequest : WebApiTagRequest
    {
        /// <summary>
        /// 
        /// </summary>
        public int Mode { get; set; }
    }



    /// <summary>
    /// 
    /// </summary>
    public class NewDatabaseRequest : WebApiDatabaseRequest
    {
        /// <summary>
        /// 
        /// </summary>
        public string Desc { get; set; }
    }


    /// <summary>
    /// 
    /// </summary>
    public class GetTagByGroupRequest : WebApiDatabaseRequest
    {
        /// <summary>
        /// 
        /// </summary>
        public string GroupName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string,string> Filters { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Index { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class ResultResponse
    {
        /// <summary>
        /// 
        /// </summary>
        public bool HasErro { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public object Result { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string ErroMsg { get; set; }
    }



}
