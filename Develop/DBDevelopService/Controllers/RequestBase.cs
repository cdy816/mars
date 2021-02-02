using Cdy.Tag;
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


    public class WebApiHisSettingUpdateRequest : WebApiDatabaseRequest
    {
        /// <summary>
        /// 
        /// </summary>
        public string DataPath { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string DataPathBackup { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int KeepTimeInDataPath { get; set; }
    }


    public class WebApiUpdateRealDataServerPortRequest : WebApiDatabaseRequest
    {
        public int Port { get; set; }
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
    public class WebApiNewDatabaseRequest : WebApiDatabaseRequest
    {
        /// <summary>
        /// 
        /// </summary>
        public string Desc { get; set; }
    }


    /// <summary>
    /// 
    /// </summary>
    public class WebApiGetTagByGroupRequest : WebApiDatabaseRequest
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
    public class WebApiAddGroupRequest : WebApiDatabaseRequest
    {
        public string Name { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string ParentName { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiRemoveGroupRequest : WebApiDatabaseRequest
    {
        public string FullName { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiRenameGroupRequest : WebApiDatabaseRequest
    {
        public string Name { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string OldFullName { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiMoveTagGroupRequest : WebApiDatabaseRequest
    {
        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string NewParentName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string OldParentName { get; set; }
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

    /// <summary>
    /// 
    /// </summary>
    public class GetTagsByGroupResponse : ResultResponse
    {
        public int TotalPages { get; set; }
    }




}
