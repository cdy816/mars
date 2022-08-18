using Cdy.Tag;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
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

        public const int PageCount = 500;

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
        public string LastErroMessage { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #region database

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sval"></param>
        /// <returns></returns>
        private string Post(string fun, string sval)
        {
            if (mClient == null)
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
            try
            {
                LoginMessage login = new LoginMessage() { UserName = username, Password = password };
                string sval = Post("Login", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<string>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    mLoginId = result.Result.ToString();
                    return true;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Logout()
        {
            try
            {
                RequestBase request = new RequestBase() { Id = mLoginId };
                Post("Logout", JsonConvert.SerializeObject(request));
                return true;
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="desc"></param>
        /// <returns></returns>
        public bool NewDatabase(string database, string desc)
        {
            if (!string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    WebApiNewDatabaseRequest nd = new WebApiNewDatabaseRequest() { Database = database, Desc = desc, Id = mLoginId };
                    string sval = Post("NewDatabase", JsonConvert.SerializeObject(nd));
                    var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                    if (result.HasErro)
                    {
                        LastErroMessage = result.ErroMsg;
                        return false;
                    }
                    else
                    {
                        return (bool)result.Result;
                    }
                }
                catch (Exception ex)
                {
                    LastErroMessage = ex.Message;
                    return false;
                }
            }
            else
            {
                LastErroMessage = "未登录";
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<Database> QueryDatabase()
        {
            if (!string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    RequestBase nd = new RequestBase() { Id = mLoginId };
                    var sval = Post("QueryDatabase", JsonConvert.SerializeObject(nd));
                    var result = JsonConvert.DeserializeObject<ResultResponse<List<Database>>>(sval);
                    if (result.HasErro)
                    {
                        LastErroMessage = result.ErroMsg;
                        return null;
                    }
                    else
                    {
                        return result.Result;
                    }
                }
                catch (Exception ex)
                {
                    LastErroMessage = ex.Message;
                    return null;
                }
            }
            else
            {
                LastErroMessage = "未登录";
                return null;
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Start(string database)
        {
            if (!string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    WebApiDatabaseRequest wr = new WebApiDatabaseRequest() { Database = database, Id = mLoginId };
                    var sval = Post("Start", JsonConvert.SerializeObject(wr));
                    var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                    if (result.HasErro)
                    {
                        LastErroMessage = result.ErroMsg;
                        return false;
                    }
                    else
                    {
                        return (bool)result.Result;
                    }
                }
                catch (Exception ex)
                {
                    LastErroMessage = ex.Message;
                    return false;
                }
            }
            else
            {
                LastErroMessage = "未登录";
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool Stop(string database)
        {
            if (!string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    WebApiDatabaseRequest wr = new WebApiDatabaseRequest() { Database = database, Id = mLoginId };
                    var sval = Post("Stop", JsonConvert.SerializeObject(wr));
                    var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                    if (result.HasErro)
                    {
                        LastErroMessage = result.ErroMsg;
                        return false;
                    }
                    else
                    {
                        return (bool)result.Result;
                    }
                }
                catch (Exception ex)
                {
                    LastErroMessage = ex.Message;
                    return false;
                }
            }
            else
            {
                LastErroMessage = "未登录";
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool ReRun(string database)
        {
            if (!string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    WebApiDatabaseRequest wr = new WebApiDatabaseRequest() { Database = database, Id = mLoginId };
                    var sval = Post("ReRun", JsonConvert.SerializeObject(wr));
                    var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                    if (result.HasErro)
                    {
                        LastErroMessage = result.ErroMsg;
                        return false;
                    }
                    else
                    {
                        return (bool)result.Result;
                    }
                }
                catch (Exception ex)
                {
                    LastErroMessage = ex.Message;
                    return false;
                }
            }
            else
            {
                LastErroMessage = "未登录";
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool IsDatabaseRunning(string database)
        {
            if (!string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    WebApiDatabaseRequest wr = new WebApiDatabaseRequest() { Database = database, Id = mLoginId };
                    var sval = Post("IsDatabaseRunning", JsonConvert.SerializeObject(wr));
                    var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                    if (result.HasErro)
                    {
                        LastErroMessage = result.ErroMsg;
                        return false;
                    }
                    else
                    {
                        return (bool)result.Result;
                    }
                }
                catch (Exception ex)
                {
                    LastErroMessage = ex.Message;
                    return false;
                }
            }
            else
            {
                LastErroMessage = "未登录";
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool IsDatabaseDirty(string database)
        {
            if (!string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    WebApiDatabaseRequest wr = new WebApiDatabaseRequest() { Database = database, Id = mLoginId };
                    var sval = Post("IsDatabaseDirty", JsonConvert.SerializeObject(wr));
                    var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                    if (result.HasErro)
                    {
                        LastErroMessage = result.ErroMsg;
                        return false;
                    }
                    else
                    {
                        return (bool)result.Result;
                    }
                }
                catch (Exception ex)
                {
                    LastErroMessage = ex.Message;
                    return false;
                }
            }
            else
            {
                LastErroMessage = "未登录";
                return false;
            }
        }

        /// <summary>
        /// 获取变量组
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public List<TagGroup> GetTagGroup(string database)
        {
            if (!string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    WebApiDatabaseRequest wr = new WebApiDatabaseRequest() { Database = database, Id = mLoginId };
                    var sval = Post("GetTagGroup", JsonConvert.SerializeObject(wr));
                    var result = JsonConvert.DeserializeObject<ResultResponse<List<TagGroup>>>(sval);
                    if (result.HasErro)
                    {
                        LastErroMessage = result.ErroMsg;
                        return null;
                    }
                    else
                    {
                        return result.Result;
                    }
                }
                catch (Exception ex)
                {
                    LastErroMessage = ex.Message;
                    return null;
                }
            }
            else
            {
                LastErroMessage = "未登录";
                return null;
            }
        }

        /// <summary>
        /// 获取所有变量
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public Dictionary<int,string> GetAllTagNames(string database)
        {
            if (!string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    WebApiDatabaseRequest wr = new WebApiDatabaseRequest() { Database = database, Id = mLoginId };
                    var sval = Post("GetAllTagNames", JsonConvert.SerializeObject(wr));
                    var result = JsonConvert.DeserializeObject<ResultResponse<Dictionary<int,string>>>(sval);
                    if (result.HasErro)
                    {
                        LastErroMessage = result.ErroMsg;
                        return null;
                    }
                    else
                    {
                        return result.Result;
                    }
                }
                catch (Exception ex)
                {
                    LastErroMessage = ex.Message;
                    return null;
                }
            }
            else
            {
                LastErroMessage = "未登录";
                return null;
            }
        }

        /// <summary>
        /// 获取某个组下的所有变量的名称
        /// </summary>
        /// <param name="database"></param>
        /// <param name="group"></param>
        /// <returns></returns>
        public Dictionary<int, string> GetAllTagNamesByGroup(string database,string group)
        {
            if (!string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    WebApiGetTagByGroupRequest wr = new WebApiGetTagByGroupRequest() { Database = database, Id = mLoginId,GroupName=group };
                    var sval = Post("GetAllTagNamesByGroup", JsonConvert.SerializeObject(wr));
                    var result = JsonConvert.DeserializeObject<ResultResponse<Dictionary<int, string>>>(sval);
                    if (result.HasErro)
                    {
                        LastErroMessage = result.ErroMsg;
                        return null;
                    }
                    else
                    {
                        return result.Result;
                    }
                }
                catch (Exception ex)
                {
                    LastErroMessage = ex.Message;
                    return null;
                }
            }
            else
            {
                LastErroMessage = "未登录";
                return null;
            }
        }

        /// <summary>
        /// 获取某个组下的变量，采用分页模式
        /// </summary>
        /// <param name="database"></param>
        /// <param name="group"></param>
        /// <param name="pageIndex"></param>
        /// <param name="mFilter"></param>
        /// <returns></returns>
        public List<Tuple<Tagbase, HisTag>> GetTagByGroup(string database, string group, int pageIndex,out int pageCount, Dictionary<string, string> mFilter = null)
        {
            if (!string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    WebApiGetTagByGroupRequest wr = new WebApiGetTagByGroupRequest() { Database = database, Id = mLoginId, GroupName = group, Index = pageIndex, Filters = mFilter };
                    var sval = Post("GetTagByGroup", JsonConvert.SerializeObject(wr));
                    var result = JsonConvert.DeserializeObject<GetTagsResponse<List<WebApiTag>>>(sval);
                    if (result.HasErro)
                    {
                        LastErroMessage = result.ErroMsg;
                        pageCount = 0;
                        return null;
                    }
                    else
                    {
                        List<Tuple<Tagbase, HisTag>> re = new List<Tuple<Tagbase, HisTag>>();
                        foreach (var vv in result.Result)
                        {
                            re.Add(new Tuple<Tagbase, HisTag>(vv.RealTag.ConvertToTagbase(), vv.HisTag));
                        }
                        pageCount = result.TotalPages;
                        return re;
                    }
                }
                catch (Exception ex)
                {
                    LastErroMessage = ex.Message;
                    pageCount = 0;
                    return null;
                }
            }
            else
            {
                LastErroMessage = "未登录";
                pageCount = 0;
                return null;
            }
        }

        /// <summary>
        /// 获取某个组下的所有变量
        /// </summary>
        /// <param name="database"></param>
        /// <param name="group"></param>
        /// <param name="pageIndex"></param>
        /// <param name="mFilter"></param>
        /// <returns></returns>
        public List<Tuple<Tagbase, HisTag>> GetAllTagByGroup(string database, string group, int pageIndex, out int pageCount, Dictionary<string, string> mFilter = null)
        {
            if (!string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    WebApiGetTagByGroupRequest wr = new WebApiGetTagByGroupRequest() { Database = database, Id = mLoginId, GroupName = group, Index = pageIndex, Filters = mFilter };
                    var sval = Post("GetAllTagByGroup", JsonConvert.SerializeObject(wr));
                    var result = JsonConvert.DeserializeObject<GetTagsResponse<List<WebApiTag>>>(sval);
                    if (result.HasErro)
                    {
                        LastErroMessage = result.ErroMsg;
                        pageCount = 0;
                        return null;
                    }
                    else
                    {
                        List<Tuple<Tagbase, HisTag>> re = new List<Tuple<Tagbase, HisTag>>();
                        foreach (var vv in result.Result)
                        {
                            re.Add(new Tuple<Tagbase, HisTag>(vv.RealTag.ConvertToTagbase(), vv.HisTag));
                        }
                        pageCount = result.TotalPages;
                        return re;
                    }
                }
                catch (Exception ex)
                {
                    LastErroMessage = ex.Message;
                    pageCount = 0;
                    return null;
                }
            }
            else
            {
                LastErroMessage = "未登录";
                pageCount = 0;
                return null;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public bool AddTagGroup(string name, string parentName, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiAddGroupRequest request = new WebApiAddGroupRequest() { Database = database, Id = mLoginId, Name = name, ParentName = parentName };
                var sval = Post("AddTagGroup", JsonConvert.SerializeObject(request));
                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 更新变量组描述
        /// </summary>
        /// <param name="groupName"></param>
        /// <param name="desc"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool UpdateTagGroupDescription(string groupName,string desc,string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiUpdateGroupDescriptionRequest request = new WebApiUpdateGroupDescriptionRequest() { Database = database, Id = mLoginId, GroupName = groupName, Desc = desc };
                var sval = Post("AddTagGroup", JsonConvert.SerializeObject(request));
                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fullName"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool RemoveTagGroup(string fullName, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiRemoveGroupRequest request = new WebApiRemoveGroupRequest() { Database = database, Id = mLoginId, FullName = fullName };
                var sval = Post("RemoveTagGroup", JsonConvert.SerializeObject(request));
                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="oldFullName"></param>
        /// <param name="newName"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool RenameTagGroup(string oldFullName, string newName, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiRenameGroupRequest request = new WebApiRenameGroupRequest() { Database = database, Id = mLoginId, Name = newName, OldFullName = oldFullName };
                var sval = Post("RenameTagGroup", JsonConvert.SerializeObject(request));
                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="oldParentName"></param>
        /// <param name="newParentName"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool MoveTagGroup(string name, string oldParentName, string newParentName, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiMoveTagGroupRequest request = new WebApiMoveTagGroupRequest() { Database = database, Id = mLoginId, Name = name, OldParentName = oldParentName, NewParentName = newParentName };
                var sval = Post("MoveTagGroup", JsonConvert.SerializeObject(request));
                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagIds"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool RemoveTags(List<int> tagIds, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiRemoveTagRequest request = new WebApiRemoveTagRequest() { Database = database, Id = mLoginId, TagIds = tagIds };
                var sval = Post("RemoveTag", JsonConvert.SerializeObject(request));
                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagId"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool RemoveTag(int tagId, string database)
        {
            return RemoveTags(new List<int>() { tagId }, database);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="realTag"></param>
        /// <param name="histag"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public int? AddTag(Tagbase realTag, HisTag histag, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return null;
            }
            try
            {
                WebApiAddTagRequest request = new WebApiAddTagRequest() { Database = database, Id = mLoginId, Tags = new List<WebApiTag>() { new WebApiTag() { RealTag = WebApiRealTag.CreatFromTagbase(realTag), HisTag = histag } } };
                var sval = Post("AddTag", JsonConvert.SerializeObject(request));
                var result = JsonConvert.DeserializeObject<ResultResponse<List<int>>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return null;
                }
                else
                {
                    return result.Result[0];
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mtags"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public List<int> AddTags(List<Tuple<Tagbase, HisTag>> mtags, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return null;
            }
            try
            {
                List<WebApiTag> ltmp = new List<WebApiTag>();
                foreach (var vv in mtags)
                {
                    ltmp.Add(new WebApiTag() { RealTag = WebApiRealTag.CreatFromTagbase(vv.Item1), HisTag = vv.Item2 });
                }
                WebApiAddTagRequest request = new WebApiAddTagRequest() { Database = database, Id = mLoginId, Tags = ltmp };
                var sval = Post("AddTag", JsonConvert.SerializeObject(request));
                var result = JsonConvert.DeserializeObject<ResultResponse<List<int>>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return null;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="histag"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool UpdateHisTag(HisTag histag, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiTagRequest request = new WebApiTagRequest() { Id = mLoginId, Database = database, Tag = new WebApiTag() { HisTag = histag } };
                var sval = Post("UpdateHisTag", JsonConvert.SerializeObject(request));
                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="realtag"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool UpdateRealTag(Tagbase realtag, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiTagRequest request = new WebApiTagRequest() { Id = mLoginId, Database = database, Tag = new WebApiTag() { RealTag = WebApiRealTag.CreatFromTagbase(realtag) } };
                var sval = Post("UpdateRealTag", JsonConvert.SerializeObject(request));
                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="realTag"></param>
        /// <param name="histag"></param>
        /// <param name="mode"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public int? Import(Tagbase realTag, HisTag histag, int mode, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return null;
            }
            try
            {
                WebApiImportTagRequest request = new WebApiImportTagRequest() { Database = database, Id = mLoginId, Mode = mode, Tag = new WebApiTag() { RealTag = WebApiRealTag.CreatFromTagbase(realTag), HisTag = histag } };
                var sval = Post("Import", JsonConvert.SerializeObject(request));
                var result = JsonConvert.DeserializeObject<ResultResponse<int>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return null;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool Save(string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiDatabaseRequest request = new WebApiDatabaseRequest() { Database = database, Id = mLoginId };
                var sval = Post("Save", JsonConvert.SerializeObject(request));
                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool Cancel(string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiDatabaseRequest request = new WebApiDatabaseRequest() { Database = database, Id = mLoginId };
                var sval = Post("Cancel", JsonConvert.SerializeObject(request));
                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }
        #endregion

        #region Database User

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Name"></param>
        /// <param name="Parent"></param>
        /// <returns></returns>
        public bool AddDatabaseUserGroup(string Name, string Parent, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiUserGroupRequest login = new WebApiUserGroupRequest() { Name = Name, Parent = Parent, Database = database, Id = mLoginId };
                string sval = Post("AddDatabaseUserGroup", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public List<WebApiUserGroup> GetDatabaseUserGroup(string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return null;
            }
            try
            {
                WebApiDatabaseRequest login = new WebApiDatabaseRequest() { Database = database, Id = mLoginId };
                string sval = Post("GetDatabaseUserGroup", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<List<WebApiUserGroup>>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return null;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="oldParentName"></param>
        /// <param name="newParentName"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool MoveDatabaseUserGroup(string name, string oldParentName, string newParentName, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiMoveUserGroupRequest login = new WebApiMoveUserGroupRequest() { Name = name, NewParentName = newParentName, OldParentName = oldParentName, Database = database, Id = mLoginId };
                string sval = Post("MoveDatabaseUserGroup", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool RemoveDatabaseUserGroup(string group, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiRequestByUserGroup login = new WebApiRequestByUserGroup() { GroupFullName = group, Database = database, Id = mLoginId };
                string sval = Post("RemoveDatabaseUserGroup", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="newName"></param>
        /// <param name="oldFullName"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool RenameDatabaseUserGroup(string newName, string oldFullName, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiRenameUserGroupRequest login = new WebApiRenameUserGroupRequest() { NewName = newName, OldFullName = oldFullName, Database = database, Id = mLoginId };
                string sval = Post("RenameDatabaseUserGroup", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="groupFullName"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public List<WebApiUserInfoWithoutPassword> GetDatabaseUserByGroup(string groupFullName, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return null;
            }
            try
            {
                WebApiRequestByUserGroup login = new WebApiRequestByUserGroup() { GroupFullName = groupFullName, Database = database, Id = mLoginId };
                string sval = Post("GetDatabaseUserByGroup", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<List<WebApiUserInfoWithoutPassword>>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return null;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="userName"></param>
        /// <param name="password"></param>
        /// <param name="group"></param>
        /// <param name="permissions"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool NewDatabaseUser(string userName, string password, string group, List<string> permissions, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiUserInfo login = new WebApiUserInfo() { UserName = userName, Password = password, Group = group, Permissions = permissions, Database = database, Id = mLoginId };
                string sval = Post("NewDatabaseUser", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="userName"></param>
        /// <param name="password"></param>
        /// <param name="group"></param>
        /// <param name="permissions"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool UpdateDatabaseUser(string userName, string password, string group, List<string> permissions, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiUserInfo login = new WebApiUserInfo() { UserName = userName, Password = password, Group = group, Permissions = permissions, Database = database, Id = mLoginId };
                string sval = Post("UpdateDatabaseUser", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="userName"></param>
        /// <param name="password"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool ModifyDatabaseUserPassword(string userName, string password, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiUserAndPassword login = new WebApiUserAndPassword() { UserName = userName, Password = password, Database = database, Id = mLoginId };
                string sval = Post("ModifyDatabaseUserPassword", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="userName"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool RemoveDatabaseUser(string userName, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiUserRequest login = new WebApiUserRequest() { UserName = userName, Database = database, Id = mLoginId };
                string sval = Post("RemoveDatabaseUser", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="desc"></param>
        /// <param name="enableWriter"></param>
        /// <param name="superPermission"></param>
        /// <param name="group"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool NewDatabasePermission(string name, string desc, bool enableWriter, bool superPermission, List<string> group, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiNewDatabasePermissionRequest login = new WebApiNewDatabasePermissionRequest() { Name = name, Desc = desc, EnableWrite = enableWriter, SuperPermission = superPermission, Group = group, Database = database, Id = mLoginId };
                string sval = Post("NewDatabasePermission", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="permission"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool RemoveDatabasePermission(string permission, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiRemoveDatabasePermissionRequest login = new WebApiRemoveDatabasePermissionRequest() { Permission = permission, Database = database, Id = mLoginId };
                string sval = Post("RemoveDatabasePermission", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="desc"></param>
        /// <param name="enableWriter"></param>
        /// <param name="superPermission"></param>
        /// <param name="group"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool UpdateDatabasePermission(string name, string desc, bool enableWriter, bool superPermission, List<string> group, string database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiNewDatabasePermissionRequest login = new WebApiNewDatabasePermissionRequest() { Name = name, Desc = desc, EnableWrite = enableWriter, SuperPermission = superPermission, Group = group, Database = database, Id = mLoginId };
                string sval = Post("UpdateDatabasePermission", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        #endregion


        #region System User

        /// <summary>
        /// 
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public bool NewUser(string username, string password)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiNewSystemUserRequest login = new WebApiNewSystemUserRequest() { UserName = username, Password = password, Id = mLoginId };
                string sval = Post("NewUser", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="oldName"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public bool ReNameUser(string oldName, string newName)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiReNameSystemUserRequest login = new WebApiReNameSystemUserRequest() { OldName = oldName, NewName = newName, Id = mLoginId };
                string sval = Post("ReNameUser", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="userName"></param>
        /// <param name="password"></param>
        /// <param name="newpassword"></param>
        /// <returns></returns>
        public bool ModifyPassword(string userName, string password, string newpassword)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiModifySystemUserPasswordRequest login = new WebApiModifySystemUserPasswordRequest() { UserName = userName, Password = password, NewPassword = newpassword, Id = mLoginId };
                string sval = Post("ModifyPassword", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="userName"></param>
        /// <param name="isAdmin"></param>
        /// <param name="newDatabasePerssion"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool UpdateUser(string userName, bool isAdmin, bool newDatabasePerssion, List<string> database)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiUpdateSystemUserRequest login = new WebApiUpdateSystemUserRequest() { UserName = userName, IsAdmin = isAdmin, NewDatabasePermission = newDatabasePerssion, Databases = database, Id = mLoginId };
                string sval = Post("ModifyPassword", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public bool UpdateUserPassword(string username, string password)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiNewSystemUserRequest login = new WebApiNewSystemUserRequest() { UserName = username, Password = password, Id = mLoginId };
                string sval = Post("UpdateUserPassword", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<WebApiSystemUserItem> GetUsers()
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return null;
            }
            try
            {
                RequestBase login = new RequestBase() { Id = mLoginId };
                string sval = Post("GetUsers", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<List<WebApiSystemUserItem>>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return null;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="userName"></param>
        /// <returns></returns>
        public bool RemoveUser(string userName)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return false;
            }
            try
            {
                WebApiRemoveSystemUserRequest login = new WebApiRemoveSystemUserRequest() { UserName = userName, Id = mLoginId };
                string sval = Post("RemoveUser", JsonConvert.SerializeObject(login));

                var result = JsonConvert.DeserializeObject<ResultResponse<bool>>(sval);
                if (result.HasErro)
                {
                    LastErroMessage = result.ErroMsg;
                    return false;
                }
                else
                {
                    return result.Result;
                }
            }
            catch (Exception ex)
            {
                LastErroMessage = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tagids"></param>
        /// <param name="startId"></param>
        /// <returns></returns>
        public Dictionary<int, int> ResetTagIds(string database, List<int> tagids,int startId)
        {
            if (string.IsNullOrEmpty(mLoginId))
            {
                LastErroMessage = "未登录";
                return null;
            }
            Dictionary<int, int> re = new Dictionary<int, int>();
            WebApiResetTagIdsRequest login = new WebApiResetTagIdsRequest() { Database = database, TagIds = tagids, StartId = startId,Id=mLoginId };

            string sval = Post("ResetTagId", JsonConvert.SerializeObject(login));
            var result = JsonConvert.DeserializeObject<ResultResponse<List<IntKeyValue>>>(sval);

            if(result.HasErro)
            {
                LastErroMessage = result.ErroMsg;
                return null;
            }
            else
            {
                foreach(var vv in result.Result)
                {
                    re.Add(vv.Key, vv.Value);
                }
            }
            return re;
        }

        #endregion

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
