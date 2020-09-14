using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cdy.Tag;
using Microsoft.AspNetCore.Mvc;

/// <summary>
/// 
/// </summary>
namespace DBDevelopService.Controllers
{
    [Route("[controller]/[action]")]
    public class DevelopServerController : Controller
    {
        /// <summary>
        /// 
        /// </summary>
        public const int PageCount = 500;

        // POST api/<controller>
        [HttpPost]
        public object Login([FromBody]LoginMessage value)
        {
            return new ResultResponse() { Result = SecurityManager.Manager.Login(value.UserName, value.Password) };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mLoginId"></param>
        /// <returns></returns>
        [HttpPost]
        public object Logout([FromBody]RequestBase request)
        {
            SecurityManager.Manager.Logout(request.Id);
            return new ResultResponse() { Result = true };
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private bool CheckLoginId(string id, string database = "")
        {
            return SecurityManager.Manager.CheckKeyAvaiable(id) && (SecurityManager.Manager.CheckDatabase(id, database) || SecurityManager.Manager.IsAdmin(id));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private bool IsAdmin(string id)
        {
            return SecurityManager.Manager.CheckKeyAvaiable(id) && SecurityManager.Manager.IsAdmin(id);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private bool HasNewDatabasePermission(string id)
        {
            return SecurityManager.Manager.CheckKeyAvaiable(id) && SecurityManager.Manager.HasNewDatabasePermission(id);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private bool HasDeleteDatabasePerssion(string id)
        {
            return SecurityManager.Manager.CheckKeyAvaiable(id) && SecurityManager.Manager.HasDeleteDatabasePermission(id);
        }

        #region database

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object NewDatabase([FromBody] WebApiNewDatabaseRequest request)
        {
            if (!IsAdmin(request.Id) && !HasNewDatabasePermission(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足",HasErro=true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                return new ResultResponse() { ErroMsg = "数据库已经存在!",HasErro=true };
            }
            else
            {
                DbManager.Instance.NewDB(request.Database, request.Desc);
                var user = SecurityManager.Manager.GetUser(request.Id);

                user.Databases.Add(request.Database);
                return new ResultResponse() { Result = true };
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object QueryDatabase([FromBody] RequestBase request)
        {
            if (!CheckLoginId(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            List<Database> re = new List<Database>();
            foreach (var vv in DbManager.Instance.ListDatabase())
            {
                re.Add(new Database() { Name = vv, Desc = DbManager.Instance.GetDatabase(vv).Desc });
            }
            return new ResultResponse() { Result = re };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public  object Start([FromBody] WebApiDatabaseRequest request)
        {
            if (!IsAdmin(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足",HasErro=true };
            }
            return new ResultResponse() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().Start(request.Database) };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object Stop([FromBody] WebApiDatabaseRequest request)
        {
            if (!IsAdmin(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足",HasErro=true };
            }
            return new ResultResponse() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().Stop(request.Database) };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object ReRun(WebApiDatabaseRequest request)
        {
            if (!IsAdmin(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            return new ResultResponse() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().Rerun(request.Database) };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object IsDatabaseRunning(WebApiDatabaseRequest request)
        {
            if (!SecurityManager.Manager.CheckKeyAvaiable(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            return new ResultResponse() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().IsRunning(request.Database) };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object IsDatabaseDirty(WebApiDatabaseRequest request)
        {
            if (!SecurityManager.Manager.CheckKeyAvaiable(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            return new ResultResponse() { Result = db.IsDirty };
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetTagGroup([FromBody] WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            List<TagGroup> re = new List<TagGroup>();

            var db = DbManager.Instance.GetDatabase(request.Database);

            if (db != null && db.RealDatabase != null && db.RealDatabase.Groups != null)
            {
                lock (db)
                {
                    foreach (var vv in db.RealDatabase.Groups)
                    {
                        re.Add(new TagGroup() { Name = vv.Key, Parent = vv.Value.Parent != null ? vv.Value.Parent.FullName : "" });
                    }
                }
            }
            return new ResultResponse() { Result = re };
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        [HttpPost]
        public object AddTagGroup(WebApiAddGroupRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            string name = request.Name;
            string parentName = request.ParentName;
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                {
                    var vtg = db.RealDatabase.Groups.ContainsKey(request.ParentName) ? db.RealDatabase.Groups[request.ParentName] : null;

                    int i = 1;
                    while (db.RealDatabase.HasChildGroup(vtg, name))
                    {
                        name = request.Name + i;
                        i++;
                    }

                    string ntmp = name;

                    if (!string.IsNullOrEmpty(parentName))
                    {
                        name = parentName + "." + name;
                    }

                    db.RealDatabase.CheckAndAddGroup(name);
                    return new ResultResponse() { Result = ntmp };
                }
            }
            return new ResultResponse() { ErroMsg = "数据库不存在", HasErro = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object RemoveTagGroup(WebApiRemoveGroupRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                    db.RealDatabase.RemoveGroup(request.FullName);
                return new ResultResponse() { Result=true };
            }
            return new ResultResponse() { ErroMsg = "数据库不存在", HasErro = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object RenameTagGroup(WebApiRenameGroupRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                {
                    var re = db.RealDatabase.ChangeGroupName(request.OldFullName, request.Name);
                    return new ResultResponse() { Result = re };
                }
            }
            return new ResultResponse() { ErroMsg = "数据库不存在", HasErro = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object MoveTagGroup(WebApiMoveTagGroupRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                    db.RealDatabase.ChangeGroupParent(request.Name, request.OldParentName, request.NewParentName);
                return new ResultResponse() { Result = true };
            }
            return new ResultResponse() { ErroMsg = "数据库不存在", HasErro = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="res"></param>
        /// <param name="vfilters"></param>
        /// <returns></returns>
        private IEnumerable<Tagbase> FilterTags(Cdy.Tag.Database db, IEnumerable<Tagbase> res, Dictionary<string,string> vfilters)
        {
            return res.Where((tag) => {
                var re = true;
                foreach (var vv in vfilters)
                {
                    switch (vv.Key)
                    {
                        case "keyword":
                            bool btmp = false;
                            string[] ss = vv.Value.Split(" ", StringSplitOptions.RemoveEmptyEntries);
                            foreach (var vvv in ss)
                            {
                                btmp |= (tag.Name.Contains(vvv) || tag.Desc.Contains(vvv));
                            }
                            re = re && btmp;
                            //re = re && (tag.Name.Contains(vv.Value) || tag.Desc.Contains(vv.Value));
                            break;
                        case "type":
                            re = re && ((int)tag.Type == int.Parse(vv.Value));
                            break;
                        case "readwritetype":
                            re = re && ((int)tag.ReadWriteType == int.Parse(vv.Value));
                            break;
                        case "recordtype":
                            int ival = int.Parse(vv.Value);
                            if (ival == 3)
                            {
                                re = true;
                            }
                            else
                            {
                                if (db.HisDatabase.HisTags.ContainsKey(tag.Id))
                                {
                                    re = re && ((int)db.HisDatabase.HisTags[tag.Id].Type == ival);
                                }
                                else
                                {
                                    re = false;
                                }
                            }
                            break;
                        case "compresstype":
                            ival = int.Parse(vv.Value);
                            if (ival == -1)
                            {
                                re = re && (db.HisDatabase.HisTags.ContainsKey(tag.Id));
                            }
                            else
                            {
                                if (db.HisDatabase.HisTags.ContainsKey(tag.Id))
                                {
                                    re = re && ((int)db.HisDatabase.HisTags[tag.Id].CompressType == ival);
                                }
                                else
                                {
                                    re = false;
                                }
                            }
                            break;
                        case "linkaddress":
                            re = re && (tag.LinkAddress.Contains(vv.Value));
                            break;


                    }

                }

                return re;

            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetTagByGroup([FromBody] WebApiGetTagByGroupRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);

            List<WebApiTag> re = new List<WebApiTag>();
            int totalpage = 0;
            int total = 0;
            if (db != null)
            {
                lock (db)
                {
                    int from = request.Index * PageCount;
                    var res = db.RealDatabase.ListAllTags().Where(e => e.Group == request.GroupName);

                    if (request.Filters != null && request.Filters.Count > 0)
                    {
                        res = FilterTags(db, res, request.Filters);
                    }

                    total = res.Count();

                    totalpage = total / PageCount;
                    totalpage = total % PageCount > 0 ? totalpage + 1 : totalpage;
                    int cc = 0;
                    foreach (var vv in res)
                    {
                        if (cc >= from && cc < (from + PageCount))
                        {
                            WebApiTag tag = new WebApiTag() { RealTag = WebApiRealTag.CreatFromTagbase(vv) };

                            if (db.HisDatabase.HisTags.ContainsKey(vv.Id))
                            {
                                var vvv = db.HisDatabase.HisTags[vv.Id];
                                tag.HisTag = vvv;
                            }

                            re.Add(tag);
                        }
                        cc++;
                    }
                }
            }
            return new GetTagsByGroupResponse() { Result = re, TotalPages = totalpage };
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object RemoveTag([FromBody] WebApiRemoveTagRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {

                lock (db)
                {
                    foreach (var vv in request.TagIds)
                    {
                        db.HisDatabase.RemoveHisTag(vv);
                        db.RealDatabase.Remove(vv);
                    }
                }
                return new ResultResponse() { Result = true };
            }
            return new ResultResponse() { Result = false };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object AddTag([FromBody] WebApiAddTagRequest request)
        {
            try
            {
                if (!CheckLoginId(request.Id, request.Database))
                {
                    return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
                }
                var db = DbManager.Instance.GetDatabase(request.Database);
                List<int> ids = new List<int>();
                if (db != null)
                {
                    lock (db)
                    {
                        foreach(var vv in request.Tags)
                        {
                            if (vv.RealTag.Id < 0)
                            {
                                db.RealDatabase.Append(vv.RealTag.ConvertToTagbase());
                            }
                            else
                            {
                                db.RealDatabase.AddOrUpdate(vv.RealTag.ConvertToTagbase());
                            }

                            var vtag = vv.HisTag;
                            if (vtag.Id != int.MaxValue)
                            {
                                db.HisDatabase.AddOrUpdate(vv.HisTag);
                            }
                            ids.Add(vv.RealTag.Id);
                        }                       
                        return new ResultResponse() { Result = ids }; ;
                    }
                }
                return new ResultResponse() { ErroMsg = "数据库不存在!",HasErro=true };
            }
            catch (Exception ex)
            {
                return new ResultResponse() { ErroMsg = ex.Message, HasErro = true };
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object UpdateHisTag([FromBody] WebApiTagRequest request)
        {
            try
            {
                if (!CheckLoginId(request.Id, request.Database))
                {
                    return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
                }
                var db = DbManager.Instance.GetDatabase(request.Database);
                if (db != null)
                {
                    lock (db)
                    {
                        db.HisDatabase.AddOrUpdate(request.Tag.HisTag);
                    }
                }
                return new ResultResponse() { Result = true };
            }
            catch (Exception ex)
            {
                return new ResultResponse() { ErroMsg = ex.Message,HasErro=true };
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object UpdateRealTag([FromBody] WebApiTagRequest request)
        {
            try
            {
                if (!CheckLoginId(request.Id, request.Database))
                {
                    return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
                }
                var tag = request.Tag.RealTag;
                var db = DbManager.Instance.GetDatabase(request.Database);

                if (db != null)
                {
                    lock (db)
                    {
                        var vtag = tag.ConvertToTagbase();
                        if (db.RealDatabase.Tags.ContainsKey(tag.Id) && tag.Id > -1)
                        {
                            db.RealDatabase.UpdateById(tag.Id, tag.ConvertToTagbase());
                            return new ResultResponse() { Result = true };
                        }
                        else if (db.RealDatabase.NamedTags.ContainsKey(vtag.FullName))
                        {
                            db.RealDatabase.Update(vtag.FullName, vtag);
                            return new ResultResponse() { Result = true };
                        }
                        else
                        {
                            return new ResultResponse() { ErroMsg = "变量不存在",HasErro=true };
                        }
                    }
                }
                return new ResultResponse() { Result = false };
            }
            catch (Exception ex)
            {
                return new ResultResponse() { ErroMsg = ex.Message, HasErro = true };
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object Import([FromBody] WebApiImportTagRequest request)
        {
            try
            {
                if (!CheckLoginId(request.Id, request.Database))
                {
                    return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
                }
                var db = DbManager.Instance.GetDatabase(request.Database);

                if (db != null)
                {
                    lock (db)
                    {
                        Cdy.Tag.Tagbase tag = request.Tag.RealTag.ConvertToTagbase();
                        var vtag = request.Tag.HisTag;
                        if (request.Mode == 0)
                        {
                            //修改模式
                            if (db.RealDatabase.NamedTags.ContainsKey(tag.FullName))
                            {
                                var vid = db.RealDatabase.NamedTags[tag.FullName].Id;
                                if (vid == tag.Id)
                                {
                                    db.RealDatabase.UpdateById(tag.Id, tag);
                                }
                                else
                                {
                                    tag.Id = vid;
                                    db.RealDatabase.UpdateById(tag.Id, tag);
                                }
                            }
                            else
                            {
                                db.RealDatabase.Append(tag);
                            }

                          
                            if (vtag != null && vtag.Id != int.MaxValue)
                            {
                                db.HisDatabase.AddOrUpdate(vtag);
                            }
                        }
                        else if (request.Mode == 1)
                        {
                            db.RealDatabase.Add(tag);
                            db.HisDatabase.AddOrUpdate(vtag);
                        }
                        else
                        {
                            //直接添加

                            if (db.RealDatabase.NamedTags.ContainsKey(tag.FullName))
                            {
                                return new ResultResponse() { ErroMsg = "名称重复", HasErro = true };
                                //return Task.FromResult(new ImportTagRequestReplyMessage() { Result = false, ErroMessage = "名称重复" });
                            }

                            db.RealDatabase.Append(tag);
                            if (vtag!=null && vtag.Id != int.MaxValue)
                            {
                                db.HisDatabase.AddOrUpdate(vtag);
                            }
                        }


                        return new ResultResponse() { Result = tag.Id };
                    }
                }
                return new ResultResponse() { ErroMsg = "数据库不存在!", HasErro = true };
            }
            catch (Exception ex)
            {
                return new ResultResponse() { ErroMsg = ex.Message, HasErro = true };
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object Save(WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                {
                    Cdy.Tag.DatabaseSerise serise = new Cdy.Tag.DatabaseSerise() { Dbase = db };
                    serise.Save();
                }
            }
            return new ResultResponse() { Result = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object Cancel(WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            lock (DbManager.Instance)
            {
                DbManager.Instance.Reload(request.Database);
            }
            return new ResultResponse() { Result = true };
        }

        #endregion

        #region database user

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object AddDatabaseUserGroup([FromBody] WebApiUserGroupRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var user = new Cdy.Tag.UserGroup() { Name = request.Name };
                var usergroup = db.Security.User.GetUserGroup(request.Parent);
                user.Parent = usergroup;
                db.Security.User.AddUserGroup(user);
            }
            return new ResultResponse() { Result = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetDatabaseUserGroup([FromBody] WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            List<WebApiUserGroup> re = new List<WebApiUserGroup>();
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                foreach (var vgg in db.Security.User.Groups)
                {
                    re.Add(new WebApiUserGroup() { Name = vgg.Value.Name, Parent = vgg.Value.Parent != null ? vgg.Value.Parent.FullName : "" });
                }
            }
            return new ResultResponse() { Result = re };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        [HttpPost]
        public object MoveDatabaseUserGroup([FromBody] WebApiMoveUserGroupRequest request)
        {

            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                string ofname = request.OldParentName + "." + request.Name;
                var pgroup = db.Security.User.GetUserGroup(request.NewParentName);

                var usergroup = db.Security.User.GetUserGroup(ofname);
                if (usergroup != null)
                {
                    db.Security.User.RemoveUserGroup(ofname);
                    usergroup.Parent = pgroup;
                    db.Security.User.AddUserGroup(usergroup);
                }
            }
            return new ResultResponse() { Result = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object RemoveDatabaseUserGroup([FromBody] WebApiRequestByUserGroup request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var usergroup = db.Security.User.GetUserGroup(request.GroupFullName);
                if (usergroup != null)
                {
                    db.Security.User.RemoveUserGroup(usergroup.FullName);
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object RenameDatabaseUserGroup([FromBody] WebApiRenameUserGroupRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var usergroup = db.Security.User.GetUserGroup(request.OldFullName);
                if (usergroup != null)
                {
                    usergroup.Name = request.NewName;
                    string sname = usergroup.FullName;
                    db.Security.User.RemoveUserGroup(request.OldFullName);
                    db.Security.User.AddUserGroup(usergroup);
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetDatabaseUserByGroup([FromBody] WebApiRequestByUserGroup request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }

            List<WebApiUserInfoWithoutPassword> re = new List<WebApiUserInfoWithoutPassword>();
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                foreach (var vgg in db.Security.User.Users.Where(e => e.Value.Group == request.GroupFullName))
                {
                    var user = new WebApiUserInfoWithoutPassword() { UserName = vgg.Value.Name, Group = vgg.Value.Group };
                    user.Permissions.AddRange(vgg.Value.Permissions);
                    re.Add(user);
                }
            }
            return new ResultResponse() { Result = re };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object NewDatabaseUser([FromBody] WebApiUserInfo request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var user = new Cdy.Tag.UserItem() { Name = request.UserName, Password = request.Password, Group = request.Group, Permissions = request.Permissions };
                db.Security.User.AddUser(user);
            }
            return new ResultResponse() { Result = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object UpdateDatabaseUser([FromBody] WebApiUserInfo request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var uss = db.Security.User.Users;
                if (uss.ContainsKey(request.UserName))
                {
                    var user = uss[request.UserName];
                    user.Permissions = request.Permissions;
                    user.Group = request.Group;
                }
                else
                {
                    var user = new Cdy.Tag.UserItem() { Name = request.UserName, Group = request.Group };
                    user.Permissions = request.Permissions;
                    db.Security.User.AddUser(user);
                }
            }
            return new ResultResponse() { Result = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object ModifyDatabaseUserPassword([FromBody] WebApiUserAndPassword request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var uss = db.Security.User.Users;
                if (uss.ContainsKey(request.UserName))
                {
                    uss[request.UserName].Password = request.Password;
                }
            }
            return new ResultResponse() { Result = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        [HttpPost]
        public object RemoveDatabaseUser([FromBody] WebApiUserRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var uss = db.Security.User.Users;
                if (uss.ContainsKey(request.UserName))
                {
                    db.Security.User.RemoveUser(request.UserName);
                }
            }
            return new ResultResponse() { Result = true };
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object NewDatabasePermission([FromBody] WebApiNewDatabasePermissionRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var pers = new Cdy.Tag.UserPermission() { Name = request.Name, Desc = request.Desc, EnableWrite = request.EnableWrite,SuperPermission = request.SuperPermission };
                pers.Group.AddRange(request.Group);
                db.Security.Permission.Add(pers);
            }
            return new ResultResponse() { Result = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object RemoveDatabasePermission([FromBody] WebApiRemoveDatabasePermissionRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                if (db.Security.Permission.Permissions.ContainsKey(request.Permission))
                {
                    db.Security.Permission.Permissions.Remove(request.Permission);
                }
            }
            return new ResultResponse() { Result = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object UpdateDatabasePermission([FromBody] WebApiNewDatabasePermissionRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                if (db.Security.Permission.Permissions.ContainsKey(request.Name))
                {
                    var pp = db.Security.Permission.Permissions[request.Name];
                    pp.Group = request.Group.ToList();
                    pp.EnableWrite = request.EnableWrite;
                    pp.Desc = request.Desc;
                    pp.SuperPermission = request.SuperPermission;
                }
                else
                {
                    var pp = new Cdy.Tag.UserPermission() { Name = request.Name, Desc = request.Desc, EnableWrite = request.EnableWrite,SuperPermission=request.SuperPermission };
                    pp.Group.AddRange(request.Group);
                    db.Security.Permission.Add(pp);
                }
            }
            return new ResultResponse() { Result = true };
        }

        #endregion

        #region System User

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object NewUser([FromBody] WebApiNewSystemUserRequest request)
        {
            if (!IsAdmin(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var user = new User() { Name = request.UserName, Password = request.Password };
            var re = SecurityManager.Manager.Securitys.User.AddUser(user);
            SecurityManager.Manager.Save();
            return new ResultResponse() { Result = re };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object ReNameUser([FromBody] WebApiReNameSystemUserRequest request)
        {
            if (!IsAdmin(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            bool re = SecurityManager.Manager.Securitys.User.RenameUser(request.OldName, request.NewName);
            SecurityManager.Manager.RenameLoginUser(request.OldName, request.NewName);
            SecurityManager.Manager.Save();
            return new ResultResponse() { Result = re };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object ModifyPassword([FromBody] WebApiModifySystemUserPasswordRequest request)
        {
            var userName = SecurityManager.Manager.GetUserName(request.Id);
            if (!SecurityManager.Manager.CheckKeyAvaiable(request.Id))
            {
                if (!(userName == request.UserName && SecurityManager.Manager.CheckPasswordIsCorrect(userName, request.Password)))
                {
                    return Task.FromResult(new BoolResultReplay() { Result = false });
                }
            }
            var user = SecurityManager.Manager.Securitys.User.GetUser(request.UserName);
            if (user != null)
            {
                user.Password = request.NewPassword;
                SecurityManager.Manager.Save();
            }
            return new ResultResponse() { Result = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object UpdateUser([FromBody] WebApiUpdateSystemUserRequest request)
        {
            if (!IsAdmin(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var user = SecurityManager.Manager.Securitys.User.GetUser(request.UserName);
            if (user != null)
            {
                user.IsAdmin = request.IsAdmin;
                user.NewDatabase = request.NewDatabasePermission;
                user.Databases = request.Databases;
                SecurityManager.Manager.Save();
            }

            return new ResultResponse() { Result = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object UpdateUserPassword([FromBody] WebApiNewSystemUserRequest request)
        {
            if (!IsAdmin(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var user = SecurityManager.Manager.Securitys.User.GetUser(request.UserName);
            if (user != null)
            {
                user.Password = request.Password;
                SecurityManager.Manager.Save();
            }
            return new ResultResponse() { Result = true };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetUsers([FromBody] RequestBase request)
        {
            if (!IsAdmin(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }

            List<WebApiSystemUserItem> re = new List<WebApiSystemUserItem>();
            foreach (var vv in SecurityManager.Manager.Securitys.User.Users)
            {
                var user = new WebApiSystemUserItem() { UserName = vv.Value.Name, IsAdmin = vv.Value.IsAdmin, NewDatabase = vv.Value.NewDatabase };
                if (vv.Value.Databases != null)
                    user.Databases.AddRange(vv.Value.Databases);
                re.Add(user);
            }
            return new ResultResponse() { Result = re };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object RemoveUser([FromBody] WebApiRemoveSystemUserRequest request)
        {
            if (!IsAdmin(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var user = SecurityManager.Manager.Securitys.User.GetUser(request.UserName);
            if (user != null)
            {
                SecurityManager.Manager.Securitys.User.RemoveUser(request.UserName);
                SecurityManager.Manager.Save();
            }
            return new ResultResponse() { Result = true };
        }

        #endregion
    }
}
