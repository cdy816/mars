using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cdy.Tag;
using Cdy.Tag.Common.Common;
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

        /// <summary>
        /// 登录
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        // POST api/<controller>
        [HttpPost]
        public object Login([FromBody]LoginMessage value)
        {
            return new ResultResponse() { Result = SecurityManager.Manager.Login(value.UserName, value.Password) };
        }

        /// <summary>
        /// 登出
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
            return SecurityManager.Manager.CheckKeyAvaiable(id) && (SecurityManager.Manager.HasNewDatabasePermission(id)|| SecurityManager.Manager.IsAdmin(id));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private bool HasDeleteDatabasePerssion(string id)
        {
            return SecurityManager.Manager.CheckKeyAvaiable(id) && (SecurityManager.Manager.HasDeleteDatabasePermission(id)|| SecurityManager.Manager.IsAdmin(id));
        }

        #region database

        /// <summary>
        /// 新建数据库
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object NewDatabase([FromBody] WebApiNewDatabaseRequest request)
        {
            if (!HasNewDatabasePermission(request.Id))
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
        /// 删除数据库
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object RemoveDatabase([FromBody] WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database) || !HasDeleteDatabasePerssion(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                if (!ServiceLocator.Locator.Resolve<IDatabaseManager>().IsRunning(request.Database))
                {
                    var re = DbManager.Instance.RemoveDB(request.Database);
                    if (re)
                    {
                        return new ResultResponse() { ErroMsg = "删除成功!", HasErro = false };
                    }
                    else
                    {
                        return new ResultResponse() { ErroMsg = "删除失败!", HasErro = true };
                    }
                }
                else
                {
                    return new ResultResponse() { ErroMsg = "数据库正在运行，请先停止运行!", HasErro = true };
                }
            }
            else
            {
                return new ResultResponse() { ErroMsg = "数据库不经存在!", HasErro = true };
            }
        }

        /// <summary>
        /// 枚举数据库
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
            var user = SecurityManager.Manager.GetUser(request.Id);
            foreach (var vv in DbManager.Instance.ListDatabase())
            {
                if (CheckLoginId(request.Id,vv))
                {
                    re.Add(new Database() { Name = vv, Desc = DbManager.Instance.GetDatabase(vv).Desc });
                }
            }
            return new ResultResponse() { Result = re };
        }

        /// <summary>
        /// 检查数据库是否打开
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object CheckOpenDatabase([FromBody] WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if(db!=null)
            {
                DbManager.Instance.CheckAndContinueLoadDatabase(db);
                return new ResultResponse() { Result = true};
            }
            else
            {
                return new ResultResponse() { Result = false, ErroMsg = "数据库不存在", HasErro = true};
            }          
        }

        /// <summary>
        /// 运行数据库
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public  object Start([FromBody] WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id,request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足",HasErro=true };
            }
            return new ResultResponse() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().Start(request.Database) };
        }

        /// <summary>
        /// 停止运行
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object Stop([FromBody] WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足",HasErro=true };
            }
            return new ResultResponse() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().Stop(request.Database) };
        }

        /// <summary>
        /// 重新运行
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object ReRun([FromBody] WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            return new ResultResponse() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().Rerun(request.Database) };
        }

        /// <summary>
        /// 数据库是否运行
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object IsDatabaseRunning([FromBody] WebApiDatabaseRequest request)
        {
            if (!SecurityManager.Manager.CheckKeyAvaiable(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            return new ResultResponse() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().IsRunning(request.Database) };
        }

        /// <summary>
        /// 数据库是否变脏
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object IsDatabaseDirty([FromBody] WebApiDatabaseRequest request)
        {
            if (!SecurityManager.Manager.CheckKeyAvaiable(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                return new ResultResponse() { Result = db.IsDirty };
            }
            else
            {
                return new ResultResponse() { Result = false,ErroMsg="数据库不存在" };
            }
        }


        /// <summary>
        /// 获取变量组
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetTagGroup([FromBody] WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id,request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            List<TagGroup> re = new List<TagGroup>();

            var db = DbManager.Instance.GetDatabase(request.Database);

            if(db!=null&&db.RealDatabase==null)
            {
                DbManager.Instance.CheckAndContinueLoadDatabase(db);
            }

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
        /// 添加变量组
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        [HttpPost]
        public object AddTagGroup([FromBody] WebApiAddGroupRequest request)
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    var vtg = db.RealDatabase.Groups.ContainsKey(request.ParentName) ? db.RealDatabase.Groups[request.ParentName] : null;

                    if(db.RealDatabase.HasChildGroup(vtg, name))
                    {
                        return new ResultResponse() { ErroMsg = "变量组重复", HasErro = true,Result=false };
                    }
                    //int i = 1;
                    //while (db.RealDatabase.HasChildGroup(vtg, name))
                    //{
                    //    name = request.Name + i;
                    //    i++;
                    //}

                    //string ntmp = name;

                    if (!string.IsNullOrEmpty(parentName))
                    {
                        name = parentName + "." + name;
                    }

                    db.RealDatabase.CheckAndAddGroup(name);
                    return new ResultResponse() { Result = true };
                }
            }
            return new ResultResponse() { ErroMsg = "数据库不存在", HasErro = true,Result=false };
        }

        /// <summary>
        /// 修改变量组描述
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object  UpdateGroupDescription([FromBody] WebApiUpdateGroupDescriptionRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                DbManager.Instance.CheckAndContinueLoadDatabase(db);
                var vtg = db.RealDatabase.Groups.ContainsKey(request.GroupName) ? db.RealDatabase.Groups[request.GroupName] : null;
                if (vtg != null)
                {
                    vtg.Description = request.Desc;
                }
                return Task.FromResult(new BoolResultReplay() { Result = true });
            }
            return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = "database not exist!" });
        }

        /// <summary>
        /// 删除变量组
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object RemoveTagGroup([FromBody] WebApiRemoveGroupRequest request)
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    db.RealDatabase.RemoveGroup(request.FullName);
                }
                return new ResultResponse() { Result=true };
            }
            return new ResultResponse() { ErroMsg = "数据库不存在", HasErro = true };
        }

        /// <summary>
        /// 变量组重命名
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object RenameTagGroup([FromBody] WebApiRenameGroupRequest request)
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    var re = db.RealDatabase.ChangeGroupName(request.OldFullName, request.Name);
                    return new ResultResponse() { Result = re };
                }
            }
            return new ResultResponse() { ErroMsg = "数据库不存在", HasErro = true };
        }

        /// <summary>
        /// 移动变量组
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object MoveTagGroup([FromBody] WebApiMoveTagGroupRequest request)
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    db.RealDatabase.ChangeGroupParent(request.Name, request.OldParentName, request.NewParentName);
                }
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
                                btmp |= (tag.FullName.Contains(vvv) || tag.Desc.Contains(vvv));
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
        /// 获取所有变量的ID和名称
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetAllTagNames([FromBody] WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if(db!=null)
            {
                lock (db)
                {
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    Dictionary<int, string> tags = new Dictionary<int, string>();
                    foreach (var vtag in db.RealDatabase.Tags)
                    {
                        tags.Add(vtag.Key, vtag.Value.FullName);
                    }
                    return new ResultResponse() { Result = tags };
                }
            }
            return new ResultResponse() { ErroMsg = "数据库不存在", HasErro = true };
        }

        /// <summary>
        /// 获取某个变量所有变量的名称
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetAllTagNamesByGroup([FromBody] WebApiGetTagByGroupRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            ResultResponse rr = new ResultResponse();
            if (db!=null)
            {
                lock(db)
                {
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    Dictionary<int, string> tags = new Dictionary<int, string>();
                    var res = db.RealDatabase.ListAllTags().Where(e => e.Group == request.GroupName);
                    if (res != null && res.Count() > 0)
                    {
                        foreach (var vtag in res)
                        {
                            tags.Add(vtag.Id, vtag.Name);
                        }
                    }
                    rr.Result = tags;
                    return rr;
                }
            }
            return new ResultResponse() { ErroMsg = "数据库不存在", HasErro = true };
        }


        /// <summary>
        /// 获取组内的变量
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
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
                            if(!string.IsNullOrEmpty(vv.Parent))
                            {
                                if(vv.Group!=null && vv.FullName.StartsWith(vv.Group+"."))
                                {
                                    tag.RealTag.Name = vv.FullName.Substring(vv.Group.Length+1);
                                }
                                else
                                {
                                    tag.RealTag.Name = vv.FullName;
                                }
                                
                            }

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
        /// 获取组内的所有变量
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetAllTagByGroup([FromBody] WebApiGetTagByGroupRequest request)
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    List<WebApiTag> re = new List<WebApiTag>();
                    var res = db.RealDatabase.ListAllTags().Where(e => e.Group == request.GroupName);
                    foreach (var vv in res)
                    {
                        WebApiTag tag = new WebApiTag() { RealTag = WebApiRealTag.CreatFromTagbase(vv) };

                        if (db.HisDatabase.HisTags.ContainsKey(vv.Id))
                        {
                            var vvv = db.HisDatabase.HisTags[vv.Id];
                            tag.HisTag = vvv;
                        }

                        re.Add(tag);
                    }
                    return new ResultResponse() { Result = re };
                }
            }
            return new ResultResponse() { ErroMsg = "数据库不存在", HasErro = true };
        }


        /// <summary>
        /// 删除变量
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
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
        /// 添加变量
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
                        DbManager.Instance.CheckAndContinueLoadDatabase(db);
                        foreach (var vv in request.Tags)
                        {
                            var vrtag = vv.RealTag.ConvertToTagbase();
                            if (vv.RealTag.Id < 0)
                            {
                                if (!db.RealDatabase.NamedTags.ContainsKey(vrtag.FullName))
                                {
                                    db.RealDatabase.Append(vrtag);
                                }
                            }
                            else
                            {
                                db.RealDatabase.AddOrUpdate(vrtag);
                            }

                            var vtag = vv.HisTag;
                            vtag.Id = vrtag.Id;

                            if (vtag.Id != int.MaxValue && vtag.Id>-1)
                            {
                                db.HisDatabase.AddOrUpdate(vtag);
                            }
                            ids.Add(vrtag.Id);
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
        /// 更新历史变量
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
                        DbManager.Instance.CheckAndContinueLoadDatabase(db);
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
        /// 更新实时变量
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
                        DbManager.Instance.CheckAndContinueLoadDatabase(db);
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
        /// 导入
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
                        DbManager.Instance.CheckAndContinueLoadDatabase(db);
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
        /// 保存
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object Save([FromBody] WebApiDatabaseRequest request)
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    Cdy.Tag.DatabaseSerise serise = new Cdy.Tag.DatabaseSerise() { Dbase = db };
                    serise.Save();
                }
            }
            return new ResultResponse() { Result = true };
        }

        /// <summary>
        /// 撤销更改
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object Cancel([FromBody] WebApiDatabaseRequest request)
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

        /// <summary>
        /// 获取实时数据服务端口
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetRealDataServerPortRequest([FromBody] WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if(db!=null)
            {
                return new RealDataServerPort() { Port = db.Setting.RealDataServerPort };
            }
            return new ResultResponse() { Result = false };
        }

        /// <summary>
        /// 更新实时数据服务端口
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object UpdateRealDataServerPortRequest([FromBody] WebApiUpdateRealDataServerPortRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                db.Setting.RealDataServerPort = request.Port;
                return new ResultResponse() { Result = true };
            }
            return new ResultResponse() { Result = false };
        }

        /// <summary>
        /// 获取历史数据配置
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetDatabaseHisSetting([FromBody] WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                return new HisSetting() { DataPath = db.HisDatabase.Setting.HisDataPathPrimary, DataPathBackup = db.HisDatabase.Setting.HisDataPathBack, KeepTimeInDataPath = db.HisDatabase.Setting.HisDataKeepTimeInPrimaryPath };
            }
            return new ResultResponse() { Result = false };
        }

        /// <summary>
        /// 更新历史数据配置
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object UpdateDatabaseHisSetting([FromBody] WebApiHisSettingUpdateRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                db.HisDatabase.Setting.HisDataPathPrimary = request.DataPath;
                db.HisDatabase.Setting.HisDataPathBack = request.DataPathBackup;
                db.HisDatabase.Setting.HisDataKeepTimeInPrimaryPath = request.KeepTimeInDataPath;
                db.HisDatabase.Setting.KeepNoZipFileDays = request.KeepNoZipFileDays;
                return new ResultResponse() { Result = true };
            }
            return new ResultResponse() { Result = false };
        }

        /// <summary>
        /// 获取Proxy接口使能状态
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetDatabaseProxyApiSetting([FromBody] WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                return new ProxyApiResponse() { EnableGrpcApi=db.Setting.EnableGrpcApi,EnableHighApi=db.Setting.EnableHighApi,EnableOpcServer=db.Setting.EnableOpcServer,EnableWebApi=db.Setting.EnableWebApi  };
            }
            return new ResultResponse() { Result = false };
        }


        /// <summary>
        /// 使能Proxy接口
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object UpdateDatabaseProxyApiSetting([FromBody] WebApiProxyApiUpdateRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                db.Setting.EnableGrpcApi = request.EnableGrpcApi;
                db.Setting.EnableHighApi = request.EnableHighApi;
                db.Setting.EnableOpcServer = request.EnableOpcServer;
                db.Setting.EnableWebApi = request.EnableWebApi;
                return new ResultResponse() { Result = true };
            }
            return new ResultResponse() { Result = false };
        }

        /// <summary>
        /// 获取驱动配置参数
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetDriverSetting([FromBody] WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new WebApiGetDriverSettingResponse() { ErroMsg = "权限不足", HasErro = true, Result = false };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                WebApiGetDriverSettingResponse res = new WebApiGetDriverSettingResponse() { Result = true };
                foreach (var vd in Cdy.Tag.DriverManager.Manager.Drivers)
                {
                    
                    var config = vd.Value.GetConfig(request.Database);
                    if (config != null)
                    {
                        StringBuilder sb = new StringBuilder();
                        foreach (var vv in config)
                        {
                            sb.Append(vv.Key + ":" + vv.Value + ",");
                        }
                        sb.Length = sb.Length > 0 ? sb.Length - 1 : sb.Length;
                        res.Settings.Add(vd.Key, sb.ToString());
                    }
                }
              return res;
            }
            return new WebApiGetDriverSettingResponse() { Result = false,HasErro=true,ErroMsg="数据库不存在" };
        }

        /// <summary>
        /// 更新Driver配置参数
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object UpdateDatabaseDriverSetting([FromBody] WebApiUpdateDriverSettingRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                foreach(var vv in request.Settings)
                {
                    if (DriverManager.Manager.Drivers.ContainsKey(vv.Key))
                    {
                        var dd = DriverManager.Manager.Drivers[vv.Key];
                        string sval = vv.Value;
                        if (!string.IsNullOrEmpty(sval))
                        {
                            Dictionary<string, string> dtmp = new Dictionary<string, string>();
                            string[] ss = sval.Split(new char[] { ',' });
                            foreach (var vvv in ss)
                            {
                                string[] svv = vvv.Split(new char[] { ':' });
                                if (!dtmp.ContainsKey(svv[0]))
                                {
                                    dtmp.Add(svv[0], svv[1]);
                                }
                            }
                            dd.UpdateConfig(request.Database, dtmp);
                        }
                    }
                }
                return new ResultResponse() { Result = true };
            }
            return new ResultResponse() { Result = false };
        }

        #endregion

        #region database user

        /// <summary>
        /// 添加数据库用户组
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
        /// 获取数据库用户组
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
        /// 移动数据库用户组
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
        /// 删除数据库用户组
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
        /// 重命名用户组
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
        /// 获取数据库用户组内的用户
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
        /// 新建数据库用户
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
        /// 更新数据库用户
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
        /// <param name="user"></param>
        /// <returns></returns>
        private bool IsApiUser(string user)
        {
            return user == "Admin";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        private void SaveApiUser(string database,string user,string password)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(user + "," + password);
            string spath = System.IO.Path.Combine(PathHelper.helper.DataPath, database, "ApiUser.sdb");
            System.IO.File.WriteAllText(spath, Md5Helper.Encode(sb.ToString()));
        }

        /// <summary>
        /// 修改数据库用户密码
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
                    if(IsApiUser(request.UserName))
                    {
                        SaveApiUser(db.Name,request.UserName, request.Password);
                    }
                }
            }
            return new ResultResponse() { Result = true };
        }

        /// <summary>
        /// 删除数据库用户
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
        /// 新建数据库权限
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
        /// 删除数据库权限
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
        /// 更新数据库权限
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

        /// <summary>
        /// 获取数据库的权限配置
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetAllDatabasePermission([FromBody] WebApiDatabaseRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            List<UserPermission> re = new List<UserPermission>();
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null && db.Security != null && db.Security.Permission != null)
            {
                re.AddRange(db.Security.Permission.Permissions.Values);
            }
            return new ResultResponse() { Result = re };
        }

        #endregion

        #region System User

        /// <summary>
        /// 新建开发服务器用户
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
        /// 重命名开发用户
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
        /// 修改开发用户密码
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
        /// 更新开发用户
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
        /// 更新开发用户密码
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
        /// 枚举开发用户
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
        /// 获取当前登录账户的配置信息
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object GetCurrentUserConfig([FromBody] RequestBase request)
        {
            var user= SecurityManager.Manager.GetUser(request.Id);
            if (user != null)
            {
                return new ResultResponse() { Result = new WebApiSystemUserItem() { UserName = user.Name, IsAdmin = user.IsAdmin, NewDatabase = user.NewDatabase,Databases=user.Databases } };
            }
            else
            {
                return new ResultResponse() { ErroMsg = "未找到用户", HasErro = true };
            }
        }

        /// <summary>
        /// 删除开发用户
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

        /// <summary>
        /// 重置变量ID
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost]
        public object ResetTagId([FromBody] WebApiResetTagIdsRequest request)
        {
            if (!CheckLoginId(request.Id, request.Database))
            {
                return new ResultResponse() { ErroMsg = "权限不足", HasErro = true };
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                Dictionary<int, int> res = new Dictionary<int, int>();
                foreach (var vv in request.TagIds.OrderBy(e => e))
                {
                    bool ishase = false;

                    int endindex = vv < request.StartId ? int.MaxValue : vv;

                    for (int i = request.StartId; i < endindex; i++)
                    {
                        if (!db.RealDatabase.Tags.ContainsKey(i))
                        {
                            ishase = true;
                            var rtag = db.RealDatabase.Tags[vv];
                            db.RealDatabase.Tags.Remove(vv);

                            rtag.Id = i;
                            db.RealDatabase.Tags.Add(i, rtag);

                            if (db.HisDatabase.HisTags.ContainsKey(vv))
                            {
                                var htag = db.HisDatabase.HisTags[vv];
                                db.HisDatabase.HisTags.Remove(vv);
                                htag.Id = i;
                                db.HisDatabase.HisTags.Add(i, htag);
                            }
                            res.Add(vv, i);
                            break;
                        }
                    }
                    if (!ishase)
                        res.Add(vv, vv);
                }
                return new ResultResponse() { Result = res.Select(e=>new IntKeyValue() { Key = e.Key, Value = e.Value }).ToList() };
            }
            return new ResultResponse() { ErroMsg = "数据库不存在", HasErro = true };
        }

        #endregion
    }
}
