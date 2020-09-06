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
        public object NewDatabase([FromBody] NewDatabaseRequest request)
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
        public object QueryDatabase([FromBody]RequestBase request)
        {
            if (!CheckLoginId(request.Id))
            {
                return new ResultResponse() { ErroMsg = "权限不足",HasErro=true };
            }
            List<Database> re = new List<Database>();
            foreach (var vv in DbManager.Instance.ListDatabase())
            {
                re.Add(new Database(){Name = vv, Desc = DbManager.Instance.GetDatabase(vv).Desc });
            }
            return re;
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
            return re;
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
        public  object GetTagByGroup([FromBody] GetTagByGroupRequest request)
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

                    if (request.Filters.Count > 0)
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
                            WebApiTag tag = new WebApiTag() { RealTag = vv };

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
            return re;
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
                                db.RealDatabase.Append(vv.RealTag);
                            }
                            else
                            {
                                db.RealDatabase.AddOrUpdate(vv.RealTag);
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
                        if (db.RealDatabase.Tags.ContainsKey(tag.Id) && tag.Id > -1)
                        {
                            db.RealDatabase.UpdateById(tag.Id, tag);
                            return new ResultResponse() { Result = true };
                        }
                        else if (db.RealDatabase.NamedTags.ContainsKey(tag.FullName))
                        {
                            db.RealDatabase.Update(tag.FullName, tag);
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
                        Cdy.Tag.Tagbase tag = request.Tag.RealTag;
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
    }
}
