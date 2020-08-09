using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Cdy.Tag;
namespace DBDevelopService
{
    public class DevelopServerService : DevelopServer.DevelopServerBase
    {
        public const int PageCount = 500;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<LoginReply> Login(LoginRequest request, ServerCallContext context)
        {
            LoginReply re = new LoginReply() { LoginId = SecurityManager.Manager.Login(request.UserName,request.Password) };
            return Task.FromResult(re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> IsAdmin(GetRequest request, ServerCallContext context)
        {
            if (IsAdmin(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = true });
            }
            else
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> CanNewDatabase(GetRequest request, ServerCallContext context)
        {
            if (HasNewDatabasePermission(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = true });
            }
            else
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> Logout(LogoutRequest request, ServerCallContext context)
        {
            SecurityManager.Manager.Logout(request.LoginId);
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private bool CheckLoginId(string id,string database="")
        {
            return SecurityManager.Manager.CheckKeyAvaiable(id) && (SecurityManager.Manager.CheckDatabase(id, database)|| SecurityManager.Manager.IsAdmin(id));
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


        #region Database User

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetAllDatabasePermissionReplay> GetAllDatabasePermission(GetAllDatabasePermissionRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId,request.Database))
            {
                return Task.FromResult(new GetAllDatabasePermissionReplay() { Result = false });
            }

            GetAllDatabasePermissionReplay re = new GetAllDatabasePermissionReplay() { Result = true };
            List<DatabasePermission> pers = new List<DatabasePermission>();
            var db = DbManager.Instance.GetDatabase(request.Database);
            if(db!=null)
            {
                if (db.Security != null && db.Security.Permission != null)
                {
                    foreach (var vv in db.Security.Permission.Permissions)
                    {
                        var dd = new DatabasePermission() { Name = vv.Value.Name, Desc = vv.Value.Desc, EnableWrite = vv.Value.EnableWrite,SuperPermission=vv.Value.SuperPermission };
                        dd.Group.AddRange(vv.Value.Group);
                        pers.Add(dd);
                    }
                }
            }
            re.Permission.AddRange(pers);
            return Task.FromResult(re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> ModifyDatabaseUserPassword(ModifyDatabaseUserPasswordRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                 var uss = db.Security.User.Users;
                if(uss.ContainsKey(request.UserName))
                {
                    uss[request.UserName].Password = request.Password;
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> NewDatabasePermission(DatabasePermissionRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var pers = new Cdy.Tag.UserPermission() { Name = request.Permission.Name, Desc = request.Permission.Desc, EnableWrite = request.Permission.EnableWrite};
                pers.Group.AddRange(request.Permission.Group);
                db.Security.Permission.Add(pers);
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> NewDatabaseUser(NewDatabaseUserRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var user = new Cdy.Tag.UserItem() { Name = request.UserName, Password = request.Password,Group = request.Group, Permissions = request.Permission.ToList() };
                db.Security.User.AddUser(user);
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> UpdateDatabasePermission(DatabasePermissionRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                if(db.Security.Permission.Permissions.ContainsKey(request.Permission.Name))
                {
                    var pp = db.Security.Permission.Permissions[request.Permission.Name];
                    pp.Group = request.Permission.Group.ToList();
                    pp.EnableWrite = request.Permission.EnableWrite;
                    pp.Desc = request.Permission.Desc;
                }
                else
                {
                    var pp = new Cdy.Tag.UserPermission() { Name = request.Permission.Name, Desc = request.Permission.Desc, EnableWrite = request.Permission.EnableWrite };
                    pp.Group.AddRange(request.Permission.Group);
                    db.Security.Permission.Add(pp);
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> RemoveDatabasePermission(RemoveDatabasePermissionRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                if (db.Security.Permission.Permissions.ContainsKey(request.Permission))
                {
                    db.Security.Permission.Permissions.Remove(request.Permission);
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> UpdateDatabaseUser(UpdateDatabaseUserRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var uss = db.Security.User.Users;
                if (uss.ContainsKey(request.UserName))
                {
                    var user = uss[request.UserName];
                    user.Permissions = request.Permission.ToList();
                    user.Group = request.Group;
                }
                else
                {
                    var user = new Cdy.Tag.UserItem() { Name = request.UserName,Group=request.Group };
                    user.Permissions = request.Permission.ToList();
                    db.Security.User.AddUser(user);
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> RemoveDatabaseUser(RemoveByNameRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var uss = db.Security.User.Users;
                if (uss.ContainsKey(request.Name))
                {
                    db.Security.User.RemoveUser(request.Name);
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> AddDatabaseUserGroup(AddGroupRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var user = new Cdy.Tag.UserGroup() { Name = request.Name };
                var usergroup = db.Security.User.GetUserGroup(request.ParentName);
                user.Parent = usergroup;
                db.Security.User.AddUserGroup(user);
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetGroupMessageReply> GetDatabaseUserGroup(GetRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetGroupMessageReply() { Result = false });
            }
            GetGroupMessageReply re = new GetGroupMessageReply();
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                foreach(var vgg in  db.Security.User.Groups)
                {
                    re.Group.Add(new Group() { Name = vgg.Value.Name, Parent = vgg.Value.Parent != null ? vgg.Value.Parent.FullName : "" });
                }
            }
            return Task.FromResult(re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetDatabaseUsersReplay> GetDatabaseUserByGroup(GetDatabaseUserByGroupRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetDatabaseUsersReplay() { Result = false });
            }

            GetDatabaseUsersReplay re = new GetDatabaseUsersReplay();
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                foreach (var vgg in db.Security.User.Users.Where(e=>e.Value.Group == request.Group))
                {
                    var user = new DatabaseUserMessage() { UserName = vgg.Value.Name, Group = vgg.Value.Group };
                    user.Permission.AddRange(vgg.Value.Permissions);
                    re.Users.Add(user);
                }
            }
            return Task.FromResult(re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> MoveDatabaseUserGroup(MoveGroupRequest request, ServerCallContext context)
        {

            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
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
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> RemoveDatabaseUserGroup(RemoveGroupRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var usergroup = db.Security.User.GetUserGroup(request.Name);
                if(usergroup!=null)
                {
                    db.Security.User.RemoveUserGroup(usergroup.Name);
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> RenameDatabaseUserGroup(RenameGroupRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
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

        

        #endregion

        #region System user

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> NewUser(NewUserRequest request, ServerCallContext context)
        {
            if (!IsAdmin(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var user = new User() { Name = request.UserName,Password = request.Password };
            SecurityManager.Manager.Securitys.User.AddUser(user);
            SecurityManager.Manager.Save();
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> ReNameUser(ReNameUserRequest request, ServerCallContext context)
        {
            if (!IsAdmin(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            bool re = SecurityManager.Manager.Securitys.User.RenameUser(request.OldName, request.NewName);
            SecurityManager.Manager.RenameLoginUser(request.OldName, request.NewName);
            SecurityManager.Manager.Save();
            return Task.FromResult(new BoolResultReplay() { Result = re });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> ModifyPassword(ModifyPasswordRequest request, ServerCallContext context)
        {
            var userName = SecurityManager.Manager.GetUserName(request.LoginId);
            if (!SecurityManager.Manager.CheckKeyAvaiable(request.LoginId)&& userName != request.UserName && SecurityManager.Manager.CheckPasswordIsCorrect(userName,request.Password))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var user = SecurityManager.Manager.Securitys.User.GetUser(request.UserName);
            if (user != null)
            {
                user.Password = request.Newpassword;
                SecurityManager.Manager.Save();
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> UpdateUser(UpdateUserRequest request, ServerCallContext context)
        {
            if (!IsAdmin(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var user = SecurityManager.Manager.Securitys.User.GetUser(request.UserName);
            if (user != null)
            {
                user.IsAdmin = request.IsAdmin;
                user.NewDatabase = request.NewDatabasePermission;
                user.Databases = request.Database.ToList();
                SecurityManager.Manager.Save();
            }

            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> UpdateUserPassword(UpdatePasswordRequest request, ServerCallContext context)
        {
            if (!IsAdmin(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var user = SecurityManager.Manager.Securitys.User.GetUser(request.UserName);
            if (user != null)
            {
                user.Password = request.Password;
                SecurityManager.Manager.Save();
            }
           
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetUsersReplay> GetUsers(GetRequest request, ServerCallContext context)
        {
            if (!IsAdmin(request.LoginId))
            {
                return Task.FromResult(new GetUsersReplay() { Result = false });
            }

            GetUsersReplay re = new GetUsersReplay() { Result = true };
            foreach (var vv in SecurityManager.Manager.Securitys.User.Users)
            {
                var user = new UserMessage() { UserName = vv.Value.Name,IsAdmin=vv.Value.IsAdmin,NewDatabase=vv.Value.NewDatabase };
                if(vv.Value.Databases!=null)
                user.Databases.AddRange(vv.Value.Databases);
                re.Users.Add(user);
            }
            return Task.FromResult(re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> RemoveUser(RemoveUserRequest request, ServerCallContext context)
        {
            if (!IsAdmin(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var user = SecurityManager.Manager.Securitys.User.GetUser(request.UserName);
            if (user != null)
            {
                SecurityManager.Manager.Securitys.User.RemoveUser(request.UserName);
                SecurityManager.Manager.Save();
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }



        #endregion

        #region database

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> NewDatabase(NewDatabaseRequest request, ServerCallContext context)
        {
            if (!IsAdmin(request.LoginId) && !HasNewDatabasePermission(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if(db!=null)
            {
                return Task.FromResult(new BoolResultReplay() { Result = false,ErroMessage="数据库已经存在!" });
            }
            else
            {
                DbManager.Instance.NewDB(request.Database,request.Desc);
                var user = SecurityManager.Manager.GetUser(request.LoginId);

                user.Databases.Add(request.Database);
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<QueryDatabaseReplay> QueryDatabase(QueryDatabaseRequest request, ServerCallContext context)
        {
            if (!SecurityManager.Manager.CheckKeyAvaiable(request.LoginId))
            {
                return Task.FromResult(new QueryDatabaseReplay() { Result = false });
            }
            

            QueryDatabaseReplay re = new QueryDatabaseReplay() { Result = true };
            var user = SecurityManager.Manager.GetUser(request.LoginId);
            var dbs = user.Databases;
            foreach (var vv in DbManager.Instance.ListDatabase())
            {
                if (dbs.Contains(vv)||user.IsAdmin)
                {
                    re.Database.Add(new KeyValueMessage() { Key = vv, Value = DbManager.Instance.GetDatabase(vv).Desc });
                }
            }
            return Task.FromResult(re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> Start(DatabasesRequest request, ServerCallContext context)
        {
            if (!IsAdmin(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            return Task.FromResult(new BoolResultReplay() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().Start(request.Database) });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> Stop(DatabasesRequest request, ServerCallContext context)
        {
            if (!IsAdmin(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            return Task.FromResult(new BoolResultReplay() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().Stop(request.Database) });
        }

        public override Task<BoolResultReplay> ReRun(DatabasesRequest request, ServerCallContext context)
        {
            if (!IsAdmin(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            return Task.FromResult(new BoolResultReplay() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().Rerun(request.Database) });
        }

        public override Task<BoolResultReplay> IsDatabaseRunning(DatabasesRequest request, ServerCallContext context)
        {
            if (!SecurityManager.Manager.CheckKeyAvaiable(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            return Task.FromResult(new BoolResultReplay() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().IsRunning(request.Database) });
        }

        #endregion

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<AddGroupReplay> AddTagGroup(AddGroupRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new AddGroupReplay() { Result = false });
            }
            string name = request.Name;
            string parentName = request.ParentName;
            var db = DbManager.Instance.GetDatabase(request.Database);
            if(db!=null)
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
                    return Task.FromResult(new AddGroupReplay() { Result = true, Group = ntmp });
                }
            }
            return Task.FromResult(new AddGroupReplay() { Result = false,ErroMessage="database not exist!" });
        }

        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<PasteGroupReplay> PasteTagGroup(PasteGroupRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new PasteGroupReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                {
                    var tags = db.RealDatabase.GetTagsByGroup(request.GroupFullName);
                    var vtg = db.RealDatabase.Groups.ContainsKey(request.TargetParentName) ? db.RealDatabase.Groups[request.TargetParentName] : null;

                    var vsg = db.RealDatabase.Groups.ContainsKey(request.GroupFullName) ? db.RealDatabase.Groups[request.GroupFullName] : null;

                    if (vtg == vsg)
                    {
                        return Task.FromResult(new PasteGroupReplay() { Result = false });
                    }

                    string sname = vsg.Name;
                    int i = 1;
                    while (db.RealDatabase.HasChildGroup(vtg, sname))
                    {
                        sname = vsg.Name + i;
                        i++;
                    }

                    Cdy.Tag.TagGroup tgg = vsg != null ? new Cdy.Tag.TagGroup() { Name = sname, Parent = vtg } : null;

                    if (tgg == null) return Task.FromResult(new PasteGroupReplay() { Result = false });

                    tgg = db.RealDatabase.CheckAndAddGroup(tgg.FullName);

                    foreach (var vv in tags)
                    {
                        var tmp = vv.Clone();
                        tmp.Group = tgg.FullName;
                        db.RealDatabase.Append(tmp);

                        var vid = vv.Id;
                        if (db.HisDatabase.HisTags.ContainsKey(vid))
                        {
                            var vhis = db.HisDatabase.HisTags[vid].Clone();
                            vhis.Id = tmp.Id;
                            db.HisDatabase.AddHisTags(vhis);
                        }
                    }
                    return Task.FromResult(new PasteGroupReplay() { Result = true, Group = sname });
                }
            }
            return Task.FromResult(new PasteGroupReplay() { Result = false, ErroMessage = "database not exist!" });
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> RenameTagGroup(RenameGroupRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                {
                    var re = db.RealDatabase.ChangeGroupName(request.OldFullName, request.NewName);
                    return Task.FromResult(new BoolResultReplay() { Result = re });
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = "database not exist!" });
        }


        /// <summary>
        /// 删除组
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> RemoveTagGroup(RemoveGroupRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                    db.RealDatabase.RemoveGroup(request.Name);
                return Task.FromResult(new BoolResultReplay() { Result = true });
            }
            return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = "database not exist!" });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> MoveTagGroup(MoveGroupRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                    db.RealDatabase.ChangeGroupParent(request.Name,request.OldParentName, request.NewParentName);
                return Task.FromResult(new BoolResultReplay() { Result = true });
            }
            return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = "database not exist!" });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetTagGroupMessageReply> GetTagGroup(GetRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetTagGroupMessageReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            GetTagGroupMessageReply re = new GetTagGroupMessageReply() { Result = true };

            if (db != null && db.RealDatabase != null && db.RealDatabase.Groups != null)
            {
                lock (db)
                {
                    foreach (var vv in db.RealDatabase.Groups)
                    {
                        re.Group.Add(new TagGroup() { Name = vv.Key, Parent = vv.Value.Parent != null ? vv.Value.Parent.FullName : "" });
                    }
                }
            }
            return Task.FromResult(re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetTagMessageReply> GetTagByGroup(GetTagByGroupRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetTagMessageReply() { Result = false, Index = request.Index });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);

            List<RealTagMessage> rre = new List<RealTagMessage>();
            List<HisTagMessage> re = new List<HisTagMessage>();
            int totalpage = 0;
            int total = 0;
            if (db != null)
            {
                lock (db)
                {
                    int from = request.Index * PageCount;
                    var res = db.RealDatabase.ListAllTags().Where(e => e.Group == request.Group);
                    total = res.Count();

                    totalpage = total / PageCount;
                    totalpage = total % PageCount > 0 ? totalpage + 1 : totalpage;
                    int cc = 0;
                    foreach (var vv in res)
                    {
                        if (cc >= from && cc < (from + PageCount))
                        {
                            rre.Add(new RealTagMessage() { Id = (uint)vv.Id, Name = vv.Name, Desc = vv.Desc, Group = vv.Group, LinkAddress = vv.LinkAddress, TagType = (uint)vv.Type, Convert = vv.Conveter != null ? vv.Conveter.SeriseToString() : string.Empty, ReadWriteMode = (int)vv.ReadWriteType, MaxValue = (vv is Cdy.Tag.NumberTagBase) ? (vv as Cdy.Tag.NumberTagBase).MaxValue : 0, MinValue = (vv is Cdy.Tag.NumberTagBase) ? (vv as Cdy.Tag.NumberTagBase).MinValue : 0, Precision = (vv is Cdy.Tag.FloatingTagBase) ? (vv as Cdy.Tag.FloatingTagBase).Precision : 0 });

                            if (db.HisDatabase.HisTags.ContainsKey(vv.Id))
                            {
                                var vvv = db.HisDatabase.HisTags[vv.Id];
                                var vitem = new HisTagMessage() { Id = (uint)vv.Id, Type = (uint)vvv.Type, TagType = (uint)vvv.TagType, CompressType = (uint)vvv.CompressType, Circle = (ulong)vvv.Circle };
                                if (vvv.Parameters != null && vvv.Parameters.Count > 0)
                                {
                                    foreach (var vvp in vvv.Parameters)
                                    {
                                        vitem.Parameter.Add(new hisTagParameterItem() { Name = vvp.Key, Value = vvp.Value });
                                    }
                                }
                                re.Add(vitem);
                            }
                        }
                        cc++;
                    }
                }
            }
            var msg = new GetTagMessageReply() { Result = true,Count= totalpage, Index=request.Index,TagCount=total };
            msg.RealTag.AddRange(rre);
            msg.HisTag.AddRange(re);
            return Task.FromResult(msg);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetTagMessageReply> GetAllTag(GetTagByGroupRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetTagMessageReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);

            List<RealTagMessage> rre = new List<RealTagMessage>();
            List<HisTagMessage> re = new List<HisTagMessage>();
            int totalpage = 0;
            int total = 0;
            if (db != null)
            {
                lock (db)
                {
                    int from = request.Index * PageCount;
                    var res = db.RealDatabase.ListAllTags();
                    total = res.Count();

                    totalpage = total / PageCount;
                    totalpage = total % PageCount > 0 ? totalpage + 1 : totalpage;
                    int cc = 0;

                    foreach (var vv in res)
                    {
                        if (cc >= from && cc < (from + PageCount))
                        {
                            rre.Add(new RealTagMessage() { Id = (uint)vv.Id, Name = vv.Name, Desc = vv.Desc, Group = vv.Group, LinkAddress = vv.LinkAddress, TagType = (uint)vv.Type, Convert = vv.Conveter != null ? vv.Conveter.SeriseToString() : string.Empty, ReadWriteMode = (int)vv.ReadWriteType, MaxValue = (vv is Cdy.Tag.NumberTagBase) ? (vv as Cdy.Tag.NumberTagBase).MaxValue : 0, MinValue = (vv is Cdy.Tag.NumberTagBase) ? (vv as Cdy.Tag.NumberTagBase).MinValue : 0, Precision = (vv is Cdy.Tag.FloatingTagBase) ? (vv as Cdy.Tag.FloatingTagBase).Precision : 0 });

                            if (db.HisDatabase.HisTags.ContainsKey(vv.Id))
                            {
                                var vvv = db.HisDatabase.HisTags[vv.Id];
                                var vitem = new HisTagMessage() { Id = (uint)vv.Id, Type = (uint)vv.Type, TagType = (uint)vvv.TagType, CompressType = (uint)vvv.CompressType, Circle = (ulong)vvv.Circle };
                                if (vvv.Parameters != null && vvv.Parameters.Count > 0)
                                {
                                    foreach (var vvp in vvv.Parameters)
                                    {
                                        vitem.Parameter.Add(new hisTagParameterItem() { Name = vvp.Key, Value = vvp.Value });
                                    }
                                }
                                re.Add(vitem);
                            }
                        }
                        cc++;
                    }
                }
            }
            var msg = new GetTagMessageReply() { Result = true, Count = totalpage, Index = request.Index,TagCount = total };
            msg.RealTag.AddRange(rre);
            msg.HisTag.AddRange(re);
            return Task.FromResult(msg);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetHistTagMessageReply> GetHisAllTag(GetRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetHistTagMessageReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            List<HisTagMessage> re = new List<HisTagMessage>();
            if (db != null)
            {
                lock (db)
                {
                    foreach (var vv in db.HisDatabase.HisTags.Values)
                    {
                        var vitem = new HisTagMessage() { Id = (uint)vv.Id, Type = (uint)vv.Type, TagType = (uint)vv.TagType, CompressType = (uint)vv.CompressType, Circle = (ulong)vv.Circle };
                        if (vv.Parameters != null && vv.Parameters.Count > 0)
                        {
                            foreach (var vvv in vv.Parameters)
                            {
                                vitem.Parameter.Add(new hisTagParameterItem() { Name = vvv.Key, Value = vvv.Value });
                            }
                        }
                        re.Add(vitem);
                        // re.Add(new HisTagMessage() { Id = (uint)vv.Id, Type = (uint)vv.Type, TagType = (uint)vv.TagType, CompressType = (uint)vv.CompressType});
                    }
                }
            }
            var msg = new GetHistTagMessageReply() { Result = true };
            msg.Messages.AddRange(re);
            return Task.FromResult(msg);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetRealTagMessageReply> GetRealAllTag(GetRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetRealTagMessageReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            List<RealTagMessage> re = new List<RealTagMessage>();
            if (db != null)
            {
                lock (db)
                {
                    foreach (var vv in db.RealDatabase.ListAllTags())
                    {
                        re.Add(new RealTagMessage() { Id = (uint)vv.Id, Name = vv.Name, Desc = vv.Desc, Group = vv.Group, LinkAddress = vv.LinkAddress, TagType = (uint)vv.Type, Convert = vv.Conveter != null ? vv.Conveter.SeriseToString() : string.Empty, ReadWriteMode = (int)vv.ReadWriteType, MaxValue = (vv is Cdy.Tag.NumberTagBase) ? (vv as Cdy.Tag.NumberTagBase).MaxValue : 0, MinValue = (vv is Cdy.Tag.NumberTagBase) ? (vv as Cdy.Tag.NumberTagBase).MinValue : 0, Precision = (vv is Cdy.Tag.FloatingTagBase) ? (vv as Cdy.Tag.FloatingTagBase).Precision : 0 });
                    }
                }
            }
            var msg = new GetRealTagMessageReply() { Result = true };
            msg.Messages.AddRange(re);
            return Task.FromResult(msg);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetHistTagMessageReply> QueryHisTag(QueryMessageRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetHistTagMessageReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if(db!=null)
            {
                if(request.Conditions.Count==0)
                    return Task.FromResult(new GetHistTagMessageReply() { Result = true });

                lock (db)
                {
                    IEnumerable<Cdy.Tag.HisTag> htags = db.HisDatabase.HisTags.Values;
                    foreach (var vv in request.Conditions)
                    {
                        switch (vv.Key.ToLower())
                        {
                            case "id":
                                htags = htags.Where(e => e.Id == int.Parse(vv.Value));
                                break;
                            case "tagtype":
                                htags = htags.Where(e => e.TagType == (Cdy.Tag.TagType)int.Parse(vv.Value));
                                break;
                            case "type":
                                htags = htags.Where(e => e.Type == (Cdy.Tag.RecordType)int.Parse(vv.Value));
                                break;
                            case "compresstype":
                                htags = htags.Where(e => e.CompressType == int.Parse(vv.Value));
                                break;
                            case "circle":
                                htags = htags.Where(e => e.Circle == long.Parse(vv.Value));
                                break;
                        }

                    }
                    List<HisTagMessage> re = new List<HisTagMessage>();
                    foreach (var vv in htags)
                    {
                        var vitem = new HisTagMessage() { Id = (uint)vv.Id, Type = (uint)vv.Type, TagType = (uint)vv.TagType, CompressType = (uint)vv.CompressType };
                        if (vv.Parameters != null && vv.Parameters.Count > 0)
                        {
                            foreach (var vvv in vv.Parameters)
                            {
                                vitem.Parameter.Add(new hisTagParameterItem() { Name = vvv.Key, Value = vvv.Value });
                            }
                        }
                        re.Add(vitem);
                    }

                    var msg = new GetHistTagMessageReply() { Result = true };
                    msg.Messages.AddRange(re);
                    return Task.FromResult(msg);
                }
            }
            return Task.FromResult(new GetHistTagMessageReply() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetRealTagMessageReply> QueryRealTag(QueryMessageRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetRealTagMessageReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                if (request.Conditions.Count == 0)
                    return Task.FromResult(new GetRealTagMessageReply() { Result = true });

                lock (db)
                {
                    IEnumerable<Cdy.Tag.Tagbase> htags = db.RealDatabase.Tags.Values;
                    foreach (var vv in request.Conditions)
                    {
                        switch (vv.Key.ToLower())
                        {
                            case "id":
                                htags = htags.Where(e => e.Id == int.Parse(vv.Value));
                                break;
                            case "name":
                                htags = htags.Where(e => e.Name.Contains(vv.Value));
                                break;
                            case "type":
                                htags = htags.Where(e => e.Type == (Cdy.Tag.TagType)int.Parse(vv.Value));
                                break;
                            case "group":
                                htags = htags.Where(e => e.Group.Contains(vv.Value));
                                break;
                            case "desc":
                                htags = htags.Where(e => e.Desc.Contains(vv.Value));
                                break;
                        }
                    }

                    List<RealTagMessage> re = new List<RealTagMessage>();
                    foreach (var vv in htags)
                    {
                        re.Add(new RealTagMessage() { Id = (uint)vv.Id, Name = vv.Name, Desc = vv.Desc, Group = vv.Group, LinkAddress = vv.LinkAddress, TagType = (uint)vv.Type, Convert = vv.Conveter != null ? vv.Conveter.SeriseToString() : string.Empty, ReadWriteMode = (int)vv.ReadWriteType, MaxValue = (vv is Cdy.Tag.NumberTagBase) ? (vv as Cdy.Tag.NumberTagBase).MaxValue : 0, MinValue = (vv is Cdy.Tag.NumberTagBase) ? (vv as Cdy.Tag.NumberTagBase).MinValue : 0, Precision = (vv is Cdy.Tag.FloatingTagBase) ? (vv as Cdy.Tag.FloatingTagBase).Precision : 0 });
                    }

                    var msg = new GetRealTagMessageReply() { Result = true };
                    msg.Messages.AddRange(re);
                    return Task.FromResult(msg);
                }
            }
            return Task.FromResult(new GetRealTagMessageReply() { Result = true });
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> RemoveTag(RemoveTagMessageRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {

                lock (db)
                {
                    foreach (var vv in request.TagId)
                    {
                        db.HisDatabase.RemoveHisTag(vv);
                        db.RealDatabase.Remove(vv);
                    }
                }
                return Task.FromResult(new BoolResultReplay() { Result = true });
            }
            return Task.FromResult(new BoolResultReplay() { Result = false });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> ClearTag(ClearTagRequestMessage request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }

            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                {
                    foreach(var vv in db.RealDatabase.GetTagsByGroup(request.GroupFullName))
                    {
                        db.HisDatabase.RemoveHisTag(vv.Id);
                        db.RealDatabase.RemoveWithoutGroupProcess(vv);
                    }
                    var grp = db.RealDatabase.GetGroup(request.GroupFullName);
                    if (grp != null)
                        grp.Tags.Clear();
                }
                return Task.FromResult(new BoolResultReplay() { Result = true });
            }
            return Task.FromResult(new BoolResultReplay() { Result = false });
        }

        /// <summary>
        /// 清空所有变量
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> ClearAllTag(GetRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                {
                    db.HisDatabase.HisTags.Clear();
                    db.RealDatabase.Tags.Clear();
                    db.RealDatabase.NamedTags.Clear();
                    db.RealDatabase.Groups.Clear();
                }
                return Task.FromResult(new BoolResultReplay() { Result = true });
            }
            return Task.FromResult(new BoolResultReplay() { Result = false });
        }

        /// <summary>
        /// remove his tag
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> RemoveHisTag(RemoveTagMessageRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                {
                    foreach (var vv in request.TagId)
                    {
                        db.HisDatabase.RemoveHisTag(vv);
                    }
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> UpdateHisTag(UpdateHisTagRequestMessage request, ServerCallContext context)
        {
            try
            {
                if (!CheckLoginId(request.LoginId, request.Database))
                {
                    return Task.FromResult(new BoolResultReplay() { Result = false });
                }
                var db = DbManager.Instance.GetDatabase(request.Database);
                var vtag = request.Tag;
                if (db != null)
                {
                    lock (db)
                    {
                        {
                            Cdy.Tag.HisTag hisTag = new Cdy.Tag.HisTag();
                            hisTag.Id = (int)vtag.Id;
                            hisTag.TagType = (Cdy.Tag.TagType)(vtag.TagType);
                            hisTag.Type = (Cdy.Tag.RecordType)(vtag.Type);
                            hisTag.CompressType = (int)(vtag.CompressType);
                            hisTag.Parameters = new Dictionary<string, double>();
                            if (vtag.Parameter != null)
                            {
                                foreach (var vv in vtag.Parameter)
                                {
                                    hisTag.Parameters.Add(vv.Name, vv.Value);
                                }
                            }
                            db.HisDatabase.AddOrUpdate(hisTag);
                        }
                    }
                }
                return Task.FromResult(new BoolResultReplay() { Result = true });
            }
            catch(Exception ex)
            {
                return Task.FromResult(new BoolResultReplay() { Result = false,ErroMessage = ex.Message });
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> UpdateRealTag(UpdateRealTagRequestMessage request, ServerCallContext context)
        {
            try
            {
                if (!CheckLoginId(request.LoginId, request.Database))
                {
                    return Task.FromResult(new BoolResultReplay() { Result = false });
                }
                var tag = GetRealTag(request.Tag);
                var db = DbManager.Instance.GetDatabase(request.Database);

                if (db != null)
                {
                    lock (db)
                    {
                        if (db.RealDatabase.Tags.ContainsKey(tag.Id) && tag.Id > -1)
                        {
                            db.RealDatabase.UpdateById(tag.Id, tag);
                            return Task.FromResult(new BoolResultReplay() { Result = true });
                        }
                        else if (db.RealDatabase.NamedTags.ContainsKey(tag.FullName))
                        {
                            db.RealDatabase.Update(tag.FullName, tag);
                            return Task.FromResult(new BoolResultReplay() { Result = true });
                        }
                        else
                        {
                            return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = "Tag not exist" });
                        }
                    }
                }
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            catch(Exception ex)
            {
                return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = ex.Message });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tmsg"></param>
        /// <returns></returns>
        private Cdy.Tag.Tagbase GetRealTag(RealTagMessage tmsg)
        {
            Cdy.Tag.Tagbase re = null;
            switch(tmsg.TagType)
            {
                case (uint)(Cdy.Tag.TagType.Bool):
                    re = new Cdy.Tag.BoolTag();
                    break;
                case (uint)(Cdy.Tag.TagType.Byte):
                    re = new Cdy.Tag.ByteTag();
                    break;
                case (uint)(Cdy.Tag.TagType.DateTime):
                    re = new Cdy.Tag.DateTimeTag();
                    break;
                case (uint)(Cdy.Tag.TagType.Double):
                    re = new Cdy.Tag.DoubleTag();
                    break;
                case (uint)(Cdy.Tag.TagType.Float):
                    re = new Cdy.Tag.FloatTag();
                    break;
                case (uint)(Cdy.Tag.TagType.Int):
                    re = new Cdy.Tag.IntTag();
                    break;
                case (uint)(Cdy.Tag.TagType.UInt):
                    re = new Cdy.Tag.UIntTag();
                    break;
                case (uint)(Cdy.Tag.TagType.ULong):
                    re = new Cdy.Tag.ULongTag ();
                    break;
                case (uint)(Cdy.Tag.TagType.UShort):
                    re = new Cdy.Tag.UShortTag();
                    break;
                case (uint)(Cdy.Tag.TagType.Long):
                    re = new Cdy.Tag.LongTag();
                    break;
                case (uint)(Cdy.Tag.TagType.Short):
                    re = new Cdy.Tag.ShortTag();
                    break;
                case (uint)(Cdy.Tag.TagType.String):
                    re = new Cdy.Tag.StringTag();
                    break;
            }
            if (re != null)
            {
                re.Name = tmsg.Name;
                re.LinkAddress = tmsg.LinkAddress;
                re.Group = tmsg.Group;
                re.Desc = tmsg.Desc;
                re.Id = (int)tmsg.Id;
                re.ReadWriteType = (Cdy.Tag.ReadWriteMode)tmsg.ReadWriteMode;
                if(!string.IsNullOrEmpty(tmsg.Convert))
                {
                    re.Conveter = tmsg.Convert.DeSeriseToValueConvert();
                }
                if(re is Cdy.Tag.NumberTagBase)
                {
                    (re as Cdy.Tag.NumberTagBase).MaxValue = tmsg.MaxValue;
                    (re as Cdy.Tag.NumberTagBase).MinValue = tmsg.MinValue;
                }

                if (re is Cdy.Tag.FloatingTagBase)
                {
                    (re as Cdy.Tag.FloatingTagBase).Precision = (byte)tmsg.Precision;
                }

            }

            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<AddTagReplyMessage> AddTag(AddTagRequestMessage request, ServerCallContext context)
        {
            try
            {
                if (!CheckLoginId(request.LoginId, request.Database))
                {
                    return Task.FromResult(new AddTagReplyMessage() { Result = false });
                }
                var db = DbManager.Instance.GetDatabase(request.Database);

                if (db != null)
                {
                    lock (db)
                    {
                        Cdy.Tag.Tagbase tag = GetRealTag(request.RealTag);

                        if (tag.Id < 0)
                        {
                            db.RealDatabase.Append(tag);
                        }
                        else
                        {
                            db.RealDatabase.AddOrUpdate(tag);
                        }

                        var vtag = request.HisTag;
                        if (vtag.Id != uint.MaxValue)
                        {
                            Cdy.Tag.HisTag hisTag = new Cdy.Tag.HisTag();
                            hisTag.Id = (int)vtag.Id;
                            hisTag.TagType = (Cdy.Tag.TagType)(vtag.TagType);
                            hisTag.Type = (Cdy.Tag.RecordType)(vtag.Type);
                            hisTag.CompressType = (int)(vtag.CompressType);

                            hisTag.Parameters = new Dictionary<string, double>();
                            if (vtag.Parameter != null)
                            {
                                foreach (var vv in vtag.Parameter)
                                {
                                    hisTag.Parameters.Add(vv.Name, vv.Value);
                                }
                            }

                            db.HisDatabase.AddOrUpdate(hisTag);
                        }
                        return Task.FromResult(new AddTagReplyMessage() { Result = true, TagId = tag.Id });
                    }
                }
                return Task.FromResult(new AddTagReplyMessage() { Result = false });
            }
            catch(Exception ex)
            {
                return Task.FromResult(new AddTagReplyMessage() { Result = false, ErroMessage=ex.Message });
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<ImportTagRequestReplyMessage> Import(ImportTagRequestMessage request, ServerCallContext context)
        {
            try
            {
                if (!CheckLoginId(request.LoginId, request.Database))
                {
                    return Task.FromResult(new ImportTagRequestReplyMessage() { Result = false });
                }
                var db = DbManager.Instance.GetDatabase(request.Database);

                if (db != null)
                {
                    lock (db)
                    {
                        Cdy.Tag.Tagbase tag = GetRealTag(request.RealTag);

                        if(request.Mode ==0)
                        {
                            //修改模式
                            if (db.RealDatabase.NamedTags.ContainsKey(tag.FullName))
                            {
                                var vid = db.RealDatabase.NamedTags[tag.FullName].Id;
                                if(vid == tag.Id)
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
                            
                            var vtag = request.HisTag;
                            if (vtag.Id != uint.MaxValue)
                            {
                                Cdy.Tag.HisTag hisTag = new Cdy.Tag.HisTag();
                                hisTag.Id = (int)vtag.Id;
                                hisTag.TagType = (Cdy.Tag.TagType)(vtag.TagType);
                                hisTag.Type = (Cdy.Tag.RecordType)(vtag.Type);
                                hisTag.CompressType = (int)(vtag.CompressType);

                                hisTag.Parameters = new Dictionary<string, double>();
                                if (vtag.Parameter != null)
                                {
                                    foreach (var vv in vtag.Parameter)
                                    {
                                        hisTag.Parameters.Add(vv.Name, vv.Value);
                                    }
                                }

                                db.HisDatabase.AddOrUpdate(hisTag);
                            }
                        }
                        else if(request.Mode==1)
                        {
                            db.RealDatabase.Add(tag);

                            var vtag = request.HisTag;
                            Cdy.Tag.HisTag hisTag = new Cdy.Tag.HisTag();
                            hisTag.Id = (int)vtag.Id;
                            hisTag.TagType = (Cdy.Tag.TagType)(vtag.TagType);
                            hisTag.Type = (Cdy.Tag.RecordType)(vtag.Type);
                            hisTag.CompressType = (int)(vtag.CompressType);

                            hisTag.Parameters = new Dictionary<string, double>();
                            if (vtag.Parameter != null)
                            {
                                foreach (var vv in vtag.Parameter)
                                {
                                    hisTag.Parameters.Add(vv.Name, vv.Value);
                                }
                            }

                            db.HisDatabase.AddOrUpdate(hisTag);
                        }
                        else
                        {
                            //直接添加

                            if(db.RealDatabase.NamedTags.ContainsKey(tag.FullName))
                            {
                                return Task.FromResult(new ImportTagRequestReplyMessage() { Result = false,ErroMessage="名称重复" });
                            }

                            db.RealDatabase.Append(tag);

                            var vtag = request.HisTag;
                            if (vtag.Id != uint.MaxValue)
                            {
                                Cdy.Tag.HisTag hisTag = new Cdy.Tag.HisTag();
                                hisTag.Id = (int)vtag.Id;
                                hisTag.TagType = (Cdy.Tag.TagType)(vtag.TagType);
                                hisTag.Type = (Cdy.Tag.RecordType)(vtag.Type);
                                hisTag.CompressType = (int)(vtag.CompressType);

                                hisTag.Parameters = new Dictionary<string, double>();
                                if (vtag.Parameter != null)
                                {
                                    foreach (var vv in vtag.Parameter)
                                    {
                                        hisTag.Parameters.Add(vv.Name, vv.Value);
                                    }
                                }

                                db.HisDatabase.AddOrUpdate(hisTag);
                            }
                        }

                        
                        return Task.FromResult(new ImportTagRequestReplyMessage() { Result = true, TagId = tag.Id });
                    }
                }
                return Task.FromResult(new ImportTagRequestReplyMessage() { Result = false });
            }
            catch (Exception ex)
            {
                return Task.FromResult(new ImportTagRequestReplyMessage() { Result = false, ErroMessage = ex.Message });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetRegisteDriversReplay> GetRegisteDrivers(GetRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetRegisteDriversReplay() { Result = false });
            }
            GetRegisteDriversReplay re = new GetRegisteDriversReplay() { Result = true };
            foreach(var vd in Cdy.Tag.DriverManager.Manager.Drivers)
            {
                Driver dd = new Driver() { Name = vd.Key };
                var reg = vd.Value.Registors;
                if(reg!=null&&reg.Length>0)
                {
                    dd.Registors.AddRange(reg);
                }
                re.Drivers.Add(dd);
            }
            return Task.FromResult(re);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> Save(GetRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
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
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> Cancel(GetRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            lock (DbManager.Instance)
            {
                DbManager.Instance.Reload();
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        
    }
}
