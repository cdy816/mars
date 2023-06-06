using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Cdy.Tag;
using System.Text;
using System.Data.Common;
using System.Text.RegularExpressions;
using Cdy.Tag.Common.Common;

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
        private void SaveApiUser(string database, string user, string password)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(user + "," + password);
            string spath = System.IO.Path.Combine(PathHelper.helper.DataPath, database, "ApiUser.sdb");
            System.IO.File.WriteAllText(spath, Md5Helper.Encode(sb.ToString()));
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
                    if (IsApiUser(request.UserName))
                    {
                        SaveApiUser(db.Name, request.UserName, request.Password);
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
        public override Task<BoolResultReplay> ReNameDatabasePermission(ReNameDatabasePermissionRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                if (db.Security.Permission.Permissions.ContainsKey(request.OldName) && !db.Security.Permission.Permissions.ContainsKey(request.NewName))
                {
                    var pp = db.Security.Permission.Permissions[request.OldName];
                    db.Security.Permission.Remove(request.OldName);
                    pp.Name = request.NewName;
                    db.Security.Permission.Add(pp);
                    return Task.FromResult(new BoolResultReplay() { Result = true });
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = false });
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

        public override Task<BoolResultReplay> RenameDatabaseUser(RenameGroupRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var uss = db.Security.User.Users;
                if (uss.ContainsKey(request.OldFullName))
                {
                    var user = uss[request.OldFullName];
                    uss.Remove(request.OldFullName);

                    if(!uss.ContainsKey(request.NewName))
                    {
                        user.Name = request.NewName;
                        uss.Add(user.Name, user);
                    }
                    else
                    {
                        return Task.FromResult(new BoolResultReplay() { Result = false });
                    }
                }
                else
                {
                    return Task.FromResult(new BoolResultReplay() { Result = false });
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
                    db.Security.User.RemoveUserGroup(usergroup.FullName);
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
            if (SecurityManager.Manager.CheckKeyAvaiable(request.LoginId))
            {
                if(!(userName == request.UserName && SecurityManager.Manager.CheckPasswordIsCorrect(userName, request.Password)))
                {
                    return Task.FromResult(new BoolResultReplay() { Result = false });
                }

                var user = SecurityManager.Manager.Securitys.User.GetUser(request.UserName);
                if (user != null)
                {
                    user.Password = request.Newpassword;
                    SecurityManager.Manager.Save();
                    return Task.FromResult(new BoolResultReplay() { Result = true });
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = false });
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
        public override Task<BoolResultReplay> CheckOpenDatabase(DatabasesRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if(db!=null)
            {
                DbManager.Instance.CheckAndContinueLoadDatabase(db);
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> NewDatabase(NewDatabaseRequest request, ServerCallContext context)
        {
            if (!HasNewDatabasePermission(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }

            lock (DbManager.Instance)
            {
                var db = DbManager.Instance.GetDatabase(request.Database);
                if (db != null)
                {
                    return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = "数据库已经存在!" });
                }
                else
                {
                    DbManager.Instance.NewDB(request.Database, request.Desc);
                    var user = SecurityManager.Manager.GetUser(request.LoginId);

                    user.Databases.Add(request.Database);
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
        public override Task<BoolResultReplay> RemoveDatabase(NewDatabaseRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId,request.Database) || !HasDeleteDatabasePerssion(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }

            lock (DbManager.Instance)
            {
                var db = DbManager.Instance.GetDatabase(request.Database);
                if (db != null)
                {
                    if (!ServiceLocator.Locator.Resolve<IDatabaseManager>().IsRunning(request.Database))
                    {
                        var re = DbManager.Instance.RemoveDB(request.Database);
                        if (re)
                        {
                            return Task.FromResult(new BoolResultReplay() { Result = true });
                        }
                        else
                        {
                            return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = "删除失败" });
                        }
                    }
                    else
                    {
                        return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = "数据库正在运行，请先停止运行!" });
                    }
                }
                else
                {
                    return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = "数据库不存在" });
                }
            }
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
                    string desc = DbManager.Instance.GetDatabase(vv).Desc;
                    re.Database.Add(new KeyValueMessage() { Key = vv, Value = string.IsNullOrEmpty(desc) ? "" : desc });
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
            if (!CheckLoginId(request.LoginId, request.Database))
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
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            return Task.FromResult(new BoolResultReplay() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().Stop(request.Database) });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> ReRun(DatabasesRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            return Task.FromResult(new BoolResultReplay() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().Rerun(request.Database) });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> IsDatabaseRunning(DatabasesRequest request, ServerCallContext context)
        {
            if (!SecurityManager.Manager.CheckKeyAvaiable(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            return Task.FromResult(new BoolResultReplay() { Result = ServiceLocator.Locator.Resolve<IDatabaseManager>().IsRunning(request.Database) });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> IsDatabaseDirty(DatabasesRequest request, ServerCallContext context)
        {
            if (!SecurityManager.Manager.CheckKeyAvaiable(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                DbManager.Instance.CheckAndContinueLoadDatabase(db);
                return Task.FromResult(new BoolResultReplay() { Result = db.IsDirty });
            }
            else
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
        }

        #endregion

        #region Tag class

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<AddClassReplay> AddTagClass(AddClassRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new AddClassReplay() { Result = false });
            }
            string name = request.Name;
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                {
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);

                    string sname = db.ComplexTagClass.CheckAndGetAvaiableClassName(request.Name);

                    db.ComplexTagClass.AddClass(new ComplexTagClass() { Name = sname, Descript= sname });
                    return Task.FromResult(new AddClassReplay() { Result = true,Group=sname });
                }
            }
            return Task.FromResult(new AddClassReplay() { Result = false, ErroMessage = "database not exist!" });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> RemoveTagClass(RemoveClassRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                bool reids = false;
                lock (db)
                {
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);

                    var cls = db.ComplexTagClass.Class[request.Name];

                    reids = db.ComplexTagClass.RemoveClass(request.Name);

                    foreach(var vv in db.RealDatabase.Tags.Where(e=>e.Value is ComplexTag &&(e.Value as ComplexTag).LinkComplexClass==cls.Id).Select(e=>e.Value).ToArray())
                    {
                        foreach (var vdb in (vv as ComplexTag).Tags)
                        {
                            db.HisDatabase.RemoveHisTag(vdb.Value.Id);
                        }

                        db.RealDatabase.Remove(vv.Id);
                    }
                }
                return Task.FromResult(new BoolResultReplay() { Result = reids });
            }
            return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = "database not exist!" });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> RenameTagClass(RenameClassRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                bool reids = false;
                lock (db)
                {
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    reids = db.ComplexTagClass.ReNameClass(request.OldFullName,request.NewName);
                    if(reids)
                    {
                        db.RealDatabase.ChangedComplexLinkClass(request.OldFullName, request.NewName);
                    }
                }
                return Task.FromResult(new BoolResultReplay() { Result = reids });
            }
            return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = "database not exist!" });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetTagClassMessageReply> GetTagClass(GetRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetTagClassMessageReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            GetTagClassMessageReply re = new GetTagClassMessageReply() { Result = true };

            if (db != null && db.ComplexTagClass != null)
            {
                lock (db)
                {
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    foreach (var vv in db.ComplexTagClass.Class)
                    {
                        re.Classes.Add(new TagClass() { Name = vv.Key,  Description = vv.Value.Descript != null ? vv.Value.Descript : "" });
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
        public override Task<BoolResultReplay> UpdateTagClassDescription(UpdateClassDescriptionRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                DbManager.Instance.CheckAndContinueLoadDatabase(db);
                var vtg = db.ComplexTagClass.Class.ContainsKey(request.Name) ? db.ComplexTagClass.Class[request.Name] : null;
                if (vtg != null)
                {
                    vtg.Descript = request.Desc;
                }
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
        public override Task<GetTagMessageReply> GetTagByClass(GetTagByGroupRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetTagMessageReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                var re = new GetTagMessageReply() { Result = true };
                DbManager.Instance.CheckAndContinueLoadDatabase(db);
                var vtg = db.ComplexTagClass.Class.ContainsKey(request.Group) ? db.ComplexTagClass.Class[request.Group] : null;
                int total = 0;
                int totalpage = 1;

                if (vtg != null)
                {
                    int from = request.Index * PageCount;
                    var res = vtg.Tags.Values;
                    total = res.Count();
                    totalpage = total / PageCount;
                    totalpage = total % PageCount > 0 ? totalpage + 1 : totalpage;
                    int cc = 0;

                    foreach (var vv in res)
                    {
                        if (cc >= from && cc < (from + PageCount))
                        {
                            re.RealTag.Add(ConvertToMessage(vv));

                            if (vtg.HisTags.ContainsKey(vv.Id))
                            {
                                var vvv = vtg.HisTags[vv.Id];
                                var vitem = new HisTagMessage() { Id = vv.Id, Type = (uint)vvv.Type, TagType = (uint)vvv.TagType, CompressType = (uint)vvv.CompressType, Circle = (uint)vvv.Circle, MaxValueCountPerSecond = (uint)vvv.MaxValueCountPerSecond };
                                if (vvv.Parameters != null && vvv.Parameters.Count > 0)
                                {
                                    foreach (var vvp in vvv.Parameters)
                                    {
                                        vitem.Parameter.Add(new hisTagParameterItem() { Name = vvp.Key, Value = vvp.Value });
                                    }
                                }
                                re.HisTag.Add(vitem);
                            }
                        }
                        cc++;
                    }
                  
                }
                re.TagCount = total;
                re.Count = totalpage;
                return Task.FromResult(re);
            }
            return Task.FromResult(new GetTagMessageReply() { Result = false, ErroMessage = "database not exist!" });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<AddTagReplyMessage> AddClassTag(AddClassTagRequestMessage request, ServerCallContext context)
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    Cdy.Tag.Tagbase tag = GetRealTag(request.RealTag);
                    tag.Group = "";
                    var vtg = db.ComplexTagClass.Class.ContainsKey(request.Classes) ? db.ComplexTagClass.Class[request.Classes] : null;
                    if (vtg != null)
                    {
                        if (tag.Id < 0)
                        {
                            vtg.AppendTag(tag);
                        }
                        else
                        {
                            vtg.AddRealTag(tag);
                        }

                        var vtag = request.HisTag;
                        if (vtag.Id != int.MaxValue)
                        {
                            Cdy.Tag.HisTag hisTag = new Cdy.Tag.HisTag();
                            hisTag.Id = (int)tag.Id;
                            hisTag.TagType = (Cdy.Tag.TagType)(vtag.TagType);
                            hisTag.Type = (Cdy.Tag.RecordType)(vtag.Type);
                            hisTag.CompressType = (int)(vtag.CompressType);
                            hisTag.Circle = (int)vtag.Circle;
                            hisTag.MaxValueCountPerSecond = (short)vtag.MaxValueCountPerSecond;
                            hisTag.Parameters = new Dictionary<string, double>();
                            if (vtag.Parameter != null)
                            {
                                foreach (var vv in vtag.Parameter)
                                {
                                    hisTag.Parameters.Add(vv.Name, vv.Value);
                                }
                            }

                            vtg.AddHisTag(hisTag);
                        }
                        return Task.FromResult(new AddTagReplyMessage() { Result = true, TagId = tag.Id });
                    }
                }
            }
            return Task.FromResult(new AddTagReplyMessage() { Result = false, ErroMessage = "database not exist!"  });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> RemoveClassTag(RemoveClassTagMessageRequest request, ServerCallContext context)
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    var vtg = db.ComplexTagClass.Class.ContainsKey(request.Classes) ? db.ComplexTagClass.Class[request.Classes] : null;
                    if(vtg!=null)
                    {
                        foreach (var vv in request.TagId)
                        {
                            vtg.RemoveRealTag(vv);
                            vtg.RemoveHisTag(vv);
                        }
                    }
                }
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
        public override Task<BoolResultReplay> RemoveClassHisTag(RemoveClassTagMessageRequest request, ServerCallContext context)
        {
            try
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
                        DbManager.Instance.CheckAndContinueLoadDatabase(db);
                        var vtg = db.ComplexTagClass.Class.ContainsKey(request.Classes) ? db.ComplexTagClass.Class[request.Classes] : null;
                        if (vtg != null)
                        {
                            foreach (var vid in request.TagId)
                            {
                                if (vtg.HisTags.ContainsKey(vid))
                                {
                                    vtg.HisTags.Remove(vid);
                                }
                            }
                        }
                    }
                }
                return Task.FromResult(new BoolResultReplay() { Result = true });
            }
            catch (Exception ex)
            {
                return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = ex.Message });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> UpdateClassHisTag(UpdateClassHisTagRequestMessage request, ServerCallContext context)
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
                        DbManager.Instance.CheckAndContinueLoadDatabase(db);
                        var vtg = db.ComplexTagClass.Class.ContainsKey(request.Classes) ? db.ComplexTagClass.Class[request.Classes] : null;
                        if (vtg != null)
                        {
                            Cdy.Tag.HisTag hisTag = new Cdy.Tag.HisTag();
                            hisTag.Id = (int)vtag.Id;
                            hisTag.TagType = (Cdy.Tag.TagType)(vtag.TagType);
                            hisTag.Type = (Cdy.Tag.RecordType)(vtag.Type);
                            hisTag.CompressType = (int)(vtag.CompressType);
                            hisTag.Circle = (int)vtag.Circle;
                            hisTag.MaxValueCountPerSecond = (short)vtag.MaxValueCountPerSecond;
                            hisTag.Parameters = new Dictionary<string, double>();
                          
                            if (vtag.Parameter != null)
                            {
                                foreach (var vv in vtag.Parameter)
                                {
                                    hisTag.Parameters.Add(vv.Name, vv.Value);
                                }
                            }
                            vtg.UpdateHisTag(hisTag);
                        }
                    }
                }
                return Task.FromResult(new BoolResultReplay() { Result = true });
            }
            catch (Exception ex)
            {
                return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = ex.Message });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> UpdateClassRealTag(UpdateClassRealTagRequestMessage request, ServerCallContext context)
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
                        DbManager.Instance.CheckAndContinueLoadDatabase(db);

                        var vtg = db.ComplexTagClass.Class.ContainsKey(request.Classes) ? db.ComplexTagClass.Class[request.Classes] : null;
                        if(vtg != null)
                        {
                            if (vtg.Tags.ContainsKey(tag.Id)&& tag.Id > -1)
                            {
                                vtg.UpdateRealTag(tag);
                                return Task.FromResult(new BoolResultReplay() { Result = true});
                            }
                        }
                        else
                        {
                            return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = "Tag not exist" });
                        }
                    }
                }
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            catch (Exception ex)
            {
                return Task.FromResult(new BoolResultReplay() { Result = false, ErroMessage = ex.Message });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> UpdateLinkedClassTags(UpdateClassRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);

            if (db != null)
            {
                var vtags = db.RealDatabase.Tags.Values.Where(e => e is ComplexTag && (e as ComplexTag).LinkComplexClass == request.Name).ToArray();
                if (vtags.Any())
                {
                    foreach(var vv in vtags)
                    {
                        db.RealDatabase.ReCreatComplexTagChild(vv as ComplexTag);
                    }
                }
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
        public override Task<PasteGroupReplay> PasteTagClass(PasteGroupRequest request, ServerCallContext context)
        {
            try
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
                        DbManager.Instance.CheckAndContinueLoadDatabase(db);

                        var vtg = db.ComplexTagClass.Class.ContainsKey(request.GroupFullName) ? db.ComplexTagClass.Class[request.GroupFullName] : null;
                        if (vtg != null)
                        {
                            var vv = vtg.Clone();
                            var vname = db.ComplexTagClass.CheckAndGetAvaiableClassName(vv.Name);
                            vv.Name = vname;
                            db.ComplexTagClass.AddClass(vv);
                            return Task.FromResult(new PasteGroupReplay() { Result = false,Group=vname });
                        }
                    }
                }
                return Task.FromResult(new PasteGroupReplay() { Result = false });
            }
            catch (Exception ex)
            {
                return Task.FromResult(new PasteGroupReplay() { Result = false, ErroMessage = ex.Message });
            }
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
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

                    Cdy.Tag.TagGroup tgg = vsg != null ? new Cdy.Tag.TagGroup() { Name = sname, Parent = vtg,Description=vsg.Description } : null;

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
        public override Task<BoolResultReplay> UpdateGroupDescription(UpdateGroupDescriptionRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                DbManager.Instance.CheckAndContinueLoadDatabase(db);
                var vtg = db.RealDatabase.Groups.ContainsKey(request.GroupName) ? db.RealDatabase.Groups[request.GroupName] : null;
                if(vtg!=null)
                {
                    vtg.Description = request.Desc;
                }
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
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
                {
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    var reids = db.RealDatabase.RemoveGroup(request.Name);
                    if (reids != null && reids.Count > 0)
                    {
                        foreach (var vv in reids)
                        {
                            db.HisDatabase.RemoveHisTag(vv);
                        }
                    }
                }
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
                {
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    db.RealDatabase.ChangeGroupParent(request.Name, request.OldParentName, request.NewParentName);
                }
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    foreach (var vv in db.RealDatabase.Groups)
                    {
                        re.Group.Add(new TagGroup() { Name = vv.Key, Parent = vv.Value.Parent != null ? vv.Value.Parent.FullName : "",Description=vv.Value.Description!=null?vv.Value.Description:"" });
                    }
                }
            }
            return Task.FromResult(re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="res"></param>
        /// <param name="vfilters"></param>
        /// <returns></returns>
        private IEnumerable<Tagbase> FilterTags(Database db,IEnumerable<Tagbase> res,List<FilterMessageItem> vfilters)
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
                            foreach(var vvv in ss)
                            {
                                btmp |= (tag.Name.Contains(vvv) || tag.Desc.Contains(vvv)||tag.Area.Contains(vvv));
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
                            if(ival==3)
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
                            if (!string.IsNullOrEmpty(vv.Value))
                                re = re && (tag.LinkAddress.Contains(vv.Value));
                            break;
                        case "area":
                            if(!string.IsNullOrEmpty(vv.Value))
                            re = re && (tag.Area.Contains(vv.Value));
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    int from = request.Index * PageCount;
                    var res = db.RealDatabase.ListAllRootTags().Where(e => e.Group == request.Group);

                    if(request.Filters.Count>0)
                    {
                        res = FilterTags(db, res, request.Filters.ToList());
                    }

                    total = res.Count();

                    totalpage = total / PageCount;
                    totalpage = total % PageCount > 0 ? totalpage + 1 : totalpage;
                    int cc = 0;
                    foreach (var vv in res)
                    {
                        if (cc >= from && cc < (from + PageCount))
                        {
                            rre.Add(ConvertToMessage(vv));

                            if (db.HisDatabase.HisTags.ContainsKey(vv.Id))
                            {
                                var vvv = db.HisDatabase.HisTags[vv.Id];
                                var vitem = new HisTagMessage() { Id = vv.Id, Type = (uint)vvv.Type, TagType = (uint)vvv.TagType, CompressType = (uint)vvv.CompressType, Circle = (uint)vvv.Circle,MaxValueCountPerSecond=(uint)vvv.MaxValueCountPerSecond };
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
        public override Task<GetTagMessageReply> GetComplexSubTags(GetComplexSubTagsRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetTagMessageReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            List<RealTagMessage> rre = new List<RealTagMessage>();
            List<HisTagMessage> re = new List<HisTagMessage>();
            if (db != null)
            {
                lock (db)
                {
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    var res = db.RealDatabase.Tags.ContainsKey(request.TagId)?db.RealDatabase.Tags[request.TagId]:null;
                    if (res != null && res is ComplexTag)
                    {
                        foreach (var vv in (res as ComplexTag).Tags.Values)
                        {
                            rre.Add(ConvertToMessage(vv)) ;

                            if (db.HisDatabase.HisTags.ContainsKey(vv.Id))
                            {
                                var vvv = db.HisDatabase.HisTags[vv.Id];
                                var vitem = new HisTagMessage() { Id = vv.Id, Type = (uint)vvv.Type, TagType = (uint)vvv.TagType, CompressType = (uint)vvv.CompressType, Circle = (uint)vvv.Circle, MaxValueCountPerSecond = (uint)vvv.MaxValueCountPerSecond };
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
                    }
                }
            }
            var msg = new GetTagMessageReply() { Result = true };
            msg.RealTag.AddRange(rre);
            msg.HisTag.AddRange(re);
            return Task.FromResult(msg);
        }

        private RealTagMessage ConvertToMessage(Tagbase vv)
        {
            return new RealTagMessage() { Id = vv.Id, Name = vv.Name, Desc = vv.Desc,Unit=vv.Unit==null?"":vv.Unit,ExtendField1=vv.ExtendField1==null?"": vv.ExtendField1, Group = vv.Group, LinkAddress = vv.LinkAddress, TagType = (uint)vv.Type, Convert = vv.Conveter != null ? vv.Conveter.SeriseToString() : string.Empty, ReadWriteMode = (int)vv.ReadWriteType, MaxValue = (vv is Cdy.Tag.NumberTagBase) ? (vv as Cdy.Tag.NumberTagBase).MaxValue : 0, MinValue = (vv is Cdy.Tag.NumberTagBase) ? (vv as Cdy.Tag.NumberTagBase).MinValue : 0, Precision = (vv is Cdy.Tag.FloatingTagBase) ? (vv as Cdy.Tag.FloatingTagBase).Precision : 0, LinkComplexClass = vv is ComplexTag ? (vv as ComplexTag).LinkComplexClass : "", Parent = vv.Parent,Area=vv.Area };
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    int from = request.Index * PageCount;
                    var res = db.RealDatabase.ListAllRootTags();
                    if (request.Filters.Count > 0)
                    {
                        res = FilterTags(db, res, request.Filters.ToList());
                    }

                    total = res.Count();

                    totalpage = total / PageCount;
                    totalpage = total % PageCount > 0 ? totalpage + 1 : totalpage;
                    int cc = 0;


                    foreach (var vv in res)
                    {
                        if (cc >= from && cc < (from + PageCount))
                        {
                            rre.Add(ConvertToMessage(vv));

                            if (db.HisDatabase.HisTags.ContainsKey(vv.Id))
                            {
                                var vvv = db.HisDatabase.HisTags[vv.Id];
                                var vitem = new HisTagMessage() { Id = vv.Id, Type = (uint)vv.Type, TagType = (uint)vvv.TagType, CompressType = (uint)vvv.CompressType, Circle = (uint)vvv.Circle,MaxValueCountPerSecond=(uint)vvv.MaxValueCountPerSecond };
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    foreach (var vv in db.HisDatabase.HisTags.Values)
                    {
                        var vitem = new HisTagMessage() { Id = vv.Id, Type = (uint)vv.Type, TagType = (uint)vv.TagType, CompressType = (uint)vv.CompressType, Circle = (uint)vv.Circle,MaxValueCountPerSecond=(uint)vv.MaxValueCountPerSecond };
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    foreach (var vv in db.RealDatabase.ListAllRootTags())
                    {
                        re.Add(ConvertToMessage(vv));
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
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
                        var vitem = new HisTagMessage() { Id = vv.Id, Type = (uint)vv.Type, TagType = (uint)vv.TagType, CompressType = (uint)vv.CompressType,Circle=(uint)vv.Circle,MaxValueCountPerSecond=(uint)vv.MaxValueCountPerSecond };
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    IEnumerable<Cdy.Tag.Tagbase> htags = db.RealDatabase.Tags.Values.Where(e=>e.Parent=="");
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
                        re.Add(ConvertToMessage(vv));
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
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
        /// 清除某个变量组内所有变量
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
                    foreach (var vv in db.RealDatabase.GetTagsByGroup(request.GroupFullName))
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);
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
                        DbManager.Instance.CheckAndContinueLoadDatabase(db);
                        {
                            Cdy.Tag.HisTag hisTag = new Cdy.Tag.HisTag();
                            hisTag.Id = (int)vtag.Id;
                            hisTag.TagType = (Cdy.Tag.TagType)(vtag.TagType);
                            hisTag.Type = (Cdy.Tag.RecordType)(vtag.Type);
                            hisTag.CompressType = (int)(vtag.CompressType);
                            hisTag.Circle = (int)vtag.Circle;
                            hisTag.MaxValueCountPerSecond = (short)vtag.MaxValueCountPerSecond;
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
                        DbManager.Instance.CheckAndContinueLoadDatabase(db);

                        if(!string.IsNullOrEmpty(tag.Parent))
                        {
                            int id= int.Parse(tag.Parent);
                            if(db.RealDatabase.Tags.ContainsKey(id))
                            {
                                tag.FullName = db.RealDatabase.Tags[id].FullName + "." + tag.Name;
                            }
                        }

                        if (tag is ComplexTag)
                        {
                            CheckRenameComplexTag(tag as ComplexTag,db.RealDatabase);
                        }

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
        /// <param name="tag"></param>
        /// <param name="database"></param>
        private void CheckRenameComplexTag(ComplexTag tag,RealDatabase database)
        {
            IEnumerable<Tagbase> tags=null;
            if(tag.Tags.Count==0)
            {
                tags = database.Tags.Where(e=>e.Value.Parent==tag.Id.ToString()).Select(e=>e.Value);
            }
            else
            {
                tags = tag.Tags.Values;
            }
            if(tags!=null)
            {
                foreach (var vv in tags)
                {
                    string sname = vv.FullName;
                    if (!string.IsNullOrEmpty(sname) && database.NamedTags.ContainsKey(sname))
                    {
                        database.NamedTags.Remove(sname);
                        vv.FullName = tag.FullName + "." + vv.Name;
                        if (!database.NamedTags.ContainsKey(vv.FullName))
                        {
                            database.NamedTags.Add(vv.FullName, vv);
                        }
                        if (vv is ComplexTag)
                        {
                            CheckRenameComplexTag(vv as ComplexTag, database);
                        }
                    }
                }
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
                case (uint)(Cdy.Tag.TagType.IntPoint):
                    re = new Cdy.Tag.IntPointTag();
                    break;
                case (uint)(Cdy.Tag.TagType.UIntPoint):
                    re = new Cdy.Tag.UIntPointTag();
                    break;
                case (uint)(Cdy.Tag.TagType.IntPoint3):
                    re = new Cdy.Tag.IntPoint3Tag();
                    break;
                case (uint)(Cdy.Tag.TagType.UIntPoint3):
                    re = new Cdy.Tag.UIntPoint3Tag();
                    break;
                case (uint)(Cdy.Tag.TagType.LongPoint):
                    re = new Cdy.Tag.LongPointTag();
                    break;
                case (uint)(Cdy.Tag.TagType.ULongPoint):
                    re = new Cdy.Tag.ULongPointTag();
                    break;
                case (uint)(Cdy.Tag.TagType.LongPoint3):
                    re = new Cdy.Tag.LongPoint3Tag();
                    break;
                case (uint)(Cdy.Tag.TagType.ULongPoint3):
                    re = new Cdy.Tag.ULongPoint3Tag();
                    break;
                case (uint)(Cdy.Tag.TagType.Complex):
                    re = new Cdy.Tag.ComplexTag();
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
                re.Unit = tmsg.Unit;
                re.ExtendField1 = tmsg.ExtendField1;
                re.Area= tmsg.Area;
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

                if(re is ComplexTag)
                {
                    (re as ComplexTag).LinkComplexClass = tmsg.LinkComplexClass;
                }
                re.Parent = tmsg.Parent;
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
                        DbManager.Instance.CheckAndContinueLoadDatabase(db);
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
                        if (vtag.Id != int.MaxValue)
                        {
                            Cdy.Tag.HisTag hisTag = new Cdy.Tag.HisTag();
                            hisTag.Id = (int)tag.Id;
                            hisTag.TagType = (Cdy.Tag.TagType)(vtag.TagType);
                            hisTag.Type = (Cdy.Tag.RecordType)(vtag.Type);
                            hisTag.CompressType = (int)(vtag.CompressType);
                            hisTag.Circle = (int)vtag.Circle;
                            hisTag.MaxValueCountPerSecond = (short)vtag.MaxValueCountPerSecond;
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
        public override Task<GetAvaiableTagNameReply> GetAvaiableTagName(GetAvaiableTagNameRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetAvaiableTagNameReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                {
                    string baseName = "tag";
                    if(!string.IsNullOrEmpty(request.Name))
                    {
                        baseName = request.Name;
                    }
                    var vtmps = db.RealDatabase.GetTagsByGroup(request.Group).Select(e => e.Name).ToList();
                    string tagName = baseName;

                    int number = GetNumberInt(baseName);
                    if (number >= 0)
                    {
                        if (tagName.EndsWith(number.ToString()))
                        {
                            tagName = tagName.Substring(0, tagName.IndexOf(number.ToString()));
                        }
                    }
                    string sname = tagName;
                    for (int i = 1; i < int.MaxValue; i++)
                    {
                        tagName = sname + i;
                        if (!vtmps.Contains(tagName))
                        {
                            return Task.FromResult(new GetAvaiableTagNameReply() { Name = tagName, Result = true });
                        }
                    }
                    return Task.FromResult(new GetAvaiableTagNameReply() { Name = tagName, Result = true });
                }
            }
            return Task.FromResult(new GetAvaiableTagNameReply() { Result = false });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetAvaiableTagNameReply> GetAvaiableClassTagName(GetAvaiableClassTagNameRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetAvaiableTagNameReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                lock (db)
                {
                    string baseName = "tag";
                    if (!string.IsNullOrEmpty(request.Name))
                    {
                        baseName = request.Name;
                    }

                    var vtg = db.ComplexTagClass.Class.ContainsKey(request.Class) ? db.ComplexTagClass.Class[request.Class] : null;
                    if (vtg != null)
                    {
                        var vtmps = vtg.Tags.Values.Select(e => e.Name).ToList();

                        string tagName = baseName;

                        int number = GetNumberInt(baseName);
                        if (number >= 0)
                        {
                            if (tagName.EndsWith(number.ToString()))
                            {
                                tagName = tagName.Substring(0, tagName.IndexOf(number.ToString()));
                            }
                        }
                        string sname = tagName;
                        for (int i = 1; i < int.MaxValue; i++)
                        {
                            tagName = sname + i;
                            if (!vtmps.Contains(tagName))
                            {
                                return Task.FromResult(new GetAvaiableTagNameReply() { Name = tagName, Result = true });
                            }
                        }
                        return Task.FromResult(new GetAvaiableTagNameReply() { Name = tagName, Result = true });
                    }
                }
            }
            return Task.FromResult(new GetAvaiableTagNameReply() { Result = false });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> CheckClassTagNameExist(GetAvaiableClassTagNameRequest request, ServerCallContext context)
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
                    var vtg = db.ComplexTagClass.Class.ContainsKey(request.Class) ? db.ComplexTagClass.Class[request.Class] : null;
                    if (vtg != null)
                    {
                        var vtmps = vtg.Tags.Values.Select(e => e.Name).ToList();
                        return Task.FromResult(new BoolResultReplay() { Result = vtmps != null & vtmps.Contains(request.Name) });
                    }
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = false });
        }

        /// <summary>
        /// 获取字符串中的数字
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>数字</returns>
        public static int GetNumberInt(string str)
        {
            int result = -1;
            if (str != null && str != string.Empty)
            {
                // 正则表达式剔除非数字字符（不包含小数点.）
                str = Regex.Replace(str, @"[^\d.\d]", "");
                // 如果是数字，则转换为decimal类型
                if (Regex.IsMatch(str, @"^[+-]?\d*[.]?\d*$"))
                {
                    try
                    {
                        if (!string.IsNullOrEmpty(str))
                            result = int.Parse(str);
                    }
                    catch
                    {

                    }
                }
            }
            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> CheckTagNameExist(GetAvaiableTagNameRequest request, ServerCallContext context)
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
                    var vtmps = db.RealDatabase.GetTagsByGroup(request.Group).Select(e => e.Name).ToList();
                    return Task.FromResult(new BoolResultReplay() { Result = vtmps != null & vtmps.Contains(request.Name) });
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = false });
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
                        DbManager.Instance.CheckAndContinueLoadDatabase(db);
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
                            if (vtag.Id != int.MaxValue)
                            {
                                Cdy.Tag.HisTag hisTag = new Cdy.Tag.HisTag();
                                hisTag.Id = (int)tag.Id;
                                hisTag.TagType = (Cdy.Tag.TagType)(vtag.TagType);
                                hisTag.Type = (Cdy.Tag.RecordType)(vtag.Type);
                                hisTag.CompressType = (int)(vtag.CompressType);
                                hisTag.Circle = (int)vtag.Circle;
                                hisTag.MaxValueCountPerSecond = (short)vtag.MaxValueCountPerSecond;
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
                            hisTag.Id = (int)tag.Id;
                            hisTag.TagType = (Cdy.Tag.TagType)(vtag.TagType);
                            hisTag.Type = (Cdy.Tag.RecordType)(vtag.Type);
                            hisTag.CompressType = (int)(vtag.CompressType);
                            hisTag.Circle = (int)vtag.Circle;
                            hisTag.MaxValueCountPerSecond = (short)vtag.MaxValueCountPerSecond;
                       
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
                            if (vtag.Id != int.MaxValue)
                            {
                                Cdy.Tag.HisTag hisTag = new Cdy.Tag.HisTag();
                                hisTag.Id = (int)tag.Id;
                                hisTag.TagType = (Cdy.Tag.TagType)(vtag.TagType);
                                hisTag.Type = (Cdy.Tag.RecordType)(vtag.Type);
                                hisTag.CompressType = (int)(vtag.CompressType);
                                hisTag.Circle = (int)vtag.Circle;
                                hisTag.MaxValueCountPerSecond = (short)vtag.MaxValueCountPerSecond;
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
                Driver dd = new Driver() { Name = vd.Key,EditType=vd.Value.EditType };
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
                    DbManager.Instance.CheckAndContinueLoadDatabase(db);

                    CheckAndRemoveEmptyHisTags(db);
                    Cdy.Tag.DatabaseSerise serise = new Cdy.Tag.DatabaseSerise() { Dbase = db };
                    serise.Save();
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        private void CheckAndRemoveEmptyHisTags(Database db)
        {
            List<int> removes = new List<int>();
            foreach(var vv in db.HisDatabase.HisTags)
            {
                if(!db.RealDatabase.Tags.ContainsKey((int)vv.Key))
                {
                    removes.Add((int)vv.Key);
                }
            }

            foreach(var vv in removes)
            {
                db.HisDatabase.RemoveHisTag(vv);
            }

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
                DbManager.Instance.Reload(request.Database);
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> SetRealDataServerPort(SetRealDataServerPortRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if(db!=null)
            {
                db.Setting.RealDataServerPort = request.Port;
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
        public override Task<IntResultReplay> GetRealDataServerPort(DatabasesRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new IntResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                return Task.FromResult(new IntResultReplay() { Result = true,Value= db.Setting.RealDataServerPort });
            }
            return Task.FromResult(new IntResultReplay() { Result = false });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetDriverSettingReplay> GetDriverSetting(GetDriverSettingRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetDriverSettingReplay() { Result = false });
            }
            if(DriverManager.Manager.Drivers.ContainsKey(request.Driver))
            {
                var re = new GetDriverSettingReplay() {  Result = true };
                var dd = DriverManager.Manager.Drivers[request.Driver];
                var config = dd.GetConfig(request.Database);
                if(config!=null)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach(var vv in config)
                    {
                        sb.Append(vv.Key + ":" + vv.Value + ",");
                    }
                    sb.Length = sb.Length > 0 ? sb.Length - 1 : sb.Length;
                    re.SettingString = sb.ToString();
                }
                return Task.FromResult(re);
            }
            return Task.FromResult(new GetDriverSettingReplay() { Result = false });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> UpdateDrvierSetting(UpdateDrvierSettingRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            if (DriverManager.Manager.Drivers.ContainsKey(request.Driver))
            {
                var dd = DriverManager.Manager.Drivers[request.Driver];
                string sval = request.SettingString;
                if(!string.IsNullOrEmpty(sval))
                {
                    Dictionary<string, string> dtmp = new Dictionary<string, string>();
                    string[] ss = sval.Split(new char[] { ',' });
                    foreach(var vv in ss)
                    {
                        string[] svv = vv.Split(new char[] { ':' });
                        if(!dtmp.ContainsKey(svv[0]))
                        {
                            dtmp.Add(svv[0], svv[1]);
                        }
                    }
                    dd.UpdateConfig(request.Database, dtmp);
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
        public override Task<GetDatabaseHisSettingReplay> GetDatabaseHisSetting(DatabasesRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetDatabaseHisSettingReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                GetDatabaseHisSettingReplay re = new GetDatabaseHisSettingReplay() { DataPath = db.HisDatabase.Setting.HisDataPathPrimary, BackDataPath = db.HisDatabase.Setting.HisDataPathBack, KeepTime = db.HisDatabase.Setting.HisDataKeepTimeInPrimaryPath,KeepNoZipFileDays=db.HisDatabase.Setting.KeepNoZipFileDays };
                return Task.FromResult(re);
            }
            return Task.FromResult(new GetDatabaseHisSettingReplay() { Result = false });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> UpdateDatabaseHisSetting(UpdateDatabaseHisSettingRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                db.HisDatabase.Setting.HisDataPathPrimary = request.DataPath;
                db.HisDatabase.Setting.HisDataPathBack = request.BackDataPath;
                db.HisDatabase.Setting.HisDataKeepTimeInPrimaryPath = request.KeepTime;
                db.HisDatabase.Setting.KeepNoZipFileDays = request.KeepNoZipFileDays;
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
        public override Task<BoolResultReplay> SetTagId(SetTagIdRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {

                if(db.RealDatabase.Tags.ContainsKey(request.TagID)&&!db.RealDatabase.Tags.ContainsKey(request.Value))
                {
                    var vtag = db.RealDatabase.Tags[request.TagID];

                    db.RealDatabase.Tags.Remove(vtag.Id);
                    vtag.Id = request.Value;
                    db.RealDatabase.Tags.Add(vtag.Id, vtag);
                    if (vtag.Id >= db.RealDatabase.MaxId)
                    {
                        db.RealDatabase.MaxId = vtag.Id;
                    }
                    return Task.FromResult(new BoolResultReplay() { Result = true });
                }
                else
                {
                    return Task.FromResult(new BoolResultReplay() { Result = false });
                }
            }
            return Task.FromResult(new BoolResultReplay() { Result = false });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<ResetTagIdReplay> ResetTagId(ResetTagIdRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new ResetTagIdReplay() { Result = false });
            }

            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                Dictionary<int, int> res = new Dictionary<int, int>();
                ResetTagIdReplay rep = new ResetTagIdReplay() { Result = true };
                foreach(var vv in request.TagIds.OrderBy(e=>e))
                {
                    bool ishase = false;

                    int endindex = vv < request.StartId ? int.MaxValue : vv;

                    for(int i=request.StartId;i< endindex; i++)
                    {
                        if(!db.RealDatabase.Tags.ContainsKey(i))
                        {
                            ishase = true;
                            var rtag = db.RealDatabase.Tags[vv];
                            db.RealDatabase.Tags.Remove(vv);

                            rtag.Id = i;
                            db.RealDatabase.Tags.Add(i, rtag);

                            if(rtag.Id>=db.RealDatabase.MaxId)
                            {
                                db.RealDatabase.MaxId = rtag.Id;
                            }


                            if(db.HisDatabase.HisTags.ContainsKey(vv))
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
                    if(!ishase)
                    res.Add(vv, vv);
                }

                rep.TagIds.AddRange(res.Select(e => new IntKeyValueMessage() { Key = e.Key, Value = e.Value }));
                return Task.FromResult(rep);
            }
            return Task.FromResult(new ResetTagIdReplay() { Result = false });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> UpdateProxyApiSetting(UpdateProxyApiSettingRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if(db!=null)
            {
                db.Setting.EnableWebApi = request.WebApi;
                db.Setting.EnableGrpcApi = request.GrpcApi;
                db.Setting.EnableHighApi = request.HighApi;
                db.Setting.EnableOpcServer = request.OpcServer;
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
        public override Task<GetProxyApiSettingReply> GetProxyApiSetting(DatabasesRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new GetProxyApiSettingReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                GetProxyApiSettingReply re = new GetProxyApiSettingReply();
                re.WebApi = db.Setting.EnableWebApi;
                re.GrpcApi = db.Setting.EnableGrpcApi;
                re.HighApi = db.Setting.EnableHighApi;
                re.OpcServer = db.Setting.EnableOpcServer;
                return Task.FromResult<GetProxyApiSettingReply>(re);
            }
            return Task.FromResult(new GetProxyApiSettingReply() { Result = false });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<ExtendFunctionReplay> ExtendFunction(GetRequest request, ServerCallContext context)
        {
            if (!CheckLoginId(request.LoginId, request.Database))
            {
                return Task.FromResult(new ExtendFunctionReplay() { Result = false });
            }

            ExtendFunctionReplay re = new ExtendFunctionReplay();
            re.Spider = HasExtendFunction("Spider");
            re.Ant = HasExtendFunction("Ant");
            re.Result = true;

            return Task.FromResult(re);
        }

        private bool HasExtendFunction(string name)
        {
            var vpath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), name);
            if(System.IO.Directory.Exists(vpath))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

    }
}
