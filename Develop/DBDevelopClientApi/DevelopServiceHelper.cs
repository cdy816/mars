//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/3/28 22:49:54.
//  Version 1.0
//  种道洋
//==============================================================

using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Linq;
using Cdy.Tag;
using System.Data.Common;

namespace DBDevelopClientApi
{
    /// <summary>
    /// 
    /// </summary>
    public class DevelopServiceHelper
    {

        #region ... Variables  ...
        
        public static DevelopServiceHelper Helper = new DevelopServiceHelper();

        private DBDevelopService.DevelopServer.DevelopServerClient mCurrentClient;

        private string mLoginId = string.Empty;

        private object mLockObj = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public bool UseTls
        {
            get;
            set;
        } = true;

        /// <summary>
        /// 
        /// </summary>
        public Action OfflineCallBack { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ip"></param>
        private DBDevelopService.DevelopServer.DevelopServerClient GetServicClient(string ip)
        {
            try
            {
               

                var httpClientHandler = new HttpClientHandler();
                httpClientHandler.ServerCertificateCustomValidationCallback =
                    HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
                
                var httpClient = new HttpClient(httpClientHandler);

                Grpc.Net.Client.GrpcChannel grpcChannel;
                if (UseTls)
                {
                    grpcChannel = Grpc.Net.Client.GrpcChannel.ForAddress(@"https://" + ip + ":5001", new GrpcChannelOptions { HttpClient = httpClient });
                }
                else
                {

                    //AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
                    grpcChannel = Grpc.Net.Client.GrpcChannel.ForAddress(@"http://" + ip + ":5001", new GrpcChannelOptions { HttpClient = httpClient });
                    //grpcChannel = Grpc.Net.Client.GrpcChannel.ForAddress(@"http://" + ip + ":5001");
                }
                return new DBDevelopService.DevelopServer.DevelopServerClient(grpcChannel);
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("DevelopService", ex.Message);
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
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.Save(new DBDevelopService.GetRequest() { Database = database, LoginId = mLoginId }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool CheckOpenDatabase(string database)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.CheckOpenDatabase(new DBDevelopService.DatabasesRequest() { Database = database, LoginId = mLoginId }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool StartDatabase(string database)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.Start(new DBDevelopService.DatabasesRequest() { Database = database, LoginId = mLoginId }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool StopDatabase(string database)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.Stop(new DBDevelopService.DatabasesRequest() { Database = database, LoginId = mLoginId }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool ReRunDatabase(string database)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.ReRun(new DBDevelopService.DatabasesRequest() { Database = database, LoginId = mLoginId }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool CancelToSaveDatabase(string database)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.Cancel(new DBDevelopService.GetRequest() { Database = database, LoginId = mLoginId }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool IsDatabaseRunning(string database)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.IsDatabaseRunning(new DBDevelopService.DatabasesRequest() { Database = database, LoginId = mLoginId }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool IsDatabaseDirty(string database)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.IsDatabaseDirty(new DBDevelopService.DatabasesRequest() { Database = database, LoginId = mLoginId }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 放弃更改
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool Cancel(string database)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.Cancel(new DBDevelopService.GetRequest() { Database = database, LoginId = mLoginId }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ip"></param>
        /// <param name="user"></param>
        /// <param name="pass"></param>
        /// <returns></returns>
        public string Login(string ip,string user,string pass)
        {
            mCurrentClient = GetServicClient(ip);
            if (mCurrentClient != null)
            {
                try
                {
                    var lres = mCurrentClient.Login(new DBDevelopService.LoginRequest() { UserName = user, Password = pass });            //var sid = await client.LoginAsync(new DBDevelopService.LoginRequest() { UserName = "admin", Password = "12345", Database = "local" });
                    if (lres != null)
                    {
                        mLoginId = lres.LoginId;
                        return lres.LoginId;
                    }
                }
                catch(Exception ex)
                {
                    LoggerService.Service.Erro("DevelopService", ex.Message);
                }
            }
            return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Dictionary<string,string> ListDatabase()
        {
            var re = new Dictionary<string, string>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var vv = mCurrentClient.QueryDatabase(new DBDevelopService.QueryDatabaseRequest() { LoginId = mLoginId }).Database.ToList();
                    foreach(var vvv in vv)
                    {
                        re.Add(vvv.Key, vvv.Value);
                    }
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="desc"></param>
        /// <returns></returns>
        public bool NewDatabase(string name,string desc)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.NewDatabase(new DBDevelopService.NewDatabaseRequest() { Database = name, LoginId = mLoginId, Desc = string.IsNullOrEmpty(desc) ? "" : desc }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<Tuple<string,string,string>> QueryTagGroups(string database)
        {
            List<Tuple<string, string, string>> re = new List<Tuple<string, string, string>>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    foreach (var vv in mCurrentClient.GetTagGroup(new DBDevelopService.GetRequest() { Database = database, LoginId = mLoginId }).Group)
                    {
                        re.Add(new Tuple<string, string, string>(vv.Name, vv.Description, vv.Parent));
                    }
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public List<Tuple<string, string>> QueryTagClass(string database)
        {
            List<Tuple<string, string>> re = new List<Tuple<string, string>>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    foreach (var vv in mCurrentClient.GetTagClass(new DBDevelopService.GetRequest() { Database = database, LoginId = mLoginId }).Classes)
                    {
                        re.Add(new Tuple<string, string>(vv.Name, vv.Description));
                    }
                }
            }
            catch(Exception ex)
            {
                Logout();
            }
            return re;
        }


        #region Develop User

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Dictionary<string,Tuple<bool,bool, List<string>>> GetUsers()
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    Dictionary<string, Tuple<bool, bool, List<string>>> dd = new Dictionary<string, Tuple<bool, bool, List<string>>>();
                    var re = mCurrentClient.GetUsers(new DBDevelopService.GetRequest() { LoginId = mLoginId }).Users;
                    foreach (var vv in re)
                    {

                        dd.Add(vv.UserName, new Tuple<bool, bool, List<string>>(vv.IsAdmin, vv.NewDatabase, vv.Databases.ToList()));
                    }
                    return dd;
                }
            }
            catch
            {
                Logout();
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public bool AddUser(string name, string password,bool isadmin,bool cannewdatabase)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.NewUser(new DBDevelopService.NewUserRequest() { LoginId = mLoginId, UserName = name, Password = password, IsAdmin = isadmin, NewDatabasePermission = cannewdatabase }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="oldname"></param>
        /// <param name="newname"></param>
        /// <returns></returns>
        public bool ReNameUser(string oldname,string newname)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.ReNameUser(new DBDevelopService.ReNameUserRequest() { LoginId = mLoginId, OldName = oldname, NewName = newname }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool IsAdmin()
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.IsAdmin(new DBDevelopService.GetRequest() { LoginId = mLoginId }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool CanNewDatabase()
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.CanNewDatabase(new DBDevelopService.GetRequest() { LoginId = mLoginId }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool RemoveUser(string name)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.RemoveUser(new DBDevelopService.RemoveUserRequest() { LoginId = mLoginId, UserName = name }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="permissions"></param>
        /// <returns></returns>
        public bool UpdateUser(string name,bool isadmin,bool cannewdatabase,List<string> databases)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var req = new DBDevelopService.UpdateUserRequest() { LoginId = mLoginId, UserName = name, IsAdmin = isadmin, NewDatabasePermission = cannewdatabase };
                    req.Database.AddRange(databases);
                    return mCurrentClient.UpdateUser(req).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 更新用户密码
        /// </summary>
        /// <param name="name"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public bool UpdateUserPassword(string name,string password)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.UpdateUserPassword(new DBDevelopService.UpdatePasswordRequest() { LoginId = mLoginId, UserName = name, Password = password }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 设置服务器用户密码
        /// </summary>
        /// <param name="name">用户</param>
        /// <param name="password">密码</param>
        /// <returns></returns>
        public bool ModifyUserPassword(string name,string password,string newpassword)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.ModifyPassword(new DBDevelopService.ModifyPasswordRequest() { LoginId = mLoginId, UserName = name, Password = password, Newpassword = newpassword }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<string> GetAllDatabase()
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.QueryDatabase(new DBDevelopService.QueryDatabaseRequest() { LoginId = mLoginId }).Database.ToList().Select(e => e.Key).ToList();
                }
            }
            catch
            {
                Logout();
            }
            return null;
        }
        
        #endregion

        #region DatabaseUser

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public Dictionary<string, string> QueryDatabaseUserGroups(string database)
        {
            Dictionary<string, string> re = new Dictionary<string, string>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    foreach (var vv in mCurrentClient.GetDatabaseUserGroup(new DBDevelopService.GetRequest() { Database = database, LoginId = mLoginId }).Group)
                    {
                        re.Add(vv.Name, vv.Parent);
                    }
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }

        public List<string> GetAllDatabaseUserNames(string database)
        {
            List<string> re = new List<string>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var groups = QueryDatabaseUserGroups(database).Keys.ToList();
                    groups.Add("");
                    foreach (var vvg in groups)
                    {
                        foreach (var vv in mCurrentClient.GetDatabaseUserByGroup(new DBDevelopService.GetDatabaseUserByGroupRequest() { Database = database, LoginId = mLoginId, Group = vvg }).Users)
                        {
                            //Cdy.Tag.UserItem user = new Cdy.Tag.UserItem() { Name = vv.UserName, Group = vv.Group };
                            //user.Permissions.AddRange(vv.Permission.ToArray());
                            re.Add(vv.UserName);
                        }
                    }
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="group"></param>
        /// <returns></returns>
        public List<Cdy.Tag.UserItem> GetUsersByGroup(string database,string group)
        {
            List<Cdy.Tag.UserItem> re = new List<Cdy.Tag.UserItem>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    foreach (var vv in mCurrentClient.GetDatabaseUserByGroup(new DBDevelopService.GetDatabaseUserByGroupRequest() { Database = database, LoginId = mLoginId, Group = group }).Users)
                    {
                        Cdy.Tag.UserItem user = new Cdy.Tag.UserItem() { Name = vv.UserName, Group = vv.Group };
                        user.Permissions.AddRange(vv.Permission.ToArray());
                        re.Add(user);
                    }
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public List<Cdy.Tag.UserPermission> GetAllDatabasePermission(string database)
        {
            List<Cdy.Tag.UserPermission> re = new List<Cdy.Tag.UserPermission>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    foreach (var vv in mCurrentClient.GetAllDatabasePermission(new DBDevelopService.GetAllDatabasePermissionRequest() { Database = database, LoginId = mLoginId }).Permission)
                    {
                        Cdy.Tag.UserPermission user = new Cdy.Tag.UserPermission() { Name = vv.Name, Group = vv.Group.ToList(), Desc = vv.Desc, EnableWrite = vv.EnableWrite, SuperPermission = vv.SuperPermission };
                        re.Add(user);
                    }
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool RemoveDatabasePermission(string database,string name)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.RemoveDatabasePermission(new DBDevelopService.RemoveDatabasePermissionRequest() { Database = database, LoginId = mLoginId, Permission = name }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="permission"></param>
        /// <returns></returns>
        public bool UpdateDatabasePermission(string database,Cdy.Tag.UserPermission permission)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var req = new DBDevelopService.DatabasePermissionRequest();
                    req.Database = database;
                    req.LoginId = mLoginId;
                    req.Permission = new DBDevelopService.DatabasePermission() { Name = permission.Name, Desc = permission.Desc, EnableWrite = permission.EnableWrite };
                    if (permission.Group != null)
                        req.Permission.Group.Add(permission.Group);
                    return mCurrentClient.UpdateDatabasePermission(req).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="name"></param>
        /// <param name="parentName"></param>
        public bool AddDatabaseUserGroup(string database, string name, string parentName)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.AddDatabaseUserGroup(new DBDevelopService.AddGroupRequest() { Database = database, LoginId = mLoginId, Name = name, ParentName = parentName }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="oldName"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public bool ReNameDatabaseUserGroup(string database, string oldName, string newName)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.RenameDatabaseUserGroup(new DBDevelopService.RenameGroupRequest() { Database = database, LoginId = mLoginId, OldFullName = oldName, NewName = newName }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool RemoveDatabaseUserGroup(string database, string name)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.RemoveDatabaseUserGroup(new DBDevelopService.RemoveGroupRequest() { Database = database, LoginId = mLoginId, Name = name }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="user"></param>
        /// <returns></returns>
        public bool UpdateDatabaseUser(string database,Cdy.Tag.UserItem user)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var req = new DBDevelopService.UpdateDatabaseUserRequest() { Database = database, LoginId = mLoginId, UserName = user.Name, Group = user.Group };
                    if (user.Permissions != null)
                        req.Permission.AddRange(user.Permissions);
                    return mCurrentClient.UpdateDatabaseUser(req).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public bool UpdateDatabaseUserPassword(String database ,string user,string password)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var req = new DBDevelopService.ModifyDatabaseUserPasswordRequest() { Database = database, LoginId = mLoginId, UserName = user, Password = password };
                    return mCurrentClient.ModifyDatabaseUserPassword(req).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool RemoveDatabaseUser(string database, string name)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.RemoveDatabaseUser(new DBDevelopService.RemoveByNameRequest() { Database = database, LoginId = mLoginId, Name = name }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        #endregion


        #region database permission

        #endregion

        public void QueryAllTag(string database,Action<int,int,Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>>> callback)
        {
           
            if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                int idx = 0;
                int count = 0;
                do
                {
                    try
                    {
                        Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> re = new Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>>();
                        var result = mCurrentClient.GetAllTag(new DBDevelopService.GetTagByGroupRequest() { Database = database, LoginId = mLoginId, Index = idx });

                        idx = result.Index;
                        count = result.Count;

                        if (!result.Result) break;

                        Dictionary<int, Cdy.Tag.Tagbase> mRealTag = new Dictionary<int, Cdy.Tag.Tagbase>();
                        foreach (var vv in result.RealTag)
                        {
                            //var tag = GetTag((int)vv.TagType);
                            //tag.Id = (int)vv.Id;
                            //tag.LinkAddress = vv.LinkAddress;
                            //tag.Name = vv.Name;
                            //tag.Desc = vv.Desc;
                            //tag.Group = vv.Group;
                            //tag.ReadWriteType = (Cdy.Tag.ReadWriteMode)vv.ReadWriteMode;
                            //tag.Conveter = !string.IsNullOrEmpty(vv.Convert) ? vv.Convert.DeSeriseToValueConvert() : null;
                            //if (tag is Cdy.Tag.NumberTagBase)
                            //{
                            //    (tag as Cdy.Tag.NumberTagBase).MaxValue = vv.MaxValue;
                            //    (tag as Cdy.Tag.NumberTagBase).MinValue = vv.MinValue;
                            //}

                            //if (tag is Cdy.Tag.FloatingTagBase)
                            //{
                            //    (tag as Cdy.Tag.FloatingTagBase).Precision = (byte)vv.Precision;
                            //}
                            //if (tag is ComplexTag)
                            //{
                            //    (tag as ComplexTag).LinkComplexClass = vv.LinkComplexClass;
                            //}
                            //mRealTag.Add(tag.Id, tag);
                            var tag = TagMessageToTag(vv);
                            mRealTag.Add(tag.Id, tag);
                        }

                        Dictionary<int, Cdy.Tag.HisTag> mHisTag = new Dictionary<int, Cdy.Tag.HisTag>();
                        foreach (var vv in result.HisTag)
                        {
                            var tag = new Cdy.Tag.HisTag { Id = (int)vv.Id, TagType = (Cdy.Tag.TagType)vv.TagType, Type = (Cdy.Tag.RecordType)vv.Type, CompressType = (int)vv.CompressType };
                            tag.Circle = (int)vv.Circle;
                            tag.MaxValueCountPerSecond = (short)vv.MaxValueCountPerSecond;
                            if (vv.Parameter.Count > 0)
                            {
                                tag.Parameters = new Dictionary<string, double>();
                                foreach (var vvv in vv.Parameter)
                                {
                                    tag.Parameters.Add(vvv.Name, vvv.Value);
                                }

                            }
                            mHisTag.Add(tag.Id, tag);
                        }

                        foreach (var vv in mRealTag)
                        {
                            if (mHisTag.ContainsKey(vv.Key))
                            {
                                re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], mHisTag[vv.Key]));
                            }
                            else
                            {
                                re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], null));
                            }
                        }
                        callback(idx, count, re);
                        idx++;
                    }
                    catch
                    {
                        Logout();
                        break;
                    }
                }
                while (idx < count);
            }
       
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> QueryAllTag(string database)
        {
            Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> re = new Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    int idx = 0;
                    int count = 0;
                    do
                    {
                        var result = mCurrentClient.GetAllTag(new DBDevelopService.GetTagByGroupRequest() { Database = database, LoginId = mLoginId, Index = idx });

                        idx = result.Index;
                        count = result.Count;

                        if (!result.Result) break;

                        Dictionary<int, Cdy.Tag.Tagbase> mRealTag = new Dictionary<int, Cdy.Tag.Tagbase>();
                        foreach (var vv in result.RealTag)
                        {
                            //var tag = GetTag((int)vv.TagType);
                            //tag.Id = (int)vv.Id;
                            //tag.LinkAddress = vv.LinkAddress;
                            //tag.Name = vv.Name;
                            //tag.Desc = vv.Desc;
                            //tag.Group = vv.Group;
                            //tag.ReadWriteType = (Cdy.Tag.ReadWriteMode)vv.ReadWriteMode;
                            //tag.Conveter = !string.IsNullOrEmpty(vv.Convert) ? vv.Convert.DeSeriseToValueConvert() : null;
                            //if (tag is Cdy.Tag.NumberTagBase)
                            //{
                            //    (tag as Cdy.Tag.NumberTagBase).MaxValue = vv.MaxValue;
                            //    (tag as Cdy.Tag.NumberTagBase).MinValue = vv.MinValue;
                            //}

                            //if (tag is Cdy.Tag.FloatingTagBase)
                            //{
                            //    (tag as Cdy.Tag.FloatingTagBase).Precision = (byte)vv.Precision;
                            //}

                            //if(tag is ComplexTag)
                            //{
                            //    (tag as ComplexTag).LinkComplexClass = vv.LinkComplexClass;
                            //}

                            //mRealTag.Add(tag.Id, tag);
                            var tag = TagMessageToTag(vv);
                            mRealTag.Add(tag.Id, tag);
                        }

                        Dictionary<int, Cdy.Tag.HisTag> mHisTag = new Dictionary<int, Cdy.Tag.HisTag>();
                        foreach (var vv in result.HisTag)
                        {
                            var tag = new Cdy.Tag.HisTag { Id = (int)vv.Id, TagType = (Cdy.Tag.TagType)vv.TagType, Type = (Cdy.Tag.RecordType)vv.Type, CompressType = (int)vv.CompressType };
                            tag.Circle = (int)vv.Circle;
                            tag.MaxValueCountPerSecond = (short)vv.MaxValueCountPerSecond;
                            if (vv.Parameter.Count > 0)
                            {
                                tag.Parameters = new Dictionary<string, double>();
                                foreach (var vvv in vv.Parameter)
                                {
                                    tag.Parameters.Add(vvv.Name, vvv.Value);
                                }

                            }
                            mHisTag.Add(tag.Id, tag);
                        }

                        foreach (var vv in mRealTag)
                        {
                            if (mHisTag.ContainsKey(vv.Key))
                            {
                                re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], mHisTag[vv.Key]));
                            }
                            else
                            {
                                re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], null));
                            }
                        }

                        idx++;
                    }
                    while (idx < count);
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="group"></param>
        /// <param name="callback"></param>
        public void QueryTagByGroup(string database, string group, Action<int, int, Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>>> callback, Dictionary<string, string> mFilters = null)
        {
            int idx = 0;
            int totalcount=0;
            int tagcount;
            do
            {
                try
                {
                    var re = QueryTagByGroup(database, group, idx, out totalcount, out tagcount, mFilters);
                    callback(idx, totalcount, re);
                    idx++;
                }
                catch
                {
                    Logout();
                    break;
                }
            }
            while (idx < totalcount);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="group"></param>
        /// <param name="totalCount"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> QueryTagByGroup(string database, string group, int index, out int totalCount,out int tagCount,Dictionary<string,string> mFilters=null)
        {
            Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> re = new Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    int idx = index;
                    var req = new DBDevelopService.GetTagByGroupRequest() { Database = database, LoginId = mLoginId, Group = group, Index = idx };
                    if (mFilters != null)
                    {
                        foreach (var vv in mFilters)
                        {
                            req.Filters.Add(new DBDevelopService.FilterMessageItem() { Key = vv.Key, Value = vv.Value });
                        }
                    }

                    var result = mCurrentClient.GetTagByGroup(req);
                    tagCount = result.TagCount;

                    totalCount = result.Count;

                    if (result.Result)
                    {
                        Dictionary<int, Cdy.Tag.Tagbase> mRealTag = new Dictionary<int, Cdy.Tag.Tagbase>();
                        foreach (var vv in result.RealTag)
                        {
                            var tag = TagMessageToTag(vv);
                            mRealTag.Add(tag.Id, tag);
                        }

                        Dictionary<int, Cdy.Tag.HisTag> mHisTag = new Dictionary<int, Cdy.Tag.HisTag>();
                        foreach (var vv in result.HisTag)
                        {
                            var tag = new Cdy.Tag.HisTag { Id = (int)vv.Id, TagType = (Cdy.Tag.TagType)vv.TagType, Type = (Cdy.Tag.RecordType)vv.Type, CompressType = (int)vv.CompressType };
                            tag.Circle = (int)vv.Circle;
                            tag.MaxValueCountPerSecond = (short)vv.MaxValueCountPerSecond;
                            if (vv.Parameter.Count > 0)
                            {
                                tag.Parameters = new Dictionary<string, double>();
                                foreach (var vvv in vv.Parameter)
                                {
                                    tag.Parameters.Add(vvv.Name, vvv.Value);
                                }

                            }
                            mHisTag.Add(tag.Id, tag);
                        }

                        foreach (var vv in mRealTag)
                        {
                            if (mHisTag.ContainsKey(vv.Key))
                            {
                                re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], mHisTag[vv.Key]));
                            }
                            else
                            {
                                re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], null));
                            }
                        }
                    }
                }
                else
                {
                    totalCount = -1;
                    tagCount = 0;
                }
            }
            catch
            {
                totalCount = 0;
                tagCount = 0;
                Logout();
            }
          
            return re;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="database"></param>
        ///// <param name="group"></param>
        ///// <param name="totalCount"></param>
        ///// <param name="index"></param>
        ///// <returns></returns>
        //public Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> QueryTagByGroupFlat(string database, string group, int index, out int totalCount, out int tagCount, Dictionary<string, string> mFilters = null)
        //{
        //    Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> re = new Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>>();
        //    try
        //    {
        //        if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
        //        {
        //            int idx = index;
        //            var req = new DBDevelopService.GetTagByGroupRequest() { Database = database, LoginId = mLoginId, Group = group, Index = idx };
        //            if (mFilters != null)
        //            {
        //                foreach (var vv in mFilters)
        //                {
        //                    req.Filters.Add(new DBDevelopService.FilterMessageItem() { Key = vv.Key, Value = vv.Value });
        //                }
        //            }

        //            var result = mCurrentClient.GetTagByGroup(req);
        //            tagCount = result.TagCount;

        //            totalCount = result.Count;

        //            if (result.Result)
        //            {
        //                Dictionary<int, Cdy.Tag.Tagbase> mRealTag = new Dictionary<int, Cdy.Tag.Tagbase>();
        //                foreach (var vv in result.RealTag)
        //                {
        //                    var tag = TagMessageToTag(vv);
        //                    mRealTag.Add(tag.Id, tag);
        //                    FillRealTag(tag, mRealTag);
        //                }

        //                Dictionary<int, Cdy.Tag.HisTag> mHisTag = new Dictionary<int, Cdy.Tag.HisTag>();
        //                foreach (var vv in result.HisTag)
        //                {
        //                    var tag = new Cdy.Tag.HisTag { Id = (int)vv.Id, TagType = (Cdy.Tag.TagType)vv.TagType, Type = (Cdy.Tag.RecordType)vv.Type, CompressType = (int)vv.CompressType };
        //                    tag.Circle = (int)vv.Circle;
        //                    tag.MaxValueCountPerSecond = (short)vv.MaxValueCountPerSecond;
        //                    if (vv.Parameter.Count > 0)
        //                    {
        //                        tag.Parameters = new Dictionary<string, double>();
        //                        foreach (var vvv in vv.Parameter)
        //                        {
        //                            tag.Parameters.Add(vvv.Name, vvv.Value);
        //                        }

        //                    }
        //                    mHisTag.Add(tag.Id, tag);
        //                }

        //                foreach (var vv in mRealTag)
        //                {
        //                    if (mHisTag.ContainsKey(vv.Key))
        //                    {
        //                        re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], mHisTag[vv.Key]));
        //                    }
        //                    else
        //                    {
        //                        re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], null));
        //                    }
        //                }
        //            }
        //        }
        //        else
        //        {
        //            totalCount = -1;
        //            tagCount = 0;
        //        }
        //    }
        //    catch
        //    {
        //        totalCount = 0;
        //        tagCount = 0;
        //        Logout();
        //    }

        //    return re;
        //}

        //private void FillRealTag(Tagbase tag, Dictionary<int, Cdy.Tag.Tagbase> result)
        //{
        //    if(tag is ComplexTag)
        //    {
        //        foreach(var vv in (tag as ComplexTag).Tags)
        //        {
        //            result.Add(vv.Key, vv.Value);
        //            if(vv.Value is ComplexTag)
        //            {
        //                FillRealTag(vv.Value as ComplexTag, result);
        //            }
        //        }
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="group"></param>
        /// <returns></returns>
        public Dictionary<int,Tuple<Cdy.Tag.Tagbase,Cdy.Tag.HisTag>> QueryTagByGroup(string database,string group, Dictionary<string, string> mFilters = null)
        {
            Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> re = new Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    int idx = 0;
                    int count = 0;
                    do
                    {
                        var req = new DBDevelopService.GetTagByGroupRequest() { Database = database, LoginId = mLoginId, Group = group, Index = idx };
                        if (mFilters != null)
                        {
                            foreach (var vv in mFilters)
                            {
                                req.Filters.Add(new DBDevelopService.FilterMessageItem() { Key = vv.Key, Value = vv.Value });
                            }
                        }

                        var result = mCurrentClient.GetTagByGroup(req);

                        idx = result.Index;
                        count = result.Count;

                        if (!result.Result) break;

                        Dictionary<int, Cdy.Tag.Tagbase> mRealTag = new Dictionary<int, Cdy.Tag.Tagbase>();
                        foreach (var vv in result.RealTag)
                        {
                            //var tag = GetTag((int)vv.TagType);
                            //tag.Id = (int)vv.Id;
                            //tag.LinkAddress = vv.LinkAddress;
                            //tag.Name = vv.Name;
                            //tag.Desc = vv.Desc;
                            //tag.Group = vv.Group;
                            //tag.ReadWriteType = (Cdy.Tag.ReadWriteMode)vv.ReadWriteMode;
                            //tag.Conveter = !string.IsNullOrEmpty(vv.Convert) ? vv.Convert.DeSeriseToValueConvert() : null;
                            //if (tag is Cdy.Tag.NumberTagBase)
                            //{
                            //    (tag as Cdy.Tag.NumberTagBase).MaxValue = vv.MaxValue;
                            //    (tag as Cdy.Tag.NumberTagBase).MinValue = vv.MinValue;
                            //}

                            //if (tag is Cdy.Tag.FloatingTagBase)
                            //{
                            //    (tag as Cdy.Tag.FloatingTagBase).Precision = (byte)vv.Precision;
                            //}
                            //if (tag is ComplexTag)
                            //{
                            //    (tag as ComplexTag).LinkComplexClass = vv.LinkComplexClass;
                            //}
                            //tag.Parent = vv.Parent;
                            var tag = TagMessageToTag(vv);
                            mRealTag.Add(tag.Id, tag);
                        }

                        Dictionary<int, Cdy.Tag.HisTag> mHisTag = new Dictionary<int, Cdy.Tag.HisTag>();
                        foreach (var vv in result.HisTag)
                        {
                            var tag = new Cdy.Tag.HisTag { Id = (int)vv.Id, TagType = (Cdy.Tag.TagType)vv.TagType, Type = (Cdy.Tag.RecordType)vv.Type, CompressType = (int)vv.CompressType };
                            tag.Circle = (int)vv.Circle;
                            tag.MaxValueCountPerSecond = (short)vv.MaxValueCountPerSecond;
                            if (vv.Parameter.Count > 0)
                            {
                                tag.Parameters = new Dictionary<string, double>();
                                foreach (var vvv in vv.Parameter)
                                {
                                    tag.Parameters.Add(vvv.Name, vvv.Value);
                                }

                            }
                            mHisTag.Add(tag.Id, tag);
                        }

                        foreach (var vv in mRealTag)
                        {
                            if (mHisTag.ContainsKey(vv.Key))
                            {
                                re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], mHisTag[vv.Key]));
                            }
                            else
                            {
                                re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], null));
                            }
                        }

                        idx++;
                    }
                    while (idx < count);
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }

        private Tagbase TagMessageToTag(DBDevelopService.RealTagMessage vv)
        {
            var tag = GetTag((int)vv.TagType);
            tag.Id = (int)vv.Id;
            tag.LinkAddress = vv.LinkAddress;
            tag.Name = vv.Name;
            tag.Desc = vv.Desc;
            tag.Group = vv.Group;
            tag.ReadWriteType = (Cdy.Tag.ReadWriteMode)vv.ReadWriteMode;
            tag.Conveter = !string.IsNullOrEmpty(vv.Convert) ? vv.Convert.DeSeriseToValueConvert() : null;
            if (tag is Cdy.Tag.NumberTagBase)
            {
                (tag as Cdy.Tag.NumberTagBase).MaxValue = vv.MaxValue;
                (tag as Cdy.Tag.NumberTagBase).MinValue = vv.MinValue;
            }

            if (tag is Cdy.Tag.FloatingTagBase)
            {
                (tag as Cdy.Tag.FloatingTagBase).Precision = (byte)vv.Precision;
            }
            if (tag is ComplexTag)
            {
                (tag as ComplexTag).LinkComplexClass = vv.LinkComplexClass;
            }
            tag.Parent = vv.Parent;
            return tag;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="group"></param>
        /// <param name="totalCount"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> QueryTagByClass(string database, string group, int index, out int totalCount, out int tagCount, Dictionary<string, string> mFilters = null)
        {
            Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> re = new Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    int idx = index;
                    var req = new DBDevelopService.GetTagByGroupRequest() { Database = database, LoginId = mLoginId, Group = group, Index = idx };
                    if (mFilters != null)
                    {
                        foreach (var vv in mFilters)
                        {
                            req.Filters.Add(new DBDevelopService.FilterMessageItem() { Key = vv.Key, Value = vv.Value });
                        }
                    }

                    var result = mCurrentClient.GetTagByClass(req);
                    tagCount = result.TagCount;

                    totalCount = result.Count;

                    if (result.Result)
                    {
                        Dictionary<int, Cdy.Tag.Tagbase> mRealTag = new Dictionary<int, Cdy.Tag.Tagbase>();
                        foreach (var vv in result.RealTag)
                        {
                            //var tag = GetTag((int)vv.TagType);
                            //tag.Id = (int)vv.Id;
                            //tag.LinkAddress = vv.LinkAddress;
                            //tag.Name = vv.Name;
                            //tag.Desc = vv.Desc;
                            //tag.Group = vv.Group;
                            //tag.ReadWriteType = (Cdy.Tag.ReadWriteMode)vv.ReadWriteMode;
                            //tag.Conveter = !string.IsNullOrEmpty(vv.Convert) ? vv.Convert.DeSeriseToValueConvert() : null;
                            //if (tag is Cdy.Tag.NumberTagBase)
                            //{
                            //    (tag as Cdy.Tag.NumberTagBase).MaxValue = vv.MaxValue;
                            //    (tag as Cdy.Tag.NumberTagBase).MinValue = vv.MinValue;
                            //}

                            //if (tag is Cdy.Tag.FloatingTagBase)
                            //{
                            //    (tag as Cdy.Tag.FloatingTagBase).Precision = (byte)vv.Precision;
                            //}
                            //if (tag is ComplexTag)
                            //{
                            //    (tag as ComplexTag).LinkComplexClass = vv.LinkComplexClass;
                            //}
                            //tag.Parent = vv.Parent;
                            //mRealTag.Add(tag.Id, tag);
                            var tag = TagMessageToTag(vv);
                            mRealTag.Add(tag.Id, tag);
                        }

                        Dictionary<int, Cdy.Tag.HisTag> mHisTag = new Dictionary<int, Cdy.Tag.HisTag>();
                        foreach (var vv in result.HisTag)
                        {
                            var tag = new Cdy.Tag.HisTag { Id = (int)vv.Id, TagType = (Cdy.Tag.TagType)vv.TagType, Type = (Cdy.Tag.RecordType)vv.Type, CompressType = (int)vv.CompressType };
                            tag.Circle = (int)vv.Circle;
                            tag.MaxValueCountPerSecond = (short)vv.MaxValueCountPerSecond;
                            if (vv.Parameter.Count > 0)
                            {
                                tag.Parameters = new Dictionary<string, double>();
                                foreach (var vvv in vv.Parameter)
                                {
                                    tag.Parameters.Add(vvv.Name, vvv.Value);
                                }

                            }
                            mHisTag.Add(tag.Id, tag);
                        }

                        foreach (var vv in mRealTag)
                        {
                            if (mHisTag.ContainsKey(vv.Key))
                            {
                                re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], mHisTag[vv.Key]));
                            }
                            else
                            {
                                re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], null));
                            }
                        }
                    }
                }
                else
                {
                    totalCount = -1;
                    tagCount = 0;
                }
            }
            catch
            {
                totalCount = 0;
                tagCount = 0;
                Logout();
            }

            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="group"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> QueryTagByClass(string database, string group, Dictionary<string, string> mFilters = null)
        {
            Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> re = new Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    int idx = 0;
                    int count = 0;
                    do
                    {
                        var req = new DBDevelopService.GetTagByGroupRequest() { Database = database, LoginId = mLoginId, Group = group, Index = idx };
                        if (mFilters != null)
                        {
                            foreach (var vv in mFilters)
                            {
                                req.Filters.Add(new DBDevelopService.FilterMessageItem() { Key = vv.Key, Value = vv.Value });
                            }
                        }

                        var result = mCurrentClient.GetTagByClass(req);

                        idx = result.Index;
                        count = result.Count;

                        if (!result.Result) break;

                        Dictionary<int, Cdy.Tag.Tagbase> mRealTag = new Dictionary<int, Cdy.Tag.Tagbase>();
                        foreach (var vv in result.RealTag)
                        {
                            //var tag = GetTag((int)vv.TagType);
                            //tag.Id = (int)vv.Id;
                            //tag.LinkAddress = vv.LinkAddress;
                            //tag.Name = vv.Name;
                            //tag.Desc = vv.Desc;
                            //tag.Group = vv.Group;
                            //tag.ReadWriteType = (Cdy.Tag.ReadWriteMode)vv.ReadWriteMode;
                            //tag.Conveter = !string.IsNullOrEmpty(vv.Convert) ? vv.Convert.DeSeriseToValueConvert() : null;
                            //if (tag is Cdy.Tag.NumberTagBase)
                            //{
                            //    (tag as Cdy.Tag.NumberTagBase).MaxValue = vv.MaxValue;
                            //    (tag as Cdy.Tag.NumberTagBase).MinValue = vv.MinValue;
                            //}

                            //if (tag is Cdy.Tag.FloatingTagBase)
                            //{
                            //    (tag as Cdy.Tag.FloatingTagBase).Precision = (byte)vv.Precision;
                            //}
                            //if (tag is ComplexTag)
                            //{
                            //    (tag as ComplexTag).LinkComplexClass = vv.LinkComplexClass;
                            //}
                            //tag.Parent = vv.Parent;
                            //mRealTag.Add(tag.Id, tag);
                            var tag = TagMessageToTag(vv);
                            mRealTag.Add(tag.Id, tag);
                        }

                        Dictionary<int, Cdy.Tag.HisTag> mHisTag = new Dictionary<int, Cdy.Tag.HisTag>();
                        foreach (var vv in result.HisTag)
                        {
                            var tag = new Cdy.Tag.HisTag { Id = (int)vv.Id, TagType = (Cdy.Tag.TagType)vv.TagType, Type = (Cdy.Tag.RecordType)vv.Type, CompressType = (int)vv.CompressType };
                            tag.Circle = (int)vv.Circle;
                            tag.MaxValueCountPerSecond = (short)vv.MaxValueCountPerSecond;
                            if (vv.Parameter.Count > 0)
                            {
                                tag.Parameters = new Dictionary<string, double>();
                                foreach (var vvv in vv.Parameter)
                                {
                                    tag.Parameters.Add(vvv.Name, vvv.Value);
                                }

                            }
                            mHisTag.Add(tag.Id, tag);
                        }

                        foreach (var vv in mRealTag)
                        {
                            if (mHisTag.ContainsKey(vv.Key))
                            {
                                re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], mHisTag[vv.Key]));
                            }
                            else
                            {
                                re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], null));
                            }
                        }

                        idx++;
                    }
                    while (idx < count);
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }

        /// <summary>
        /// 获取某个
        /// </summary>
        /// <param name="database"></param>
        /// <param name="id"></param>
        /// <param name="mFilters"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> QueryTagSubTags(string database, int id, Dictionary<string, string> mFilters = null)
        {
            Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> re = new Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var req = new DBDevelopService.GetComplexSubTagsRequest() { Database = database, LoginId = mLoginId, TagId = id };
                    if (mFilters != null)
                    {
                        foreach (var vv in mFilters)
                        {
                            req.Filters.Add(new DBDevelopService.FilterMessageItem() { Key = vv.Key, Value = vv.Value });
                        }
                    }

                    var result = mCurrentClient.GetComplexSubTags(req);

                    if (!result.Result) return re;

                    Dictionary<int, Cdy.Tag.Tagbase> mRealTag = new Dictionary<int, Cdy.Tag.Tagbase>();
                    foreach (var vv in result.RealTag)
                    {
                        //var tag = GetTag((int)vv.TagType);
                        //tag.Id = (int)vv.Id;
                        //tag.LinkAddress = vv.LinkAddress;
                        //tag.Name = vv.Name;
                        //tag.Desc = vv.Desc;
                        //tag.Group = vv.Group;
                        //tag.ReadWriteType = (Cdy.Tag.ReadWriteMode)vv.ReadWriteMode;
                        //tag.Conveter = !string.IsNullOrEmpty(vv.Convert) ? vv.Convert.DeSeriseToValueConvert() : null;
                        //if (tag is Cdy.Tag.NumberTagBase)
                        //{
                        //    (tag as Cdy.Tag.NumberTagBase).MaxValue = vv.MaxValue;
                        //    (tag as Cdy.Tag.NumberTagBase).MinValue = vv.MinValue;
                        //}

                        //if (tag is Cdy.Tag.FloatingTagBase)
                        //{
                        //    (tag as Cdy.Tag.FloatingTagBase).Precision = (byte)vv.Precision;
                        //}
                        //if (tag is ComplexTag)
                        //{
                        //    (tag as ComplexTag).LinkComplexClass = vv.LinkComplexClass;
                        //}
                        //tag.Parent = vv.Parent;
                        //mRealTag.Add(tag.Id, tag);
                        var tag = TagMessageToTag(vv);
                        mRealTag.Add(tag.Id, tag);
                    }

                    Dictionary<int, Cdy.Tag.HisTag> mHisTag = new Dictionary<int, Cdy.Tag.HisTag>();
                    foreach (var vv in result.HisTag)
                    {
                        var tag = new Cdy.Tag.HisTag { Id = (int)vv.Id, TagType = (Cdy.Tag.TagType)vv.TagType, Type = (Cdy.Tag.RecordType)vv.Type, CompressType = (int)vv.CompressType };
                        tag.Circle = (int)vv.Circle;
                        tag.MaxValueCountPerSecond = (short)vv.MaxValueCountPerSecond;
                        if (vv.Parameter.Count > 0)
                        {
                            tag.Parameters = new Dictionary<string, double>();
                            foreach (var vvv in vv.Parameter)
                            {
                                tag.Parameters.Add(vvv.Name, vvv.Value);
                            }

                        }
                        mHisTag.Add(tag.Id, tag);
                    }

                    foreach (var vv in mRealTag)
                    {
                        if (mHisTag.ContainsKey(vv.Key))
                        {
                            re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], mHisTag[vv.Key]));
                        }
                        else
                        {
                            re.Add(vv.Key, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(mRealTag[vv.Key], null));
                        }
                    }
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tag"></param>
        /// <returns></returns>
        public bool UpdateTag(string database,Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag> tag)
        {
            bool re = true;
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    re &= mCurrentClient.UpdateRealTag(new DBDevelopService.UpdateRealTagRequestMessage() { Database = database, LoginId = mLoginId, Tag = ConvertToRealTagMessage(tag.Item1) }).Result;
                    if (tag.Item2 != null)
                    {
                        re &= mCurrentClient.UpdateHisTag(new DBDevelopService.UpdateHisTagRequestMessage() { Database = database, LoginId = mLoginId, Tag = ConvertToHisTagMessage(tag.Item2) }).Result;
                    }
                    else
                    {
                        var msg = new DBDevelopService.RemoveTagMessageRequest() { Database = database, LoginId = mLoginId };
                        msg.TagId.Add(tag.Item1.Id);
                        re &= mCurrentClient.RemoveHisTag(msg).Result;
                    }
                }
                else
                {
                    re = false;
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="cls"></param>
        /// <param name="tag"></param>
        /// <returns></returns>
        public bool UpdateClassTag(string database,string cls, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag> tag)
        {
            bool re = true;
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    re &= mCurrentClient.UpdateClassRealTag(new DBDevelopService.UpdateClassRealTagRequestMessage() { Database = database,Classes=cls, LoginId = mLoginId, Tag = ConvertToRealTagMessage(tag.Item1) }).Result;
                    if (tag.Item2 != null)
                    {
                        re &= mCurrentClient.UpdateClassHisTag(new DBDevelopService.UpdateClassHisTagRequestMessage() { Database = database,Classes=cls, LoginId = mLoginId, Tag = ConvertToHisTagMessage(tag.Item2) }).Result;
                    }
                    else
                    {
                        var msg = new DBDevelopService.RemoveClassTagMessageRequest() { Database = database, LoginId = mLoginId,Classes=cls};
                        msg.TagId.Add(tag.Item1.Id);
                        re &= mCurrentClient.RemoveClassHisTag(msg).Result;
                    }
                }
                else
                {
                    re = false;
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="cls"></param>
        /// <returns></returns>
        public bool UpdateLinkedClassTags(string database,string cls)
        {
            bool re = true;
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    re &= mCurrentClient.UpdateLinkedClassTags(new DBDevelopService.UpdateClassRequest() { Database = database, Name = cls, LoginId = mLoginId }).Result;
                    
                }
                else
                {
                    re = false;
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tag"></param>
        /// <returns></returns>
        public bool AddTag(string database, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag> tag, out int id)
        {
            bool re = false;
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var res = mCurrentClient.AddTag(new DBDevelopService.AddTagRequestMessage() { Database = database, LoginId = mLoginId, RealTag = ConvertToRealTagMessage(tag.Item1), HisTag = ConvertToHisTagMessage(tag.Item2) });
                    re = res.Result;
                    id = res.TagId;
                }
                else
                {
                    id = -1;
                }
            }
            catch
            {
                Logout();
                id = -1;
            }

            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="classname"></param>
        /// <param name="tag"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool AddClassTag(string database,string classname, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag> tag, out int id)
        {
            bool re = false;
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var res = mCurrentClient.AddClassTag(new DBDevelopService.AddClassTagRequestMessage() { Database = database,Classes=classname, LoginId = mLoginId, RealTag = ConvertToRealTagMessage(tag.Item1), HisTag = ConvertToHisTagMessage(tag.Item2) });
                    re = res.Result;
                    id = res.TagId;
                }
                else
                {
                    id = -1;
                }
            }
            catch
            {
                Logout();
                id = -1;
            }

            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tag"></param>
        /// <param name="mode"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool Import(string database,Tuple<Cdy.Tag.Tagbase,Cdy.Tag.HisTag> tag,int mode,out int id)
        {
            bool re = false;
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var res = mCurrentClient.Import(new DBDevelopService.ImportTagRequestMessage() { Database = database, LoginId = mLoginId, RealTag = ConvertToRealTagMessage(tag.Item1), HisTag = ConvertToHisTagMessage(tag.Item2), Mode = mode });
                    re = res.Result;
                    id = res.TagId;
                }
                else
                {
                    id = -1;
                }
            }
            catch
            {
                id=-1;
                Logout();
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="group"></param>
        /// <returns></returns>
        public bool ClearTagByGroup(string database,string group)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var res = mCurrentClient.ClearTag(new DBDevelopService.ClearTagRequestMessage() { Database = database, LoginId = mLoginId, GroupFullName = group });
                    return res.Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool ClearTagAll(string database)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var res = mCurrentClient.ClearAllTag(new DBDevelopService.GetRequest() { Database = database, LoginId = mLoginId });
                    return res.Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="name"></param>
        /// <param name="parentName"></param>
        public string AddTagGroup(string database,string name,string parentName)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.AddTagGroup(new DBDevelopService.AddGroupRequest() { Database = database, LoginId = mLoginId, Name = name, ParentName = parentName }).Group;
                }
            }
            catch
            {
                Logout();
            }
            return string.Empty;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="groupname"></param>
        /// <param name="desc"></param>
        /// <returns></returns>
        public bool UpdateGroupDescription(string database, string groupname, string desc)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.UpdateGroupDescription(new DBDevelopService.UpdateGroupDescriptionRequest() { Database = database, LoginId = mLoginId, GroupName = groupname, Desc = desc }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="groupname"></param>
        /// <param name="desc"></param>
        /// <returns></returns>
        public bool UpdateClassDescription(string database, string groupname, string desc)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.UpdateTagClassDescription(new DBDevelopService.UpdateClassDescriptionRequest() { Database = database, LoginId = mLoginId, Name = groupname, Desc = desc }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="sourceGroup"></param>
        /// <param name="targetParentGroup"></param>
        /// <returns></returns>
        public string PasteTagGroup(string database, string sourceGroup,string targetParentGroup)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.PasteTagGroup(new DBDevelopService.PasteGroupRequest() { Database = database, LoginId = mLoginId, GroupFullName = sourceGroup, TargetParentName = targetParentGroup }).Group;
                }
            }
            catch
            {
                Logout();
            }
            return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="sourceGroup"></param>
        /// <returns></returns>
        public string PasteTagClass(string database, string sourceGroup)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.PasteTagClass(new DBDevelopService.PasteGroupRequest() { Database = database, LoginId = mLoginId, GroupFullName = sourceGroup }).Group;
                }
            }
            catch
            {
                Logout();
            }
            return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="oldName"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public bool ReNameTagGroup(string database,string oldName,string newName)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.RenameTagGroup(new DBDevelopService.RenameGroupRequest() { Database = database, LoginId = mLoginId, OldFullName = oldName, NewName = newName }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool RemoveTagGroup(string database,string name)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.RemoveTagGroup(new DBDevelopService.RemoveGroupRequest() { Database = database, LoginId = mLoginId, Name = name }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public string AddTagClass(string database, string name)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.AddTagClass(new DBDevelopService.AddClassRequest() { Database = database, LoginId = mLoginId, Name = name }).Group;
                }
            }
            catch
            {
                Logout();
            }
            return string.Empty;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="oldName"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public bool ReNameTagClass(string database, string oldName, string newName)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.RenameTagClass(new DBDevelopService.RenameClassRequest() { Database = database, LoginId = mLoginId, OldFullName = oldName, NewName = newName }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool RemoveTagClass(string database, string name)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.RemoveTagClass(new DBDevelopService.RemoveClassRequest() { Database = database, LoginId = mLoginId, Name = name }).Result;
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool Remove(string database, int id)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var msg = new DBDevelopService.RemoveTagMessageRequest() { Database = database, LoginId = mLoginId };
                    msg.TagId.Add(id);
                    return mCurrentClient.RemoveTag(msg).Result;
                }
                return false;
            }
            catch
            {
                Logout();
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool RemoveClassTag(string database,string cls, int id)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var msg = new DBDevelopService.RemoveClassTagMessageRequest() { Database = database,Classes=cls, LoginId = mLoginId };
                    msg.TagId.Add(id);
                    return mCurrentClient.RemoveClassTag(msg).Result;
                }
                return false;
            }
            catch
            {
                Logout();
                return false;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        private DBDevelopService.RealTagMessage ConvertToRealTagMessage(Cdy.Tag.Tagbase tag)
        {
            DBDevelopService.RealTagMessage re = new DBDevelopService.RealTagMessage();
            re.Id = tag.Id;
            re.LinkAddress = tag.LinkAddress;
            re.Name = tag.Name;
            re.TagType = (uint)tag.Type;
            re.Group = tag.Group;
            re.Desc = tag.Desc;
            re.Convert = tag.Conveter != null ? tag.Conveter.SeriseToString() : string.Empty;
            re.ReadWriteMode = (int)tag.ReadWriteType;
            if(tag is NumberTagBase)
            {
                re.MaxValue = (tag as NumberTagBase).MaxValue;
                re.MinValue = (tag as NumberTagBase).MinValue;
            }

            if (tag is FloatingTagBase)
            {
                re.Precision = (tag as FloatingTagBase).Precision;
            }
            if(tag is ComplexTag)
            {
                re.LinkComplexClass= (tag as ComplexTag).LinkComplexClass;
            }
            else
            {
                re.LinkComplexClass = "";
            }
            re.Parent = tag.Parent;
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        private DBDevelopService.HisTagMessage ConvertToHisTagMessage(Cdy.Tag.HisTag tag)
        {
            DBDevelopService.HisTagMessage re = new DBDevelopService.HisTagMessage();
            if (tag != null)
            {
                re.Id = tag.Id;
                re.TagType = (uint)tag.TagType;
                re.Type = (uint)tag.Type;
                re.CompressType = (uint)tag.CompressType;
                re.Circle = (uint)tag.Circle;
                re.MaxValueCountPerSecond = (uint)tag.MaxValueCountPerSecond;
                if (tag.Parameters != null)
                {
                    foreach (var vv in tag.Parameters)
                    {
                        re.Parameter.Add(new DBDevelopService.hisTagParameterItem() { Name = vv.Key, Value = vv.Value });
                    }
                }
            }
            else
            {
                re.Id = int.MaxValue;
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public Dictionary<string,Tuple<string[],string>> GetRegistorDrivers(string database)
        {
            Dictionary<string, Tuple<string[], string>> re = new Dictionary<string, Tuple<string[], string>>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var vv = mCurrentClient.GetRegisteDrivers(new DBDevelopService.GetRequest() { Database = database, LoginId = mLoginId });
                    if (vv.Result && vv.Drivers.Count > 0)
                    {
                        foreach (var vvv in vv.Drivers)
                        {
                            re.Add(vvv.Name,new Tuple<string[], string>(vvv.Registors.ToArray(),vvv.EditType));
                        }
                    }
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public Tuple<string,string,int, double> GetHisSetting(string database)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var res = mCurrentClient.GetDatabaseHisSetting(new DBDevelopService.DatabasesRequest() { Database = database, LoginId = mLoginId });
                    return new Tuple<string, string, int, double>(res.DataPath, res.BackDataPath, res.KeepTime, res.KeepNoZipFileDays);
                }
                else
                {
                    return new Tuple<string, string, int, double>("", "", -1, -1);
                }
            }
            catch
            {
                Logout();
                return new Tuple<string, string, int, double>("", "", -1, -1);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public Tuple<bool, bool, bool,bool> GetApiSetting(string database)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var res = mCurrentClient.GetProxyApiSetting(new DBDevelopService.DatabasesRequest() { Database = database, LoginId = mLoginId });
                    return new Tuple<bool, bool, bool,bool>(res.WebApi, res.GrpcApi, res.HighApi,res.OpcServer);
                }
                else
                {
                    return new Tuple<bool, bool, bool, bool>(false, false, false,false);
                }
            }
            catch
            {
                Logout();
                return new Tuple<bool, bool, bool, bool>(false, false, false,false);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="web"></param>
        /// <param name="grpc"></param>
        /// <param name="highapi"></param>
        public void UpdateApiSetting(string database,bool web,bool grpc,bool highapi,bool opcserver)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var res = mCurrentClient.UpdateProxyApiSetting(new DBDevelopService.UpdateProxyApiSettingRequest() { Database = database, LoginId = mLoginId, WebApi = web, GrpcApi = grpc, HighApi = highapi ,OpcServer=opcserver});
                }
            }
            catch
            {
                Logout();
            }
        }
    

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="datapath"></param>
        /// <param name="backdatapath"></param>
        /// <param name="keeptime"></param>
        /// <param name="keepNoZipFileDays"></param>
        /// <returns></returns>
        public bool UpdateHisSetting(string database,string datapath,string backdatapath,int keeptime,double keepNoZipFileDays=-1)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.UpdateDatabaseHisSetting(new DBDevelopService.UpdateDatabaseHisSettingRequest() { Database = database, LoginId = mLoginId, DataPath = datapath, BackDataPath = backdatapath, KeepTime = keeptime, KeepNoZipFileDays = keepNoZipFileDays }).Result;
                }
                else
                {
                    return false;
                }
            }
            catch
            {
                Logout();
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="group"></param>
        /// <param name="baseTagName"></param>
        /// <returns></returns>
        public string GetAvaiableTagName(string database,string group="",string baseTagName="")
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.GetAvaiableTagName(new DBDevelopService.GetAvaiableTagNameRequest() { Database = database, LoginId = mLoginId, Group = group, Name = baseTagName }).Name;
                }
                else
                {
                    return string.Empty;
                }
            }
            catch
            {
                Logout();
                return String.Empty;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="group"></param>
        /// <param name="baseTagName"></param>
        /// <returns></returns>
        public string GetAvaiableClassTagName(string database, string cls = "", string baseTagName = "")
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.GetAvaiableClassTagName(new DBDevelopService.GetAvaiableClassTagNameRequest() { Database = database, LoginId = mLoginId, Class = cls, Name = baseTagName }).Name;
                }
                else
                {
                    return string.Empty;
                }
            }
            catch
            {
                Logout();
                return String.Empty;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="cls"></param>
        /// <param name="tagname"></param>
        /// <returns></returns>
        public bool CheckClassTagNameExits(string database, string cls, string tagname)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.CheckClassTagNameExist(new DBDevelopService.GetAvaiableClassTagNameRequest() { Database = database, LoginId = mLoginId, Class = cls, Name = tagname }).Result;
                }
                else
                {
                    return false;
                }
            }
            catch
            {
                Logout();
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="group"></param>
        /// <param name="tagname"></param>
        /// <returns></returns>
        public bool CheckTagNameExits(string database,string group,string tagname)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mCurrentClient.CheckTagNameExist(new DBDevelopService.GetAvaiableTagNameRequest() { Database = database, LoginId = mLoginId, Group = group, Name = tagname }).Result;
                }
                else
                {
                    return false;
                }
            }
            catch
            {
                Logout();
                return false;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public int GetRealServerPort(string database)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var res = mCurrentClient.GetRealDataServerPort(new DBDevelopService.DatabasesRequest() { Database = database, LoginId = mLoginId });
                    return res.Value;
                }
                else
                {
                    return -1;
                }
            }
            catch
            {
                Logout();
                return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="port"></param>
        /// <returns></returns>
        public bool SetRealServerPort(string database,int port)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var res = mCurrentClient.SetRealDataServerPort(new DBDevelopService.SetRealDataServerPortRequest() { Database = database, LoginId = mLoginId, Port = port });
                    return res.Result;
                }
                else
                {
                    return false;
                }
            }
            catch
            {
                Logout();
                return false;
            }
        }

        /// <summary>
        /// 获取驱动配置
        /// </summary>
        /// <param name="database"></param>
        /// <param name="driver"></param>
        /// <returns></returns>
        public Dictionary<string,string> GetDriverSetting(string database,string driver)
        {
            Dictionary<string, string> re = new Dictionary<string, string>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var res = mCurrentClient.GetDriverSetting(new DBDevelopService.GetDriverSettingRequest() { Database = database, LoginId = mLoginId, Driver = driver });
                    var sval = res.SettingString;
                    if (!string.IsNullOrEmpty(sval))
                    {
                        string[] ss = sval.Split(new char[] { ',' });
                        foreach (var vv in ss)
                        {
                            string[] svv = vv.Split(new char[] { ':' });
                            if (!re.ContainsKey(svv[0]))
                            {
                                re.Add(svv[0], svv[1]);
                            }
                        }
                    }
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }

        /// <summary>
        /// 更新驱动配置
        /// </summary>
        /// <param name="database"></param>
        /// <param name="driver"></param>
        /// <param name="settings"></param>
        /// <returns></returns>
        public bool UpdateDriverSetting(string database,string driver,Dictionary<string,string> settings)
        {
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    if (settings != null && settings.Count > 0)
                    {
                        StringBuilder sb = new StringBuilder();
                        foreach (var vv in settings)
                        {
                            sb.Append(vv.Key + ":" + vv.Value + ",");
                        }
                        sb.Length = sb.Length > 0 ? sb.Length - 1 : sb.Length;

                        return mCurrentClient.UpdateDrvierSetting(new DBDevelopService.UpdateDrvierSettingRequest() { Database = database, Driver = driver, LoginId = mLoginId, SettingString = sb.ToString() }).Result;

                    }
                }
            }
            catch
            {
                Logout();
            }
            return false;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagType"></param>
        /// <returns></returns>
        private Cdy.Tag.Tagbase GetTag(int tagType)
        {
            switch (tagType)
            {
                case (int)Cdy.Tag.TagType.Bool:
                    return new Cdy.Tag.BoolTag();
                case (int)Cdy.Tag.TagType.Byte:
                    return new Cdy.Tag.ByteTag();
                case (int)Cdy.Tag.TagType.DateTime:
                    return new Cdy.Tag.DateTimeTag();
                case (int)Cdy.Tag.TagType.Double:
                    return new Cdy.Tag.DoubleTag();
                case (int)Cdy.Tag.TagType.Float:
                    return new Cdy.Tag.FloatTag();
                case (int)Cdy.Tag.TagType.Int:
                    return new Cdy.Tag.IntTag();
                case (int)Cdy.Tag.TagType.Long:
                    return new Cdy.Tag.LongTag();
                case (int)Cdy.Tag.TagType.Short:
                    return new Cdy.Tag.ShortTag();
                case (int)Cdy.Tag.TagType.String:
                    return new Cdy.Tag.StringTag();
                case (int)Cdy.Tag.TagType.UInt:
                    return new Cdy.Tag.UIntTag();
                case (int)Cdy.Tag.TagType.ULong:
                    return new Cdy.Tag.ULongTag();
                case (int)Cdy.Tag.TagType.UShort:
                    return new Cdy.Tag.UShortTag();
                case (int)(Cdy.Tag.TagType.IntPoint):
                    return new Cdy.Tag.IntPointTag();
                case (int)(Cdy.Tag.TagType.UIntPoint):
                    return new Cdy.Tag.UIntPointTag();
                case (int)(Cdy.Tag.TagType.IntPoint3):
                    return new Cdy.Tag.IntPoint3Tag();
                case (int)(Cdy.Tag.TagType.UIntPoint3):
                    return new Cdy.Tag.UIntPoint3Tag();
                case (int)(Cdy.Tag.TagType.LongPoint):
                    return new Cdy.Tag.LongPointTag();
                case (int)(Cdy.Tag.TagType.ULongPoint):
                    return new Cdy.Tag.ULongPointTag();
                case (int)(Cdy.Tag.TagType.LongPoint3):
                    return new Cdy.Tag.LongPoint3Tag();
                case (int)(Cdy.Tag.TagType.ULongPoint3):
                    return new Cdy.Tag.ULongPoint3Tag();
                case (int)(Cdy.Tag.TagType.Complex):
                    return new Cdy.Tag.ComplexTag();
            }
            return null;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagids"></param>
        /// <returns></returns>
        public Dictionary<int,int> ResetTagIds(string database,List<int> tagids,int startId)
        {
            Dictionary<int, int> re = new Dictionary<int, int>();
            try
            {
                if (mCurrentClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var qq = new DBDevelopService.ResetTagIdRequest() { Database = database, LoginId = mLoginId, StartId = startId };
                    qq.TagIds.AddRange(tagids);
                    var res = mCurrentClient.ResetTagId(qq);
                    var sval = res.TagIds;
                    if (sval.Count > 0)
                    {
                        foreach (var vv in sval)
                        {
                            re.Add(vv.Key, vv.Value);
                        }
                    }
                }
            }
            catch
            {
                Logout();
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        private void Logout()
        {
            lock(mLockObj)
            {
                if(!string.IsNullOrEmpty(mLoginId))
                {
                    mLoginId = "";
                    OfflineCallBack?.Invoke();
                }
            }
            
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
