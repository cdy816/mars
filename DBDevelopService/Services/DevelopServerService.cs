using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace DBDevelopService
{
    public class DevelopServerService : DevelopServer.DevelopServerBase
    {
        //private readonly ILogger<DevelopServerService> _logger;
        //public DevelopServerService(ILogger<DevelopServerService> logger)
        //{
        //    _logger = logger;
        //}

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
        private bool CheckLoginId(string id,string permission="")
        {
            return SecurityManager.Manager.CheckKeyAvaiable(id) && SecurityManager.Manager.CheckPermission(id,permission);
        }

        #region System user

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> NewUser(NewUserRequest request, ServerCallContext context)
        {
            if (CheckLoginId(request.LoginId, PermissionDocument.AdminPermission))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var user = new User() { Name = request.UserName,Password = request.Password,Permissions = request.Permission.ToList() };
            SecurityManager.Manager.Securitys.User.AddUser(user);
            return Task.FromResult(new BoolResultReplay() { Result = true });
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
            if (CheckLoginId(request.LoginId, PermissionDocument.AdminPermission)&& userName != request.UserName)
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var user = SecurityManager.Manager.Securitys.User.GetUser(request.UserName);
            if(user!=null)
            user.Password = request.Password;

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
            if (CheckLoginId(request.LoginId, PermissionDocument.AdminPermission))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var user = SecurityManager.Manager.Securitys.User.GetUser(request.UserName);
            if(user!=null)
            {
                user.Permissions = request.Permission.ToList();
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
            if (CheckLoginId(request.LoginId, PermissionDocument.AdminPermission))
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
                DbManager.Instance.NewDB(request.Database);
            }
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<QueryDatabaseReplay> QueryDatabase(QueryDatabaseRequst request, ServerCallContext context)
        {
            if (CheckLoginId(request.LoginId))
            {
                return Task.FromResult(new QueryDatabaseReplay() { Result = false });
            }
            QueryDatabaseReplay re = new QueryDatabaseReplay() { Result = true };
            re.Database.AddRange(DbManager.Instance.ListDatabase());
            return Task.FromResult(re);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> Start(GetRequest request, ServerCallContext context)
        {
            // to do start
            return base.Start(request, context);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> Stop(GetRequest request, ServerCallContext context)
        {
            //to do stop
            return base.Stop(request, context);
        }
        #endregion

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetHistTagMessageReply> GetHisAllTag(GetRequest request, ServerCallContext context)
        {
            if (CheckLoginId(request.LoginId))
            {
                return Task.FromResult(new GetHistTagMessageReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            List<HisTagMessage> re = new List<HisTagMessage>();
            if (db != null)
            {
                foreach (var vv in db.HisDatabase.HisTags.Values)
                {
                    re.Add(new HisTagMessage() { Id = (uint)vv.Id, Type = (uint)vv.Type, TagType = (uint)vv.TagType, CompressType = (uint)vv.CompressType, CompressParameter1 = vv.CompressParameter1, CompressParameter2 = vv.CompressParameter2, CompressParameter3 = vv.CompressParameter3 });
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
            if (CheckLoginId(request.LoginId))
            {
                return Task.FromResult(new GetRealTagMessageReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            List<RealTagMessage> re = new List<RealTagMessage>();
            if (db != null)
            {
                foreach (var vv in db.RealDatabase.ListAllTags())
                {
                    re.Add(new RealTagMessage() { Id = (uint)vv.Id, Name = vv.Name,  Desc = vv.Desc,Group = vv.Group,  LinkAddress = vv.LinkAddress,TagType=(uint)vv.Type });
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
        public override Task<GetHistTagMessageReply> QueryHisTag(QueryMessage request, ServerCallContext context)
        {
            if (CheckLoginId(request.LoginId))
            {
                return Task.FromResult(new GetHistTagMessageReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if(db!=null)
            {
                if(request.Conditions.Count==0)
                    return Task.FromResult(new GetHistTagMessageReply() { Result = true });

                IEnumerable<Cdy.Tag.HisTag> htags= db.HisDatabase.HisTags.Values;
                foreach(var vv in request.Conditions)
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
                    re.Add(new HisTagMessage() { Id = (uint)vv.Id, Type = (uint)vv.Type, TagType = (uint)vv.TagType, CompressType = (uint)vv.CompressType, CompressParameter1 = vv.CompressParameter1, CompressParameter2 = vv.CompressParameter2, CompressParameter3 = vv.CompressParameter3 });
                }
                var msg = new GetHistTagMessageReply() { Result = true };
                msg.Messages.AddRange(re);
                return Task.FromResult(msg);
            }
            return Task.FromResult(new GetHistTagMessageReply() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetRealTagMessageReply> QueryRealTag(QueryMessage request, ServerCallContext context)
        {
            if (CheckLoginId(request.LoginId))
            {
                return Task.FromResult(new GetRealTagMessageReply() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            if (db != null)
            {
                if (request.Conditions.Count == 0)
                    return Task.FromResult(new GetRealTagMessageReply() { Result = true });

                IEnumerable<Cdy.Tag.Tagbase> htags = db.RealDatabase.Tags.Values;
                foreach (var vv in request.Conditions)
                {
                    switch (vv.Key.ToLower())
                    {
                        case "id":
                            htags = htags.Where(e => e.Id == int.Parse(vv.Value));
                            break;
                        case "name":
                            htags = htags.Where(e =>  e.Name.Contains(vv.Value));
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
                    re.Add(new RealTagMessage() { Id = (uint)vv.Id, Name = vv.Name, Desc = vv.Desc, Group = vv.Group, LinkAddress = vv.LinkAddress, TagType = (uint)vv.Type });
                }
                var msg = new GetRealTagMessageReply() { Result = true };
                msg.Messages.AddRange(re);
                return Task.FromResult(msg);
            }
            return Task.FromResult(new GetRealTagMessageReply() { Result = true });
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolResultReplay> RemoveTag(RemoveTagMessage request, ServerCallContext context)
        {
            if (CheckLoginId(request.LoginId, PermissionDocument.DeletePermission))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            foreach(var vv in request.TagId)
            {
                db.HisDatabase.RemoveHisTag(vv);
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
                if (CheckLoginId(request.LoginId, PermissionDocument.ModifyPermission))
                {
                    return Task.FromResult(new BoolResultReplay() { Result = false });
                }
                var db = DbManager.Instance.GetDatabase(request.Database);
                var vtag = request.Tag;
                if (db.HisDatabase.HisTags.ContainsKey(vtag.Id))
                {
                    Cdy.Tag.HisTag hisTag = new Cdy.Tag.HisTag();
                    hisTag.Id = (int)vtag.Id;
                    hisTag.TagType = (Cdy.Tag.TagType)(vtag.TagType);
                    hisTag.Type = (Cdy.Tag.RecordType)(vtag.Type);
                    hisTag.CompressType = (int)(vtag.CompressType);
                    hisTag.CompressParameter1 = vtag.CompressParameter1;
                    hisTag.CompressParameter2 = vtag.CompressParameter2;
                    hisTag.CompressParameter3 = vtag.CompressParameter3;
                    db.HisDatabase.AddOrUpdate(hisTag);
                }
                return Task.FromResult(new BoolResultReplay() { Result = true });
            }
            catch(Exception ex)
            {
                return Task.FromResult(new BoolResultReplay() { Result = true,ErroMessage = ex.Message });
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
                if (CheckLoginId(request.LoginId,PermissionDocument.ModifyPermission))
                {
                    return Task.FromResult(new BoolResultReplay() { Result = false });
                }
                var tag = GetRealTag(request.Tag);
                var db = DbManager.Instance.GetDatabase(request.Database);
                if (db.RealDatabase.Tags.ContainsKey(tag.Id))
                {
                    db.RealDatabase.AddOrUpdate(tag);
                }
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            catch(Exception ex)
            {
                return Task.FromResult(new BoolResultReplay() { Result = true, ErroMessage = ex.Message });
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
                if (CheckLoginId(request.LoginId, PermissionDocument.NewPermission))
                {
                    return Task.FromResult(new AddTagReplyMessage() { Result = false });
                }
                var db = DbManager.Instance.GetDatabase(request.Database);

                Cdy.Tag.Tagbase tag = GetRealTag(request.RealTag);

                db.RealDatabase.Append(tag);

                var vtag = request.HisTag;
                if (db.HisDatabase.HisTags.ContainsKey(tag.Id))
                {
                    Cdy.Tag.HisTag hisTag = new Cdy.Tag.HisTag();
                    hisTag.Id = (int)vtag.Id;
                    hisTag.TagType = (Cdy.Tag.TagType)(vtag.TagType);
                    hisTag.Type = (Cdy.Tag.RecordType)(vtag.Type);
                    hisTag.CompressType = (int)(vtag.CompressType);
                    hisTag.CompressParameter1 = vtag.CompressParameter1;
                    hisTag.CompressParameter2 = vtag.CompressParameter2;
                    hisTag.CompressParameter3 = vtag.CompressParameter3;
                    db.HisDatabase.AddOrUpdate(hisTag);
                }
                return Task.FromResult(new AddTagReplyMessage() { Result = true, TagId = tag.Id });
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
        public override Task<BoolResultReplay> Save(GetRequest request, ServerCallContext context)
        {
            if (CheckLoginId(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            var db = DbManager.Instance.GetDatabase(request.Database);
            Cdy.Tag.DatabaseSerise serise = new Cdy.Tag.DatabaseSerise() { Dbase = db };
            serise.Save();
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
            if (CheckLoginId(request.LoginId))
            {
                return Task.FromResult(new BoolResultReplay() { Result = false });
            }
            DbManager.Instance.Reload();
            return Task.FromResult(new BoolResultReplay() { Result = true });
        }

        
    }
}
