//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/14 11:00:38.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Cdy.Tag;
using DotNetty.Buffers;

namespace DBRuntime.Api
{
    public class TagInfoServerProcess : ServerProcessBase, IAPINotify
    {

        #region ... Variables  ...
        
        public const byte GetTagIdByNameFun = 0;

        public const byte Login = 1;

        public const byte RegistValueCallBack = 2;

        public const byte GetdatabaseName = 3;

        public const byte SyncRealTagConfig = 30;

        public const byte SyncHisTagConfig = 31;

        public const byte SyncSecuritySetting = 32;

       

        private List<string> mClients = new List<string>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...



        #endregion ...Constructor...

        #region ... Properties ...

        public override byte FunId => ApiFunConst.TagInfoRequest;

        #endregion ...Properties...

        #region ... Methods    ...



        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        protected unsafe override void ProcessSingleData(string client, IByteBuffer data)
        {
            var mm = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.ITagManager>();
            byte sfun = data.ReadByte();
            switch (sfun)
            {
                case GetTagIdByNameFun:
                    long loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        int count = data.ReadInt();
                        if (count > 0)
                        {
                            var re = BufferManager.Manager.Allocate(ApiFunConst.TagInfoRequest, count * 4);
                            for (int i = 0; i < count; i++)
                            {
                                var ival = mm.GetTagIdByName(data.ReadString());
                                if (ival.HasValue)
                                {
                                    re.WriteInt(ival.Value);
                                }
                                else
                                {
                                    re.WriteInt((int)-1);
                                }
                            }
                            Parent.AsyncCallback(client, re);
                        }
                    }
                    break;
                case Login:
                    string user = data.ReadString();
                    string pass = data.ReadString();
                    long result = Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().Login(user, pass, client);

                    if (result > 0)
                    {
                        mClients.Add(client);
                    }

                    Parent.AsyncCallback(client, ToByteBuffer(ApiFunConst.TagInfoRequest, result));
                    break;
                case GetdatabaseName:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        Parent.AsyncCallback(client, ToByteBuffer(ApiFunConst.TagInfoRequest, Runner3.CurrentDatabase + "," + Runner3.CurrentDatabaseVersion + "," + Runner3.CurrentDatabaseLastUpdateTime));
                    }
                    break;
                case SyncRealTagConfig:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        var vss = mm.SeriseToStream();
                        Parent.AsyncCallback(client, ToByteBuffer(ApiFunConst.SyncRealTagConfig, vss));
                    }
                    break;
                case SyncSecuritySetting:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        var vss = (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>() as SecurityRunner).Document.SeriseToStream();
                        Parent.AsyncCallback(client, ToByteBuffer(ApiFunConst.SyncSecuritySetting, vss));
                    }
                    break;

                case SyncHisTagConfig:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        var vss = (Cdy.Tag.ServiceLocator.Locator.Resolve<IHisTagQuery>() as HisEnginer3).HisTagManager.SeriseToStream();
                        Parent.AsyncCallback(client, ToByteBuffer(ApiFunConst.SyncHisTagConfig, vss));
                    }
                    break;
            }
            base.ProcessSingleData(client, data);
        }


        /// <summary>
        /// 通知数据发送变化
        /// </summary>
        public void NotifyDatabaseChanged(bool realchanged,bool hischanged,bool securitychanged)
        {
            byte val = 0;
            if (realchanged) val += 1;
            if (hischanged) val += 2;
            if (securitychanged) val += 4;
            if (val > 0)
            {
                IByteBuffer data = ToByteBuffer(ApiFunConst.TagInfoNotify, ApiFunConst.DatabaseChangedNotify, val);
                foreach (var vv in mClients)
                {
                    Parent.AsyncCallback(vv, data);
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>

        public override void OnClientDisconnected(string id)
        {
            if(mClients.Contains(id))
            {
                mClients.Remove(id);
            }
            base.OnClientDisconnected(id);
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
