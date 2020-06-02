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
using System.Diagnostics;
using System.Text;
using System.Threading;
using Cdy.Tag;
using DotNetty.Buffers;

namespace DBHighApi.Api
{
    public class TagInfoServerProcess : ServerProcessBase
    {

        #region ... Variables  ...
        
        public const byte GetTagIdByNameFun = 0;

        public const byte Login = 1;

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
                            var re = BufferManager.Manager.Allocate(ApiFunConst.TagInfoRequest,count * 4);
                            for(int i=0;i<count;i++)
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
                    long result = Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().Login(user, pass,client);
                    Parent.AsyncCallback(client, ToByteBuffer(ApiFunConst.TagInfoRequest, result));
                    Debug.Print("处理登录并返回:"+result);
                    break;
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
