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
using Cheetah;
using DotNetty.Buffers;

namespace DBRuntime.Api
{
    public class TagInfoServerProcess : ServerProcessBase
    {

        #region ... Variables  ...
        
        public const byte GetTagIdByNameFun = 0;

        public const byte Login = 1;

        public const byte RegistValueCallBack = 2;

        public const byte GetdatabaseName = 3;

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
        protected unsafe override void ProcessSingleData(string client, ByteBuffer data)
        {
            var mm = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.ITagManager>();
            byte sfun = data.ReadByte();
            switch (sfun)
            {
                case Login:
                    string user = data.ReadString();
                    string pass = data.ReadString();
                    string result = Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().Login(user, pass);
                    if (!string.IsNullOrEmpty(result))
                    {
                        Parent.AsyncCallback(client, ToByteBuffer(ApiFunConst.TagInfoRequest,result));
                    }
                    else
                    {
                        Parent.AsyncCallback(client, ToByteBuffer(ApiFunConst.TagInfoRequest, ""));
                    }
                    break;


            }

           
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
