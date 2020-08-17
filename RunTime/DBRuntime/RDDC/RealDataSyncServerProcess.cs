//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/8/17 15:52:53.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using DBRuntime.Api;
using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace DBRuntime.RDDC
{
    public class RealDataSyncServerProcess : RDDCServerProcessBase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public override byte FunId => 2;


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        protected override void ProcessSingleData(string client, IByteBuffer data)
        {
            ProcessRealDataSync(client);
            base.ProcessSingleData(client, data);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        private void ProcessRealDataSync(string clientId)
        {
            var service = ServiceLocator.Locator.Resolve<IRealData>();
            var real = (service as RealEnginer);
            var re = BufferManager.Manager.Allocate(ApiFunConst.RealDataSyncFun,4);
            re.WriteInt(real.Memory.Length);
            re = Unpooled.CompositeBuffer().AddComponents(true, re, Unpooled.WrappedBuffer(real.Memory, 0, real.Memory.Length));
            Parent.AsyncCallback(clientId, re);
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
