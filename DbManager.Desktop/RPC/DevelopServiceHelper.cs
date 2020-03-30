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

namespace DBInStudio.Desktop.RPC
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

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

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

                Grpc.Net.Client.GrpcChannel grpcChannel = Grpc.Net.Client.GrpcChannel.ForAddress(@"https://" + ip + ":5001", new GrpcChannelOptions { HttpClient = httpClient });
                return new DBDevelopService.DevelopServer.DevelopServerClient(grpcChannel);
            }
            catch
            {
                return null;
            }
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
                catch
                {
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

            }
            return new Dictionary<string, string>();
        }

        public bool NewDatabase(string name,string desc)
        {
            if(mCurrentClient!=null&&!string.IsNullOrEmpty(mLoginId))
            {
               return mCurrentClient.NewDatabase(new DBDevelopService.NewDatabaseRequest() { Database = name, LoginId = mLoginId,Desc=desc }).Result;
            }
            return false;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
