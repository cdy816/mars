//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/9 21:48:50.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public interface IRuntimeSecurity
    {

        #region ... Variables  ...

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
        /// <param name="id"></param>
        /// <returns></returns>
        bool CheckLogin(string id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        bool CheckLogin(long id);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        bool FreshUserId(string id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <param name="pass"></param>
        /// <returns></returns>
        string Login(string user, string pass);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <param name="pass"></param>
        /// <returns></returns>
        long Login(string user, string pass,string clientId);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        bool Logout(string id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        bool Logout(long id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        bool LogoutByClientId(string id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        string GetUserByLoginId(string id);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <returns></returns>
        List<UserPermission> GetPermission(string user);

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
