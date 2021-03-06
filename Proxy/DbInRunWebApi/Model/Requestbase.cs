﻿using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DbInRunWebApi.Model
{
    /// <summary>
    /// 
    /// </summary>
    public class ReponseBase
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
        public string Token { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class LoginResponse:ReponseBase
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
        public string LoginTime { get; set; }

        /// <summary>
        /// 超时时间
        /// </summary>
        public long TimeOut { get; set; }

        /// <summary>
        /// 结果
        /// </summary>
        public bool Result { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }



    /// <summary>
    /// 
    /// </summary>
    public class Requestbase
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
        public string Token { get; set; }

        ///// <summary>
        ///// 
        ///// </summary>
        //public string Time { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    ///// <summary>
    ///// 
    ///// </summary>
    //public class Requestbase2
    //{

    //    #region ... Variables  ...

    //    #endregion ...Variables...

    //    #region ... Events     ...

    //    #endregion ...Events...

    //    #region ... Constructor...

    //    #endregion ...Constructor...

    //    #region ... Properties ...

    //    /// <summary>
    //    /// 
    //    /// </summary>
    //    public string Token { get; set; }

    //    #endregion ...Properties...

    //    #region ... Methods    ...

    //    #endregion ...Methods...

    //    #region ... Interfaces ...

    //    #endregion ...Interfaces...
    //}

    /// <summary>
    /// 
    /// </summary>
    public class RealDataRequest:Requestbase
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
        public string Group { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<string> TagNames { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class RealDataSetRequest : Requestbase
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
        public Dictionary<string,string> Values { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class HisDataRequest : Requestbase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 值的拟合方式
        /// </summary>
        public QueryValueMatchType MatchType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string TagName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<string> Times { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class MutiTagHisDataRequest : Requestbase
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
        public QueryValueMatchType MatchType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<string> TagNames { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<string> Times { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class HisDataRequest2 : Requestbase
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
        public QueryValueMatchType MatchType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string TagName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string StartTime { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public string EndTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Duration { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


    /// <summary>
    /// 
    /// </summary>
    public class MutiTagHisDataRequest2 : Requestbase
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
        public QueryValueMatchType MatchType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<string> TagNames { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string StartTime { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public string EndTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Duration { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


    /// <summary>
    /// 
    /// </summary>
    public class StatisticsDataRequest : Requestbase
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
        public string TagName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string StartTime { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public string EndTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Duration { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class AllHisDataRequest : Requestbase
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
        public string TagName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string StartTime { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public string EndTime { get; set; }


        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class AllMutiTagHisDataRequest : Requestbase
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
        public List<string> TagNames { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string StartTime { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public string EndTime { get; set; }


        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

}
