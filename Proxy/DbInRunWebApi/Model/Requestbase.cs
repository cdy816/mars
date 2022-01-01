using Cdy.Tag;
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
        /// 登录Token
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
        /// 登录时间
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
        /// 登录Token
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
    /// 实时数据请求
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
        /// 变量组
        /// 不为空时，请求组+变量名组合而成的变量的全名的变量的值
        /// </summary>
        public string Group { get; set; }

        /// <summary>
        /// 变量名集合
        /// </summary>
        public List<string> TagNames { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 设置实时变量的的值
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
        /// 多个变量的值的集合
        /// 键值对 [变量-值]
        /// </summary>
        public Dictionary<string,string> Values { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 单个变量的历史数据请求
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
        /// 当指定的时间点，没有记录值，采用改时间点前后两个时刻的值进行拟合时，所采用的拟合方式
        /// 0:取前一个值,1:取后一个值,2:取较近的值,3:线性插值
        /// </summary>
        public QueryValueMatchType MatchType { get; set; }

        /// <summary>
        /// 变量名称
        /// </summary>
        public string TagName { get; set; }

        /// <summary>
        /// 时间点集合
        /// </summary>
        public List<string> Times { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 多个变量的历史数据请求
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
        /// 值的拟合方式
        /// 当指定的时间点，没有记录值，采用改时间点前后两个时刻的值进行拟合时，所采用的拟合方式
        /// 0:取前一个值,1:取后一个值,2:取较近的值,3:线性插值
        /// </summary>
        public QueryValueMatchType MatchType { get; set; }

        /// <summary>
        /// 变量的集合
        /// </summary>
        public List<string> TagNames { get; set; }

        /// <summary>
        /// 时间点集合
        /// </summary>
        public List<string> Times { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 单个变量的历史数据请求
    /// 指定开始、结束时间，时间间隔
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
        /// 值的拟合方式
        /// 当指定的时间点，没有记录值，采用改时间点前后两个时刻的值进行拟合时，所采用的拟合方式
        /// 0:取前一个值,1:取后一个值,2:取较近的值,3:线性插值
        /// </summary>
        public QueryValueMatchType MatchType { get; set; }

        /// <summary>
        /// 变量名称
        /// </summary>
        public string TagName { get; set; }

        /// <summary>
        /// 开始时间
        /// </summary>
        public string StartTime { get; set; }


        /// <summary>
        /// 结束时间
        /// </summary>
        public string EndTime { get; set; }

        /// <summary>
        /// 时间间隔：单位秒
        /// </summary>
        public string Duration { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


    /// <summary>
    /// 多个变量的历史数据请求
    /// 指定开始、结束时间，时间间隔
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
        /// 值的拟合方式
        /// 当指定的时间点，没有记录值，采用改时间点前后两个时刻的值进行拟合时，所采用的拟合方式
        /// 0:取前一个值,1:取后一个值,2:取较近的值,3:线性插值
        /// </summary>
        public QueryValueMatchType MatchType { get; set; }

        /// <summary>
        /// 变量的集合
        /// </summary>
        public List<string> TagNames { get; set; }

        /// <summary>
        /// 开始时间
        /// </summary>
        public string StartTime { get; set; }


        /// <summary>
        /// 结束时间
        /// </summary>
        public string EndTime { get; set; }

        /// <summary>
        /// 时间间隔：单位秒
        /// </summary>
        public string Duration { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


    /// <summary>
    /// 获取统计信息
    /// 统计信息是单独记录的，查询速度要优于查询出所有值再做统计的方式
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
        /// 变量名称
        /// </summary>
        public string TagName { get; set; }

        /// <summary>
        /// 开始时间
        /// </summary>
        public string StartTime { get; set; }


        /// <summary>
        /// 结束时间
        /// </summary>
        public string EndTime { get; set; }

        /// <summary>
        /// 时间间隔：单位秒
        /// </summary>
        public string Duration { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 获取某个变量的在某个时间段内历史记录的原始值
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
        /// 变量
        /// </summary>
        public string TagName { get; set; }

        /// <summary>
        /// 开始时间
        /// </summary>
        public string StartTime { get; set; }


        /// <summary>
        /// 结束时间
        /// </summary>
        public string EndTime { get; set; }


        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 获取多个变量在一段时间内历史记录的原始值
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
        /// 变量集合
        /// </summary>
        public List<string> TagNames { get; set; }

        /// <summary>
        /// 开始时间
        /// </summary>
        public string StartTime { get; set; }


        /// <summary>
        /// 结束时间
        /// </summary>
        public string EndTime { get; set; }


        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 比较类型
    /// </summary>
    public enum CompareType
    {
        /// <summary>
        /// 大于
        /// </summary>
        Great,
        /// <summary>
        /// 小于
        /// </summary>
        Low,
        /// <summary>
        /// 等于
        /// </summary>
        Equal
    }

    /// <summary>
    /// 
    /// </summary>
    public class FindTagValueEqualRequest : AllHisDataRequest
    {
        /// <summary>
        /// 值比较类型
        /// 0:大于，1:小于，2:等于
        /// </summary>
        public CompareType ValueCompareType { get; set; }

        /// <summary>
        /// 值
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Interval { get; set; } = "0";
    }

    /// <summary>
    /// 
    /// </summary>
    public class FindTagValueResult : ResponseBase
    {
        /// <summary>
        /// 变量名
        /// </summary>
        public string TagName { get; set; }

        /// <summary>
        /// 结果值
        /// </summary>
        public object Value { get; set; }
    }



}
