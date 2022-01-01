using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DbInRunWebApi.Model
{
    /// <summary>
    /// 实时值(时间、质量戳、值)
    /// </summary>
    public class RealValue
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 时间
        /// </summary>
        public DateTime Time { get; set; }

        /// <summary>
        /// 质量戳
        /// </summary>
        public byte Quality { get; set; }

        /// <summary>
        /// 值
        /// </summary>
        public object Value { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...



        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 实时值包括质量戳
    /// </summary>
    public class RealValueAndQuality
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 质量戳
        /// </summary>
        public byte Quality { get; set; }

        /// <summary>
        /// 值
        /// </summary>
        public object Value { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...



        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 历史值
    /// </summary>
    public class HisValue: ResponseBase
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
        public string tagName { get; set; }

        /// <summary>
        /// 历史数据值的集合
        /// </summary>
        public List<ValueItem> Values { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 值
    /// </summary>
    public class ValueItem
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 时间
        /// </summary>
        public DateTime Time { get; set; }
        /// <summary>
        /// 值
        /// </summary>
        public string Value { get; set; }
        /// <summary>
        /// 质量戳
        /// </summary>
        public byte Quality { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 统计值
    /// </summary>
    public class StatisticsValue : ResponseBase
    {
        /// <summary>
        /// 变量名
        /// </summary>
        public string tagName { get; set; }

        /// <summary>
        /// 统计值集合
        /// </summary>
        public List<StatisticsValueItem> Values { get; set; } = new List<StatisticsValueItem>();
    }

    /// <summary>
    /// 单个统计点的值
    /// </summary>
    public class StatisticsValueItem
    {
        /// <summary>
        /// 时间
        /// </summary>
        public DateTime Time { get; set; }

        /// <summary>
        /// 平均值
        /// </summary>
        public double AvgValue { get; set; }

        /// <summary>
        /// 最大值时间
        /// </summary>
        public DateTime MaxValueTime { get; set; }

        /// <summary>
        /// 最大值
        /// </summary>
        public double MaxValue { get; set; }

        /// <summary>
        /// 最小值时间
        /// </summary>
        public DateTime MinValueTime { get; set; }

        /// <summary>
        /// 最小值
        /// </summary>
        public double MinValue { get; set; }
    }




    /// <summary>
    /// 实时值请求返回
    /// 包括时间、值、质量
    /// </summary>
    public class RealValueQueryResponse : ResponseBase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 实时值集合
        /// </summary>
        public List<RealValue> Datas { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 实时值请求返回
    /// 仅包括值、质量
    /// </summary>
    public class RealValueAndQualityQueryResponse : ResponseBase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 实时值集合
        /// </summary>
        public List<RealValueAndQuality> Datas { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
    /// <summary>
    /// 实时值请求返回
    /// 仅包括值
    /// </summary>
    public class RealValueOnlyQueryResponse : ResponseBase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 值集合
        /// </summary>
        public List<object> Datas { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 实时值设置返回
    /// </summary>
    public class RealDataSetResponse : ResponseBase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 返回是否成功结果
        /// True：成功，False：失败
        /// </summary>
        public Dictionary<string,bool> SetResults { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 历史值请求返回结果
    /// </summary>
    public class HisValueQueryResponse : ResponseBase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 历史值
        /// </summary>
        public HisValue Datas { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


    /// <summary>
    /// 请求返回结果基类
    /// </summary>
    public class ResponseBase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 操作结果
        /// True：请求成功
        /// </summary>
        public bool Result { get; set; }

        /// <summary>
        /// 错误消息
        /// </summary>
        public string ErroMessage { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

}
