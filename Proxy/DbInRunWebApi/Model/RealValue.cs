using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DbInRunWebApi.Model
{
    /// <summary>
    /// 实时值
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
        /// 
        /// </summary>
        public DateTime Time { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public byte Quality { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public object Value { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...



        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
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
        /// 
        /// </summary>
        public byte Quality { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public object Value { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...



        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
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
        /// 
        /// </summary>
        public string tagName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<ValueItem> Values { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
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
        /// 
        /// </summary>
        public DateTime Time { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string Value { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public byte Quality { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class StatisticsValue : ResponseBase
    {
        /// <summary>
        /// 
        /// </summary>
        public string tagName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<StatisticsValueItem> Values { get; set; } = new List<StatisticsValueItem>();
    }

    /// <summary>
    /// 
    /// </summary>
    public class StatisticsValueItem
    {
        /// <summary>
        /// 
        /// </summary>
        public DateTime Time { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public double AvgValue { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime MaxValueTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public double MaxValue { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime MinValueTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public double MinValue { get; set; }
    }




    /// <summary>
    /// 
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
        /// 
        /// </summary>
        public List<RealValue> Datas { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


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
        /// 
        /// </summary>
        public List<RealValueAndQuality> Datas { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

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
        /// 
        /// </summary>
        public List<object> Datas { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

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
        /// 
        /// </summary>
        public Dictionary<string,bool> SetResults { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
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
        /// 
        /// </summary>
        public HisValue Datas { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


    /// <summary>
    /// 
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
        /// 
        /// </summary>
        public bool Result { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string ErroMessage { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

}
