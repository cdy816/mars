using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cdy.Tag;
using DbInRunWebApi.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace DbInRunWebApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class HisDataController : ControllerBase
    {
        private HisValue ProcessResult<T>(string tagname, object datas)
        {
            HisValue re = new HisValue() { tagName = tagname, Result = true };
            List<ValueItem> values = new List<ValueItem>();
            var vdata = datas as HisQueryResult<T>;
            if (vdata != null)
            {
                for (int i = 0; i < vdata.Count; i++)
                {
                    byte qu;
                    DateTime time;
                    var val = vdata.GetValue(i, out time, out qu);
                    values.Add(new ValueItem() { Time = time, Quality = qu, Value = val.ToString() });
                }
                re.Values = values;
            }
            return re;
        }


        //private HisValue ProcessResult<T>(string tagname, Dictionary<DateTime, Tuple<object, byte>> datas)
        //{
        //    HisValue re = new HisValue() { tagName = tagname, Result = true };
        //    List<ValueItem> values = new List<ValueItem>();
        //    if (datas != null)
        //    {
        //        foreach (var vv in datas)
        //        {
        //            values.Add(new ValueItem() { Time = vv.Key, Value = vv.Value.Item1.ToString(), Quality = vv.Value.Item2 });
        //        }
        //    }
        //    re.Values = values;
        //    return re;
        //}

        private List<DateTime> ConvertToTimes(List<string> time)
        {
            return time.Select(e => DateTime.Parse(e)).ToList();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet()]
        public HisValue Get([FromBody] HisDataRequest request)
        {
            var tag  = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
            if (tag == null) return null;
            object res;
            HisValue revals=null;
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<bool>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<bool>(request.TagName,res);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<byte>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<byte>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<DateTime>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<DateTime>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<double>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<double>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<float>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<float>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<int>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<int>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<long>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<long>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<short>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<short>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<string>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<string>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<uint>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<uint>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ulong>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<ulong>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ushort>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<ushort>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPointData>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<IntPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPointData>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<UIntPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPoint3Data>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPoint3Data>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPointData>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<LongPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPointTag>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<ULongPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPoint3Data>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPoint3Data>(tag.Id,ConvertToTimes(request.Times),request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(request.TagName, res);
                    break;
            }

            
            return revals;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetByTimeSpan")]
        public HisValue GetByTimeSpan([FromBody] HisDataRequest2 request)
        {
            var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
            if (tag == null) return null;
            object res;
            HisValue revals = null;
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<bool>(tag.Id, ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<bool>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<byte>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<byte>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<DateTime>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<DateTime>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<double>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<double>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<float>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<float>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<int>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<int>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<long>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<long>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<short>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<short>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<string>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<string>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<uint>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<uint>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ulong>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ulong>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ushort>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ushort>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPointData>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<IntPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPointData>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<UIntPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<IntPoint3Data>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<IntPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<UIntPoint3Data>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<UIntPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPointData>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<LongPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPointTag>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ULongPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<LongPoint3Data>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<LongPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryHisData<ULongPoint3Data>(tag.Id,  ConvertToDateTime(request.StartTime),ConvertToDateTime(request.EndTime),ConvertToTimeSpan(request.Duration), request.MatchType);
                    revals = ProcessResult<ULongPoint3Data>(request.TagName, res);
                    break;
            }


            return revals;
          
        }

        private DateTime ConvertToDateTime(string time)
        {
            return DateTime.Parse(time);
        }

        private TimeSpan ConvertToTimeSpan(string time)
        {
            return TimeSpan.FromSeconds(int.Parse(time));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetAllValue")]
        public HisValue GetAllValue([FromBody] AllHisDataRequest request)
        {
            var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
            if (tag == null) return null;
            object res;
            HisValue revals = null;
            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<bool>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<bool>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<byte>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<byte>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<DateTime>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<DateTime>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<double>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<double>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<float>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<float>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<int>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<long>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<long>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<short>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<short>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<string>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<string>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<uint>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ulong>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ulong>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ushort>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ushort>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<int>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<IntPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<uint>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<UIntPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<IntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<IntPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<UIntPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<UIntPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<LongPointData>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<LongPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ULongPointTag>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ULongPointData>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<LongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<LongPoint3Data>(request.TagName, res);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ULongPoint3Data>(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));
                    revals = ProcessResult<ULongPoint3Data>(request.TagName, res);
                    break;
            }


            return revals;
        }


        /// <summary>
        /// 获取统计信息
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetStatisticsValue")]
        public StatisticsValue GetStatisticsValue([FromBody] AllHisDataRequest request)
        {
            var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
            if (tag == null) return null;
            StatisticsValue revals = new StatisticsValue() { tagName = request.TagName };
           var res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryStatisticsHisData(tag.Id, ConvertToDateTime(request.StartTime), ConvertToDateTime(request.EndTime));

            if(res!=null)
            {
                double avgvalue, maxvalue, minvalue;
                DateTime time, maxtime, mintime;
                for(int i=0;i<res.Count;i++)
                {
                    res.ReadValue(i, out time, out avgvalue, out maxvalue, out maxtime, out minvalue, out mintime);
                    revals.Values.Add(new StatisticsValueItem() { Time = time, AvgValue = avgvalue, MaxValue = maxvalue, MinValue = minvalue, MaxValueTime = maxtime, MinValueTime = mintime });
                }
            }

            return revals;
        }

        /// <summary>
        /// 获取统计信息，通过时间点
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetStatisticsValueByTimeSpan")]
        public StatisticsValue GetStatisticsValueByTimeSpan([FromBody] StatisticsDataRequest request)
        {
            var tag = ServiceLocator.Locator.Resolve<ITagManager>().GetTagByName(request.TagName);
            if (tag == null) return null;
            StatisticsValue revals = new StatisticsValue() { tagName = request.TagName };

            List<DateTime> ltmp = new List<DateTime>();
            DateTime dtime = ConvertToDateTime(request.StartTime);
            DateTime etime = ConvertToDateTime(request.EndTime);
            while (dtime <= etime)
            {
                ltmp.Add(dtime);
                dtime = dtime.Add(ConvertToTimeSpan(request.Duration));
            }

            var res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryStatisticsHisData(tag.Id, ltmp);

            if (res != null)
            {
                double avgvalue, maxvalue, minvalue;
                DateTime time, maxtime, mintime;
                for (int i = 0; i < res.Count; i++)
                {
                    res.ReadValue(i, out time, out avgvalue, out maxvalue, out maxtime, out minvalue, out mintime);
                    revals.Values.Add(new StatisticsValueItem() { Time = time, AvgValue = avgvalue, MaxValue = maxvalue, MinValue = minvalue, MaxValueTime = maxtime, MinValueTime = mintime });
                }
            }

            return revals;
        }

    }
}
