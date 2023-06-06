using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cdy.Tag.Interface
{
    /// <summary>
    /// 
    /// </summary>
    public class MultiHisQueryResult : IDisposable
    {

        public Dictionary<string,IHisQueryResult> Columns { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public MultiHisQueryResult()
        {
            Columns = new Dictionary<string, IHisQueryResult>();    
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public IEnumerable<object> Read(int row)
        {
            List<object> list = new List<object>();
            int i = 0;
            foreach(var vv in Columns)
            {
                if(i==0)
                {
                    list.Add(vv.Value.GetTime(row));
                }
                list.Add(vv.Value.GetValue(row));
                i++;
            }
            return list;
        }

        /// <summary>
        /// 读书带列明的值
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public Dictionary<string,object> ReadAsKeyValue(int row)
        {
            Dictionary<string,object> vals = new Dictionary<string, object>();
            int i = 0;
            foreach (var vv in Columns)
            {
                if (i == 0)
                {
                    vals.Add("_time",vv.Value.GetTime(row));
                }
                vals.Add(vv.Key,vv.Value.GetValue(row));
                i++;
            }
            return vals;
        }

        /// <summary>
        /// 枚举所有值
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Dictionary<string,object>> ListVallValues()
        {
            if(Columns.Count == 0)
            {
                yield return null;
            }
            int count = Columns.Values.First().Count;
            for(int i = 0; i < count; i++)
            {
                yield return ReadAsKeyValue(i);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="column"></param>
        /// <param name="row"></param>
        /// <returns></returns>
        public object ReadValue(string column, int row)
        {
            if (Columns.ContainsKey(column))
            {
                return Columns[column].GetValue(row);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool AddRow(DateTime time, object[] values)
        {
            int i = 0;
            foreach(var vv in Columns.Values)
            {
                if(i<  values.Length)
                {
                    vv.Add(values[i], time, 0);
                }
                else
                {
                    break;
                }
                i++;
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="column"></param>
        public void AddColumn(string name, IHisQueryResult column)
        {
            if(Columns.Count == 0)
            {
                Columns.Add(name, column);
            }
            else
            {
                if (Columns.First().Value.Count == column.Count)
                {
                    Columns.Add(name, column);
                }
            }
        }

        public void Dispose()
        {
            foreach(var vv in Columns.Values)
            {
                (vv as IDisposable).Dispose();
            }
        }
    }
}
