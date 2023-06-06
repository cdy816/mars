using Cdy.Tag;
using Google.Protobuf.WellKnownTypes;
using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class FilterAction: IFilterAction
    {
        /// <summary>
        /// 变量ID
        /// </summary>
        public int TagId { get; set; }

        public string TagName { get; set; }

        /// <summary>
        /// 值
        /// </summary>
        public object Target { get; set; }

        public bool IgnorFit { get; set; }

        public abstract bool IsFit(Dictionary<string,object> value);

        public bool IsEnqeue(object source, object target)
        {
            var code = Convert.GetTypeCode(target);
            try
            {
                switch (code)
                {
                    case TypeCode.Boolean:
                        bool bval = Convert.ToBoolean(source);
                        return bval == Convert.ToBoolean(target);
                    case TypeCode.Byte:
                        var nbval = Convert.ToByte(source);
                        return nbval == Convert.ToByte(target);
                    case TypeCode.Int16:
                        var sval = Convert.ToInt16(source);
                        return sval == Convert.ToInt16(target);
                    case TypeCode.Int32:
                        var ival = Convert.ToInt32(source);
                        return ival == Convert.ToInt32(target);
                    case TypeCode.Int64:
                        var iival = Convert.ToInt64(source);
                        return iival == Convert.ToInt64(target);
                    case TypeCode.Single:
                        var fval = Convert.ToSingle(source);
                        return fval == Convert.ToSingle(target);
                    case TypeCode.Double:
                        var dval = Convert.ToDouble(source);
                        return dval == Convert.ToDouble(target);
                    case TypeCode.UInt16:
                        var uinval = Convert.ToUInt16(source);
                        return uinval == Convert.ToUInt16(target);
                    case TypeCode.UInt32:
                        var uival = Convert.ToUInt32(source);
                        return uival == Convert.ToUInt32(target);
                    case TypeCode.UInt64:
                        var ulval = Convert.ToUInt64(source);
                        return ulval == Convert.ToUInt64(target);
                    case TypeCode.DateTime:
                        var dtime = DateTime.Parse(source.ToString());
                        return dtime == Convert.ToDateTime(target);
                    default:
                        return source.ToString() == target.ToString();

                }
            }
            catch
            {

            }
            return false;
        }

        
    }

    public class ConstAction:FilterAction
    {
        public override bool IsFit(Dictionary<string, object> value)
        {
            return Convert.ToBoolean(value);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class EqualAction:FilterAction
    {
        public override bool IsFit(Dictionary<string, object> value)
        {
            if(!IgnorFit && value.ContainsKey(this.TagId.ToString()))
            {
                return IsEnqeue(Target ,value[TagId.ToString()]);
            }
            else
            {
                return true;
            }
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.IsNullOrEmpty(TagName) ? TagId.ToString() : TagName + "=" + Target;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class NotEqualAction : FilterAction
    {
        public override bool IsFit(Dictionary<string, object> value)
        {
            if (!IgnorFit && value.ContainsKey(this.TagId.ToString()))
            {
                return !IsEnqeue(Target , value[TagId.ToString()]);
            }
            else
            {
                return true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.IsNullOrEmpty(TagName) ? TagId.ToString() : TagName + "!=" + Target;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class GreatAction:FilterAction
    {
        public override bool IsFit(Dictionary<string, object> value)
        {
            try
            {
                if (!IgnorFit && value.ContainsKey(this.TagId.ToString()))
                {
                    var val = value[this.TagId.ToString()];
                    var code = Convert.GetTypeCode(val);
                    switch(code)
                    {
                        case TypeCode.Boolean:
                            var bval = bool.Parse(Target.ToString());
                            return Convert.ToByte(bval) > Convert.ToByte(val);
                        case TypeCode.String:
                        case TypeCode.Empty:
                        case TypeCode.Object:
                            return Target.ToString().CompareTo(val.ToString())>0;
                        case TypeCode.DateTime:
                            var dtval = DateTime.Parse(Target.ToString());
                            return dtval> Convert.ToDateTime(val);
                        default:
                            double dtmp = Convert.ToDouble(Target);
                            double dval = Convert.ToDouble(value[TagId.ToString()]);
                            return dval > dtmp;
                    }
                   
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.IsNullOrEmpty(TagName) ? TagId.ToString() : TagName + ">" + Target;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class GreatEqualAction : FilterAction
    {
        public override bool IsFit(Dictionary<string, object> value)
        {
            try
            {
                if (!IgnorFit && value.ContainsKey(this.TagId.ToString()))
                {
                    var val = value[this.TagId.ToString()];
                    var code = Convert.GetTypeCode(val);
                    switch (code)
                    {
                        case TypeCode.Boolean:
                            var bval = bool.Parse(Target.ToString());
                            return Convert.ToByte(bval) >= Convert.ToByte(val);
                        case TypeCode.String:
                        case TypeCode.Empty:
                        case TypeCode.Object:
                            return Target.ToString().CompareTo(val.ToString()) >= 0;
                        case TypeCode.DateTime:
                            var dtval = DateTime.Parse(Target.ToString());
                            return dtval >= Convert.ToDateTime(val);
                        default:
                            double dtmp = Convert.ToDouble(Target);
                            double dval = Convert.ToDouble(value[TagId.ToString()]);
                            return dval >= dtmp;
                    }
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        public override string ToString()
        {
            return string.IsNullOrEmpty(TagName) ? TagId.ToString() : TagName + ">=" + Target;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class LowerAction : FilterAction
    {
        public override bool IsFit(Dictionary<string, object> value)
        {
            try
            {
                if (!IgnorFit && value.ContainsKey(this.TagId.ToString()))
                {
                    var val = value[this.TagId.ToString()];
                    var code = Convert.GetTypeCode(val);
                    switch (code)
                    {
                        case TypeCode.Boolean:
                            var bval = bool.Parse(Target.ToString());
                            return Convert.ToByte(bval)< Convert.ToByte(val);
                        case TypeCode.String:
                        case TypeCode.Empty:
                        case TypeCode.Object:
                            return Target.ToString().CompareTo(val.ToString()) < 0;
                        case TypeCode.DateTime:
                            var dtval = DateTime.Parse(Target.ToString());
                            return dtval < Convert.ToDateTime(val);
                        default:
                            double dtmp = Convert.ToDouble(Target);
                            double dval = Convert.ToDouble(value[TagId.ToString()]);
                            return dval < dtmp;
                    }
                }
                return true;
            }
            catch
            {
                return false;
            }
        }


        public override string ToString()
        {
            return string.IsNullOrEmpty(TagName)? TagId.ToString():TagName + "<" + Target;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class LowerEqualAction : FilterAction
    {
        public override bool IsFit(Dictionary<string, object> value)
        {
            try
            {
                if (!IgnorFit && value.ContainsKey(this.TagId.ToString()))
                {
                    var val = value[this.TagId.ToString()];
                    var code = Convert.GetTypeCode(val);
                    switch (code)
                    {
                        case TypeCode.Boolean:
                            var bval = bool.Parse(Target.ToString());
                            return Convert.ToByte(bval) <= Convert.ToByte(val);
                        case TypeCode.String:
                        case TypeCode.Empty:
                        case TypeCode.Object:
                            return Target.ToString().CompareTo(val.ToString()) <= 0;
                        case TypeCode.DateTime:
                            var dtval = DateTime.Parse(Target.ToString());
                            return dtval <= Convert.ToDateTime(val);
                        default:
                            double dtmp = Convert.ToDouble(Target);
                            double dval = Convert.ToDouble(value[TagId.ToString()]);
                            return dval <= dtmp;
                    }
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.IsNullOrEmpty(TagName) ? TagId.ToString() : TagName + "<=" + Target;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class ExpressFilter: IFilterAction
    {
        /// <summary>
        /// 
        /// </summary>
        public FilterAction LowerTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public FilterAction UpperTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<IFilterAction> Filters { get; set; }= new List<IFilterAction>();

        /// <summary>
        /// 
        /// </summary>
        public List<FilterUnion> Unions { get; set; }=new List<FilterUnion>();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="union"></param>
        /// <param name="action"></param>
        public ExpressFilter PushFilter(FilterUnion union,FilterAction action)
        {
            if(union != null)
            {
                if (Unions == null)
                {
                    Unions = new List<FilterUnion>();
                }
                Unions.Add(union);
            }
            Filters.Add(action);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool IsFit(Dictionary<string, object> value)
        {
            if(Unions!=null&&Unions.Count>0)
            {
                var bval = Filters[0].IsFit(value);
                for(int i=1;i< Filters.Count;i++)
                {
                    bval = bval.Union(Filters[i], value, Unions[i-1]);
                }
                return bval;
            }
            else
            {
                return Filters[0].IsFit(value);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<int> ListTagIds()
        {
            List<int> list = new List<int>();
            foreach(var vv in Filters)
            {
                if (vv is FilterAction && !list.Contains((vv as FilterAction).TagId))
                {
                    list.Add((vv as FilterAction).TagId);
                }
                else if(vv is ExpressFilter)
                {
                    ListTagIdsInner(vv as ExpressFilter, list);
                }
            }
            return list;
        }

        private void ListTagIdsInner(ExpressFilter filter,List<int> list)
        {
            foreach (var vv in filter.Filters)
            {
                if (vv is FilterAction && !list.Contains((vv as FilterAction).TagId))
                {
                    list.Add((vv as FilterAction).TagId);
                }
                else if (vv is ExpressFilter)
                {
                    ListTagIdsInner(vv as ExpressFilter, list);
                }
            }
        }

        public override string ToString()
        {
            StringBuilder sb= new StringBuilder();
            sb.Append("(");
            for(int i=0; i < Filters.Count;i++)
            {
                sb.Append(Filters[i].ToString()+" ");
                if(i<Unions.Count)
                {
                    sb.Append(Unions[i].ToString()+" ");
                }
            }
            sb.Length = sb.Length > 2 ? sb.Length - 1 : sb.Length;

            sb.Append(")");
            return sb.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        public ExpressFilter FromString(string str)
        {
            string stmp = str;
            ExpressReader er = new ExpressReader(stmp);

            List<string> ls = new List<string>();
            bool isbegininner = false;
            int mcount = 0;
            int leftcount = 0;
            int rightcount = 0;
            foreach (var vv in er.ReadKeyWords())
            {
                if(string.IsNullOrEmpty(vv)) continue;
                if(vv=="(")
                {
                    leftcount++;
                    mcount++;
                    isbegininner = true;
                    continue;
                }
                else if(vv==")")
                {
                    rightcount++;
                    mcount--;
                    if (mcount == 0)
                    {
                        isbegininner = false;
                        var ff = new ExpressFilter().FromListString(ls);
                        Filters.Add(ff);
                        if (this.LowerTime == null && ff.LowerTime != null)
                        {
                            this.LowerTime = ff.LowerTime;
                        }
                        if (this.UpperTime == null && ff.UpperTime != null)
                        {
                            this.UpperTime = ff.UpperTime;
                        }
                        ls.Clear();
                    }
                    continue;
                }
                else if(!isbegininner)
                {
                    if(vv=="and")
                    {
                        AddNewFilter(ls);
                        Unions.Add(new FilterUnion() { Type = FilterUnion.UnionType.And });
                        ls.Clear();
                    }
                    else if(vv=="or")
                    {
                        AddNewFilter(ls);
                        Unions.Add(new FilterUnion() { Type = FilterUnion.UnionType.Or });
                        ls.Clear();
                    }
                    else
                    {
                        ls.Add(vv);
                    }
                }
                else
                {
                    ls.Add(vv);
                }
            }
            if (leftcount != rightcount)
            {
                throw new Exception("解析 where 错误:()不匹配!");
            }
            return this;
        }

        public ExpressFilter FromListString(List<string> val)
        {
            List<string> ls = new List<string>();
            bool isbegininner = false;
            int leftcount = 0;
            int rightcount = 0;
            foreach (var vv in val)
            {
                if (vv.EndsWith("("))
                {
                    leftcount++;
                    isbegininner = true;
                    continue;
                }
                else if (vv.EndsWith(')'))
                {
                    isbegininner = false;
                    rightcount++;
                    var ff = new ExpressFilter().FromListString(ls);
                    if (this.LowerTime == null && ff.LowerTime != null)
                    {
                        this.LowerTime = ff.LowerTime;
                    }

                    if (this.UpperTime == null && ff.UpperTime != null)
                    {
                        this.UpperTime = ff.UpperTime;
                    }
                    Filters.Add(ff);
                    ls.Clear();
                    continue;

                }
                else if (!isbegininner)
                {
                    if (vv == "and")
                    {
                        AddNewFilter(ls);
                        Unions.Add(new FilterUnion() { Type = FilterUnion.UnionType.And });
                        ls.Clear();
                    }
                    else if (vv == "or")
                    {
                        AddNewFilter(ls);
                        Unions.Add(new FilterUnion() { Type = FilterUnion.UnionType.Or });
                        ls.Clear();
                    }
                    else
                    {
                        ls.Add(vv);
                    }
                }
                else
                {
                    if (!string.IsNullOrEmpty(vv))
                        ls.Add(vv);
                }
            }
            if(ls.Count > 0)
            {
                AddNewFilter(ls);
            }
            if(leftcount!=rightcount)
            {
                throw new Exception("解析 where 错误:()不匹配!");
            }
            return this;
        }

        private void AddNewFilter(List<string> ls)
        {
            if(ls.Count>=3)
            {
                if (ls[1]=="==")
                {
                    this.Filters.Add(new EqualAction() { TagName = ls[0],Target = ls[2] });
                }
                else if (ls[1]=="!=")
                {
                    this.Filters.Add(new NotEqualAction() { TagName = ls[0], Target = ls[2] });
                }
                else if (ls[1] == ">=")
                {
                    var ga = new GreatEqualAction() { TagName = ls[0], Target = ls[2] };
                    if (ls[0].ToLower()=="time")
                    {
                        LowerTime = ga;
                        ga.IgnorFit = true;
                    }
                    this.Filters.Add(ga);
                }
                else if (ls[1] == "<=")
                {
                    var ga = new LowerEqualAction() { TagName = ls[0], Target = ls[2] };
                    if (ls[0].ToLower() == "time")
                    {
                        UpperTime = ga;
                        ga.IgnorFit = true;
                    }
                    this.Filters.Add(ga);
                }
                else if (ls[1] == "<")
                {
                    var ga = new LowerAction() { TagName = ls[0], Target = ls[2] };
                    if (ls[0].ToLower() == "time")
                    {
                        UpperTime = ga;
                        ga.IgnorFit = true;
                    }
                    this.Filters.Add(ga);
                }
                else if (ls[1] == ">")
                {
                    var ga = new GreatAction() { TagName = ls[0], Target = ls[2] };
                    if (ls[0].ToLower() == "time")
                    {
                        LowerTime = ga;
                        ga.IgnorFit = true;
                    }
                    this.Filters.Add(ga);
                }
            }
        }
    }



    public class FilterUnion
    {
        public enum UnionType { And, Or };

        public UnionType Type { get; set; }

        public bool Cal(bool left, IFilterAction right, Dictionary<string, object> value)
        {
            if (Type == UnionType.And)
            {
                return left & right.IsFit(value);
            }
            else
            {
                return left | right.IsFit(value);
            }
        }

        public bool Cal(IFilterAction left, IFilterAction right, Dictionary<string, object> value)
        {
            if (Type == UnionType.And)
            {
                return left.IsFit(value) & right.IsFit(value);
            }
            else
            {
                return left.IsFit(value) | right.IsFit(value);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Type.ToString().ToLower();
        }
    }

    public static class FilterUnionExtend
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="value"></param>
        /// <param name="union"></param>
        /// <returns></returns>
        public static bool Union(this bool left, IFilterAction right, Dictionary<string, object> value,FilterUnion union)
        {
            return union.Cal(left, right, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="value"></param>
        /// <param name="union"></param>
        /// <returns></returns>
        public static bool Union(this IFilterAction left, IFilterAction right, Dictionary<string, object> value, FilterUnion union)
        {
            return union.Cal(left, right, value);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public interface IFilterAction
    {
        bool IsFit(Dictionary<string, object> value);
    }


    public class ExpressReader
    {
        public char[] KeyChar = new char[] { '(', ')','=','!',' ', '>', '<',',','\'','"' };
        private string mExpress;

        public ExpressReader(string express)
        {
            mExpress = express;
        }

        public IEnumerable<string> ReadKeyWords()
        {
            bool miscontinue1 = false;
            bool miscontinue2 = false;
            StringBuilder sb = new StringBuilder();
            for(int i=0; i<mExpress.Length; i++)
            {
                char c = mExpress[i];
                if(KeyChar.Contains(c))
                {
                    if (miscontinue1 || miscontinue2)
                    {
                        switch (c)
                        {
                            case '"':
                                if (!miscontinue2)
                                {
                                    miscontinue2 = true;
                                }
                                else
                                {
                                    miscontinue2 = false;
                                    if (sb.Length > 0)
                                        yield return sb.ToString();
                                }
                                sb.Clear();
                                break;
                            case '\'':
                                if (!miscontinue1)
                                {
                                    miscontinue1 = true;
                                }
                                else
                                {
                                    miscontinue1 = false;
                                    if (sb.Length > 0)
                                        yield return sb.ToString();
                                }
                                sb.Clear();
                                break;
                            default:
                                sb.Append(c);
                                break;
                        }
                    }
                    else
                    {
                        if (sb.Length > 0)
                        {
                            yield return sb.ToString();
                        }
                        switch (c)
                        {
                            case '(':
                                yield return "(";
                                sb.Clear();
                                break;
                            case '\'':
                                if (!miscontinue1)
                                {
                                    miscontinue1 = true;
                                }
                                else
                                {
                                    miscontinue1 = false;
                                    yield return sb.ToString();
                                }
                                sb.Clear();
                                break;
                            case '"':
                                if (!miscontinue2)
                                {
                                    miscontinue2 = true;
                                }
                                else
                                {
                                    miscontinue2 = false;
                                    yield return sb.ToString();
                                }
                                sb.Clear();
                                break;
                            case ')':
                                yield return ")";
                                sb.Clear();
                                break;
                            case ',':
                                yield return ",";
                                sb.Clear();
                                break;
                            case ' ':
                                sb.Clear();
                                continue;
                            case '=':
                                if ((i + 1 < mExpress.Length) && mExpress[i + 1] == '=')
                                {
                                    i++;
                                    yield return "==";
                                }
                                else
                                {
                                    yield return "=";
                                }
                                sb.Clear();
                                break;
                            case '!':
                                if ((i + 1 < mExpress.Length) && mExpress[i + 1] == '=')
                                {
                                    i++;
                                    yield return "!=";
                                }
                                else
                                {
                                    yield return "!";
                                }
                                sb.Clear();
                                break;
                            case '>':
                                if ((i + 1 < mExpress.Length) && mExpress[i + 1] == '=')
                                {
                                    i++;
                                    yield return ">=";
                                }
                                else
                                {
                                    yield return ">";
                                }
                                sb.Clear();
                                break;
                            case '<':
                                if ((i + 1 < mExpress.Length) && mExpress[i + 1] == '=')
                                {
                                    i++;
                                    yield return "<=";
                                }
                                else
                                {
                                    yield return "<";
                                }
                                sb.Clear();
                                break;
                        }
                    }
                   
                }
                else
                {
                    sb.Append(c);
                }
            }
            yield return sb.ToString();
        }

    }


    public abstract class SelectFunBase
    {
        /// <summary>
        /// 变量名称
        /// </summary>
        public string TagName { get; set; }

        /// <summary>
        /// 变量ID
        /// </summary>
        public int TagId { get; set; }
        /// <summary>
        /// 计算
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        public abstract object Cal(HisQueryTableResult val);
    }

    public class NoneSelectFun : SelectFunBase
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public override object Cal(HisQueryTableResult val)
        {
            return val;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class CountSelectFun:SelectFunBase
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        public override object Cal(HisQueryTableResult val)
        {
            return val.RowCount;
        }
    }

    public class SumSelectFun:SelectFunBase
    {
        public override object Cal(HisQueryTableResult val)
        {
            double dval = 0;
            foreach (var item in val.ReadColumns(TagId.ToString()))
            {
                try{
                    dval += Convert.ToDouble(item);
                }
                catch
                {
                    throw new Exception("数据转换错误!");
                }
            }
            return dval;
        }
    }

    public class AvgSelectFun : SelectFunBase
    {
        public override object Cal(HisQueryTableResult val)
        {
            double dval = 0;
            try
            {
                dval = val.ReadColumns(TagId.ToString()).Select(e => Convert.ToDouble(e)).Average();
            }
            catch
            {
                throw new Exception("数据转换错误!");
            }
            return dval;
        }
    }

    public class MaxSelectFun : SelectFunBase
    {
        public override object Cal(HisQueryTableResult val)
        {
            double dval = double.MinValue;
            try
            {
                dval = val.ReadColumns(TagId.ToString()).Select(e => Convert.ToDouble(e)).Max();
            }
            catch
            {

            }
            return dval;
        }
    }

    public class MinSelectFun : SelectFunBase
    {
        public override object Cal(HisQueryTableResult val)
        {
            double dval = double.MaxValue;
            try
            {
                dval = val.ReadColumns(TagId.ToString()).Select(e => Convert.ToDouble(e)).Min();
            }
            catch
            {

            }
            return dval;
        }
    }

    public class SelectExpress
    {
        public List<SelectFunBase> Selects { get; set; } = new List<SelectFunBase>();


        public SelectExpress FromList(List<string> ls)
        {
            Selects.Clear();
            List<string> ll = new List<string>();
            StringBuilder stringBuilder = new StringBuilder();
            foreach (string s in ls)
            {
                if(s==",")
                {
                   ll.Add(stringBuilder.ToString());
                    stringBuilder = new StringBuilder();
                }
                else
                {
                    stringBuilder.Append(s);
                }
            }
            if(stringBuilder.Length > 0)
            {
                ll.Add(stringBuilder.ToString());
            }
            foreach (var vv in ll)
            {
                ParseFun(vv);
            }
            return this;
        }

        private bool CheckAvaiable(string sval)
        {
            if(sval.IndexOf("(") != sval.LastIndexOf("("))
            {
                return false;
            }

            if (sval.IndexOf(")") != sval.LastIndexOf(")"))
            {
                return false;
            }
            return true;
        }

        private void ParseFun(string val)
        {
            try
            {
                if(!CheckAvaiable(val))
                {
                    throw new Exception("select 解析错误!");
                }

                if (val.Contains("(") && val.Contains(")"))
                {
                    string fun = val.Substring(0, val.IndexOf("(")).ToLower();
                    string tag = val.Substring(val.IndexOf("(")+1, val.IndexOf(")")- val.IndexOf("(")-1);
                    switch (fun)
                    {
                        case "count":
                            Selects.Add(new CountSelectFun() { TagName = tag });
                            break;
                        case "sum":
                            Selects.Add(new SumSelectFun() { TagName = tag });
                            break;
                        case "avg":
                            Selects.Add(new AvgSelectFun() { TagName = tag });
                            break;
                        case "max":
                            Selects.Add(new MaxSelectFun() { TagName = tag });
                            break;
                        case "min":
                            Selects.Add(new MinSelectFun() { TagName = tag });
                            break;
                    }
                }
                else
                {
                    Selects.Add(new NoneSelectFun() { TagName = val });
                }
            }
            catch
            {
                throw new Exception("select 解析错误!");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool IsAllNone()
        {
            return !(Selects.Where(e=>!(e is NoneSelectFun)).ToArray().Any());
        }
    }

    public class SqlExpress
    {
        /// <summary>
        /// 
        /// </summary>
        public ExpressFilter Where { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public SelectExpress Select { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string From { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public SqlExpress FromString(string str)
        {
            ExpressReader er = new ExpressReader(str);
            List<string> ls = new List<string>();
            bool isselectstart = false;
            bool isfromstart = false;
            bool iswherestart = false;
            foreach (var vv in er.ReadKeyWords())
            {
                if (vv == "select")
                {
                    isselectstart = true;
                }
                else if (vv == "from")
                {
                    if (isselectstart)
                    {
                        this.Select = new SelectExpress().FromList(ls);
                        isselectstart = false;
                    }
                    ls.Clear();
                    isfromstart = true;
                }
                else if (vv == "where")
                {
                    if (isfromstart)
                    {
                        this.From = ls.First();
                        isfromstart = false;
                    }
                    else if (isselectstart)
                    {
                        this.Select = new SelectExpress().FromList(ls);
                        isselectstart = false;
                    }
                    ls.Clear();
                    iswherestart = true;
                }
                else
                {
                    if(!string.IsNullOrEmpty(vv))
                    {
                        ls.Add(vv);
                    }
                }
            }
            if (iswherestart)
            {
                this.Where = new ExpressFilter().FromListString(ls);
            }
            if(isfromstart)
            {
                this.From = ls.First();
            }
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="str"></param>
        public void RebuildSelect(List<string> ls)
        {
            this.Select = new SelectExpress().FromList(ls);
        }

    }
}
