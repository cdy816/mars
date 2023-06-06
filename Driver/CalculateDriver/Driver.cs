using Cdy.Tag;
using Cdy.Tag.Driver;
using Microsoft.CodeAnalysis.Scripting;
using Microsoft.CodeAnalysis.Scripting.Hosting;
using System.Text;
using System.Text.RegularExpressions;

namespace CalculateDriver
{
    /// <summary>
    /// 
    /// </summary>
    public class Driver : IProducterDriver
    {
        
        private Dictionary<int, List<CalItem>> mTagIdCach = new Dictionary<int, List<CalItem>>();

        private Dictionary<int,CalItem> mCompileTagCach = new Dictionary<int, CalItem>();

        private IRealTagProduct? mTagService;

        private ValueChangedNotifyProcesser? mValueChangedNotifier;

        private bool mIsClosed = false;

        private Thread? mScanThread;

        /// <summary>
        /// 
        /// </summary>
        private ManualResetEvent? resetEvent;

        private bool mIsPaused = false;


        /// <summary>
        /// 
        /// </summary>
        public string Name => "Calculate";

        /// <summary>
        /// 
        /// </summary>
        public string[] Registors => new string[0];

        /// <summary>
        /// 
        /// </summary>
        public string EditType => "Script";

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public Dictionary<string, string> GetConfig(string database)
        {
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Init()
        {
            resetEvent = new ManualResetEvent(false);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="arg"></param>
        public void OnHisTagChanged(HisTagChangedArg arg)
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="arg"></param>
        public void OnRealTagChanged(TagChangedArg arg)
        {
            ScriptOptions sop = ScriptOptions.Default;
            sop = sop.AddReferences(typeof(System.Collections.Generic.ReferenceEqualityComparer).Assembly).AddReferences(this.GetType().Assembly).WithImports("CalculateDriver", "System", "System.Collections.Generic");
            InteractiveAssemblyLoader ass = new InteractiveAssemblyLoader();

            var tags = arg.AddedTags.Where(e => e.Value.StartsWith(this.Name));
            List<CalItem> citems = new List<CalItem>();

            foreach (var vv in tags)
            {
                var vtag = mTagService.GetTagById(vv.Key);
                CalItem citem = new CalItem() { Name = vtag.Name, Id = vtag.Id, Express = vtag.LinkAddress.Substring(this.Name.Length + 1), TagService = mTagService, TagType = vtag.Type };
                citem.Init();
                var vsp = Microsoft.CodeAnalysis.CSharp.Scripting.CSharpScript.Create(citem.CompiledExpress, sop, typeof(CalItem), ass);
                try
                {
                    var cp = vsp.Compile();
                    if (cp != null && cp.Length > 0)
                    {
                        StringBuilder sb = new StringBuilder();
                        foreach (var vvp in cp)
                        {
                            sb.Append(vvp.ToString());
                        }
                        LoggerService.Service.Warn("Calculate", vtag.Name + " " + sb.ToString());
                    }
                    else
                    {
                        citem.Script = vsp;
                        lock (mCompileTagCach)
                            mCompileTagCach.Add(vtag.Id, citem);

                        if (citem.Tags.Any())
                        {
                            var vtmp = mTagService.GetTagIdByName(citem.Tags);
                            foreach (var vvtag in vtmp)
                            {
                                if (vvtag.HasValue && vvtag.Value > -1)
                                {
                                    if (mTagIdCach.ContainsKey(vvtag.Value))
                                    {
                                        mTagIdCach[vvtag.Value].Add(citem);
                                    }
                                    else
                                    {
                                        mTagIdCach.Add(vvtag.Value, new List<CalItem>() { citem });
                                    }
                                }
                            }
                        }
                        citems.Add(citem);

                    }

                }
                catch (Exception ex)
                {
                    LoggerService.Service.Erro("Calculate", ex.Message);
                }
            }

            //var cgtags = arg.ChangedTags;
            //foreach(var vv in cgtags)
            //{
            //    RemoveCompiledItem(vv.Key);
            //}

            tags = arg.ChangedTags.Where(e => e.Value.StartsWith(this.Name));

            foreach (var vv in tags)
            {
                var vtag = mTagService.GetTagById(vv.Key);

                if (mCompileTagCach.ContainsKey(vtag.Id))
                {
                    if ((this.Name + ":" + mCompileTagCach[vtag.Id].Express) != vtag.LinkAddress)
                    {
                        RemoveCompiledItem(vtag.Id);

                        CalItem citem = new CalItem() { Name = vtag.Name, Id = vtag.Id, Express = vtag.LinkAddress.Substring(this.Name.Length + 1), TagService = mTagService, TagType = vtag.Type };
                        citem.Init();
                        var vsp = Microsoft.CodeAnalysis.CSharp.Scripting.CSharpScript.Create(citem.CompiledExpress, sop, typeof(CalItem), ass);
                        try
                        {
                            var cp = vsp.Compile();
                            if (cp != null && cp.Length > 0)
                            {
                                StringBuilder sb = new StringBuilder();
                                foreach (var vvp in cp)
                                {
                                    sb.Append(vvp.ToString());
                                }
                                LoggerService.Service.Warn("Calculate", vtag.Name + " " + sb.ToString());
                            }
                            else
                            {
                                citem.Script = vsp;
                                lock (mCompileTagCach)
                                    mCompileTagCach.Add(vtag.Id, citem);

                                if (citem.Tags.Any())
                                {
                                    var vtmp = mTagService.GetTagIdByName(citem.Tags);
                                    foreach (var vvtag in vtmp)
                                    {
                                        if (vvtag.HasValue && vvtag.Value > -1)
                                        {
                                            if (mTagIdCach.ContainsKey(vvtag.Value))
                                            {
                                                mTagIdCach[vvtag.Value].Add(citem);
                                            }
                                            else
                                            {
                                                mTagIdCach.Add(vvtag.Value, new List<CalItem>() { citem });
                                            }
                                        }
                                    }
                                }
                                citems.Add(citem);
                            }
                        }
                        catch (Exception ex)
                        {
                            LoggerService.Service.Erro("Calculate", ex.Message);
                        }
                    }
                }
            }

            if (arg.RemoveTags != null)
            {
                tags = arg.RemoveTags.Where(e => e.Value.StartsWith(this.Name));

                foreach (var vv in tags)
                {
                    RemoveCompiledItem(vv.Key);
                }

            }

            foreach(var vv in citems)
            {
                vv.Execute();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        private void RemoveCompiledItem(int id)
        {
            if(mCompileTagCach.ContainsKey(id))
            {
                var vitem = mCompileTagCach[id];
                if (vitem != null)
                {
                    if (vitem.Tags != null)
                    {
                        var vtmp = mTagService.GetTagIdByName(vitem.Tags);
                        foreach (var vv in vtmp)
                        {
                            if (mTagIdCach.ContainsKey(vv.Value))
                            {
                                mTagIdCach[vv.Value].Remove(vitem);
                            }
                        }
                    }
                    vitem.Dispose();
                }
                mCompileTagCach.Remove(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Pause()
        {
            mIsPaused = true;
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Resume()
        {
            mIsPaused = false;
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        private void InitTag()
        {
            ScriptOptions sop = ScriptOptions.Default;
            sop = sop.AddReferences(typeof(System.Collections.Generic.ReferenceEqualityComparer).Assembly).AddReferences(this.GetType().Assembly).WithImports("CalculateDriver", "System", "System.Collections.Generic");
            InteractiveAssemblyLoader ass = new InteractiveAssemblyLoader();

            var vtags = mTagService.GetTagByLinkAddressStartHeadString(this.Name);
            if(vtags.Any())
            {
                foreach (var vv in vtags)
                {
                    CalItem citem = new CalItem() { Name = vv.Name, Id = vv.Id, Express = vv.LinkAddress.Substring(this.Name.Length + 1), TagService = mTagService, TagType = vv.Type };
                    citem.Init();
                    var vsp = Microsoft.CodeAnalysis.CSharp.Scripting.CSharpScript.Create(citem.CompiledExpress, sop, typeof(CalItem), ass);
                    try
                    {
                        var cp = vsp.Compile();
                        if (cp != null && cp.Length > 0)
                        {
                            StringBuilder sb = new StringBuilder();
                            foreach (var vvp in cp)
                            {
                                sb.Append(vvp.ToString());
                            }
                            LoggerService.Service.Warn("Calculate", vv.Name + " " + sb.ToString());
                        }
                        else
                        {
                            citem.Script = vsp;
                            mCompileTagCach.Add(vv.Id, citem);

                            if(citem.Tags.Any())
                            {
                                var vtmp = mTagService.GetTagIdByName(citem.Tags);

                                foreach (var vtag in vtmp)
                                {
                                    if (vtag.HasValue && vtag.Value > -1)
                                    {
                                        if (mTagIdCach.ContainsKey(vtag.Value))
                                        {
                                            mTagIdCach[vtag.Value].Add(citem);
                                        }
                                        else
                                        {
                                            mTagIdCach.Add(vtag.Value, new List<CalItem>() { citem });
                                        }
                                    }
                                }
                            }

                        }

                    }
                    catch (Exception ex)
                    {
                        LoggerService.Service.Erro("Calculate", ex.Message);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagQuery"></param>
        /// <param name="tagHisValueService"></param>
        /// <returns></returns>
        public bool Start(IRealTagProduct tagQuery, ITagHisValueProduct tagHisValueService)
        {

            mIsClosed = false;
            mTagService = tagQuery;
            InitTag();
            //注册值改变处理
            mValueChangedNotifier = ServiceLocator.Locator.Resolve<IRealDataNotify>().SubscribeValueChangedForConsumer(this.Name, new ValueChangedNotifyProcesser.ValueChangedDelegate((ids, len) => {

                for (int i = 0; i < len; i++)
                {
                    try
                    {
                        var vid = ids[i];
                        if (mTagIdCach.ContainsKey(vid))
                        {
                            foreach(var vv in mTagIdCach[vid])
                            {
                                vv.IsNeedCal = true;
                            }
                        }
                    }
                    catch
                    {

                    }
                }
                resetEvent?.Set();
            }), null, new Func<IEnumerable<int>>(() => { return mTagIdCach.Keys; }), RealDataNotifyType.Tag);
            ExecuteAll();

            mScanThread = new Thread(ThreadPro);
            mScanThread.IsBackground = true;
            mScanThread.Start();
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        private void ThreadPro()
        {
            while(!mIsClosed)
            {
                resetEvent?.WaitOne();
                resetEvent?.Reset();

                if (mIsPaused)
                {
                    Thread.Sleep(100);
                    continue;
                }
                
                if (mIsClosed) 
                    return;
               
                lock (mCompileTagCach)
                {
                    foreach (var vv in mCompileTagCach.Values.Where(e => e.IsNeedCal))
                        //foreach (var vv in mCompileTagCach.Values)
                    {
                        //LoggerService.Service.Info("CalculateDriver", "执行脚本!");
                        vv.IsNeedCal = false;
                        vv.Execute();
                    }
                }
                Thread.Sleep(10);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void ExecuteAll()
        {
            foreach(var vv in mCompileTagCach)
            {
                vv.Value.Execute();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Stop()
        {
            mIsClosed = true;
            if (mValueChangedNotifier != null)
            {
                mValueChangedNotifier.Close();
                mValueChangedNotifier.Dispose();
                mValueChangedNotifier = null;
            }
            resetEvent?.Set();
            
            if (mScanThread != null)
            {
                while (mScanThread.IsAlive) 
                    Thread.Sleep(1);
                mScanThread = null;
            }
            foreach(var vv in mCompileTagCach)
            {
                vv.Value.Dispose();
            }
            mCompileTagCach.Clear();
            mTagIdCach.Clear();
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="config"></param>
        public void UpdateConfig(string database, Dictionary<string, string> config)
        {
            
        }


    }

    /// <summary>
    /// 
    /// </summary>
    public class ValueObject:IConvertible
    {
        public IConvertible Value { get; set; }

        public ValueObject(IConvertible value)
        {
            Value = value;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static ValueObject operator +(ValueObject b, ValueObject c)
        {
            if (b == null|| c==null)
            {
                return null;
            }

            var btype = b.GetTypeCode();
            var ctype = b.GetTypeCode();
            if (btype == TypeCode.Empty || ctype == TypeCode.Empty)
            {
                return null;
            }
            else if (btype == TypeCode.String || ctype == TypeCode.String)
            {
                return new ValueObject(Convert.ToString(b.Value) + Convert.ToString(c.Value));
            }
            else if (btype == TypeCode.Double || ctype == TypeCode.Double)
            {
                return new ValueObject(Convert.ToDouble(b.Value) + Convert.ToDouble(c.Value));
            }
            else if (btype == TypeCode.Single || ctype == TypeCode.Single)
            {
                return new ValueObject(Convert.ToSingle(b.Value) + Convert.ToSingle(c.Value));
            }
            else if (btype == TypeCode.Decimal || ctype == TypeCode.Decimal)
            {
                return new ValueObject(Convert.ToDecimal(b.Value) + Convert.ToDecimal(c.Value));
            }
            else if (btype == TypeCode.UInt64 || ctype == TypeCode.UInt64)
            {
                return new ValueObject(Convert.ToUInt64(b.Value) + Convert.ToUInt64(c.Value));
            }
            else if (btype == TypeCode.Int64 || ctype == TypeCode.Int64)
            {
                return new ValueObject(Convert.ToInt64(b.Value) + Convert.ToInt64(c.Value));
            }
            else if (btype == TypeCode.UInt32 || ctype == TypeCode.UInt32)
            {
                return new ValueObject(Convert.ToUInt32(b.Value) + Convert.ToUInt32(c.Value));
            }
            else if (btype == TypeCode.Int32 || ctype == TypeCode.Int32)
            {
                return new ValueObject(Convert.ToInt32(b.Value) + Convert.ToInt32(c.Value));
            }
            else if (btype == TypeCode.UInt16 || ctype == TypeCode.UInt16)
            {
                return new ValueObject(Convert.ToUInt16(b.Value) + Convert.ToUInt16(c.Value));
            }
            else if (btype == TypeCode.Int16 || ctype == TypeCode.Int16)
            {
                return new ValueObject(Convert.ToInt16(b.Value) + Convert.ToInt16(c.Value));
            }
            else if (btype == TypeCode.Byte || ctype == TypeCode.Byte || btype == TypeCode.Char || ctype == TypeCode.Char)
            {
                return new ValueObject(Convert.ToByte(b.Value) + Convert.ToByte(c.Value));
            }
            else if (btype == TypeCode.Boolean || ctype == TypeCode.Boolean)
            {
                return new ValueObject(Convert.ToByte(b.Value) + Convert.ToByte(c.Value));
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static ValueObject operator -(ValueObject b, ValueObject c)
        {
            var btype = b.GetTypeCode();
            var ctype = b.GetTypeCode();
            if (btype == TypeCode.Empty || ctype == TypeCode.Empty|| btype == TypeCode.String || ctype == TypeCode.String)
            {
                return null;
            }
            else if (btype == TypeCode.Double || ctype == TypeCode.Double)
            {
                return new ValueObject(Convert.ToDouble(b.Value) - Convert.ToDouble(c.Value));
            }
            else if (btype == TypeCode.Single || ctype == TypeCode.Single)
            {
                return new ValueObject(Convert.ToSingle(b.Value) - Convert.ToSingle(c.Value));
            }
            else if (btype == TypeCode.Decimal || ctype == TypeCode.Decimal)
            {
                return new ValueObject(Convert.ToDecimal(b.Value) - Convert.ToDecimal(c.Value));
            }
            else if (btype == TypeCode.UInt64 || ctype == TypeCode.UInt64)
            {
                return new ValueObject(Convert.ToUInt64(b.Value) - Convert.ToUInt64(c.Value));
            }
            else if (btype == TypeCode.Int64 || ctype == TypeCode.Int64)
            {
                return new ValueObject(Convert.ToInt64(b.Value) - Convert.ToInt64(c.Value));
            }
            else if (btype == TypeCode.UInt32 || ctype == TypeCode.UInt32)
            {
                return new ValueObject(Convert.ToUInt32(b.Value) - Convert.ToUInt32(c.Value));
            }
            else if (btype == TypeCode.Int32 || ctype == TypeCode.Int32)
            {
                return new ValueObject(Convert.ToInt32(b.Value) - Convert.ToInt32(c.Value));
            }
            else if (btype == TypeCode.UInt16 || ctype == TypeCode.UInt16)
            {
                return new ValueObject(Convert.ToUInt16(b.Value) - Convert.ToUInt16(c.Value));
            }
            else if (btype == TypeCode.Int16 || ctype == TypeCode.Int16)
            {
                return new ValueObject(Convert.ToInt16(b.Value) - Convert.ToInt16(c.Value));
            }
            else if (btype == TypeCode.Byte || ctype == TypeCode.Byte || btype == TypeCode.Char || ctype == TypeCode.Char)
            {
                return new ValueObject(Convert.ToByte(b.Value) - Convert.ToByte(c.Value));
            }
            else if (btype == TypeCode.Boolean || ctype == TypeCode.Boolean)
            {
                return new ValueObject(Convert.ToByte(b.Value) - Convert.ToByte(c.Value));
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static ValueObject operator /(ValueObject b, ValueObject c)
        {
            var btype = b.GetTypeCode();
            var ctype = b.GetTypeCode();
            if (btype == TypeCode.Empty || ctype == TypeCode.Empty || btype == TypeCode.String || ctype == TypeCode.String)
            {
                return null;
            }
            else if (btype == TypeCode.Double || ctype == TypeCode.Double)
            {
                return new ValueObject(Convert.ToDouble(b.Value) / Convert.ToDouble(c.Value));
            }
            else if (btype == TypeCode.Single || ctype == TypeCode.Single)
            {
                return new ValueObject(Convert.ToSingle(b.Value) / Convert.ToSingle(c.Value));
            }
            else if (btype == TypeCode.Decimal || ctype == TypeCode.Decimal)
            {
                return new ValueObject(Convert.ToDecimal(b.Value) / Convert.ToDecimal(c.Value));
            }
            else if (btype == TypeCode.UInt64 || ctype == TypeCode.UInt64)
            {
                return new ValueObject(Convert.ToUInt64(b.Value) / Convert.ToUInt64(c.Value));
            }
            else if (btype == TypeCode.Int64 || ctype == TypeCode.Int64)
            {
                return new ValueObject(Convert.ToInt64(b.Value) / Convert.ToInt64(c.Value));
            }
            else if (btype == TypeCode.UInt32 || ctype == TypeCode.UInt32)
            {
                return new ValueObject(Convert.ToUInt32(b.Value) / Convert.ToUInt32(c.Value));
            }
            else if (btype == TypeCode.Int32 || ctype == TypeCode.Int32)
            {
                return new ValueObject(Convert.ToInt32(b.Value) / Convert.ToInt32(c.Value));
            }
            else if (btype == TypeCode.UInt16 || ctype == TypeCode.UInt16)
            {
                return new ValueObject(Convert.ToUInt16(b.Value) / Convert.ToUInt16(c.Value));
            }
            else if (btype == TypeCode.Int16 || ctype == TypeCode.Int16)
            {
                return new ValueObject(Convert.ToInt16(b.Value) / Convert.ToInt16(c.Value));
            }
            else if (btype == TypeCode.Byte || ctype == TypeCode.Byte || btype == TypeCode.Char || ctype == TypeCode.Char)
            {
                return new ValueObject(Convert.ToByte(b.Value) / Convert.ToByte(c.Value));
            }
            else if (btype == TypeCode.Boolean || ctype == TypeCode.Boolean)
            {
                return new ValueObject(Convert.ToByte(b.Value) / Convert.ToByte(c.Value));
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static ValueObject operator *(ValueObject b, ValueObject c)
        {
            var btype = b.GetTypeCode();
            var ctype = b.GetTypeCode();
            if (btype == TypeCode.Empty || ctype == TypeCode.Empty || btype == TypeCode.String || ctype == TypeCode.String)
            {
                return null;
            }
            else if (btype == TypeCode.Double || ctype == TypeCode.Double)
            {
                return new ValueObject(Convert.ToDouble(b.Value) * Convert.ToDouble(c.Value));
            }
            else if (btype == TypeCode.Single || ctype == TypeCode.Single)
            {
                return new ValueObject(Convert.ToSingle(b.Value) * Convert.ToSingle(c.Value));
            }
            else if (btype == TypeCode.Decimal || ctype == TypeCode.Decimal)
            {
                return new ValueObject(Convert.ToDecimal(b.Value) * Convert.ToDecimal(c.Value));
            }
            else if (btype == TypeCode.UInt64 || ctype == TypeCode.UInt64)
            {
                return new ValueObject(Convert.ToUInt64(b.Value) * Convert.ToUInt64(c.Value));
            }
            else if (btype == TypeCode.Int64 || ctype == TypeCode.Int64)
            {
                return new ValueObject(Convert.ToInt64(b.Value) * Convert.ToInt64(c.Value));
            }
            else if (btype == TypeCode.UInt32 || ctype == TypeCode.UInt32)
            {
                return new ValueObject(Convert.ToUInt32(b.Value) * Convert.ToUInt32(c.Value));
            }
            else if (btype == TypeCode.Int32 || ctype == TypeCode.Int32)
            {
                return new ValueObject(Convert.ToInt32(b.Value) * Convert.ToInt32(c.Value));
            }
            else if (btype == TypeCode.UInt16 || ctype == TypeCode.UInt16)
            {
                return new ValueObject(Convert.ToUInt16(b.Value) * Convert.ToUInt16(c.Value));
            }
            else if (btype == TypeCode.Int16 || ctype == TypeCode.Int16)
            {
                return new ValueObject(Convert.ToInt16(b.Value) * Convert.ToInt16(c.Value));
            }
            else if (btype == TypeCode.Byte || ctype == TypeCode.Byte || btype == TypeCode.Char || ctype == TypeCode.Char)
            {
                return new ValueObject(Convert.ToByte(b.Value) * Convert.ToByte(c.Value));
            }
            else if (btype == TypeCode.Boolean || ctype == TypeCode.Boolean)
            {
                return new ValueObject(Convert.ToByte(b.Value) * Convert.ToByte(c.Value));
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator ValueObject(double val)
        {
            return new ValueObject(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator ValueObject(float val)
        {
            return new ValueObject(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator ValueObject(int val)
        {
            return new ValueObject(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator ValueObject(uint val)
        {
            return new ValueObject(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator ValueObject(long val)
        {
            return new ValueObject(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator ValueObject(ulong val)
        {
            return new ValueObject(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator ValueObject(string val)
        {
            return new ValueObject(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator ValueObject(short val)
        {
            return new ValueObject(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator ValueObject(ushort val)
        {
            return new ValueObject(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator ValueObject(byte val)
        {
            return new ValueObject(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator ValueObject(DateTime val)
        {
            return new ValueObject(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator ValueObject(bool val)
        {
            return new ValueObject(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator ValueObject(char val)
        {
            return new ValueObject(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator ValueObject(decimal val)
        {
            return new ValueObject(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator double(ValueObject val)
        {
            return Convert.ToDouble(val.Value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public static implicit operator float(ValueObject val)
        {
            return Convert.ToSingle(val.Value);
        }

        public static implicit operator int(ValueObject val)
        {
            return Convert.ToInt32(val.Value);
        }

        public static implicit operator long(ValueObject val)
        {
            return Convert.ToInt64(val.Value);
        }

        public static implicit operator short(ValueObject val)
        {
            return Convert.ToInt16(val.Value);
        }

        public static implicit operator byte(ValueObject val)
        {
            return Convert.ToByte(val.Value);
        }

        public static implicit operator uint(ValueObject val)
        {
            return Convert.ToUInt32(val.Value);
        }

        public static implicit operator ulong(ValueObject val)
        {
            return Convert.ToUInt64(val.Value);
        }

        public static implicit operator ushort(ValueObject val)
        {
            return Convert.ToUInt16(val.Value);
        }

        public static implicit operator string(ValueObject val)
        {
            return Convert.ToString(val.Value);
        }

        public static implicit operator DateTime(ValueObject val)
        {
            return Convert.ToDateTime(val.Value);
        }

        public static implicit operator char(ValueObject val)
        {
            return Convert.ToChar(val.Value);
        }

        public static implicit operator bool(ValueObject val)
        {
            return Convert.ToBoolean(val.Value);
        }

        public static implicit operator decimal(ValueObject val)
        {
            return Convert.ToDecimal(val.Value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        public static bool operator true(ValueObject val)
        {
            return Convert.ToBoolean(val.Value);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        public static bool operator false(ValueObject val)
        {
            return Convert.ToBoolean(val.Value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static ValueObject operator ==(ValueObject b, ValueObject c)
        {
            if(b is null && c is null)
            {
                return new ValueObject(true);
            }
            else if(!(b is null)&&!(c is null))
            {
                return new ValueObject(b.Value == c.Value);
            }
            return new ValueObject(false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static ValueObject operator !=(ValueObject b, ValueObject c)
        {
            return new ValueObject(b.Value != c.Value);
        }

        /// <summary>
        /// 取位
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public bool this[int index]
        {
            get
            {
                var lval = Convert.ToInt64(Value);
                return new System.Collections.BitArray(BitConverter.GetBytes(lval))[index];
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return Value!=null?Value.GetHashCode():base.GetHashCode();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object? obj)
        {
            if(obj is ValueObject)
            {
                return Value.Equals((obj as ValueObject).Value);
            }
            return Value.Equals(obj);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public TypeCode GetTypeCode()
        {
            return (Value as IConvertible).GetTypeCode();
        }

        public bool ToBoolean(IFormatProvider? provider)
        {
            return Convert.ToBoolean(Value);
        }

        public byte ToByte(IFormatProvider? provider)
        {
            return Convert.ToByte(Value);
        }

        public char ToChar(IFormatProvider? provider)
        {
            return Convert.ToChar(Value);
        }

        public DateTime ToDateTime(IFormatProvider? provider)
        {
            if (Value is DateTime)
                return Convert.ToDateTime(Value);
            else
                return DateTime.MinValue;
        }

        public decimal ToDecimal(IFormatProvider? provider)
        {
            return Convert.ToDecimal(Value);
        }

        public double ToDouble(IFormatProvider? provider)
        {
            return Convert.ToDouble(Value);
        }

        public short ToInt16(IFormatProvider? provider)
        {
            return Convert.ToInt16(Value);
        }

        public int ToInt32(IFormatProvider? provider)
        {
            return Convert.ToInt32(Value);
        }

        public long ToInt64(IFormatProvider? provider)
        {
            return Convert.ToInt64(Value);
        }

        public sbyte ToSByte(IFormatProvider? provider)
        {
            return Convert.ToSByte(Value);
        }

        public float ToSingle(IFormatProvider? provider)
        {
            return Convert.ToSingle(Value);
        }

        public string ToString(IFormatProvider? provider)
        {
            return Convert.ToString(Value);
        }

        public object ToType(Type conversionType, IFormatProvider? provider)
        {
            return Value;
        }

        public ushort ToUInt16(IFormatProvider? provider)
        {
            return Convert.ToUInt16(Value);
        }

        public uint ToUInt32(IFormatProvider? provider)
        {
            return Convert.ToUInt32(Value);
        }

        public ulong ToUInt64(IFormatProvider? provider)
        {
            return Convert.ToUInt64(Value);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Value.ToString();
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class TagCallContext : IDisposable
    {

        /// <summary>
        /// 
        /// </summary>
        public IRealTagProduct? TagService { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, int> TagMaps = new Dictionary<string, int>();


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        public ValueObject GetValue(string tag)
        {
            try
            {
                if (TagMaps.ContainsKey(tag))
                {
                    var id = TagMaps[tag];
                    if (id > -1)
                    {
                        var val = TagService?.GetTagValueForProductor(id) as IConvertible;
                        //LoggerService.Service.Info("CalculateDriver", $"获取变量{id} 的值 {val}!");
                        return new ValueObject(val);
                    }

                  
                }
            }
            catch(Exception ex)
            {

            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool SetValue(string tag, object value)
        {
            try
            {
                if (TagMaps.ContainsKey(tag))
                {
                    var Id = TagMaps[tag];
                    if (Id > -1)
                    {
                        var tins = TagService.GetTagById(Id);
                        if (tins != null)
                        {
                            switch (tins.Type)
                            {
                                case TagType.Bool:
                                    var bval = Convert.ToBoolean(value);
                                    TagService.SetTagValue(Id, ref bval, 0);
                                    break;
                                case TagType.Byte:
                                    var bbval = Convert.ToByte(value);
                                    TagService.SetTagValue(Id, ref bbval, 0);
                                    break;
                                case TagType.Short:
                                    var sval = Convert.ToInt16(value);
                                    TagService.SetTagValue(Id, ref sval, 0);
                                    break;
                                case TagType.UShort:
                                    var usval = Convert.ToByte(value);
                                    TagService.SetTagValue(Id, ref usval, 0);
                                    break;
                                case TagType.Int:
                                    var ival = Convert.ToInt32(value);
                                    TagService.SetTagValue(Id, ref ival, 0);
                                    break;
                                case TagType.UInt:
                                    var uival = Convert.ToUInt32(value);
                                    TagService.SetTagValue(Id, ref uival, 0);
                                    break;
                                case TagType.Long:
                                    var lval = Convert.ToInt64(value);
                                    TagService.SetTagValue(Id, ref lval, 0);
                                    break;
                                case TagType.ULong:
                                    var ulval = Convert.ToUInt64(value);
                                    TagService.SetTagValue(Id, ref ulval, 0);
                                    break;
                                case TagType.Double:
                                    var dval = Convert.ToDouble(value);
                                    TagService.SetTagValue(Id, ref dval, 0);
                                    break;
                                case TagType.Float:
                                    var fval = Convert.ToSingle(value);
                                    TagService.SetTagValue(Id, ref fval, 0);
                                    break;
                                case TagType.DateTime:
                                    var dtval = Convert.ToDateTime(value);
                                    TagService.SetTagValue(Id, ref dtval, 0);
                                    break;
                                case TagType.String:
                                    var ssval = Convert.ToString(value);
                                    TagService.SetTagValue(Id, ssval, 0);
                                    break;
                            }
                        }
                        return true;
                    }
                }
                return false;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tags"></param>
        /// <returns></returns>
        public double TagValueSum(params string[] tags)
        {
            try
            {
                double[] dtmps = new double[tags.Length];
                for (int i = 0; i < tags.Length; i++)
                {
                    dtmps[i] = GetValue(tags[i]);
                }
                return dtmps.Sum();
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("Calculate", ex.StackTrace);
            }
            return 0;
        }

        /// <summary>
        /// 对变量的值求平局
        /// </summary>
        /// <param name="tags">变量名</param>
        /// <returns></returns>
        public double TagValueAvg(params string[] tags)
        {
            try
            {
                double[] dtmps = new double[tags.Length];
                for (int i = 0; i < tags.Length; i++)
                {
                    dtmps[i] = GetValue(tags[i]);
                }
                return dtmps.Average();
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("Calculate", ex.StackTrace);
            }
            return 0;
        }

        /// <summary>
        /// 对变量的值取最大
        /// </summary>
        /// <param name="tags">变量名</param>
        /// <returns></returns>
        public double TagValueMax(params string[] tags)
        {
            try
            {
                double[] dtmps = new double[tags.Length];
                for (int i = 0; i < tags.Length; i++)
                {
                    dtmps[i] = GetValue(tags[i]);
                }
                return dtmps.Max();
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("Calculate", ex.StackTrace);
            }
            return 0;
        }

        /// <summary>
        /// 对变量的值取最小
        /// </summary>
        /// <param name="tags">变量名</param>
        /// <returns></returns>
        public double TagValueMin(params string[] tags)
        {
            try
            {
                double[] dtmps = new double[tags.Length];
                for (int i = 0; i < tags.Length; i++)
                {
                    dtmps[i] = GetValue(tags[i]);
                }
                return dtmps.Min();
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("Calculate", ex.StackTrace);
            }
            return 0;
        }

        /// <summary>
        /// 对数值进行请平均值
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public double Avg(params object[] values)
        {
            try
            {
                double[] dtmps = new double[values.Length];
                for (int i = 0; i < values.Length; i++)
                {
                    dtmps[i] = Convert.ToDouble(values[i]);
                }
                return dtmps.Average();
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("Calculate", ex.StackTrace);
            }
            return 0;
        }

        /// <summary>
        /// 对数值进行取最大值
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public double Max(params object[] values)
        {
            try
            {
                double[] dtmps = new double[values.Length];
                for (int i = 0; i < values.Length; i++)
                {
                    dtmps[i] = Convert.ToDouble(values[i]);
                }
                return dtmps.Max();
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("Calculate", ex.StackTrace);
            }
            return 0;
        }

        /// <summary>
        /// 对数值进行取最小值
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public double Min(params object[] values)
        {
            try
            {
                double[] dtmps = new double[values.Length];
                for (int i = 0; i < values.Length; i++)
                {
                    dtmps[i] = Convert.ToDouble(values[i]);
                }
                return dtmps.Min();
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("Calculate", ex.StackTrace);
            }
            return 0;
        }

        /// <summary>
        /// 对值进行取位
        /// </summary>
        /// <param name="value">值</param>
        /// <param name="index">要取位的序号，从0开始</param>
        /// <returns></returns>
        public byte Bit(object value, byte index)
        {
            var val = Convert.ToInt64(value);
            return (byte)(val >> index & 0x01);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            TagMaps.Clear();
            TagService=null;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class CalItem:IDisposable
    {
        /// <summary>
        /// 
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Script<object>? Script { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsNeedCal { get; set; } = true;

        /// <summary>
        /// 
        /// </summary>
        public List<string>? Tags { get; set; }=new List<string>();

        /// <summary>
        /// 
        /// </summary>
        public TagCallContext? Tag { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string? Express { get; set; } = "";

        /// <summary>
        /// 
        /// </summary>
        public string? CompiledExpress { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public TagType TagType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public IRealTagProduct? TagService { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public CalItem Init()
        {
            Tag = new TagCallContext() { TagService=TagService};

            //List<string> ll = new List<string>();
            Tags = AnalysizeTags(Express);
            List<string> vTags=null;

            StringBuilder errb = new StringBuilder();

            if (Tags.Count > 0)
            {
                vTags = Tags.Select(e => e.Substring(4)).ToList();
                var ids = TagService.GetTagIdByName(vTags);
                for(int i = 0; i < ids.Count; i++)
                {
                    var val = ids[i];
                    if(val.HasValue)
                    {
                        Tag.TagMaps.Add(vTags[i], val.Value);
                    }
                    else
                    {
                        Tag.TagMaps.Add(vTags[i], -1);
                        errb.Append(vTags[i]+",");
                    }
                }
            }

            StringBuilder sb = new StringBuilder("return " + Express+";");
            var vtags = Tags.Distinct().OrderByDescending(e=>e.Length);
            foreach(var vtag in vtags)
            {
                sb.Replace(vtag, "Tag.GetValue(\"" + vtag.Substring(4) + "\")");
            }
            CompiledExpress=sb.ToString();
            Tags = vTags;

            if(errb.Length>0)
            {
                errb.Length = errb.Length - 1;
                LoggerService.Service.Warn(this.Name, $"tags {errb.ToString()} is not exist which referenced by {Name}");
            }

            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="exp"></param>
        /// <returns></returns>
        private List<string> AnalysizeTags(string exp)
        {
            Regex regex = new Regex(@"\bTag((\.\w*)(?!\())*\b",
             RegexOptions.IgnoreCase | RegexOptions.Multiline |
             RegexOptions.IgnorePatternWhitespace | RegexOptions.Compiled);

            List<string> ltmp = new List<string>();

            var vvs = regex.Matches(exp);
            if (vvs.Count > 0)
            {
                foreach (var vv in vvs)
                {
                    ltmp.Add(vv.ToString());
                }
            }

            return ltmp;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Execute()
        {
            if (IsNeedCal || Tag!=null)
            {
                lock (Tag)
                    IsNeedCal = false;
                try
                {
                    var val = Script.RunAsync(this).Result.ReturnValue;
                    switch (TagType)
                    {
                        case TagType.Bool:
                            var bval = Convert.ToBoolean(val);
                            TagService.SetTagValue(Id,ref bval, 0);
                            break;
                        case TagType.Byte:
                            var bbval = Convert.ToByte(val);
                            TagService.SetTagValue(Id, ref bbval, 0);
                            break;
                        case TagType.Short:
                            var sval = Convert.ToInt16(val);
                            TagService.SetTagValue(Id, ref sval, 0);
                            break;
                        case TagType.UShort:
                            var usval = Convert.ToByte(val);
                            TagService.SetTagValue(Id, ref usval, 0);
                            break;
                        case TagType.Int:
                            var ival = Convert.ToInt32(val);
                            TagService.SetTagValue(Id, ref ival, 0);
                            break;
                        case TagType.UInt:
                            var uival = Convert.ToUInt32(val);
                            TagService.SetTagValue(Id, ref uival, 0);
                            break;
                        case TagType.Long:
                            var lval = Convert.ToInt64(val);
                            TagService.SetTagValue(Id, ref lval, 0);
                            break;
                        case TagType.ULong:
                            var ulval = Convert.ToUInt64(val);
                            TagService.SetTagValue(Id, ref ulval, 0);
                            break;
                        case TagType.Double:
                            var dval = Convert.ToDouble(val);
                            TagService.SetTagValue(Id, ref dval, 0);
                            break;
                        case TagType.Float:
                            var fval = Convert.ToSingle(val);
                            TagService.SetTagValue(Id, ref fval, 0);
                            break;
                        case TagType.DateTime:
                            var dtval = Convert.ToDateTime(val);
                            TagService.SetTagValue(Id, ref dtval, 0);
                            break;
                        case TagType.String:
                            var ssval = Convert.ToString(val);
                            TagService.SetTagValue(Id, ssval, 0);
                            break;
                    }
                }
                catch (Exception ex)
                {
                    TagService.SetTagQuality(Id, (byte)QualityConst.Bad,DateTime.Now);
                    LoggerService.Service.Erro("CalculateDriver", Name + " : " + ex.Message);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            TagService=null;
            Tags.Clear();
            Tag?.Dispose();
            Script=null;
        }
    }
}