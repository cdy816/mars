using Cdy.Tag;
using Opc.Ua;
using Opc.Ua.Server;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBOpcServer
{
    /// <summary>
    /// Mars 数据节点
    /// </summary>
    internal class MarsNodeManager : CustomNodeManager2
    {

        private Dictionary<string, FolderState> mFolders = new Dictionary<string, FolderState>();

        //private Dictionary<int, MarsTag> mTags = new Dictionary<int, MarsTag>();

        private Dictionary<NodeId, MarsTag> mIdTagMaps = new Dictionary<NodeId, MarsTag>();

        private bool mIsClosed = false;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="server"></param>
        /// <param name="configuration"></param>
        public MarsNodeManager(IServerInternal server, ApplicationConfiguration configuration) : base(server, configuration, "http://cdyfoundationorg/Mars")
        {
        }

        protected MarsNodeManager(IServerInternal server, ApplicationConfiguration configuration, params string[] namespaceUris) : base(server, configuration, namespaceUris)
        {
        }

        /// <summary>
        /// 重写NodeId生成方式(目前采用'_'分隔,如需更改,请修改此方法)
        /// </summary>
        /// <param name="context"></param>
        /// <param name="node"></param>
        /// <returns></returns>
        public override NodeId New(ISystemContext context, NodeState node)
        {
            BaseInstanceState instance = node as BaseInstanceState;

            if (instance != null && instance.Parent != null)
            {
                string id = instance.Parent.NodeId.Identifier as string;

                if (id != null)
                {
                    return new NodeId(id + "_" + instance.SymbolicName, instance.Parent.NodeId.NamespaceIndex);
                }
            }

            return node.NodeId;
        }

        /// <summary>
        /// 重写获取节点句柄的方法
        /// </summary>
        /// <param name="context"></param>
        /// <param name="nodeId"></param>
        /// <param name="cache"></param>
        /// <returns></returns>
        protected override NodeHandle GetManagerHandle(ServerSystemContext context, NodeId nodeId, IDictionary<NodeId, NodeState> cache)
        {
            lock (Lock)
            {
                // quickly exclude nodes that are not in the namespace. 
                if (!IsNodeIdInNamespace(nodeId))
                {
                    return null;
                }

                NodeState node = null;

                if (!PredefinedNodes.TryGetValue(nodeId, out node))
                {
                    return null;
                }

                NodeHandle handle = new NodeHandle();

                handle.NodeId = nodeId;
                handle.Node = node;
                handle.Validated = true;

                return handle;
            }
        }

        /// <summary>
        /// 重写节点的验证方式
        /// </summary>
        /// <param name="context"></param>
        /// <param name="handle"></param>
        /// <param name="cache"></param>
        /// <returns></returns>
        protected override NodeState ValidateNode(ServerSystemContext context, NodeHandle handle, IDictionary<NodeId, NodeState> cache)
        {
            // not valid if no root.
            if (handle == null)
            {
                return null;
            }

            // check if previously validated.
            if (handle.Validated)
            {
                return handle.Node;
            }
            // TBD
            return null;
        }

        /// <summary>
        /// 创建对外服务的目录结构
        /// </summary>
        /// <param name="externalReferences"></param>
        public override void CreateAddressSpace(IDictionary<NodeId, IList<IReference>> externalReferences)
        {
            lock (Lock)
            {
                IList<IReference> references = null;

                if (!externalReferences.TryGetValue(ObjectIds.ObjectsFolder, out references))
                {
                    externalReferences[ObjectIds.ObjectsFolder] = references = new List<IReference>();
                }

                FolderState root = CreateFolder(null, "tag", "tag");
                root.AddReference(ReferenceTypes.Organizes, true, ObjectIds.ObjectsFolder);
                references.Add(new NodeStateReference(ReferenceTypes.Organizes, false, root.NodeId));
                root.EventNotifier = EventNotifiers.SubscribeToEvents;
                AddRootNotifier(root);
                mFolders.Add("", root);

                FillFolderNode(root, "");
                FillTagNode(root, "");

                AddPredefinedNode(SystemContext, root);

                StartDataSync();
            }
        }


        private void FillFolderNode(NodeState parent,string parentFoldName)
        {
            var mm = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.ITagManager>();
            IEnumerable<string> grps = mm.GetTagGroup(parentFoldName);
            if (grps != null && grps.Count()>0)
            {
                foreach (var vv in grps)
                {
                    var fs = CreateFolder(parent, vv, vv);

                    string sfullname = string.IsNullOrEmpty(parentFoldName) ? vv : parentFoldName + "." + vv;

                    mFolders.Add(sfullname, fs);

                    //parent.AddChild(fs);
                    FillTagNode(fs, sfullname);

                    FillFolderNode(fs, sfullname);
                  
                }

            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="group"></param>
        private void FillTagNode(NodeState parent,string group)
        {
            var mm = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.ITagManager>();
            var tags = mm.GetTagsByGroup(group);
            if(tags!=null)
            {
                foreach(var vv in tags)
                {
                    CreateVariable(parent,vv);
                }
            }
        }
        
        /// <summary>
        /// Creates a new folder.
        /// </summary>
        private FolderState CreateFolder(NodeState parent, string path, string name)
        {
            FolderState folder = new FolderState(parent);

            folder.SymbolicName = name;
            folder.ReferenceTypeId = ReferenceTypes.Organizes;
            folder.TypeDefinitionId = ObjectTypeIds.FolderType;
            folder.NodeId = new NodeId(path, NamespaceIndex);
            folder.BrowseName = new QualifiedName(path, NamespaceIndex);
            folder.DisplayName = new LocalizedText("en", name);
            folder.WriteMask = AttributeWriteMask.None;
            folder.UserWriteMask = AttributeWriteMask.None;
            folder.EventNotifier = EventNotifiers.None;

            if (parent != null)
            {
                parent.AddChild(folder);
            }

            return folder;
        }

        /// <summary>
        /// 创建节点
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="path"></param>
        /// <param name="name"></param>
        /// <param name="dataType"></param>
        /// <param name="valueRank"></param>
        /// <returns></returns>
        private MarsTag CreateVariable(NodeState parent, Cdy.Tag.Tagbase tag)
        {
            MarsTag variable = new MarsTag(parent);

            variable.SymbolicName = tag.Name;
            variable.ReferenceTypeId = ReferenceTypes.Organizes;
            variable.TypeDefinitionId = VariableTypeIds.BaseDataVariableType;
            variable.NodeId = new NodeId(tag.FullName, NamespaceIndex);
            variable.BrowseName = new QualifiedName(tag.Name);
            variable.DisplayName = new LocalizedText("en", tag.Name);
            variable.WriteMask = AttributeWriteMask.DisplayName | AttributeWriteMask.Description;
            variable.UserWriteMask = AttributeWriteMask.DisplayName | AttributeWriteMask.Description;
            variable.ValueRank = ValueRanks.Scalar;

            switch (tag.Type)
            {
                case Cdy.Tag.TagType.Bool:
                    variable.DataType = DataTypeIds.Boolean;
                    break;
                case Cdy.Tag.TagType.Short:
                    variable.DataType = DataTypeIds.Int16;
                    break;
                case Cdy.Tag.TagType.UShort:
                    variable.DataType = DataTypeIds.UInt16;
                    break;
                case Cdy.Tag.TagType.Int:
                    variable.DataType = DataTypeIds.Int32;
                    break;
                case Cdy.Tag.TagType.UInt:
                    variable.DataType = DataTypeIds.UInt32;
                    break;
                case Cdy.Tag.TagType.Long:
                    variable.DataType = DataTypeIds.Int64;
                    break;
                case Cdy.Tag.TagType.ULong:
                    variable.DataType = DataTypeIds.UInt64;
                    break;
                case Cdy.Tag.TagType.Double:
                    variable.DataType = DataTypeIds.Double;
                    break;
                case Cdy.Tag.TagType.Float:
                    variable.DataType = DataTypeIds.Float;
                    break;
                case Cdy.Tag.TagType.DateTime:
                    variable.DataType = DataTypeIds.DateTime;
                    break;
                case Cdy.Tag.TagType.String:
                    variable.DataType = DataTypeIds.String;
                    break;
                case Cdy.Tag.TagType.Byte:
                    variable.DataType = DataTypeIds.Byte;
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    variable.DataType = DataTypeIds.String;
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    variable.DataType = DataTypeIds.String;
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    variable.DataType = DataTypeIds.String;
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                case Cdy.Tag.TagType.LongPoint:
                case Cdy.Tag.TagType.ULongPoint:
                case Cdy.Tag.TagType.LongPoint3:
                case Cdy.Tag.TagType.ULongPoint3:
                    variable.DataType = DataTypeIds.String;
                    break;
            }
            variable.Description = tag.Desc;
            switch(tag.ReadWriteType)
            {
                case Cdy.Tag.ReadWriteMode.Write:
                    variable.AccessLevel = AccessLevels.CurrentWrite;
                    variable.UserAccessLevel = AccessLevels.CurrentWrite;
                    break;
                case Cdy.Tag.ReadWriteMode.ReadWrite:
                    variable.AccessLevel = AccessLevels.CurrentReadOrWrite;
                    variable.UserAccessLevel = AccessLevels.CurrentReadOrWrite;
                    break;
                case Cdy.Tag.ReadWriteMode.Read:
                    variable.AccessLevel = AccessLevels.CurrentRead;
                    variable.UserAccessLevel = AccessLevels.CurrentRead;
                    break;
            }
            variable.Historizing = false;
            variable.StatusCode = StatusCodes.Good;
            variable.Timestamp = DateTime.Now;
            variable.OnWriteValue = OnWriteDataValue;
            variable.Id = tag.Id;

            if (parent != null)
            {
                parent.AddChild(variable);
            }
            //mTags.Add(tag.Id, variable);
            mIdTagMaps.Add(variable.NodeId, variable);
            return variable;
        }

        public override void Read(OperationContext context, double maxAge, IList<ReadValueId> nodesToRead, IList<DataValue> values, IList<ServiceResult> errors)
        {
            base.Read(context, maxAge, nodesToRead, values, errors);
        }

        /// <summary>
        /// 客户端写入值时触发(绑定到节点的写入事件上)
        /// </summary>
        /// <param name="context"></param>
        /// <param name="node"></param>
        /// <param name="indexRange"></param>
        /// <param name="dataEncoding"></param>
        /// <param name="value"></param>
        /// <param name="statusCode"></param>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        private ServiceResult OnWriteDataValue(ISystemContext context, NodeState node, NumericRange indexRange, QualifiedName dataEncoding,
            ref object value, ref StatusCode statusCode, ref DateTime timestamp)
        {
            BaseDataVariableState variable = node as BaseDataVariableState;
            try
            {
                //验证数据类型
                TypeInfo typeInfo = TypeInfo.IsInstanceOfDataType(
                    value,
                    variable.DataType,
                    variable.ValueRank,
                    context.NamespaceUris,
                    context.TypeTable);

                if (typeInfo == null || typeInfo == TypeInfo.Unknown)
                {
                    return StatusCodes.BadTypeMismatch;
                }

               if(mIdTagMaps.TryGetValue(node.NodeId, out var tag))
                {
                    var vid =  tag.Id;
                    var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
                    if(service.SetTagValueForConsumer(vid, value))
                    {
                        return ServiceResult.Good;
                    }
                    else
                    {
                        return StatusCodes.Bad;
                    }
                }

                return ServiceResult.Good;
            }
            catch (Exception)
            {
                return StatusCodes.BadTypeMismatch;
            }
        }

        private void StartDataSync()
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            Task.Run(() => { 
                while(!mIsClosed)
                {
                    foreach(var vv in mIdTagMaps.Values)
                    {
                        var val = service.GetTagValue(vv.Id, out byte qua, out DateTime time, out byte vtype);
                        var oldval = vv.Value;
                        switch ((TagType)(vtype))
                        {
                            case Cdy.Tag.TagType.IntPoint:
                            case Cdy.Tag.TagType.UIntPoint:
                            case Cdy.Tag.TagType.IntPoint3:
                            case Cdy.Tag.TagType.UIntPoint3:
                            case Cdy.Tag.TagType.LongPoint:
                            case Cdy.Tag.TagType.ULongPoint:
                            case Cdy.Tag.TagType.LongPoint3:
                            case Cdy.Tag.TagType.ULongPoint3:
                                vv.Value = val.ToString();
                                break;
                            default:
                                vv.Value = val;
                                break;
                        }

                        vv.Timestamp = time;
                        vv.StatusCode = IsGoodQuality(qua) ? StatusCodes.Good: StatusCodes.Bad;
                        if(oldval != val)
                        {
                            vv.ClearChangeMasks(SystemContext, false);
                        }
                    }
                    Thread.Sleep(500);
                }
            });
        }

        private bool IsGoodQuality(byte qua)
        {
            return true;
        }

        

        public override void HistoryRead(OperationContext context, HistoryReadDetails details, TimestampsToReturn timestampsToReturn, bool releaseContinuationPoints, IList<HistoryReadValueId> nodesToRead, IList<HistoryReadResult> results, IList<ServiceResult> errors)
        {
            ReadProcessedDetails readDetail = details as ReadProcessedDetails;
            //假设查询历史数据  都是带上时间范围的
            if (readDetail == null || readDetail.StartTime == DateTime.MinValue || readDetail.EndTime == DateTime.MinValue)
            {
                errors[0] = StatusCodes.BadHistoryOperationUnsupported;
                return;
            }

            var starttime = readDetail.StartTime;
            var endtime = readDetail.EndTime;
            object res;
            List <ValueItem> revals=null;
            for (int i = 0; i < nodesToRead.Count; i++)
            {
                HistoryReadValueId nodeToRead = nodesToRead[i];
                if(mIdTagMaps.ContainsKey(nodeToRead.NodeId))
                {
                    var sid = mIdTagMaps[nodeToRead.NodeId].Id;
                    var mm = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.ITagManager>();
                    var tag = mm.GetTagById(sid);
                    if (tag != null)
                    {
                        switch (tag.Type)
                        {
                            case Cdy.Tag.TagType.Bool:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<bool>(tag.Id, starttime, endtime);
                                revals = ProcessResult<bool>(res);
                                break;
                            case Cdy.Tag.TagType.Byte:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<byte>(tag.Id, starttime, endtime);
                                revals = ProcessResult<byte>(res);
                                break;
                            case Cdy.Tag.TagType.DateTime:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<DateTime>(tag.Id, starttime, endtime);
                                revals = ProcessResult<DateTime>(res);
                                break;
                            case Cdy.Tag.TagType.Double:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<double>(tag.Id, starttime, endtime);
                                revals = ProcessResult<double>(res);
                                break;
                            case Cdy.Tag.TagType.Float:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<float>(tag.Id, starttime, endtime);
                                revals = ProcessResult<float>(res);
                                break;
                            case Cdy.Tag.TagType.Int:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<int>(tag.Id, starttime, endtime);
                                revals = ProcessResult<int>(res);
                                break;
                            case Cdy.Tag.TagType.Long:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<long>(tag.Id, starttime, endtime);
                                revals = ProcessResult<long>(res);
                                break;
                            case Cdy.Tag.TagType.Short:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<short>(tag.Id, starttime, endtime);
                                revals = ProcessResult<short>(res);
                                break;
                            case Cdy.Tag.TagType.String:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<string>(tag.Id, starttime, endtime);
                                revals = ProcessResult<string>(res);
                                break;
                            case Cdy.Tag.TagType.UInt:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<uint>(tag.Id, starttime, endtime);
                                revals = ProcessResult<uint>(res);
                                break;
                            case Cdy.Tag.TagType.ULong:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ulong>(tag.Id, starttime, endtime);
                                revals = ProcessResult<ulong>(res);
                                break;
                            case Cdy.Tag.TagType.UShort:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ushort>(tag.Id, starttime, endtime);
                                revals = ProcessResult<ushort>(res);
                                break;
                            case Cdy.Tag.TagType.IntPoint:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<int>(tag.Id, starttime, endtime);
                                revals = ProcessResult<IntPointData>(res);
                                break;
                            case Cdy.Tag.TagType.UIntPoint:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<uint>(tag.Id, starttime, endtime);
                                revals = ProcessResult<UIntPointData>(res);
                                break;
                            case Cdy.Tag.TagType.IntPoint3:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<IntPoint3Data>(tag.Id, starttime, endtime);
                                revals = ProcessResult<IntPoint3Data>(res);
                                break;
                            case Cdy.Tag.TagType.UIntPoint3:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<UIntPoint3Data>(tag.Id, starttime, endtime);
                                revals = ProcessResult<UIntPoint3Data>(res);
                                break;
                            case Cdy.Tag.TagType.LongPoint:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<LongPointData>(tag.Id, starttime, endtime);
                                revals = ProcessResult<LongPointData>(res);
                                break;
                            case Cdy.Tag.TagType.ULongPoint:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ULongPointTag>(tag.Id, starttime, endtime);
                                revals = ProcessResult<ULongPointData>(res);
                                break;
                            case Cdy.Tag.TagType.LongPoint3:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<LongPoint3Data>(tag.Id, starttime, endtime);
                                revals = ProcessResult<LongPoint3Data>(res);
                                break;
                            case Cdy.Tag.TagType.ULongPoint3:
                                res = DBRuntime.Proxy.DatabaseRunner.Manager.Proxy.QueryAllHisData<ULongPoint3Data>(tag.Id, starttime, endtime);
                                revals = ProcessResult<ULongPoint3Data>(res);
                                break;
                        }
                        if (revals != null)
                        {
                            results[i] = new HistoryReadResult()
                            {
                                StatusCode = StatusCodes.Good,
                                HistoryData = new ExtensionObject(revals)
                            };
                        }
                        else
                        {
                            results[i] = new HistoryReadResult()
                            {
                                StatusCode = StatusCodes.GoodNoData
                            };
                        }
                    }
                    else
                    {
                        results[i] = new HistoryReadResult()
                        {
                            StatusCode = StatusCodes.BadNotFound
                        };
                    }
                }
                else
                {
                    results[i] = new HistoryReadResult()
                    {
                        StatusCode = StatusCodes.BadNotFound
                    };
                }
            }
            base.HistoryRead(context, details, timestampsToReturn, releaseContinuationPoints, nodesToRead, results, errors);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tagname"></param>
        /// <param name="datas"></param>
        /// <returns></returns>
        private List<ValueItem> ProcessResult<T>(object datas)
        {
            if (datas != null)
            {
                List<ValueItem> re = new List<ValueItem>();
                List<ValueItem> values = new List<ValueItem>();
                var vdata = datas as HisQueryResult<T>;
                if (vdata != null)
                {
                    for (int i = 0; i < vdata.Count; i++)
                    {
                        byte qu;
                        DateTime time;
                        var val = vdata.GetValue(i, out time, out qu);
                        values.Add(new ValueItem() { Time = time, Quality = qu, Value = new Variant( val )});
                    }
                    vdata.Dispose();
                }
                else
                {
                    if (datas != null && datas is IDisposable)
                    {
                        (datas as IDisposable).Dispose();
                    }
                }
                return re;
            }
            return null;
        }

    }


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
        public Variant Value { get; set; }
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


}
