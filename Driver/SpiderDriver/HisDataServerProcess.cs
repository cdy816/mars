//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/9/23 15:44:01.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using Cdy.Tag.Driver;
using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace SpiderDriver
{
    /// <summary>
    /// 
    /// </summary>
    public class HisDataServerProcess : ServerProcessBase
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
        public override byte FunId => APIConst.HisValueFun;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        protected override void ProcessSingleData(string client, IByteBuffer data)
        {
            if (data.ReferenceCount == 0)
            {
                LoggerService.Service.Warn("SpiderDriver_HisDataServerProcess", "invailed data buffer in HisDataServerProcess");
                return;
            }
            byte cmd = data.ReadByte();
            long id = data.ReadLong();
            if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(id))
            {
                switch (cmd)
                {
                    case APIConst.SetTagHisValue:
                        ProcessSetHisData(client, data);
                        break;
                }

            }
            base.ProcessSingleData(client, data);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetHisData(string clientid, IByteBuffer block)
        {
            var id = block.ReadInt();
            var count = block.ReadInt();
            var typ = block.ReadByte();
            int timedu = block.ReadInt();

            List<TagValue> tagvalues = new List<TagValue>();
            switch (typ)
            {
                case (byte)TagType.Bool:
                    for(int i=0;i<count;i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadBoolean();
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Byte:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadByte();
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Short:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadShort();
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.UShort:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadUnsignedShort();
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Int:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadInt();
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.UInt:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadUnsignedInt();
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Long:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadLong();
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.ULong:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = (ulong)block.ReadLong();
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Float:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadFloat();
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Double:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadDouble();
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.String:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadString();
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.DateTime:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = DateTime.FromBinary(block.ReadLong());
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.IntPoint:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = new IntPointData() { X = block.ReadInt(), Y = block.ReadInt() };
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.UIntPoint:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = new UIntPointData() { X = block.ReadUnsignedInt(), Y = block.ReadUnsignedInt() };
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.IntPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = new IntPoint3Data() { X = block.ReadInt(), Y = block.ReadInt(),Z=block.ReadInt() };
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.UIntPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = new UIntPoint3Data() { X = block.ReadUnsignedInt(), Y = block.ReadUnsignedInt(), Z = block.ReadUnsignedInt() };
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.LongPoint:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = new LongPointData() { X = block.ReadLong(), Y = block.ReadLong() };
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.ULongPoint:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = new ULongPointData() { X = (ulong)block.ReadLong(), Y = (ulong)block.ReadLong() };
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.LongPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = new LongPoint3Data() { X = block.ReadLong(), Y = block.ReadLong(),Z=block.ReadLong() };
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.ULongPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = new ULongPoint3Data() { X = (ulong)block.ReadLong(), Y = (ulong)block.ReadLong(), Z = (ulong)block.ReadLong() };
                        var qa = block.ReadByte();
                        tagvalues.Add(new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
            }
            var service = ServiceLocator.Locator.Resolve<ITagHisValueProduct>();
            
            service.SetTagHisValue(id, tagvalues, timedu);

            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
