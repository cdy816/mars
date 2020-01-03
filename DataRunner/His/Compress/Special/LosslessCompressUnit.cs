//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class LosslessCompressUnit : CompressUnitbase
    {
        /// <summary>
        /// 
        /// </summary>
        public override string Desc => "无损压缩";

        /// <summary>
        /// 
        /// </summary>
        public override int TypeCode => 1;

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override CompressUnitbase Clone()
        {
            return new LosslessCompressUnit();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetAddr"></param>
        /// <returns></returns>
        public override int Compress(MemoryBlock source, int sourceAddr, MemoryBlock target, int targetAddr, int size)
        {
            return 0;
        }

        public override int DeCompressAllValue(MemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<bool> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(MemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<byte> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(MemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<short> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(MemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ushort> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(MemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<int> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(MemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<uint> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(MemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<long> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(MemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ulong> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(MemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<float> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(MemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<double> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(MemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<DateTime> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(MemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<string> result)
        {
            throw new NotImplementedException();
        }

        public override bool DeCompressBoolValue(MemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressBoolValue(MemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<bool> result)
        {
            throw new NotImplementedException();
        }

        public override byte DeCompressByteValue(MemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressByteValue(MemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<byte> result)
        {
            throw new NotImplementedException();
        }

        public override DateTime DeCompressDateTimeValue(MemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressDateTimeValue(MemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<DateTime> result)
        {
            throw new NotImplementedException();
        }

        public override double DeCompressDoubleValue(MemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressDoubleValue(MemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<double> result)
        {
            throw new NotImplementedException();
        }

        public override float DeCompressFloatValue(MemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressFloatValue(MemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<float> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressIntValue(MemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressIntValue(MemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<int> result)
        {
            throw new NotImplementedException();
        }

        public override long DeCompressLongValue(MemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressLongValue(MemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<long> result)
        {
            throw new NotImplementedException();
        }

        public override short DeCompressShortValue(MemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressShortValue(MemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<short> result)
        {
            throw new NotImplementedException();
        }

        public override string DeCompressStringValue(MemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressStringValue(MemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<string> result)
        {
            throw new NotImplementedException();
        }

        public override uint DeCompressUIntValue(MemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressUIntValue(MemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<uint> result)
        {
            throw new NotImplementedException();
        }

        public override ulong DeCompressULongValue(MemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressULongValue(MemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ulong> result)
        {
            throw new NotImplementedException();
        }

        public override ushort DeCompressUShortValue(MemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressUShortValue(MemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ushort> result)
        {
            throw new NotImplementedException();
        }
    }
}
