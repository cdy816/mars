using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class DeadAreaCompressUnit : CompressUnitbase
    {
        /// <summary>
        /// 
        /// </summary>
        public override int TypeCode => 2;

        /// <summary>
        /// 
        /// </summary>
        public override string Desc => "死区压缩";

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override CompressUnitbase Clone()
        {
            return new DeadAreaCompressUnit();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetAddr"></param>
        /// <returns></returns>
        public override int Compress(RecordMemory source, int sourceAddr, RecordMemory target, int targetAddr, int size)
        {
            return 0;
        }

        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<bool> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<byte> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<short> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ushort> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<int> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<uint> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<long> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ulong> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<float> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<double> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<DateTime> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<string> result)
        {
            throw new NotImplementedException();
        }

        public override bool DeCompressBoolValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressBoolValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<bool> result)
        {
            throw new NotImplementedException();
        }

        public override byte DeCompressByteValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressByteValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<byte> result)
        {
            throw new NotImplementedException();
        }

        public override DateTime DeCompressDateTimeValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressDateTimeValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<DateTime> result)
        {
            throw new NotImplementedException();
        }

        public override double DeCompressDoubleValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressDoubleValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<double> result)
        {
            throw new NotImplementedException();
        }

        public override float DeCompressFloatValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressFloatValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<float> result)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressIntValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressIntValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<int> result)
        {
            throw new NotImplementedException();
        }

        public override long DeCompressLongValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressLongValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<long> result)
        {
            throw new NotImplementedException();
        }

        public override short DeCompressShortValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressShortValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<short> result)
        {
            throw new NotImplementedException();
        }

        public override string DeCompressStringValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressStringValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<string> result)
        {
            throw new NotImplementedException();
        }

        public override uint DeCompressUIntValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressUIntValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<uint> result)
        {
            throw new NotImplementedException();
        }

        public override ulong DeCompressULongValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressULongValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ulong> result)
        {
            throw new NotImplementedException();
        }

        public override ushort DeCompressUShortValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            throw new NotImplementedException();
        }

        public override int DeCompressUShortValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ushort> result)
        {
            throw new NotImplementedException();
        }
    }
}
