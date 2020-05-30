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
    public class VarintCodeMemory:IDisposable
    {

        #region ... Variables  ...
        private byte[] mDataBuffer;
        private int position = 0;
        private readonly int limit;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public VarintCodeMemory(int size)
        {
            mDataBuffer = new byte[size];
            position = 0;
            limit = size;
        }

        /// <summary>
        /// 
        /// </summary>
        public VarintCodeMemory(byte[] buffer)
        {
            mDataBuffer = buffer;
            position = 0;
            limit = buffer.Length;
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public byte[] Buffer
        {
            get
            {
                return mDataBuffer;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public int Position
        {
            get
            {
                return position;
            }
            set
            {
                position = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int Length
        {
            get
            {
                return mDataBuffer.Length;
            }
        }
    

        #endregion ...Properties...

        #region ... Methods    ...

       


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteSInt32(int value)
        {
            WriteInt32(EncodeZigZag32(value));
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteSInt64(long value)
        {
            WriteInt64(EncodeZigZag64(value));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteInt32(int value)
        {
            WriteInt32((uint)value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteInt64(long value)
        {
            WriteInt64((ulong)value);
        }


        /// <summary>
        /// Writes a 32 bit value as a varint. The fast route is taken when
        /// there's enough buffer space left to whizz through without checking
        /// for each byte; otherwise, we resort to calling WriteRawByte each time.
        /// </summary>

        public void WriteInt32(uint value)
        {
            while (value > 127 && position < limit)
            {
                mDataBuffer[position++] = (byte)((value & 0x7F) | 0x80);
                value >>= 7;
            }

            while (value > 127)
            {
                WriteRawByte((byte)((value & 0x7F) | 0x80));
                value >>= 7;
            }

            if (position < limit)
            {
                this.mDataBuffer[position++] = (byte)value;
            }
            else
            {
                WriteRawByte((byte)value);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteInt64(ulong value)
        {
            while (value > 127 && position < limit)
            {
                mDataBuffer[position++] = (byte)((value & 0x7F) | 0x80);
                value >>= 7;
            }

            while (value > 127)
            {
                WriteRawByte((byte)((value & 0x7F) | 0x80));
                value >>= 7;
            }
            if (position < limit)
            {
                mDataBuffer[position++] = (byte)value;
            }
            else
            {
                WriteRawByte((byte)value);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteRawByte(byte value)
        {
            if (position == limit)
            {
                return;
            }
            this.mDataBuffer[position++] = value;
        }

        /// <summary>
        /// Reads an sint32 field value from the stream.
        /// </summary>   
        public int ReadSInt32()
        {
            return DecodeZigZag32(ReadRawVarint32());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public int ReadInt32()
        {
            return (int)ReadRawVarint32();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public long ReadSInt64()
        {
            return DecodeZigZag64(ReadRawVarint64());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public long ReadInt64()
        {
            return (long)ReadRawVarint64();
        }

        /// <summary>
        /// Read a raw Varint from the stream.  If larger than 32 bits, discard the upper bits.
        /// This method is optimised for the case where we've got lots of data in the buffer.
        /// That means we can check the size just once, then just read directly from the buffer
        /// without constant rechecking of the buffer length.
        /// </summary>

        public uint ReadRawVarint32()
        {
            if (this.position + 5 > limit)
            {
                return SlowReadRawVarint32();
            }

            int tmp = this.mDataBuffer[position++];
            if (tmp < 128)
            {
                return (uint)tmp;
            }

            int result = tmp & 0x7f;
            if ((tmp = mDataBuffer[position++]) < 128)
            {
                result |= tmp << 7;
            }
            else
            {
                result |= (tmp & 0x7f) << 7;
                if ((tmp = mDataBuffer[position++]) < 128)
                {
                    result |= tmp << 14;
                }
                else
                {
                    result |= (tmp & 0x7f) << 14;
                    if ((tmp = mDataBuffer[position++]) < 128)
                    {
                        result |= tmp << 21;
                    }
                    else
                    {
                        result |= (tmp & 0x7f) << 21;
                        result |= (tmp = mDataBuffer[position++]) << 28;
                        if (tmp >= 128)
                        {

                            // Discard upper 32 bits.
                            // Note that this has to use ReadRawByte() as we only ensure we've
                            // got at least 5 bytes at the start of the method. This lets us
                            // use the fast path in more cases, and we rarely hit this section of code.
                            for (int i = 0; i < 5; i++)
                            {
                                if (ReadRawByte() < 128)
                                {
                                    return (uint)result;
                                }
                            }
                            throw new Exception("out of buffer");
                        }
                    }
                }
            }
            return (uint)result;
        }

        /// <summary>
        /// Same code as ReadRawVarint32, but read each byte individually, checking for  buffer overflow.
        /// </summary>
        /// <returns></returns>
        private uint SlowReadRawVarint32()
        {
            int tmp = ReadRawByte();
            if (tmp < 128)
            {
                return (uint)tmp;
            }

            int result = tmp & 0x7f;
            if ((tmp = ReadRawByte()) < 128)
            {
                result |= tmp << 7;
            }
            else
            {
                result |= (tmp & 0x7f) << 7;
                if ((tmp = ReadRawByte()) < 128)
                {
                    result |= tmp << 14;
                }
                else
                {
                    result |= (tmp & 0x7f) << 14;
                    if ((tmp = ReadRawByte()) < 128)
                    {
                        result |= tmp << 21;
                    }
                    else
                    {
                        result |= (tmp & 0x7f) << 21;
                        result |= (tmp = ReadRawByte()) << 28;
                        if (tmp >= 128)
                        {
                            // Discard upper 32 bits.
                            for (int i = 0; i < 5; i++)
                            {
                                if (ReadRawByte() < 128)
                                {
                                    return (uint)result;
                                }
                            }
                            throw new Exception("out of buffer");
                        }
                    }
                }
            }
            return (uint)result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ulong ReadRawVarint64()
        {
            int shift = 0;
            ulong result = 0;
            while (shift < 64)
            {
                byte b = ReadRawByte();
                result |= (ulong)(b & 0x7F) << shift;
                if ((b & 0x80) == 0)
                {
                    return result;
                }
                shift += 7;
            }
            throw new Exception("out of buffer");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public byte ReadRawByte()
        {
            return this.mDataBuffer[this.position++];
        }


        /// <summary>
        /// Encode a 32-bit value with ZigZag encoding.
        /// </summary>
        /// <remarks>
        /// ZigZag encodes signed integers into values that can be efficiently
        /// encoded with varint.  (Otherwise, negative values must be 
        /// sign-extended to 64 bits to be varint encoded, thus always taking
        /// 10 bytes on the wire.)
        /// </remarks>
        public static uint EncodeZigZag32(int n)
        {
            // Note:  the right-shift must be arithmetic
            return (uint)((n << 1) ^ (n >> 31));
        }



        /// <summary>
        /// Encode a 64-bit value with ZigZag encoding.
        /// </summary>
        /// <remarks>
        /// ZigZag encodes signed integers into values that can be efficiently
        /// encoded with varint.  (Otherwise, negative values must be 
        /// sign-extended to 64 bits to be varint encoded, thus always taking
        /// 10 bytes on the wire.)
        /// </remarks>
        public static ulong EncodeZigZag64(long n)
        {
            return (ulong)((n << 1) ^ (n >> 63));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="n"></param>
        /// <returns></returns>
        public static int DecodeZigZag32(uint n)
        {
            return (int)(n >> 1) ^ -(int)(n & 1);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="n"></param>
        /// <returns></returns>
        public static long DecodeZigZag64(ulong n)
        {
            return (long)(n >> 1) ^ -(long)(n & 1);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            this.mDataBuffer = null;
            //GC.Collect();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public static class VarintCodeMemoryExtends
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public static byte[] GetAvailableDatas(this VarintCodeMemory memory)
        {
            byte[] bvals = new byte[memory.Length];
            Buffer.BlockCopy(memory.Buffer, 0, bvals, 0,bvals.Length);
            return bvals;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static List<int> ToIntList(this VarintCodeMemory memory)
        {
            List<int> re = new List<int>();
            memory.Position = 0;
            while(memory.Position < memory.Length)
            {
                re.Add(memory.ReadInt32());
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static List<long> ToLongList(this VarintCodeMemory memory)
        {
            List<long> re = new List<long>();
            memory.Position = 0;
            while (memory.Position < memory.Length)
            {
                re.Add(memory.ReadInt64());
            }
            return re;
        }
    }
}
