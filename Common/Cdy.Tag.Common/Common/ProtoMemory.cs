//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/28 12:20:33.
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
    public class ProtoMemory:IDisposable
    {

        #region ... Variables  ...
        private Google.Protobuf.CodedOutputStream outputStream;
        private Google.Protobuf.CodedInputStream inputStream;
        private byte[] mDataBuffer;
        private int position = 0;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public ProtoMemory(int size)
        {
            mDataBuffer = new byte[size];
            position = 0;
            outputStream = new Google.Protobuf.CodedOutputStream(mDataBuffer);
            inputStream = new Google.Protobuf.CodedInputStream(mDataBuffer);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        public ProtoMemory(byte[] data)
        {
            mDataBuffer = data;
            position = 0;
            outputStream = new Google.Protobuf.CodedOutputStream(mDataBuffer);
            inputStream = new Google.Protobuf.CodedInputStream(mDataBuffer);
        }


        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public byte[] DataBuffer
        {
            get
            {
                return mDataBuffer;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public long ReadPosition
        {
            get
            {
                return inputStream.Position;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public long WritePosition
        {
            get
            {
                return outputStream.Position;
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Reset()
        {
            if(outputStream!=null)
            {
                outputStream.Dispose();
            }
            
            if(inputStream!=null)
            {
                inputStream.Dispose();
            }

            outputStream = new Google.Protobuf.CodedOutputStream(mDataBuffer);
            inputStream = new Google.Protobuf.CodedInputStream(mDataBuffer);
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteInt32(uint value)
        {
            outputStream.WriteUInt32(value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public int ReadInt32()
        {
           return inputStream.ReadInt32();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public int ReadSInt32()
        {
            return inputStream.ReadSInt32();
        }

        public long ReadSInt64()
        {
            return inputStream.ReadSInt64();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public uint ReadUInt32()
        {
            return inputStream.ReadUInt32();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteInt32(int value)
        {
            outputStream.WriteInt32(value);
        }

        public void WriteSInt32(int value)
        {
            outputStream.WriteSInt32(value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteSInt64(long value)
        {
            outputStream.WriteSInt64(value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteInt64(long value)
        {
            outputStream.WriteInt64(value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public long ReadInt64()
        {
            return inputStream.ReadInt64();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ulong ReadUInt64()
        {
            return inputStream.ReadUInt64();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteInt64(ulong value)
        {
            outputStream.WriteUInt64(value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteDouble(double value)
        {
            outputStream.WriteDouble(value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public double ReadDouble()
        {
            return inputStream.ReadDouble();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Flush()
        {
            if (outputStream != null) outputStream.Flush();
        }

        public void Dispose()
        {
            if (outputStream != null)
            {
                outputStream.Dispose();
            }

            if (inputStream != null)
            {
                inputStream.Dispose();
            }
            mDataBuffer = null;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
