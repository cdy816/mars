// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// ReSharper disable ConvertToAutoProperty
namespace DotNetty.Buffers
{
    using System;
    using System.Diagnostics.Contracts;
    using System.IO;
    using System.Runtime.CompilerServices;
    using System.Runtime.InteropServices;
    using System.Threading;
    using System.Threading.Tasks;
    using DotNetty.Common.Internal;
    using DotNetty.Common.Utilities;

    public unsafe class UnpooledUnsafeDirectByteBuffer : AbstractReferenceCountedByteBuffer
    {
        readonly IByteBufferAllocator allocator;

        int capacity;
        bool doNotFree;
        byte[] buffer;
        IntPtr mBufferPointer;

        public UnpooledUnsafeDirectByteBuffer(IByteBufferAllocator alloc, int initialCapacity, int maxCapacity)
            : base(maxCapacity)
        {
            Contract.Requires(alloc != null);
            Contract.Requires(initialCapacity >= 0);
            Contract.Requires(maxCapacity >= 0);

            if (initialCapacity > maxCapacity)
            {
                throw new ArgumentException($"initialCapacity({initialCapacity}) > maxCapacity({maxCapacity})");
            }

            this.allocator = alloc;
            // this.SetByteBuffer(this.NewArray(initialCapacity), false);

            mBufferPointer = Marshal.AllocHGlobal(initialCapacity);
        }

        protected UnpooledUnsafeDirectByteBuffer(IByteBufferAllocator alloc, byte[] initialBuffer, int maxCapacity, bool doFree)
            : base(maxCapacity)
        {
            Contract.Requires(alloc != null);
            Contract.Requires(initialBuffer != null);

            int initialCapacity = initialBuffer.Length;
            if (initialCapacity > maxCapacity)
            {
                throw new ArgumentException($"initialCapacity({initialCapacity}) > maxCapacity({maxCapacity})");
            }

            this.allocator = alloc;
            this.doNotFree = !doFree;
            this.SetByteBuffer(initialBuffer, false);

        }

        protected virtual IntPtr AllocateDirect(int initialCapacity) => this.NewArray(initialCapacity);

        protected IntPtr NewArray(int initialCapacity) => Marshal.AllocHGlobal(initialCapacity);

        //protected virtual void FreeDirect(byte[] array)
        //{
        //    // NOOP rely on GC.
        //}

        protected virtual void FreeDirect(IntPtr ptr,int capacity)
        {
            Marshal.Release(ptr);
        }

        void SetByteBuffer(byte[] array, bool tryFree)
        {
            if (tryFree)
            {
                //byte[] oldBuffer = this.buffer;
                //if (oldBuffer != null)
                //{
                if (this.doNotFree)
                {
                    this.doNotFree = false;
                }
                else
                {
                    if (mBufferPointer != IntPtr.Zero)
                    {
                        FreeDirect(mBufferPointer, capacity);
                    }
                    //this.FreeDirect(oldBuffer,capacity);
                }
                //}

            }
            mBufferPointer = (IntPtr)array.AsMemory().Pin().Pointer;
            this.capacity = array.Length;
        }

        public override bool IsDirect => true;

        public override int Capacity => this.capacity;

        public override IByteBuffer AdjustCapacity(int newCapacity)
        {
            this.CheckNewCapacity(newCapacity);

            int rIdx = this.ReaderIndex;
            int wIdx = this.WriterIndex;

            int oldCapacity = this.capacity;
            if (newCapacity > oldCapacity)
            {
                //byte[] oldBuffer = this.buffer;
                //byte[] newBuffer = this.AllocateDirect(newCapacity);

                IntPtr oldBuffer = this.mBufferPointer;
                IntPtr newBuffer = this.AllocateDirect(newCapacity);
                Buffer.MemoryCopy((byte*)oldBuffer, (byte*)newBuffer, newCapacity, oldCapacity);
                Marshal.Release(oldBuffer);

                //PlatformDependent.CopyMemory(oldBuffer, 0, newBuffer, 0, oldCapacity);
                //this.SetByteBuffer(newBuffer, true);
            }
            else if (newCapacity < oldCapacity)
            {
                //byte[] oldBuffer = this.buffer;
                //byte[] newBuffer = this.AllocateDirect(newCapacity);
                IntPtr oldBuffer = this.mBufferPointer;
                IntPtr newBuffer = this.AllocateDirect(newCapacity);
                if (rIdx < newCapacity)
                {
                    if (wIdx > newCapacity)
                    {
                        this.SetWriterIndex(wIdx = newCapacity);
                    }
                    Buffer.MemoryCopy((byte*)(oldBuffer+rIdx), (byte*)newBuffer, newCapacity, wIdx - rIdx);
                    // PlatformDependent.CopyMemory(oldBuffer, rIdx, newBuffer, 0, wIdx - rIdx);
                }
                else
                {
                    this.SetIndex(newCapacity, newCapacity);
                }
                Marshal.Release(oldBuffer);
                // this.SetByteBuffer(newBuffer, true);
            }
            return this;
        }

        public override IByteBufferAllocator Allocator => this.allocator;

        public override bool HasArray => true;

        public override byte[] Array
        {
            get
            {
                this.EnsureAccessible();
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public override Span<byte> ArraySpan => new Span<byte>((void*)(mBufferPointer+ArrayOffset),Capacity);

        public override int ArrayOffset => 0;

        public override bool HasMemoryAddress => true;

        public override ref byte GetPinnableMemoryAddress()
        {
            this.EnsureAccessible();
            return ref *((byte*)mBufferPointer);
        }

        public override IntPtr AddressOfPinnedMemory() => mBufferPointer;

        protected internal override byte _GetByte(int index)
        {
            return *this.Addr(index);
        }

        protected internal override short _GetShort(int index)
        {
            return UnsafeByteBufferUtil.GetShort(this.Addr(index));
        }

        protected internal override short _GetShortLE(int index)
        {
            return UnsafeByteBufferUtil.GetShortLE(this.Addr(index));
        }

        protected internal override int _GetUnsignedMedium(int index)
        {
            return UnsafeByteBufferUtil.GetUnsignedMedium(this.Addr(index));
        }

        protected internal override int _GetUnsignedMediumLE(int index)
        {
            return UnsafeByteBufferUtil.GetUnsignedMediumLE(this.Addr(index));
        }

        protected internal override int _GetInt(int index)
        {
            return UnsafeByteBufferUtil.GetInt(this.Addr(index));
        }

        protected internal override int _GetIntLE(int index)
        {
            return UnsafeByteBufferUtil.GetIntLE(this.Addr(index));
        }

        protected internal override long _GetLong(int index)
        {

            return UnsafeByteBufferUtil.GetLong(this.Addr(index));
        }

        protected internal override long _GetLongLE(int index)
        {

            return UnsafeByteBufferUtil.GetLongLE(this.Addr(index));
        }

        public override IByteBuffer GetBytes(int index, IByteBuffer dst, int dstIndex, int length)
        {
            this.CheckIndex(index, length);

            UnsafeByteBufferUtil.GetBytes(this, this.Addr(index), index, dst, dstIndex, length);
            return this;
        }

        public override IByteBuffer GetBytes(int index, byte[] dst, int dstIndex, int length)
        {
            this.CheckIndex(index, length);

            UnsafeByteBufferUtil.GetBytes(this, this.Addr(index), index, dst, dstIndex, length);
            return this;
        }

        protected internal override void _SetByte(int index, int value) => this.buffer[index] = unchecked((byte)value);

        protected internal override void _SetShort(int index, int value)
        {

            UnsafeByteBufferUtil.SetShort(this.Addr(index), value);
        }

        protected internal override void _SetShortLE(int index, int value)
        {

            UnsafeByteBufferUtil.SetShortLE(this.Addr(index), value);
        }

        protected internal override void _SetMedium(int index, int value)
        {

            UnsafeByteBufferUtil.SetMedium(this.Addr(index), value);
        }

        protected internal override void _SetMediumLE(int index, int value)
        {
            UnsafeByteBufferUtil.SetMediumLE(this.Addr(index), value);
        }

        protected internal override void _SetInt(int index, int value)
        {
            UnsafeByteBufferUtil.SetInt(this.Addr(index), value);
        }

        protected internal override void _SetIntLE(int index, int value)
        {
            UnsafeByteBufferUtil.SetIntLE(this.Addr(index), value);
        }

        protected internal override void _SetLong(int index, long value)
        {
            UnsafeByteBufferUtil.SetLong(this.Addr(index), value);
        }

        protected internal override void _SetLongLE(int index, long value)
        {
            UnsafeByteBufferUtil.SetLongLE(this.Addr(index), value);
        }

        public override IByteBuffer SetBytes(int index, IByteBuffer src, int srcIndex, int length)
        {
            this.CheckIndex(index, length);
            UnsafeByteBufferUtil.SetBytes(this, this.Addr(index), index, src, srcIndex, length);
            return this;
        }

        public override IByteBuffer SetBytes(int index, byte[] src, int srcIndex, int length)
        {
            this.CheckIndex(index, length);
            if (length != 0)
            {
                UnsafeByteBufferUtil.SetBytes(this, this.Addr(index), index, src, srcIndex, length);
            }
            return this;
        }

        public override IByteBuffer GetBytes(int index, Stream output, int length)
        {
            this.CheckIndex(index, length);
            UnsafeByteBufferUtil.GetBytes(this, this.Addr(index), index, output, length);
            return this;
        }

        public override Task<int> SetBytesAsync(int index, Stream src, int length, CancellationToken cancellationToken)
        {
            this.CheckIndex(index, length);
            return UnsafeByteBufferUtil.SetBytesAsync(this, this.Addr(index), index, src, length, cancellationToken);
        }

        public override int IoBufferCount => 1;

        public override ArraySegment<byte> GetIoBuffer(int index, int length)
        {
            this.CheckIndex(index, length);
            return new ArraySegment<byte>(this.buffer, index, length);
        }

        public override ArraySegment<byte>[] GetIoBuffers(int index, int length) => new[] { this.GetIoBuffer(index, length) };

        public override IByteBuffer Copy(int index, int length)
        {
            this.CheckIndex(index, length);

            return UnsafeByteBufferUtil.Copy(this, this.Addr(index), index, length);
        }

        protected internal override void Deallocate()
        {
            byte[] buf = this.buffer;
            if (buf == null)
            {
                return;
            }

            this.buffer = null;

            if (!this.doNotFree)
            {
                this.FreeDirect(this.mBufferPointer,this.capacity);
            }
        }

        public override IByteBuffer Unwrap() => null;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        byte* Addr(int index) => (byte*)(this.mBufferPointer + index);

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //ref byte Addr(int index) => ref this.buffer[index];

        public override IByteBuffer SetZero(int index, int length)
        {
            this.CheckIndex(index, length);

            UnsafeByteBufferUtil.SetZero(this.Addr(index), length);
            return this;
        }

        public override IByteBuffer WriteZero(int length)
        {
            this.EnsureWritable(length);
            int wIndex = this.WriterIndex;

            UnsafeByteBufferUtil.SetZero(this.Addr(wIndex), length);
            this.SetWriterIndex(wIndex + length);
            return this;
        }
    }
}