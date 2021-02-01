//==============================================================
//  Copyright (C) 2021  Inc. All rights reserved.
//
//==============================================================
//  Create by chongdaoyang at 2021/1/27 12:54:55.
//  Version 1.0
//  CHONGDAOYANGPC
//==============================================================
using Cdy.Tag;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    /*
 * ****His 文件结构****
 * 一个文件头 + 多个数据区组成 ， 一个数据区：数据区头+数据块指针区+数据块区
 * [] 表示重复的一个或多个内容
 * 
 HisData File Structor
 FileHead(84) + [HisDataRegion]

 FileHead: dataTime(8)(FileTime)+dateTime(8)(LastUpdateTime)+DataRegionCount(4)+DatabaseName(64)
 
 HisDataRegion Structor: RegionHead + DataBlockPoint Area + DataBlock Area

 RegionHead:          PreDataRegionPoint(8) + NextDataRegionPoint(8) + Datatime(8)+ tagcount(4)+ tagid sum(8)+file duration(4)+block duration(4)+Time tick duration(4)
 DataBlockPoint Area: [ID]+[block Point]
 [block point]:       [[tag1 block1 point(12),tag1 block2 point(12),....][tag2 block1 point(12),tag2 block2 point(12),...].....]   以时间单位对变量的数去区指针进行组织,
 [tag block point]:   offset pointer(4)+ datablock area point(8)   offset pointer: bit 32 标识data block 类型,1:标识非压缩区域，0:压缩区域,bit1~bit31 偏移地址
 DataBlock Area:      [[tag1 block1 size + tag1 block1 data][tag1 block2 size + tag1 block2 data]....][[tag2 block1 size + tag2 block1 data][tag2 block2 size + tag2 block2 data]....]....
*/

    /// <summary>
    /// 对历史数据文件格式内容进行重新整理,将一个文件内的同一个变量的数据合并在一起,提高数据查询效率
    /// </summary>
    public class HisDataArrange
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        public static HisDataArrange Arrange = new HisDataArrange();

        const int bufferLenght = 2 * 1024 * 1024;

        private bool mIsPaused = false;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        public HisDataArrange()
        {
            
        }

        #endregion ...Constructor...

        #region ... Properties ...



        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Paused()
        {
            mIsPaused = true;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Resume()
        {
            mIsPaused = false;
        }

        /// <summary>
        /// 
        /// </summary>
        private void CheckPaused()
        {
            while (mIsPaused) Thread.Sleep(100);
        }

        /// <summary>
        /// 拷贝文件头部
        /// </summary>
        private void CopyFileHead(System.IO.Stream msource,System.IO.Stream mtarget)
        {
            byte[] bval = ArrayPool<byte>.Shared.Rent(DataFileManager.FileHeadSize);
            msource.Read(bval, 0, DataFileManager.FileHeadSize);
            mtarget.Write(bval, 0, DataFileManager.FileHeadSize);
            ArrayPool<byte>.Shared.Return(bval);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="msource"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        private int ReadInt(System.IO.Stream msource,long offset)
        {
            int re = 0;
            byte[] bval = ArrayPool<byte>.Shared.Rent(4);
            msource.Position = offset;
            msource.Read(bval, 0, 4);
            re = BitConverter.ToInt32(bval);
            ArrayPool<byte>.Shared.Return(bval);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="msource"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        private long ReadLong(System.IO.Stream msource, long offset)
        {
            long re = 0;
            byte[] bval = ArrayPool<byte>.Shared.Rent(8);
            msource.Position = offset;
            var size = msource.Read(bval, 0, 8);
            if (size == 8)
            {
                re = BitConverter.ToInt64(bval);
            }
            else
            {
                re = -1;
            }
            ArrayPool<byte>.Shared.Return(bval);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="msource"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        private unsafe MarshalFixedMemoryBlock Read(System.IO.Stream msource, long offset,int size)
        {
            msource.Position = offset;
            MarshalFixedMemoryBlock re = new MarshalFixedMemoryBlock(size);
            msource.Read(new Span<byte>((void*)re.StartMemory, size));
            return re;
        }

        /// <summary>
        /// 拷贝数据区
        /// </summary>
        /// <param name="msource"></param>
        /// <param name="offset"></param>
        /// <param name="mtarget"></param>
        private bool CopyDataRegion(System.IO.Stream msource,long sourceOffset,System.IO.Stream mtarget)
        {
            msource.Position = sourceOffset;

            int fileDuration = 0;
            int blockDuration = 0;
            int tagcount = 0;

            CheckPaused();

            //copy data region head
            byte[] bval = ArrayPool<byte>.Shared.Rent(48);
            msource.Read(bval, 0, 48);
            mtarget.Write(bval, 0, 48);

            tagcount = BitConverter.ToInt32(bval, 24);
            fileDuration = BitConverter.ToInt32(bval, 36);
            blockDuration = BitConverter.ToInt32(bval, 40);

            int blockcount = fileDuration * 60 / blockDuration;

            //copy data ids
            msource.Read(bval, 0, 4);
            int size = BitConverter.ToInt32(bval);
            mtarget.Write(bval, 0, 4);

            if (size > 0)
            {
                var bval2 = ArrayPool<byte>.Shared.Rent(size);
                msource.Read(bval2, 0, size);
                mtarget.Write(bval2, 0, size);
                ArrayPool<byte>.Shared.Return(bval2);
            }
            ArrayPool<byte>.Shared.Return(bval);

            long pheadpointlocation = mtarget.Position;

            CheckPaused();
            var sourceheadpoint = Read(msource, msource.Position, tagcount * 12 * blockcount);

            MarshalFixedMemoryBlock targetheadpoint;

            CopyRegionData(msource, sourceheadpoint,tagcount,blockcount, mtarget,out targetheadpoint);
            long lp = mtarget.Position;

            if (targetheadpoint != null)
            {
                //write tag data pointer
                mtarget.Position = pheadpointlocation;
                targetheadpoint.WriteToStream(mtarget);

                mtarget.Position = lp;

                targetheadpoint.Dispose();
                sourceheadpoint.Dispose();
                return true;
            }
            else
            {
                sourceheadpoint.Dispose();
                return false;
            }

            
        }

        /// <summary>
        /// 拷贝DataRegion数据
        /// </summary>
        /// <param name="msource">源流</param>
        /// <param name="sourceheadpoint">源数据块指针</param>
        /// <param name="tagcount">变量个数</param>
        /// <param name="blockcount">数据块个数</param>
        /// <param name="mtarget">目标流</param>
        /// <param name="targetheadpoint">目标数据块</param>
        private unsafe void CopyRegionData(System.IO.Stream msource, MarshalFixedMemoryBlock sourceheadpoint, int tagcount, int blockcount, System.IO.Stream mtarget, out MarshalFixedMemoryBlock targetheadpoint)
        {
            //Stopwatch sw = new Stopwatch();
            //sw.Start();

            MarshalFixedMemoryBlock re = new MarshalFixedMemoryBlock(sourceheadpoint.Length);
            MarshalFixedMemoryBlock data = new MarshalFixedMemoryBlock(blockcount * 302 * 264 * 2);
         
            //int bufferLenght = 2*1024 * 1024;
            IntPtr[] databuffers = new IntPtr[blockcount];
            long[] databufferLocations = new long[blockcount];
            int[] databufferLens = new int[blockcount];

            for(int i=0;i<blockcount;i++)
            {
                databuffers[i] = Marshal.AllocHGlobal(bufferLenght);
            }
            //sw.Stop();
            //LoggerService.Service.Debug("HisDataArrange", "初始化分配内存 耗时:"+ ltmp1 +"," +(sw.ElapsedMilliseconds-ltmp1));

            mtarget.Position += tagcount * 12 * blockcount;

            long mtargetposition = mtarget.Position;
            int offset = 0;
            for (int i = 0; i < tagcount; i++)
            {
               
                data.Clear();
                offset = 0;
                mtargetposition = mtarget.Position;
                for (int j = 0; j < blockcount; j++)
                {
                    CheckPaused();

                    var dataoffset = sourceheadpoint.ReadInt(j * tagcount * 12 + i * 12);
                    var baseaddress = sourceheadpoint.ReadLong(j * tagcount * 12 + i * 12 + 4);

                    if (dataoffset < 0)
                    {
                        baseaddress = baseaddress + (dataoffset & 0x7FFFFFFF);
                    }
                    else
                    {
                        baseaddress = baseaddress + (dataoffset);
                    }

                    if (baseaddress > 0)
                    {

                        //重新组织数据块指针的分布形式，使得一个变量的数据块指针在一起
                        re.WriteInt(j * 12 + i * blockcount * 12, (int)(offset | 0x80000000));
                        re.WriteLong(j * 12 + i * blockcount * 12 + 4, mtargetposition);

                        int datasize = 0;
                        int dataloc = 0;
                        if (baseaddress >= databufferLocations[j] && (baseaddress - databufferLocations[j] + 4) <= databufferLens[j] && (baseaddress - databufferLocations[j] + 4 + MemoryHelper.ReadInt32((void*)databuffers[j], baseaddress - databufferLocations[j])) <= databufferLens[j])
                        {
                            datasize = MemoryHelper.ReadInt32((void*)databuffers[j], baseaddress - databufferLocations[j]);
                            dataloc = (int)(baseaddress - databufferLocations[j] + 4);
                        }
                        else
                        {
                            int len = (int)Math.Min(bufferLenght, msource.Length - msource.Position);
                            msource.Position = baseaddress;
                            msource.Read(new Span<byte>((void*)databuffers[j], len));
                            databufferLocations[j] = baseaddress;
                            databufferLens[j] = len;
                            datasize = MemoryHelper.ReadInt32((void*)databuffers[j], 0);
                            dataloc = (int)(baseaddress - databufferLocations[j] + 4);
                        }

    
                        data.CheckAndResize(data.Position + datasize + 4, 0.5);
                        data.WriteInt(offset, datasize);
                        offset += 4;

                        
                        if ((dataloc + datasize) > bufferLenght)
                        {
                            targetheadpoint = null;
                            return;
                        }

                        Buffer.MemoryCopy((void*)(databuffers[j] + dataloc), (void*)(data.Buffers + offset), datasize, datasize);
                        data.Position += datasize;
                        offset += datasize;
                    }
                    else
                    {
                        //重新组织数据块指针的分布形式，使得一个变量的数据块指针在一起
                        re.WriteInt(j * 12 + i * blockcount * 12, 0);
                        re.WriteLong(j * 12 + i * blockcount * 12 + 4, 0);
                    }

                }

                data.WriteToStream(mtarget, 0, offset);
            }

            data.Dispose();

            for (int i = 0; i < blockcount; i++)
            {
                Marshal.FreeHGlobal(databuffers[i]);
            }

            targetheadpoint = re;
        }

        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="msource"></param>
        /// <returns></returns>
        private List<long> ListDataAreas(System.IO.Stream msource)
        {
            List<long> re = new List<long>();
            msource.Position = DataFileManager.FileHeadSize;
            re.Add(msource.Position);
            long nextp = ReadLong(msource, msource.Position + 8);
            while( nextp>0)
            {
                re.Add(nextp);
                nextp = ReadLong(msource,nextp + 8);
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sourceFile"></param>
        /// <param name="targetFile"></param>
        public bool ReArrange(string sourceFile,string targetFile)
        {
            bool result = true;
            try
            {
               
                var source = GetDataFileSerise(sourceFile);
                var target = GetDataFileSerise(targetFile);

                var sstream = source.GetStream();
                var tstream = target.GetStream();

                CopyFileHead(sstream, tstream);

                var dataarealoc = ListDataAreas(sstream);

                long lastarealoc = -1;

                foreach (var vv in dataarealoc)
                {
                    result = CopyDataRegion(sstream, vv, tstream);
                    if (lastarealoc > 0)
                    {
                        //更新下个指针数据区
                        var lp = tstream.Position;
                        target.Write(lp, lastarealoc + 8);
                        //更新上一个区域的指针
                        target.Write(lastarealoc, lp);
                        lastarealoc = lp;
                    }
                }
                source.Dispose();
                target.Dispose();
            }
            catch
            {
                return false;
            }
            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        private DataFileSeriserbase GetDataFileSerise(string file)
        {
            var re = DataFileSeriserManager.manager.GetDefaultFileSersie().New();
            re.CreatOrOpenFile(file);
            return re;
        }

        private DateTime? ParseFileNameToDateTime(System.IO.FileInfo file)
        {
            string sname = System.IO.Path.GetFileNameWithoutExtension(file.FullName);
            string stime = sname.Substring(sname.Length - 12, 12);
            int yy = 0, mm = 0, dd = 0;

            int id = -1;
            int.TryParse(sname.Substring(sname.Length - 15, 3), out id);

            if (id == -1)
                return null;

            if (!int.TryParse(stime.Substring(0, 4), out yy))
            {
                return null;
            }

            if (!int.TryParse(stime.Substring(4, 2), out mm))
            {
                return null;
            }

            if (!int.TryParse(stime.Substring(6, 2), out dd))
            {
                return null;
            }
            int hhspan = int.Parse(stime.Substring(8, 2));

            int hhind = int.Parse(stime.Substring(10, 2));

            int hh = hhspan * hhind;
            return new DateTime(yy, mm, dd, hh, 0, 0);
        }

        /// <summary>
        /// 检查并转化历史文件内容格式
        /// </summary>
        /// <param name="file"></param>
        /// <param name="fileDuration"></param>
        /// <param name="removeold"></param>
        public bool CheckAndReArrangeHisFile(string file,out string targetfile,int fileDuration=4,bool removeold=true)
        {
            try
            {
                if (System.IO.File.Exists(file))
                {
                    System.IO.FileInfo finfo = new System.IO.FileInfo(file);

                    DateTime? dt = ParseFileNameToDateTime(finfo);

                    if (!dt.HasValue)
                    {
                        targetfile = string.Empty;
                        return false;
                    }

                    Debug.Print(file + ": total hours:" + (DateTime.UtcNow - dt.Value).TotalHours  +"   "+ DateTime.Now);
                    //如果文件的创建时间，到现在超过一个文件能够保存的最大时间
                    if ((DateTime.UtcNow - dt.Value).TotalHours > (fileDuration + 0.5))
                    {
                        string tdirect = System.IO.Path.GetDirectoryName(file);
                        targetfile = System.IO.Path.Combine(tdirect, System.IO.Path.GetFileNameWithoutExtension(file) + ".his");

                        if (!System.IO.File.Exists(targetfile))
                        {
                            Stopwatch sw = new Stopwatch();
                            sw.Start();
                            if (ReArrange(file, targetfile))
                            {
                                if (removeold)
                                {
                                    System.IO.File.Delete(file);
                                }
                                sw.Stop();
                                LoggerService.Service.Info("HisDataArrange", "历史文件 " + System.IO.Path.GetDirectoryName(file) + " 内容重组织耗时:" + sw.ElapsedMilliseconds, ConsoleColor.Cyan);
                                return true;
                            }
                            sw.Stop();
                            LoggerService.Service.Info("HisDataArrange", "历史文件 "+ System.IO.Path.GetDirectoryName(file) +" 内容重组织耗时:"+sw.ElapsedMilliseconds,ConsoleColor.Cyan);
                        }
                    }
                }
            }
            catch
            {

            }
            targetfile = string.Empty;
            return false;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
