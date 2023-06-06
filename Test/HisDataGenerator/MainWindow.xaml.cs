using Cdy.Tag;
using DBRuntime.His;
using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Forms;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace HisDataGenerator
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {

        private CompressMemory4 mCompress;
        private SeriseFileItem7 mSerise;
        private double mTotalCount = 0;
        private double mCurrentCount = 0;
        private int mTagCount = 10000;

        public MainWindow()
        {
            InitializeComponent();
            CompressUnitManager2.Manager.Init();
            DataFileSeriserManager.manager.Init();

            mCompress = new CompressMemory4() { Id = 0, Name = "Compress memory" };
            mCompress.Init(new HisDataMemoryBlockCollection3());
            mSerise = new SeriseFileItem7() { DatabaseName = "test", Id = 0,FileDuration=4,BlockDuration=5,TagCountOneFile= 100000,StatisticsMemory=new StatisticsMemoryMap() };

            mSerise.FileWriter = DataFileSeriserManager.manager.GetSeriser("LocalFile").New();
            mSerise.MetaFileWriter = DataFileSeriserManager.manager.GetSeriser("LocalFile").New();
            mSerise.FileWriter2 = DataFileSeriserManager.manager.GetSeriser("LocalFile").New();
            mSerise.MetaFileWriter2 = DataFileSeriserManager.manager.GetSeriser("LocalFile").New();

            Console.SetOut(Console.Out);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void pb_Click(object sender, RoutedEventArgs e)
        {
            FolderBrowserDialog fbd = new FolderBrowserDialog();
            if(fbd.ShowDialog() == System.Windows.Forms.DialogResult.OK)
            {
                this.outputpath.Text = fbd.SelectedPath;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void GB_Click(object sender, RoutedEventArgs e)
        {
            DateTime stime = starttime.SelectedDate.Value;
            DateTime etime = endtime.SelectedDate.Value;
            string outpath = outputpath.Text;
            SeriseEnginer7.HisDataPath = outpath;
            GB.IsEnabled = false;
            //mTotalCount = (int)((etime.Date.AddDays(1) - stime.Date).TotalMinutes / 5);
            mCurrentCount = 0;
            npb.Maximum = 100;
            npb.Minimum = 0;
            mTagCount = int.Parse(tagcount.Text);

            string stmp = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), DateTime.Now.Ticks+ ".txt");
            StreamWriter writer = new StreamWriter(stmp);
            Console.SetOut(writer);

            Task.Run(() =>
            {
                GeneratorHisData(stime, etime);
                System.Windows.Application.Current.Dispatcher.BeginInvoke(() =>
                {
                    GB.IsEnabled = true;
                    npb.Value = 0;
                    System.Windows.MessageBox.Show("历史数据生成完成!");
                    writer.Close();
                });
            });

        }

        private void NotifyProcessChanged(double ival)
        {
            System.Windows.Application.Current.Dispatcher.BeginInvoke(() => {
                npb.Value = (mCurrentCount/mTotalCount)*100;
                this.pbt.Text = $"{mCurrentCount}/{mTotalCount}";
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="outpath"></param>
        private void GeneratorHisData(DateTime startTime,DateTime endTime)
        {
            DateTime stime = startTime.Date;
            DateTime etime = endTime.Date;
            DateTime stmp = stime;

            mTotalCount = (etime - stime).TotalDays * 24 * 12 * mTagCount;

            while(stmp<=etime)
            {
                GeneratorHisData(stmp);
                stmp = stmp.AddDays(1);
                Thread.Sleep(100);
            }
        }

        private void GeneratorHisData(DateTime startTime)
        {
            for(int i=0;i<24*12;i++)
            {
                List<MarshalMemoryBlock> mBufferDatas = new List<MarshalMemoryBlock>();
                for (int j=0;j< mTagCount; j++)
                GeneratorHisDataBlock(startTime.AddMinutes(i * 5), TagType.Double,j, mBufferDatas);

                mSerise.FreshManualDataToDisk();
                foreach(var vv in mBufferDatas)
                {
                    MarshalMemoryBlockPool.Pool.Release(vv);
                    // vv.Dispose();
                }
                mBufferDatas.Clear();
            }
        }

        private void GeneratorHisDataBlock(DateTime startTime,TagType typ,int id,List<MarshalMemoryBlock> mBufferDatas)
        {
            var css = CalCachDatablockSizeForManualRecord(typ, 0, 5 * 60, out int valueOffset, out int qulityOffset);
            var hb = ManualHisDataMemoryBlockPool.Pool.Get(css);
            hb.Time = startTime;
            hb.RealTime = startTime;
            hb.MaxCount = CachMemoryTime;
            hb.TimeUnit = 1;
            hb.TimeLen = 4;
            hb.TimerAddress = 0;
            hb.ValueAddress = valueOffset;
            hb.QualityAddress = qulityOffset;
            hb.Id = id;
            hb.CurrentCount = 0;

            var datetime = startTime;
            for (int i = 0; i < 300; i++)
            {
                datetime = datetime.AddSeconds(1);
                object value = i;
                var vtime = (int)((datetime - hb.Time).TotalMilliseconds / 1);
                //写入时间戳
                hb.WriteInt(hb.TimerAddress + hb.CurrentCount * 4, vtime);
                 
                switch (typ)
                {
                    case TagType.Bool:
                        hb.WriteByteDirect(hb.ValueAddress + hb.CurrentCount * 1, Convert.ToByte(Convert.ToBoolean(value)));
                        break;
                    case TagType.Byte:
                        hb.WriteByteDirect(hb.ValueAddress + hb.CurrentCount * 1, Convert.ToByte(value));
                        break;
                    case TagType.Short:
                        hb.WriteShortDirect(hb.ValueAddress + hb.CurrentCount * 2, Convert.ToInt16(value));
                        break;
                    case TagType.UShort:
                        hb.WriteUShortDirect(hb.ValueAddress + hb.CurrentCount * 2, Convert.ToUInt16(value));
                        break;
                    case TagType.Int:
                        hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * 4, Convert.ToInt32(value));
                        break;
                    case TagType.UInt:
                        hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * 4, Convert.ToUInt32(value));
                        break;
                    case TagType.Long:
                        hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * 8, Convert.ToInt64(value));
                        break;
                    case TagType.ULong:
                        hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * 8, Convert.ToUInt64(value));
                        break;
                    case TagType.Float:
                        hb.WriteFloatDirect(hb.ValueAddress + hb.CurrentCount * 4, (float)Math.Round(Convert.ToSingle(value), 4));
                        break;
                    case TagType.Double:
                        hb.WriteDoubleDirect(hb.ValueAddress + hb.CurrentCount * 8, Math.Round(Convert.ToDouble(value), 4));
                        break;
                    case TagType.String:
                        hb.WriteStringDirect(hb.ValueAddress + hb.CurrentCount * 256, Convert.ToString(value), Encoding.Unicode);
                        break;
                    case TagType.DateTime:
                        hb.WriteDatetime(hb.ValueAddress + hb.CurrentCount * 8, Convert.ToDateTime(value));
                        break;
                    case TagType.UIntPoint:
                        UIntPointData data = (UIntPointData)(object)value;
                        hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * 8, data.X);
                        hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * 8 + 4, data.Y);
                        break;
                    case TagType.IntPoint:
                        IntPointData idata = (IntPointData)(object)value;
                        hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * 8, idata.X);
                        hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * 8 + 4, idata.Y);
                        break;
                    case TagType.UIntPoint3:
                        UIntPoint3Data udata3 = (UIntPoint3Data)(object)value;
                        hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * 12, udata3.X);
                        hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * 12 + 4, udata3.Y);
                        hb.WriteUIntDirect(hb.ValueAddress + hb.CurrentCount * 12 + 8, udata3.Z);
                        break;
                    case TagType.IntPoint3:
                        IntPoint3Data idata3 = (IntPoint3Data)(object)value;
                        hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * 12, idata3.X);
                        hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * 12 + 4, idata3.Y);
                        hb.WriteIntDirect(hb.ValueAddress + hb.CurrentCount * 12 + 8, idata3.Z);
                        break;

                    case TagType.ULongPoint:
                        ULongPointData udata = (ULongPointData)(object)value;
                        hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * 16, udata.X);
                        hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * 16 + 8, udata.Y);
                        break;
                    case TagType.LongPoint:
                        LongPointData lidata = (LongPointData)(object)value;
                        hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * 16, lidata.X);
                        hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount *16 + 8, lidata.Y);
                        break;
                    case TagType.ULongPoint3:
                        ULongPoint3Data ludata3 = (ULongPoint3Data)(object)value;
                        hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * 24, ludata3.X);
                        hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * 24 + 8, ludata3.Y);
                        hb.WriteULongDirect(hb.ValueAddress + hb.CurrentCount * 24 + 16, ludata3.Z);
                        break;
                    case TagType.LongPoint3:
                        LongPoint3Data lidata3 = (LongPoint3Data)(object)value;
                        hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * 24, lidata3.X);
                        hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * 24 + 8, lidata3.Y);
                        hb.WriteLongDirect(hb.ValueAddress + hb.CurrentCount * 24 + 16, lidata3.Z);
                        break;
                }
                hb.WriteByte(hb.QualityAddress + hb.CurrentCount, 0);
                hb.EndTime = datetime;
                hb.CurrentCount++;
                hb.LastQuality = 0;
                hb.LastValue = value;
            }
           
            var vdata =  mCompress.CompressBlockMemory(hb,1,RecordType.Driver,typ,4,null);

            ManualHisDataMemoryBlockPool.Pool.Release(hb);

            mSerise.AppendManualSeriseFile(0, vdata);

            mSerise.MaxTagId = mTagCount;

            //mSerise.FreshManualDataToDisk();

            //MarshalMemoryBlockPool.Pool.Release(vdata);

            mBufferDatas.Add(vdata);

            mCurrentCount++;
            NotifyProcessChanged(mCurrentCount);
        }

        public int CachMemoryTime = 60 * 5;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagType"></param>
        /// <param name="headSize"></param>
        /// <param name="valueCount"></param>
        /// <param name="dataOffset"></param>
        /// <param name="qulityOffset"></param>
        /// <returns></returns>
        private int CalCachDatablockSizeForManualRecord(Cdy.Tag.TagType tagType, int headSize, int valueCount, out int dataOffset, out int qulityOffset)
        {
            //单个数据块内容包括：时间戳(2)+数值+质量戳(1)

            qulityOffset = headSize;
            int count = Math.Max(CachMemoryTime, valueCount);

            //数据区偏移,时间戳占4个字节,质量戳占1个字节
            dataOffset = headSize + count * 4;
            switch (tagType)
            {
                case Cdy.Tag.TagType.Byte:
                case Cdy.Tag.TagType.Bool:
                    qulityOffset = dataOffset + count;
                    return qulityOffset + count;
                case Cdy.Tag.TagType.Short:
                case Cdy.Tag.TagType.UShort:
                    qulityOffset = dataOffset + count * 2;
                    return qulityOffset + count;
                case Cdy.Tag.TagType.Int:
                case Cdy.Tag.TagType.UInt:
                case Cdy.Tag.TagType.Float:
                    qulityOffset = dataOffset + count * 4;
                    return qulityOffset + count;
                case Cdy.Tag.TagType.Long:
                case Cdy.Tag.TagType.ULong:
                case Cdy.Tag.TagType.Double:
                case Cdy.Tag.TagType.DateTime:
                case Cdy.Tag.TagType.UIntPoint:
                case Cdy.Tag.TagType.IntPoint:
                    qulityOffset = dataOffset + count * 8;
                    return qulityOffset + count;
                case Cdy.Tag.TagType.IntPoint3:
                case Cdy.Tag.TagType.UIntPoint3:
                    qulityOffset = dataOffset + count * 12;
                    return qulityOffset + count;

                case Cdy.Tag.TagType.LongPoint:
                case Cdy.Tag.TagType.ULongPoint:
                    qulityOffset = dataOffset + count * 16;
                    return qulityOffset + count;

                case Cdy.Tag.TagType.LongPoint3:
                case Cdy.Tag.TagType.ULongPoint3:
                    qulityOffset = dataOffset + count * 24;
                    return qulityOffset + count;
                case Cdy.Tag.TagType.String:
                    qulityOffset = dataOffset + count * Cdy.Tag.Const.StringSize;
                    return qulityOffset + count;
                default:
                    return 0;
            }
        }
    }
}
