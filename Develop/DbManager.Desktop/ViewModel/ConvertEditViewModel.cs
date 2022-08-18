using Cdy.Tag;
using Google.Protobuf.Reflection;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;

namespace DBInStudio.Desktop.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class ConvertEditViewModel: WindowViewModelBase
    {

        private ConvertViewModel mCurrentSelectModel;

        /// <summary>
        /// 
        /// </summary>
        private System.Collections.ObjectModel.Collection<ConvertViewModel> mItems = new System.Collections.ObjectModel.Collection<ConvertViewModel>();

        /// <summary>
        /// 
        /// </summary>
        static ConvertEditViewModel()
        {
            ValueConvertManager.manager.Registor(new LinerConvert());
            ValueConvertManager.manager.Registor(new BitConvert());
            ValueConvertManager.manager.Registor(new StringFormatConvert());
        }

        /// <summary>
        /// 
        /// </summary>
        public ConvertEditViewModel()
        {
            DefaultWidth = 400;
            DefaultHeight = 200;
        }

        /// <summary>
        /// 
        /// </summary>
        public System.Collections.ObjectModel.Collection<ConvertViewModel> Items
        {
            get
            {
                return mItems;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ConvertViewModel CurrentSelectModel
        {
            get
            {
                return mCurrentSelectModel;
            }
            set
            {
                mCurrentSelectModel = value;
                OnPropertyChanged("CurrentSelectModel");
            }
        }

      

        /// <summary>
        /// 
        /// </summary>
        public void Init(Cdy.Tag.Tagbase tag)
        {
            mItems.Add(new LinearConvertViewModel() { Model = new LinerConvert() });
            mItems.Add(new BitConvertViewModel() { Model = new BitConvert() });
            mItems.Add(new StringFormatConvertViewModel() { Model = new StringFormatConvert() });

            foreach(var vv in mItems.ToArray())
            {
                if(!vv.Model.SupportTag(tag))
                {
                    mItems.Remove(vv);
                }
            }

            if(mItems.Count>0)
            CurrentSelectModel = mItems.First();
        }

        public void SetSelectConvert(string cstring)
        {
            var cc = cstring.DeSeriseToValueConvert();
            if (cc == null) return;

            foreach(var vv in Items)
            {
                if(vv.Name == cc.Name)
                {
                    CurrentSelectModel = vv;
                    CurrentSelectModel.Model = cc;
                }
            }
        }

    }

    /// <summary>
    /// 
    /// </summary>
    public class ConvertViewModel : ViewModelBase
    {
        /// <summary>
        /// 
        /// </summary>
        public string Name { get { return Model.Name; } }

        public string DisplayName { get { return Res.Get(Name); } }

        /// <summary>
        /// 
        /// </summary>
        public IValueConvert  Model { get; set; }

    }

    /// <summary>
    /// 
    /// </summary>
    public class LinearConvertViewModel : ConvertViewModel
    {
        /// <summary>
        /// 
        /// </summary>
        public double K
        {
            get
            {
                return (Model as LinerConvert).K;
            }
            set
            {
                var mm = Model as LinerConvert;
                if(mm.K!=value)
                {
                    mm.K = value;
                    OnPropertyChanged("K");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public double T
        {
            get
            {
                return (Model as LinerConvert).T;
            }
            set
            {
                var mm = Model as LinerConvert;
                if (mm.T != value)
                {
                    mm.T = value;
                    OnPropertyChanged("T");
                }
            }
        }
    }

    public class BitConvertViewModel : ConvertViewModel
    {
        /// <summary>
        /// 
        /// </summary>
        public byte Index
        {
            get
            {
                return (Model as BitConvert).Index;
            }
            set
            {
                var mm = Model as BitConvert;
                if (mm.Index != value)
                {
                    mm.Index = value;
                    OnPropertyChanged("Index");
                }
            }
        }

    }

    public class StringFormatConvertViewModel : ConvertViewModel
    {
        /// <summary>
        /// 
        /// </summary>
        public string Format
        {
            get
            {
                return (Model as StringFormatConvert).Format;
            }
            set
            {
                var mm = Model as StringFormatConvert;
                if (mm.Format != value)
                {
                    mm.Format = value;
                    OnPropertyChanged("Format");
                }
            }
        }

    }
}
