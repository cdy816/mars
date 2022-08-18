using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBInStudio.Desktop.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class RegistorEditTypeManager
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static RegistorEditTypeManager Instance = new RegistorEditTypeManager();
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...
        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            Registor(new ScriptEditViewModel());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        public void Registor(IRegistorEditType type)
        {
            ServiceLocator.Locator.Registor(type.Name + "Command",new Func<string, string>((exp) => {
                return type.Edit(exp);
            }));
        }
        
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public interface IRegistorEditType
    {
        string Name { get;}
        /// <summary>
        /// 
        /// </summary>
        /// <param name="exp"></param>
        /// <returns></returns>
        string Edit(string exp);
    }

    /// <summary>
    /// 
    /// </summary>
    public class ScriptEditViewModel : WindowViewModelBase, IRegistorEditType
    {

        public ScriptEditViewModel()
        {
            DefaultWidth = 400;
            DefaultHeight = 200;
            Title = Res.Get("Express");
        }

        private string mExpress = "";

        /// <summary>
        /// 
        /// </summary>
        public string Name { get => "Script";  }

        /// <summary>
            /// 
            /// </summary>
        public string Express
        {
            get
            {
                return mExpress;
            }
            set
            {
                if (mExpress != value)
                {
                    mExpress = value;
                    OnPropertyChanged("Express");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="exp"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public string Edit(string exp)
        {
            ScriptEditViewModel mm = new ScriptEditViewModel();
            mm.Express = exp;
            if(mm.ShowDialog().Value)
            {
                return mm.Express;
            }
            else
            {
                return exp;
            }
        }
    }

}
