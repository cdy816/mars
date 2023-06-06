using System.ComponentModel;
using System.Text;

namespace DBWebStudio.Data
{
    public class UserPermissionModel : ISelectable
    {
        private Cdy.Tag.UserPermission mModel;
        public UserPermissionModel(Cdy.Tag.UserPermission model)
        {
            mModel = model;
        }

        public Cdy.Tag.UserPermission Model { get { return mModel; } set { mModel = value; } }

        public string GroupString
        {
            get
            {
                return mModel.Group != null ? GroupToString() : "";
            }
            set
            {
                if(mModel!=null)
                {
                    if(string.IsNullOrEmpty(value))
                    {
                        mModel.Group = new List<string>();
                    }
                    else
                    {
                        mModel.Group = value.Split(',').ToList();
                    }
                }
            }
        }

        private string GroupToString()
        {
           StringBuilder sb = new StringBuilder();
            foreach (var item in mModel.Group)
            {
                sb.Append(item.ToString()+",");
            }
            sb.Length = sb.Length > 0 ? sb.Length - 1 : sb.Length;
            return sb.ToString();
        }
        private bool mIsSelected = false;
        public bool IsSelected { get => mIsSelected; set{ mIsSelected = value; PropertyChanged(this, new PropertyChangedEventArgs("IsSelected")); } }

        public event PropertyChangedEventHandler PropertyChanged;
    }
}
