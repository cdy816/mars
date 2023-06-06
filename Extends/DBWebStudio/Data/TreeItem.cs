using Cdy.Tag;

namespace DBWebStudio.Data
{
    public class TreeItem:IDisposable
    {
        public event Action<bool> SelectedChangedEvent;
        private bool mIsSelected = false;
        private Action mClick;
        private Action mAddAction;
        private Action mRemoveAction;
        private Action mReNameAction;
        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Url { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Icon { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public  bool IsSelected { get { return mIsSelected; } set 
            {
                if (mIsSelected != value)
                {
                    mIsSelected = value;
                    SelectChangedAction?.Invoke(this, value);
                    SelectedChangedEvent?.Invoke(value);
                }
               
            } }

        /// <summary>
        /// 
        /// </summary>
        public virtual bool CanAdd { get { return mAddAction!=null; } }

        /// <summary>
        /// 
        /// </summary>
        public virtual bool CanRemove { get { return mRemoveAction!=null;} }

        public virtual bool CanReName { get { return mReNameAction != null; } }

        /// <summary>
        /// 
        /// </summary>
        public TreeItem Parent { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Action Click
        {
            get
            {
                if(mClick==null)
                {
                    mClick = () => {
                        IsSelected = true;
                        if(ClickAction!=null)
                        {
                            ClickAction(this);
                        }
                        ItemClick();
                    };
                }
                return mClick;
            }
        }

        protected virtual void ItemClick()
        {

        }

        public virtual void Dispose()
        {
            mClick = null;
            mRemoveAction = null;
            mAddAction = null;
            ClickAction = null;
            Parent = null;
        }

        public Action<TreeItem> ClickAction { get; set; }

        public Action<TreeItem,bool> SelectChangedAction { get; set; }

        public Action AddAction { get { return mAddAction; } set { mAddAction = value; } }

        public Action RemoveAction { get { return mRemoveAction; } set { mRemoveAction = value; } }

        public Action ReNameAction { get { return mReNameAction; } set { mReNameAction = value; } }
    }

    public class TreeItemCollection:TreeItem
    {
        private bool mIsExpanded = false;
        public List<TreeItem> Items { get; set; }=new List<TreeItem>();

        public bool IsExpanded { get { return mIsExpanded; } set { mIsExpanded = value; OnIsExpanded(value); } }

      

        /// <summary>
        /// 
        /// </summary>
        /// <param name="item"></param>
        public void Add(TreeItem item)
        {
            this.Items.Add(item);
            item.Parent = this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="item"></param>
        public void Remove(TreeItem item)
        {
            if(Items.Contains(item))
            {
                Items.Remove(item);
            }
            item.Dispose();
        }

        /// <summary>
        /// 
        /// </summary>
        protected override void ItemClick()
        {
            if (!IsExpanded)
                IsExpanded = true;
        }

        protected virtual void OnIsExpanded(bool isExpanded)
        {

        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class RootTagGroupItem : TagGroupItem
    { }


    public class TagGroupItem : TreeItemCollection
    {
        public MarsProxy Proxy { get; set; }

        public string FullName => (Parent != null && !(Parent is RootTagGroupItem)) ? (Parent as TagGroupItem).FullName + "." + Name : Name;

        /// <summary>
        /// 描述
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="groups"></param>
        public void InitData(List<Tuple<string, string, string>> groups,Action addaction,Action renameaction, Action<TreeItem, bool> selaction)
        {
            foreach (var vv in groups.Where(e => e.Item3 == this.FullName))
            {
                string sname = vv.Item1;
                if (!string.IsNullOrEmpty(vv.Item3))
                {
                    sname = sname.Substring(sname.IndexOf(vv.Item3) + vv.Item3.Length + 1);
                }
                TagGroupItem groupViewModel = new TagGroupItem() { Name = sname, Description = vv.Item2,SelectChangedAction=selaction,AddAction=addaction,ReNameAction=renameaction };
                this.Add(groupViewModel);
                groupViewModel.InitData(groups,addaction,renameaction,selaction);
            }
        }

        public override void Dispose()
        {
            Proxy = null;

            base.Dispose();
        }

    }

    /// <summary>
    /// 
    /// </summary>
    public class ComplexTagClassItem:TreeItem
    {
        public string Description { get; set; }
    }

}
