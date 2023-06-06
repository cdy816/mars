using System.ComponentModel;

namespace DBWebStudio.Data
{
    public interface ISelectable
    {
        public bool IsSelected { get; set; }

        public event PropertyChangedEventHandler PropertyChanged;
    }
}
