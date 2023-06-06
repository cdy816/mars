using System.ComponentModel;

namespace DBWebMonitor
{
    public interface ISelectable
    {
        public bool IsSelected { get; set; }

        public event PropertyChangedEventHandler PropertyChanged;
    }
}
