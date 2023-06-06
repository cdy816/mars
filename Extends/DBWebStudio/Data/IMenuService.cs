namespace DBWebStudio
{
    public interface IMenuService
    {
        void RefreshMenu();
    }

    public class MenuServiceManager
    {
        public IMenuService Service { get; set; }

        public void RefreshMenu()
        {
            Service?.RefreshMenu();
        }
    }
}
