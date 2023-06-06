namespace DBWebStudio
{
    /// <summary>
    /// 
    /// </summary>
    public class NotifyManager
    {
        /// <summary>
        /// 
        /// </summary>
        public Action<double> ValueChanged { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Action Finished { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Notify(double value)
        {
            ValueChanged?.Invoke(value);
        }

        public void NotifyFinished()
        {
            Finished?.Invoke();
        }
    }
}
