using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class ArrayList<T>:IDisposable, IEnumerable<T>, IEnumerable
    {
        private T[] array;
        
        private int mIndex;

        private int mcapital;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="capital"></param>
        public ArrayList(int capital)
        {
            array = ArrayPool<T>.Shared.Rent(capital);
            array.AsSpan<T>().Fill(default(T));
            mcapital =capital;
        }

        /// <summary>
        /// 
        /// </summary>
        public ArrayList():this(64)
        {

        }

        private int mCountInner = 0;

        private object mLocker = new object();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Add(T value)
        {
            lock (mLocker)
            {
                if (mIndex < array.Length)
                {
                    array[mIndex++] = value;
                }
                else
                {
                    var aar2 = ArrayPool<T>.Shared.Rent(array.Length * 2);
                    aar2.AsSpan<T>().Fill(default(T));
                    array.CopyTo(aar2, 0);
                    ArrayPool<T>.Shared.Return(array);
                    array = aar2;
                    array[mIndex++] = value;
                }
            }
            mCountInner++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void AddRange(IEnumerable<T> values)
        {
            foreach(var vv in values)
            {
                Add(vv);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void Remove(IEnumerable<T> values)
        {
            var array2 = ArrayPool<T>.Shared.Rent(mcapital);
            List<T> ll = new List<T>(this.array.Take(mIndex));
            foreach(var vv in values)
            {
                if(ll.Contains(vv))
                ll.Remove(vv);
            }
            ll.CopyTo(array2);
            this.mIndex = ll.Count;
            array = array2;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public T this[int index]
        {
            get
            {
                if(index<this.mIndex)
                return array[index];
                else
                {
                    return default(T);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int Count
        {
            get
            {
                return mIndex;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Clear()
        {
            array.AsSpan().Clear();
            mIndex = 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<T> List()
        {
            for(int i=0;i< mIndex; i++)
            {
                yield return array[i];
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            ArrayPool<T>.Shared.Return(array);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IEnumerator<T> GetEnumerator()
        {
            return new Enumerator(this);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return new Enumerator(this);
        }

        /// <summary>
        /// 
        /// </summary>
        public struct Enumerator : IEnumerator<T>, IDisposable, IEnumerator
        {
            private ArrayList<T> list;
            private T currentElement;
            private int mIndex;
            internal Enumerator(ArrayList<T> stack)
            {
                list = stack;
                mIndex = 0;
                if(list.Count>0)
                this.currentElement = list[0];
                else
                {
                    this.currentElement = default(T);
                }
            }

            /// <summary>
            /// 
            /// </summary>
            public object Current => currentElement;

            /// <summary>
            /// 
            /// </summary>
            T IEnumerator<T>.Current => currentElement;

            /// <summary>
            /// 
            /// </summary>
            public void Dispose()
            {
                list = null;
            }

            /// <summary>
            /// 
            /// </summary>
            /// <returns></returns>
            public bool MoveNext()
            {
                if(mIndex < list.mIndex)
                {
                    currentElement = list[mIndex];
                    mIndex++;
                    return true;
                }
                else
                {
                    return false;
                }
            }

            /// <summary>
            /// /
            /// </summary>
            public void Reset()
            {
                mIndex = 0;
                if (list.Count > 0)
                    this.currentElement = list[0];
                else
                {
                    this.currentElement = default(T);
                }
            }
        }
        
    }
}
