using System.Collections.Generic;

namespace Internal.Reflection
{
    /// <summary>
    /// A classes inspected and cached by hte Type inspection engine
    /// </summary>
    internal static class ReflectionCache
    {
        /// <summary>
        /// Add an item to cache if it is not already in it
        /// </summary>
        /// <param name="info">Item to add</param>
        /// <param name="keyName">The key name of the object</param>
        public static void TrySet(Class info, string keyName)
        {
            lock (_lock)
            {
                if (!_cache.ContainsKey(keyName))
                {
                    _cache.Add(keyName, info);
                }
            }
        }

        /// <summary>
        /// Fetch an item from cache
        /// </summary>
        /// <param name="keyName">Key name of object</param>
        /// <returns>Cached information or NULL</returns>
        public static Class? TryGet(string keyName)
        {
            if (!_cache.TryGetValue(keyName, out Class? info))
            {
                return null;
            }

            return info;
        }

        private static readonly Dictionary<string, Class> _cache = new();
        private static readonly object _lock = new();
    }
}
