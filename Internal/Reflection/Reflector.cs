using SujaySarma.Data.Files.TokenLimitedFiles.Attributes;

using System;
using System.Collections.Generic;
using System.Reflection;

namespace Internal.Reflection
{
    internal class Reflector
    {
        /// <summary>
        /// Inspect a class for Azure Tables
        /// </summary>
        /// <typeparam name="ClassT">Type of business class to be stored into Azure Tables</typeparam>
        /// <returns>Reflected class metadata</returns>
        public static Class? InspectForFileFieldAttributes<ClassT>()
            where ClassT : class
        => InspectForFileFieldAttributes(typeof(ClassT));


        /// <summary>
        /// Inspect a class for Azure Tables
        /// </summary>
        /// <param name="classType">Type of business class to be stored into Azure Tables</param>
        /// <returns>Reflected class metadata</returns>
        public static Class? InspectForFileFieldAttributes(Type classType)
        {
            string cacheKeyName = classType.FullName ?? classType.Name;
            Class? objectMetadata = ReflectionCache.TryGet(cacheKeyName);
            if (objectMetadata != null)
            {
                // cache hit
                return objectMetadata;
            }

            List<MemberProperty> properties = new();
            List<MemberField> fields = new();

            foreach (MemberInfo member in classType.GetMembers(MEMBER_SEARCH_FLAGS))
            {
                foreach(Attribute attribute in member.GetCustomAttributes<FileFieldAttribute>(true))
                {
                    if (attribute is FileFieldAttribute ffa)
                    {
                        switch (member.MemberType)
                        {
                            case MemberTypes.Field:
                                FieldInfo? fi = member as FieldInfo;
                                if (fi != null)
                                {
                                    fields.Add(new MemberField(fi, ffa));
                                }
                                break;

                            case MemberTypes.Property:
                                PropertyInfo? pi = member as PropertyInfo;
                                if (pi != null)
                                {
                                    properties.Add(new MemberProperty(pi, ffa));
                                }
                                break;
                        }
                    }
                }
            }

            if ((properties.Count == 0) && (fields.Count == 0))
            {
                return null;
            }

            objectMetadata = new Class(classType.Name, properties, fields);
            ReflectionCache.TrySet(objectMetadata, cacheKeyName);
            return objectMetadata;
        }

        private static readonly BindingFlags MEMBER_SEARCH_FLAGS = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;
    }
}
