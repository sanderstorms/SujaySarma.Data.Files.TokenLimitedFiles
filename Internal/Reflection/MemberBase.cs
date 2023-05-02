using SujaySarma.Data.Files.TokenLimitedFiles.Attributes;

using System.Reflection;
using System;

namespace Internal.Reflection
{
    /// <summary>
    /// Base class implementing functionality common to class fields and properties
    /// </summary>
    internal class MemberBase
    {

        /// <summary>
        /// Name of the member
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Flag indicating if the member can be written (has a Setter)
        /// </summary>
        public bool CanWrite { get; private set; } = true;

        /// <summary>
        /// Data Type of the member
        /// </summary>
        public Type Type { get; private set; } = typeof(object);

        /// <summary>
        /// If true, the CLR type allows NULLs
        /// </summary>
        public bool IsNullableType { get; private set; } = true;

        /// <summary>
        /// If mapped to a data entity's fields, reference to the FileFieldAttribute for that.
        /// </summary>
        public FileFieldAttribute EntityColumn { get; set; } = default!;

        /// <summary>
        /// Discovered mapping to data column
        /// </summary>
        public System.Data.DataColumn? DataColumn { get; set; } = default;


        /// <summary>
        /// Initialize the structure
        /// </summary>
        /// <param name="property">The property</param>
        /// <param name="attribute">The FileFieldAttribute</param>
        protected MemberBase(System.Reflection.PropertyInfo property, FileFieldAttribute attribute)
        {
            Name = property.Name;
            CanWrite = property.CanWrite;
            CommonInit(property.PropertyType, attribute);
        }

        /// <summary>
        /// Initialize the structure
        /// </summary>
        /// <param name="field">The field</param>
        /// <param name="attribute">The FileFieldAttribute</param>
        protected MemberBase(System.Reflection.FieldInfo field, FileFieldAttribute attribute)
        {
            Name = field.Name;
            CanWrite = (!field.IsInitOnly);
            CommonInit(field.FieldType, attribute);
        }

        /// <summary>
        /// Common initialization
        /// </summary>
        /// <param name="dataType">Type of property/field</param>
        /// <param name="attribute">The FileFieldAttribute</param>
        private void CommonInit(Type dataType, FileFieldAttribute attribute)
        {
            Type = dataType;
            EntityColumn = attribute;

            Type? underlyingType = Nullable.GetUnderlyingType(Type);
            IsNullableType = (dataType == typeof(string)) || (underlyingType == typeof(string)) || (underlyingType != null);
        }

        /// <summary>
        /// Read the value from the property/field and return it
        /// </summary>
        /// <typeparam name="ObjType">Data type of the object to read the property/field of</typeparam>
        /// <param name="obj">The object instance to read the value from</param>
        /// <returns>The value</returns>
        public virtual object? Read<ObjType>(ObjType obj) => null;

        /// <summary>
        /// Writes the provided value to the property/field of the object instance
        /// </summary>
        /// <typeparam name="ObjType">Data type of the object to read the property/field of</typeparam>
        /// <param name="obj">The object instance to write the value to</param>
        /// <param name="value">Value to write out</param>
        public virtual void Write<ObjType>(ObjType obj, object? value) { }


        /// <summary>
        /// Binding flags for read and write of properties/fields
        /// </summary>
        protected readonly BindingFlags FLAGS_READ_WRITE = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;
    }
}
