using SujaySarma.Data.Files.TokenLimitedFiles.Attributes;

using System;
using System.Reflection;

namespace Internal.Reflection
{
    /// <summary>
    /// Contains metadata about a discovered interesting class Field
    /// </summary>
    internal sealed class MemberField : MemberBase
    {

        /// <summary>
        /// There are no Get/set methods for the field. Reflection performs dirty unsafe tricks to 
        /// read and write values, using internal-only methods. The best way not to corrupt the heap/stack is 
        /// to cache the FieldInfo.
        /// </summary>
        private readonly FieldInfo _field;

        /// <summary>
        /// Initialize the structure for a field
        /// </summary>
        /// <param name="field">Field to import</param>
        /// <param name="attribute">The FileFieldAttribute</param>
        public MemberField(FieldInfo field, FileFieldAttribute attribute) : base(field, attribute) => _field = field;

        /// <summary>
        /// Read the value from the field and return it
        /// </summary>
        /// <typeparam name="ObjType">Data type of the object to read the field of</typeparam>
        /// <param name="obj">The object instance to read the value from</param>
        /// <returns>The value</returns>
        public override object? Read<ObjType>(ObjType obj)
        {
            if (obj == null)
            {
                throw new ArgumentNullException(nameof(obj));
            }

            return _field.GetValue(obj);
        }

        /// <summary>
        /// Writes the provided value to the object instance
        /// </summary>
        /// <typeparam name="ObjType">Data type of the object to read the property or field of</typeparam>
        /// <param name="obj">The object instance to write the value to</param>
        /// <param name="value">Value to write out</param>
        public override void Write<ObjType>(ObjType obj, object? value)
        {
            if (obj == null)
            {
                throw new ArgumentNullException(nameof(obj));
            }

            if (!CanWrite)
            {
                throw new InvalidOperationException($"Field '{Name}' in '{obj.GetType().FullName}' can only be instantiated in a class constructor.");
            }

            if ((value == null) && (!IsNullableType))
            {
                // Trying to set NULL into a non-Nullable type
                return;
            }

            _field.SetValue(obj, value);
        }

    }
}
