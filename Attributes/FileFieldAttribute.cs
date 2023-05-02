using System;

namespace SujaySarma.Data.Files.TokenLimitedFiles.Attributes
{
    /// <summary>
    /// Provide metadata about the field/column in the delimited file
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = true)]
    public sealed class FileFieldAttribute : Attribute
    {

        #region Properties

        /// <summary>
        /// Index of the field in the delimited file's record. If file has headers, you may provide the Name instead of the index. Valid indexes start from zero.
        /// </summary>
        public int Index { get; private set; } = -1;

        /// <summary>
        /// Name of the field in the delimited file's record. If file has headers, and the Name is provided, it must exist in the file -- 
        /// otherwise, exception will be thrown.
        /// </summary>
        public string? Name { get; private set; } = null;

        /// <summary>
        /// Value to set into the class property/field when the file's column is NULL. Default is NULL. If the data type is not nullable, 
        /// we use the default for the data-type.
        /// </summary>
        public object? NullValue { get; set; } = null;

        /// <summary>
        /// (Only on WRITE) If the underlying type is an Enum and this property is TRUE, we write out the integer-equivalent value instead of the name.
        /// </summary>
        public bool UseEnumIntegerValue { get; set; } = false;

        /// <summary>
        /// True values for boolean types, case insensitive (default set: "true", "yes", "1")
        /// </summary>
        public string[] BooleanTrues { get; set; } = new string[] { "true", "yes", "1" };

        /// <summary>
        /// False values for boolean types, case insensitive (default set: "false", "no", "0")
        /// </summary>
        public string[] BooleanFalses { get; set; } = new string[] { "false", "no", "0" };

        #endregion



        /// <summary>
        /// Provide metadata about the field/column in the delimited file
        /// </summary>
        /// <param name="index">Index of field (zero-based)</param>
        public FileFieldAttribute(int index) : this(index, null) { }

        /// <summary>
        /// Provide metadata about the field/column in the delimited file
        /// </summary>
        /// <param name="name">Name of field (in the header)</param>
        public FileFieldAttribute(string? name) : this(-1, name) { }

        /// <summary>
        /// Provide metadata about the field/column in the delimited file
        /// </summary>
        /// <param name="index">Index of field (zero-based)</param>
        /// <param name="name">Name of field (in the header). Set to NULL or empty string if we do not have headers in the file</param>
        public FileFieldAttribute(int index, string? name) : this(index, name, null) { }

        /// <summary>
        /// Provide metadata about the field/column in the delimited file
        /// </summary>
        /// <param name="index">Index of field (zero-based)</param>
        /// <param name="name">Name of field (in the header). Set to NULL or empty string if we do not have headers in the file</param>
        /// <param name="nullValue">Value to set into the class property/field when the file's column is NULL. Default is NULL</param>
        public FileFieldAttribute(int index, string? name, object? nullValue)
        {
            if ((index > -1) && string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            Index = index;
            Name = (string.IsNullOrWhiteSpace(name) ? null : name);
            NullValue = nullValue;
        }

    }
}
