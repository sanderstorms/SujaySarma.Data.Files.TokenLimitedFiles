using System;
using System.ComponentModel;
using System.Reflection;

namespace Internal.Reflection
{
    internal class ReflectionUtils
    {

        /// <summary>
        /// Returns a value that matches the destination type
        /// </summary>
        /// <param name="sourceType">Type of value being provided</param>
        /// <param name="destinationType">Type of the destination container</param>
        /// <param name="value">Value to convert/change</param>
        /// <returns>The value of type destinationType</returns>
        public static object? GetAcceptableValue(Type sourceType, Type destinationType, object? value)
        {
            if ((value == null) || (value == DBNull.Value))
            {
                return null;
            }

            Type convertFromType = Nullable.GetUnderlyingType(sourceType) ?? sourceType;
            Type convertToType = Nullable.GetUnderlyingType(destinationType) ?? destinationType;

            if (!convertFromType.FullName!.Equals(convertToType.FullName, StringComparison.Ordinal))
            {
                return ConvertTo(convertFromType, convertToType, value);
            }

            return value;
        }

        /// <summary>
        /// Convert between types
        /// </summary>
        /// <param name="sourceType">CLR type of source</param>
        /// <param name="destinationType">CLR Type of destination</param>
        /// <param name="value">The value to convert</param>
        /// <returns>The converted value</returns>
        public static object? ConvertTo(Type sourceType, Type destinationType, object value)
        {
            //NOTE: value is not null -- already been checked by caller before calling here

            if (destinationType.IsEnum && (value is string val))
            {
                // Input is a string, destination is an Enum, Enum.Parse() it to convert!
                // We are using Parse() and not TryParse() with good reason. Bad values will throw exceptions to the top-level caller 
                // and we WANT that to happen! -- not only that, TryParse requires an extra typed storage that we do not want to provide here!

                return Enum.Parse(destinationType, val);
            }

            if (sourceType.IsEnum && (destinationType == typeof(string)))
            {
                return Enum.GetName(sourceType, value);
            }

            TypeConverter converter = TypeDescriptor.GetConverter(destinationType);
            if ((converter != null) && (converter.CanConvertTo(destinationType)))
            {
                return converter.ConvertTo(value, destinationType);
            }

            // see if type has a Parse static method

            MethodInfo? parser = destinationType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, new Type[] { sourceType });
            if (parser != null)
            {
                return parser.Invoke(null, new object?[] { value });
            }

            parser = destinationType.GetMethod("TryParse", BindingFlags.Public | BindingFlags.Static, new Type[] { sourceType });
            if (parser != null)
            {
                object?[]? parameters = new object?[] { value, null };
                bool? tpResult = (bool?)parser.Invoke(null, parameters);
                return ((tpResult.HasValue && tpResult.Value) ? parameters[1] : default);
            }

            throw new TypeLoadException($"Could not find type converters for '{destinationType.Name}' type.");
        }
       

    }
}
