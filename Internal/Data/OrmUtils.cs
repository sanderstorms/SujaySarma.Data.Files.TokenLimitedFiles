using Internal.Reflection;

using SujaySarma.Data.Files.TokenLimitedFiles.Attributes;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Internal.Data
{
    /// <summary>
    /// Utilities for converting tables to objects
    /// </summary>
    internal static class OrmUtils
    {

        /// <summary>
        /// Convert a list of object to a data table
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="list">List containing data</param>
        /// <returns>DataTable of items</returns>
        public static DataTable FromList<T>(List<T> list)
            where T : class, new()
        {
            Class? info = Reflector.InspectForFileFieldAttributes<T>();
            if (info == null)
            {
                throw new InvalidCastException($"The type '{typeof(T).FullName}' is not decorated with the '{nameof(FileFieldAttribute)}' attribute.");
            }

            DataTable table = new("Table 1");
            int colIndex = 0;

            foreach (MemberBase member in info.Members)
            {
                DataColumn? col = default;
                if (member.EntityColumn.Name != null)
                {
                    col = new DataColumn(member.EntityColumn.Name, member.Type);
                }
                else
                {
                    if ((member.EntityColumn.Index >= 0) && (member.EntityColumn.Index < table.Columns.Count))
                    {
                        col = new DataColumn($"Column {++colIndex}", member.Type);
                    }
                }

                if (col != default)
                {
                    member.DataColumn = col;
                    table.Columns.Add(col);
                    if (member.EntityColumn.Index >= 0)
                    {
                        col.SetOrdinal(member.EntityColumn.Index);
                    }
                }
            }

            foreach (T item in list)
            {
                if (item == default)
                {
                    continue;
                }

                DataRow row = table.NewRow();

                foreach (MemberField member in info.Fields)
                {
                    if (member.DataColumn != default)
                    {
                        row[member.DataColumn] = member.Read(item);
                    }
                }

                foreach (MemberProperty member in info.Properties)
                {
                    if (member.DataColumn != default)
                    {
                        row[member.DataColumn] = member.Read(item);
                    }
                }
                table.Rows.Add(row);
            }

            return table;
        }


        /// <summary>
        /// Convert a the table to a list of objects
        /// </summary>
        /// <typeparam name="T">Type of object</typeparam>
        /// <param name="table">Table containing data</param>
        /// <returns>List of objects</returns>
        /// <exception cref="InvalidCastException">Thrown if no members of the object 'T' is not decorated with the <see cref="FileFieldAttribute"/> attribute.</exception>
        public static List<T> ToList<T>(this DataTable table)
            where T : class, new()
        {
            Class? classInfo = Reflector.InspectForFileFieldAttributes<T>();
            if (classInfo == null)
            {
                throw new InvalidCastException($"The type '{typeof(T).FullName}' is not decorated with the '{nameof(FileFieldAttribute)}' attribute.");
            }

            DiscoverColumnMappings(classInfo, table);

            List<T> list = new(table.Rows.Count);
            for (int index = 0; index < table.Rows.Count; index++)
            {
                try
                {
                    list.Add(Instantiate<T>(table, index, classInfo));
                }
                catch (Exception msg)
                {
                    Console.Error.WriteLine($"Error parsing row# {index + 1} of table: {msg.InnerException?.Message ?? msg.Message}");
                }
            }

            return list;
        }

        /// <summary>
        /// Convert a the table to a IEnumerable of objects
        /// </summary>
        /// <typeparam name="T">Type of object</typeparam>
        /// <param name="table">Table containing data</param>
        /// <returns>IEnumerable of objects</returns>
        /// <exception cref="InvalidCastException">Thrown if no members of the object 'T' is not decorated with the <see cref="FileFieldAttribute"/> attribute.</exception>
        public static IEnumerable<T> ToEnumerable<T>(this DataTable table)
            where T : class, new()
        {
            Class? classInfo = Reflector.InspectForFileFieldAttributes<T>();
            if (classInfo == null)
            {
                throw new InvalidCastException($"The type '{typeof(T).FullName}' is not decorated with the '{nameof(FileFieldAttribute)}' attribute.");
            }

            DiscoverColumnMappings(classInfo, table);

            for (int index = 0; index < table.Rows.Count; index++)
            {
                yield return Instantiate<T>(table, index, classInfo);
            }
        }


        public static T Instantiate<T>(DataTable table, int rowIndex, Class info)
            where T : class, new()
        {
            T obj = new();
#pragma warning disable IDE0059 // Unnecessary assignment of a value
            object? val = default;
#pragma warning restore IDE0059 // Unnecessary assignment of a value

            foreach (MemberBase member in info.Members)
            {
                if (member.DataColumn != default)
                {
                    val = table.Rows[rowIndex][member.DataColumn];
                    if ((val == default) || (val == null) || (val == DBNull.Value))
                    {
                        member.Write(obj, default);
                    }
                    else
                    {
                        member.Write(obj, ReflectionUtils.GetAcceptableValue(member.DataColumn.DataType, member.Type, val));
                    }
                }
            }

            return obj;
        }


        public static void DiscoverColumnMappings(Class info, DataTable table)
        {
            foreach (MemberBase member in info.Members)
            {
                if (member.EntityColumn.Name != null)
                {
                    member.DataColumn = table.Columns[member.EntityColumn.Name];
                }
                else
                {
                    if ((member.EntityColumn.Index >= 0) && (member.EntityColumn.Index < table.Columns.Count))
                    {
                        member.DataColumn = table.Columns[member.EntityColumn.Index];
                    }
                }
            }
        }

    }
}
