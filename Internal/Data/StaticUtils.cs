using System;
using System.IO;
using System.Text;

namespace Internal.Data
{
    /// <summary>
    /// Misc static functions and utilities
    /// </summary>
    internal static class StaticUtils
    {

        /// <summary>
        /// Parse a delimited string and return an array of strings
        /// </summary>
        /// <param name="s">[ByRef] String to parse</param>
        /// <param name="delimiter">Delimiter, defaults to comma (',')</param>
        /// <returns>Array of nullable strings, can be NULL as well if <paramref name="s"/> is NULL)</returns>
        public static string?[]? ParseDelimitedString(ref string? s, char delimiter = ',')
        {
            if (string.IsNullOrWhiteSpace(s))
            {
                return null;
            }

            StringBuilder builder = new();

            string?[] rowFields = new string[24];
            int fieldCount = -1, quoteCount = 0, r;
            bool considerBuilderBuffer;
            char c;

            using StringReader reader = new(s);

            do
            {
                considerBuilderBuffer = true;

                r = reader.Read();
                if (r == -1)
                {
                    break;
                }

                c = (char)r;
                if (c == '"')
                {
                    if (reader.Peek() == '"')
                    {
                        // escaped quote. Add it to the element and skip over the second one
                        builder.Append('"');
                        reader.Read();

                        quoteCount++;
                    }

                    quoteCount++;
                    continue;
                }

                if (c == delimiter)
                {
                    if ((quoteCount % 2) != 0)
                    {
                        // delimiter is not a delimiter in this context, add it to the current element
                        builder.Append(c);
                        continue;
                    }

                    flushBuilder();
                    continue;
                }
                else if ((c == '\r') || (c == '\n'))
                {
                    if ((c == '\r') && (reader.Peek() == '\n'))
                    {
                        reader.Read();
                    }

                    if ((quoteCount > 0) && ((quoteCount % 2) == 0))
                    {
                        flushBuilder();

                        // only break out if we got a non-empty row
                        if (fieldCount > -1)
                        {
                            break;
                        }
                    }

                    continue;
                }

                builder.Append(c);
            } while (r > -1);

            flushBuilder();
            if (rowFields.Length > fieldCount)
            {
                // we know exactly how much is left now
                Array.Resize(ref rowFields, fieldCount + 1);
            }

            return rowFields;


            // helper function to flush the 'builder' stringbuilder buffer 
            // and create a new element to the stream
            void flushBuilder()
            {
                if (!considerBuilderBuffer)
                {
                    return;
                }

                string remainingData = builder.ToString();
                builder.Clear();
                considerBuilderBuffer = false;

                bool isBufferEmpty = string.IsNullOrWhiteSpace(remainingData);
                if ((fieldCount == -1) && isBufferEmpty)
                {
                    return;
                }

                fieldCount++;
                if (fieldCount >= rowFields!.Length)
                {
                    // we know exactly how much is left now
                    Array.Resize(ref rowFields, rowFields.Length + 1);
                }

                if (isBufferEmpty)
                {
                    rowFields[fieldCount] = null;
                }
                else if (remainingData == "\"")
                {
                    // case when the CSV has something like a [,"",] where a quoted string field is empty
                    // we would end up adding a single quote to our builder
                    // DEBATE: String.Empty or NULL ?

                    rowFields[fieldCount] = string.Empty;
                }
                else
                {
                    rowFields[fieldCount] = remainingData;
                }
            }
        }


    }
}
