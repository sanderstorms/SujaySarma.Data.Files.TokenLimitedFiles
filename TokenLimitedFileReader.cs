using Internal.Data;
using Internal.Reflection;

using SujaySarma.Data.Files.TokenLimitedFiles.Attributes;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

namespace SujaySarma.Data.Files.TokenLimitedFiles
{
    /// <summary>
    /// Reads, parses and returns data from various token-limited flat-files. Default token is the comma (','). 
    /// </summary>
    public class TokenLimitedFileReader : IDisposable
    {

        #region Properties

        /// <summary>
        /// Field delimiter. Default is comma (',').
        /// </summary>
        public char Delimiter { get; set; } = ',';

        /// <summary>
        /// Returns the current text encoding being used
        /// </summary>
        public Encoding CurrentEncoding { get => _reader.CurrentEncoding; }

        /// <summary>
        /// Returns if the current position is the EOF
        /// </summary>
        public bool EndOfStream { get => _reader.EndOfStream; }

        /// <summary>
        /// The number of rows read (so far). Since we stream the data, this is not the Total count!
        /// </summary>
        public ulong RowCount { get => ROWS_READ; }

        /// <summary>
        /// The last row that was read. Will be NULL initially and when nothing is read
        /// </summary>
        public string?[]? LastRowRead { get; private set; } = null;
        private int _maxFieldsRead = 0;

        #endregion

        #region Constructors

        /// <summary>
        /// Initialize reader with a stream and other options
        /// </summary>
        /// <param name="stream">Stream to open the reader on</param>
        /// <param name="encoding">Specific encoding. NULL to auto-detect</param>
        /// <param name="autoDetectEncoding">If set, auto-detects</param>
        /// <param name="bufferSize">Minimum stream buffer size</param>
        /// <param name="leaveStreamOpen">Set to dispose the stream when this object is disposed</param>
        public TokenLimitedFileReader(Stream stream, Encoding? encoding = default, bool autoDetectEncoding = true, int bufferSize = -1, bool leaveStreamOpen = false)
        {
            _reader = new(stream, encoding, autoDetectEncoding, bufferSize, leaveStreamOpen);
            _leaveStreamOpenOnDispose = leaveStreamOpen;
        }

        /// <summary>
        /// Initialize reader with path to file and other options
        /// </summary>
        /// <param name="path">Path to file (absolute preferred)</param>
        /// <param name="encoding">Specific encoding. NULL for autodetect</param>
        /// <param name="autoDetectEncoding">If set, auto-detects</param>
        /// <param name="leaveStreamOpen">Set to dispose the stream when this object is disposed</param>
        public TokenLimitedFileReader(string path, Encoding? encoding = default, bool autoDetectEncoding = true, bool leaveStreamOpen = false)
        {
            if (encoding == default) { encoding = Encoding.UTF8; }

            _reader = new StreamReader(path, encoding, autoDetectEncoding, 4096);
            _leaveStreamOpenOnDispose = leaveStreamOpen;
        }

        #endregion

        #region Methods

        /// <summary>
        /// Closes the stream, including the underlying stream. The stream is also disposed. 
        /// No further operation must be attempted on the stream after this call.
        /// </summary>
        public void Close()
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException("TokenLimitedFileReader");
            }

            _reader.Close();
            Dispose(true);
        }

        /// <summary>
        /// Read and return a complete row
        /// </summary>
        /// <returns>Number of fields read. Will be -1 if nothing was read.</returns>
        public int ReadRow()
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException("TokenLimitedFileReader");
            }

            // Exit early
            if (END_OF_TABLE || _reader.EndOfStream)
            {
                if (!END_OF_TABLE)
                {
                    END_OF_TABLE = true;
                }

                return -1;
            }

            // 24 is an arbitrary count!
            // Most data should be well-within, meaning we avoid unnecessary re-allocs.
            if (LastRowRead == null)
            {
                LastRowRead = ((_maxFieldsRead > 0) ? new string[_maxFieldsRead] : new string[24]);
            }
            else
            {
                Array.Clear(LastRowRead, 0, LastRowRead.Length);
            }

            int fieldCount = -1, quoteCount = 0;
            bool considerBuilderBuffer = false;
            StringBuilder builder = new();

            while (!_reader.EndOfStream)
            {
                considerBuilderBuffer = true;
                int r = _reader.Read();
                if (r == -1)
                {
                    break;
                }

                char c = (char)r;

                if (c == '"')
                {
                    if (_reader.Peek() == '"')
                    {
                        // escaped quote. Add it to the element and skip over the second one
                        builder.Append('"');
                        _reader.Read();
                        quoteCount++;
                    }

                    quoteCount++;
                    continue;
                }

                if (c == Delimiter)
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

                if ((c == '\r') || (c == '\n'))
                {
                    if ((c == '\r') && (_reader.Peek() == '\n'))
                    {
                        // skip over CRLF sequence
                        _reader.Read();
                    }

                    if ((quoteCount % 2) == 0)
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
            }

            if (_reader.EndOfStream)
            {
                END_OF_TABLE = true;
            }

            flushBuilder();

            if (fieldCount <= 0)
            {
                LastRowRead = null;
                return -1;
            }

            if (_maxFieldsRead < fieldCount)
            {
                _maxFieldsRead = fieldCount;
            }

            if ((LastRowRead != null) && (LastRowRead.Length > fieldCount))
            {
                // Right-size the array
                string?[]? temp = LastRowRead;
                Array.Resize(ref temp, fieldCount + 1);
                LastRowRead = temp;
            }

            ROWS_READ++;
            return fieldCount;

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

                if (LastRowRead.Length <= fieldCount)
                {
                    // Right-size the array
                    string?[]? temp = LastRowRead;
                    Array.Resize(ref temp, fieldCount + 1);
                    LastRowRead = temp;
                }

                if (isBufferEmpty)
                {
                    LastRowRead[fieldCount] = null;
                }
                else if (remainingData == "\"")
                {
                    // case when the CSV has something like a [,"",] where a quoted string field is empty
                    // we would end up adding a single quote to our builder
                    // DEBATE: String.Empty or NULL ?

                    LastRowRead[fieldCount] = string.Empty;
                }
                else
                {
                    LastRowRead[fieldCount] = remainingData;
                }
            }
        }

        #endregion

        #region Static Methods

        private static DataTable GetTable(TokenLimitedFileReader reader, bool hasHeaderRow = true, ulong headerRowIndex = 1)
        {
            DataTable table = new("Table 1");

            while (!reader.EndOfStream)
            {
                int count = reader.ReadRow();
                if (count > -1)
                {
                    if (hasHeaderRow && (reader.RowCount == headerRowIndex))
                    {
                        for (int i = 0; i < reader.LastRowRead!.Length; i++)
                        {
                            table.Columns.Add((string.IsNullOrWhiteSpace(reader.LastRowRead[i]) ? $"Column {i}" : reader.LastRowRead[i]));
                        }

                        continue;
                    }

                    DataRow newRow = table.NewRow();
                    for (int i = 0; i < reader.LastRowRead!.Length; i++)
                    {
                        Type colType = table.Columns[i].DataType;
                        try
                        {
                            newRow[i] = ReflectionUtils.GetAcceptableValue(typeof(string), colType, reader.LastRowRead[i]);
                        }
                        catch
                        {
                            throw new InvalidCastException($"Column {i} ('{table.Columns[i].ColumnName}') expects a '{table.Columns[i].DataType.Name}' type value.");
                        }
                    }
                    table.Rows.Add(newRow);
                }
            }

            return table;
        }

        /// <summary>
        /// Get the data from a stream as a DataTable
        /// </summary>
        /// <param name="stream">Stream to open the reader on</param>
        /// <param name="hasHeaderRow">Set to TRUE if the stream has a header row</param>
        /// <param name="headerRowIndex">If <paramref name="hasHeaderRow"/> is TRUE, this should contain a 1-based index [starting from current stream-pos] of the row where the header row lives.</param>
        /// <param name="encoding">Specific encoding. NULL to auto-detect</param>
        /// <param name="autoDetectEncoding">If set, auto-detects</param>
        /// <param name="bufferSize">Minimum stream buffer size</param>
        /// <param name="leaveStreamOpen">Set to dispose the stream when this object is disposed</param>
        /// <returns>Data from the stream as a DataTable</returns>
        public static DataTable GetTable(Stream stream, bool hasHeaderRow = true, ulong headerRowIndex = 1, Encoding? encoding = default, bool autoDetectEncoding = true, int bufferSize = -1, bool leaveStreamOpen = false)
        {
            using TokenLimitedFileReader reader = new(stream, encoding, autoDetectEncoding, bufferSize, leaveStreamOpen);
            return GetTable(reader, hasHeaderRow, headerRowIndex);
        }

        /// <summary>
        /// Get the data from a stream as a DataTable
        /// </summary>
        /// <param name="path">Path to file (absolute preferred)</param>
        /// <param name="hasHeaderRow">Set to TRUE if the stream has a header row</param>
        /// <param name="headerRowIndex">If <paramref name="hasHeaderRow"/> is TRUE, this should contain a 1-based index [starting from current stream-pos] of the row where the header row lives.</param>
        /// <param name="encoding">Specific encoding. NULL for autodetect</param>
        /// <param name="autoDetectEncoding">If set, auto-detects</param>
        /// <param name="leaveStreamOpen">Set to dispose the stream when this object is disposed</param>
        /// <returns>Data from the stream as a DataTable</returns>
        public static DataTable GetTable(string path, char delimiter = ',', bool hasHeaderRow = true, ulong headerRowIndex = 1, Encoding? encoding = default, bool autoDetectEncoding = true, bool leaveStreamOpen = false)
        {
            using TokenLimitedFileReader reader = new(path, encoding, autoDetectEncoding, leaveStreamOpen);
            reader.Delimiter = delimiter;
            return GetTable(reader, hasHeaderRow, headerRowIndex);
        }

        /// <summary>
        /// Get the data from a stream as an IEnumerable[T]
        /// </summary>
        /// <typeparam name="T">Business object type, must be a class with a public default constructor</typeparam>
        /// <param name="stream">Stream to open the reader on</param>
        /// <param name="hasHeaderRow">Set to TRUE if the stream has a header row</param>
        /// <param name="headerRowIndex">If <paramref name="hasHeaderRow"/> is TRUE, this should contain a 1-based index [starting from current stream-pos] of the row where the header row lives.</param>
        /// <param name="encoding">Specific encoding. NULL to auto-detect</param>
        /// <param name="autoDetectEncoding">If set, auto-detects</param>
        /// <param name="bufferSize">Minimum stream buffer size</param>
        /// <param name="leaveStreamOpen">Set to dispose the stream when this object is disposed</param>
        /// <param name="action">An action to perform on the DataTable before conversion to list</param>
        /// <returns>Data from the stream as an IEnumerable[T]</returns>
        public static IEnumerable<T> GetEnumerable<T>(Stream stream, bool hasHeaderRow = true, ulong headerRowIndex = 1, Encoding? encoding = default, bool autoDetectEncoding = true, int bufferSize = -1, bool leaveStreamOpen = false, Action<DataTable>? action = null)
            where T : class, new()
        {
            DataTable table = GetTable(stream, hasHeaderRow, headerRowIndex, encoding, autoDetectEncoding, bufferSize, leaveStreamOpen);
            action?.Invoke(table);
            return OrmUtils.ToEnumerable<T>(table);
        }

        /// <summary>
        /// Get the data from a stream as an IEnumerable[T]
        /// </summary>
        /// <typeparam name="T">Business object type, must be a class with a public default constructor</typeparam>
        /// <param name="path">Path to file (absolute preferred)</param>
        /// <param name="hasHeaderRow">Set to TRUE if the stream has a header row</param>
        /// <param name="headerRowIndex">If <paramref name="hasHeaderRow"/> is TRUE, this should contain a 1-based index [starting from current stream-pos] of the row where the header row lives.</param>
        /// <param name="encoding">Specific encoding. NULL for autodetect</param>
        /// <param name="autoDetectEncoding">If set, auto-detects</param>
        /// <param name="leaveStreamOpen">Set to dispose the stream when this object is disposed</param>
        /// <param name="action">An action to perform on the DataTable before conversion to list</param>
        /// <returns>Data from the stream as an IEnumerable[T]</returns>
        public static IEnumerable<T> GetEnumerable<T>(string path, char delimiter = ',', bool hasHeaderRow = true, ulong headerRowIndex = 1, Encoding? encoding = default, bool autoDetectEncoding = true, bool leaveStreamOpen = false, Action<DataTable>? action = null)
            where T : class, new()
        {
            DataTable table = GetTable(path, delimiter, hasHeaderRow, headerRowIndex, encoding, autoDetectEncoding, leaveStreamOpen);
            action?.Invoke(table);
            return OrmUtils.ToEnumerable<T>(table);
        }

        /// <summary>
        /// Get the data from a stream as an IList[T]
        /// </summary>
        /// <typeparam name="T">Business object type, must be a class with a public default constructor</typeparam>
        /// <param name="stream">Stream to open the reader on</param>
        /// <param name="hasHeaderRow">Set to TRUE if the stream has a header row</param>
        /// <param name="headerRowIndex">If <paramref name="hasHeaderRow"/> is TRUE, this should contain a 1-based index [starting from current stream-pos] of the row where the header row lives.</param>
        /// <param name="encoding">Specific encoding. NULL to auto-detect</param>
        /// <param name="autoDetectEncoding">If set, auto-detects</param>
        /// <param name="bufferSize">Minimum stream buffer size</param>
        /// <param name="leaveStreamOpen">Set to dispose the stream when this object is disposed</param>
        /// <param name="action">An action to perform on the DataTable before conversion to list</param>
        /// <returns>Data from the stream as an IList[T]</returns>
        public static List<T> GetList<T>(Stream stream, bool hasHeaderRow = true, ulong headerRowIndex = 1, Encoding? encoding = default, bool autoDetectEncoding = true, int bufferSize = -1, bool leaveStreamOpen = false, Action<DataTable>? action = null)
            where T : class, new()
        {
            DataTable table = GetTable(stream, hasHeaderRow, headerRowIndex, encoding, autoDetectEncoding, bufferSize, leaveStreamOpen);
            action?.Invoke(table);
            return OrmUtils.ToList<T>(table);
        }

    /// <summary>
    /// Get the data from a stream as an IList[T]
    /// </summary>
    /// <typeparam name="T">Business object type, must be a class with a public default constructor</typeparam>
    /// <param name="path">Path to file (absolute preferred)</param>
    /// <param name="hasHeaderRow">Set to TRUE if the stream has a header row</param>
    /// <param name="headerRowIndex">If <paramref name="hasHeaderRow"/> is TRUE, this should contain a 1-based index [starting from current stream-pos] of the row where the header row lives.</param>
    /// <param name="encoding">Specific encoding. NULL for autodetect</param>
    /// <param name="autoDetectEncoding">If set, auto-detects</param>
    /// <param name="leaveStreamOpen">Set to dispose the stream when this object is disposed</param>
    /// <param name="action">An action to perform on the DataTable before conversion to list</param>
    /// <returns>Data from the stream as an IList[T]</returns>
    public static List<T> GetList<T>(string path, char delimiter = ',', bool hasHeaderRow = true, ulong headerRowIndex = 1, Encoding? encoding = default, bool autoDetectEncoding = true, bool leaveStreamOpen = false, Action<DataTable>? action = null)
            where T : class, new()
        {
            DataTable table = GetTable(path, delimiter, hasHeaderRow, headerRowIndex, encoding, autoDetectEncoding, leaveStreamOpen);
            action?.Invoke(table);
            return OrmUtils.ToList<T>(table);
        }

        /// <summary>
        /// Convert the provided table to a list
        /// </summary>
        /// <typeparam name="T">Business object type, must be a class with a public default constructor</typeparam>
        /// <param name="table">DataTable to convert</param>
        /// <param name="action">An optional action to perform on the DataTable before conversion to list</param>
        /// <returns>List of objects</returns>
        public static List<T> GetList<T>(DataTable table, Action<DataTable>? action)
            where T : class, new()
        {
            action?.Invoke(table);

            return OrmUtils.ToList<T>(table);
        }

        /// <summary>
        /// Convert a single row to an object
        /// </summary>
        /// <typeparam name="T">Business object type, must be a class with a public default constructor</typeparam>
        /// <param name="table">DataTable with values to process</param>
        /// <param name="rowIndex">Index of particular row in the table</param>
        /// <returns>Instantiated object</returns>
        /// <exception cref="InvalidCastException">If the 'T' object members are not decorated with 'FileFieldAttribute'</exception>
        public static T Get<T>(DataTable table, int rowIndex)
            where T : class, new()
        {
            Class? classInfo = Reflector.InspectForFileFieldAttributes<T>();
            if (classInfo == null)
            {
                throw new InvalidCastException($"The type '{typeof(T).FullName}' is not decorated with the '{nameof(FileFieldAttribute)}' attribute.");
            }

            OrmUtils.DiscoverColumnMappings(classInfo, table);
            return OrmUtils.Instantiate<T>(table, rowIndex, classInfo);
        }

        #endregion


        #region Private fields and data

        private ulong ROWS_READ = 0;
        private bool END_OF_TABLE = false;
        private readonly bool _leaveStreamOpenOnDispose = false;
        private readonly StreamReader _reader = default!;

        #endregion

        #region IDisposable implementation

        // IDisposable
        /// <inheritdoc/>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <inheritdoc/>
        protected virtual void Dispose(bool disposing)
        {
            if (!isDisposed)
            {
                isDisposed = true;

                if (!_leaveStreamOpenOnDispose)
                {
                    _reader.Close();
                }
            }
        }
        private bool isDisposed = false;

        #endregion

    }
}