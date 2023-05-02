using Internal.Data;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

namespace SujaySarma.Data.Files.TokenLimitedFiles
{
    /// <summary>
    /// Writes token-limited flat-files. Default token is the comma (',').
    /// </summary>
    public class TokenLimitedFileWriter : IDisposable
    {

        #region Properties

        /// <summary>
        /// Field delimiter. Default is comma (',').
        /// </summary>
        public char Delimiter { get; init; } = ',';

        /// <summary>
        /// Returns the text encoding being used
        /// </summary>
        public Encoding Encoding { get => _writer.Encoding; }

        /// <summary>
        /// Get/set if the stream automatically flushes written data to the backing file
        /// </summary>
        public bool AutoFlush { get => _writer.AutoFlush; set => _writer.AutoFlush = value; }

        /// <summary>
        /// New line character used by the writer
        /// </summary>
        public string NewLine { get => _writer.NewLine; set => _writer.NewLine = value; }


        /// <summary>
        /// The number of rows actually written (so far). Since we stream the data, this is not the Total count!
        /// </summary>
        public ulong RowCount { get => ROWS_WRITTEN; }

        #endregion

        #region Constructors

        /// <summary>
        /// Initialize writer with a stream and other options
        /// </summary>
        /// <param name="stream">Stream to open the reader on</param>
        /// <param name="encoding">Specific encoding</param>
        /// <param name="bufferSize">Minimum stream buffer size</param>
        /// <param name="leaveStreamOpen">Set to dispose the stream when this object is disposed</param>
        public TokenLimitedFileWriter(Stream stream, Encoding? encoding = default, int bufferSize = -1, bool leaveStreamOpen = false)
        {
            _writer = new(stream, encoding, bufferSize, leaveStreamOpen);
            _leaveStreamOpenOnDispose = leaveStreamOpen;
        }

        /// <summary>
        /// Initialize writer with path to file and other options
        /// </summary>
        /// <param name="path">Path to file (absolute preferred)</param>
        /// <param name="encoding">Specific encoding. NULL for autodetect</param>
        /// <param name="leaveStreamOpen">Set to dispose the stream when this object is disposed</param>
        public TokenLimitedFileWriter(string path, Encoding? encoding = default, bool leaveStreamOpen = false)
        {
            if (encoding == default) { encoding = Encoding.UTF8; }
            FileStreamOptions options = new()
            {
                Access = FileAccess.Write,
                Mode = FileMode.Create,
                Options = FileOptions.None,
                Share = FileShare.Read,
                BufferSize = 4096
            };

            _writer = new(path, encoding, options);
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
                throw new ObjectDisposedException("TokenLimitedFileWriter");
            }

            _writer.Close();
            Dispose(true);
        }

        /// <summary>
        /// Write a complete row
        /// </summary>
        /// <param name="row">A single row of data</param>
        public void Write(string?[]? row)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException("TokenLimitedFileWriter");
            }

            if (row == default)
            {
                return;
            }

            for (int h = 0; h < row.Length; h++)
            {
                string? element = row[h];
                if (element != null)
                {
                    _writer.Write(element);
                }

                if (h < (row.Length - 1))
                {
                    _writer.Write(Delimiter);
                }
            }
            _writer.WriteLine();

            ROWS_WRITTEN++;
        }

        /// <summary>
        /// Write a complete row
        /// </summary>
        /// <param name="row">A string of arbitrary information</param>
        public void Write(string? row)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException("TokenLimitedFileWriter");
            }

            if (row == default)
            {
                return;
            }

            _writer.Write(row);
            _writer.WriteLine();

            ROWS_WRITTEN++;
        }

        /// <summary>
        /// Write a newline to the stream
        /// </summary>
        public void WriteNewLine() => _writer.WriteLine();

        #endregion

        #region Static Methods

        private static void WriteRecords(TokenLimitedFileWriter writer, DataTable table, bool quoteAllStrings)
        {
            string?[]? header = new string[table.Columns.Count];

            for (int i = 0; i < table.Columns.Count; i++)
            {
                header[i] = (quoteAllStrings ? $"\"{table.Columns[i].ColumnName}\"" : table.Columns[i].ColumnName);
            }
            writer.Write(header);

            string? colData;

            for (int r = 0; r < table.Rows.Count; r++)
            {
                string?[]? data = new string[table.Columns.Count];

                for (int c = 0; c < table.Columns.Count; c++)
                {
                    if (table.Columns[c].DataType == typeof(string))
                    {
                        colData = table.Rows[r][c] as string;
                    }
                    else
                    {
                        colData = (string?)Internal.Reflection.ReflectionUtils.GetAcceptableValue(table.Columns[c].DataType, typeof(string), table.Rows[r][c]);
                    }

                    if ((colData != default) && (quoteAllStrings || (colData.Contains(writer.Delimiter))))
                    {
                        colData = $"{colData}";
                    }

                    data[c] = colData;
                }

                writer.Write(data);
            }
        }

        /// <summary>
        /// Write record from DataTable to the stream
        /// </summary>
        /// <param name="table">DataTable with records to write</param>
        /// <param name="stream">Stream to open the reader on</param>
        /// <param name="encoding">Specific encoding</param>
        /// <param name="bufferSize">Minimum stream buffer size</param>
        /// <param name="leaveStreamOpen">Set to dispose the stream when this object is disposed</param>
        /// <param name="quoteAllStrings">Set to quote all string values in the output</param>
        /// <returns>Number of records written</returns>
        public static ulong WriteRecords(DataTable table, Stream stream, Encoding? encoding = default, int bufferSize = -1, bool leaveStreamOpen = false, bool quoteAllStrings = true)
        {
            using TokenLimitedFileWriter writer = new(stream, encoding, bufferSize, leaveStreamOpen);
            WriteRecords(writer, table, quoteAllStrings);
            return writer.ROWS_WRITTEN;
        }

        /// <summary>
        /// Write record from DataTable to the file at the path
        /// </summary>
        /// <param name="table">DataTable with records to write</param>
        /// <param name="path">Path to the disk file to open the reader on</param>
        /// <param name="encoding">Specific encoding</param>
        /// <param name="leaveStreamOpen">Set to dispose the stream when this object is disposed</param>
        /// <param name="quoteAllStrings">Set to quote all string values in the output</param>
        /// <returns>Number of records written</returns>
        public static ulong WriteRecords(DataTable table, string path, Encoding? encoding = default, bool leaveStreamOpen = false, bool quoteAllStrings = true)
        {
            using TokenLimitedFileWriter writer = new(path, encoding, leaveStreamOpen);
            WriteRecords(writer, table, quoteAllStrings);
            return writer.ROWS_WRITTEN;
        }

        /// <summary>
        /// Write record from object to the file at the path
        /// </summary>
        /// <typeparam name="T">Type of objects</typeparam>
        /// <param name="list">List of item to convert to record</param>
        /// <param name="path">Path to the disk file to open the reader on</param>
        /// <param name="encoding">Specific encoding</param>
        /// <param name="leaveStreamOpen">Set to dispose the stream when this object is disposed</param>
        /// <param name="quoteAllStrings">Set to quote all string values in the output</param>
        /// <returns>Number of records written</returns>
        public static ulong WriteRecords<T>(List<T> list, string path, Encoding? encoding = default, bool leaveStreamOpen = false, bool quoteAllStrings = true)
            where T : class, new()
        {
            using TokenLimitedFileWriter writer = new(path, encoding, leaveStreamOpen);
            DataTable table = OrmUtils.FromList(list);
            WriteRecords(writer, table, quoteAllStrings);
            return writer.ROWS_WRITTEN;
        }



        #endregion

        #region Private fields and data

        private ulong ROWS_WRITTEN = 0;
        private readonly bool _leaveStreamOpenOnDispose = false;
        private readonly StreamWriter _writer = default!;

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
                    _writer.Close();
                }
            }
        }
        private bool isDisposed = false;

        #endregion
    }
}
