using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;

namespace FMSoftlab.DataAccess
{
    public interface ISqlConnectionProvider : IDisposable
    {
        SqlConnection Connection { get; }
        SqlTransaction BeginTransaction(IsolationLevel iso);
        void Open();
        Task OpenAsync();
    }

    public class SqlConnectionProvider : IDisposable, ISqlConnectionProvider
    {
        private readonly bool _ownsConnection;
        private readonly SqlConnection _sqlConnection;
        private readonly ILogger _log;

        SqlConnectionProvider(SqlConnection sqlConnection) : this(sqlConnection, false, null)
        {

        }
        public SqlConnectionProvider(SqlConnection sqlConnection, bool logServerMessages, ILogger log)
        {
            _sqlConnection = sqlConnection;
            _ownsConnection=false;
            _log=log;
            if (logServerMessages)
            {
                _sqlConnection.InfoMessage += new SqlInfoMessageEventHandler(OnInfoMessage);
            }
        }

        public SqlConnectionProvider(string connectionString) : this(connectionString, false, null)
        {

        }

        public SqlConnectionProvider(string connectionString, bool logServerMessages, ILogger log)
        {
            _sqlConnection=new SqlConnection(connectionString);
            _ownsConnection = true;
            _log=log;
            if (logServerMessages)
            {
                _sqlConnection.InfoMessage += new SqlInfoMessageEventHandler(OnInfoMessage);
            }
        }
        private void OnInfoMessage(object sender, SqlInfoMessageEventArgs e)
        {
            if (e is null)
                return;
            _log.LogDebug(e.Message);
            foreach (SqlError info in e.Errors)
            {
                _log.LogError(@"{Message} Procedure:{Procedure}, Line:{LineNumber}, Server:{Server}", info.Message, info.Procedure, info.LineNumber, info.Server);
            }
        }

        public void Open()
        {
            if (_sqlConnection.State == ConnectionState.Closed)
            {
                _sqlConnection.Open();
            }
            if (_sqlConnection.State == ConnectionState.Broken)
            {
                _sqlConnection.Close();
                _sqlConnection.Open();
            }
        }
        public async Task OpenAsync()
        {
            if (_sqlConnection.State == ConnectionState.Closed)
            {
                await _sqlConnection.OpenAsync();
            }
            if (_sqlConnection.State == ConnectionState.Broken)
            {
                _sqlConnection.Close();
                await _sqlConnection.OpenAsync();
            }
        }

        public SqlTransaction BeginTransaction(IsolationLevel iso)
        {
            Open();
            return _sqlConnection.BeginTransaction(iso);
        }
        public async Task<SqlTransaction> BeginTransactionAsync(IsolationLevel iso)
        {
            await OpenAsync();
            return _sqlConnection.BeginTransaction(iso);
        }

        public void Dispose()
        {
            if (_ownsConnection)
            {
                try
                {
                    _sqlConnection.Close();
                }
                finally
                {
                    _sqlConnection.Dispose();
                }
            }
        }
        public SqlConnection Connection { get { return _sqlConnection; } }
    }

}
