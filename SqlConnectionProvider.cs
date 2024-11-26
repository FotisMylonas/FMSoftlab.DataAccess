using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FMSoftlab.DataAccess
{
    public interface ISqlConnectionProvider : IDisposable
    {
        SqlConnection Connection { get; }
        IDbTransaction BeginTransaction(IsolationLevel iso);
        Task<IDbTransaction> BeginTransactionAsync(IsolationLevel iso);
        void Open();
        Task OpenAsync();
    }

    public class SqlConnectionProvider : IDisposable, ISqlConnectionProvider
    {
        private readonly bool _ownsConnection;
        private readonly SqlConnection _sqlConnection;
        private readonly ILogger _log;

        public SqlConnectionProvider(SqlConnection sqlConnection, bool logServerMessages, ILogger log)
        {
            if (logServerMessages)
            {
                sqlConnection.InfoMessage += new SqlInfoMessageEventHandler(OnInfoMessage);
            }
            _sqlConnection = sqlConnection;
            _ownsConnection=false;
            _log=log;
        }
        public SqlConnectionProvider(IExecutionContext executionContext, ILogger log)
        {
            SqlConnection con = new SqlConnection(executionContext.ConnectionString);
            if (executionContext.LogServerMessages)
            {
                con.InfoMessage += new SqlInfoMessageEventHandler(OnInfoMessage);
            }
            _sqlConnection = con;
            _ownsConnection = true;
            _log=log;
        }
        public SqlConnectionProvider(string connectionString) : this(new ExecutionContext(connectionString), null)
        {

        }

        public IDbTransaction BeginTransaction(IsolationLevel iso)
        {
            Open();
            _log?.LogDebug("Begin transaction with isolation level {IsolationLevel}", iso);
            return _sqlConnection.BeginTransaction(iso);
        }
        public async Task<IDbTransaction> BeginTransactionAsync(IsolationLevel iso)
        {
            await OpenAsync();
            _log?.LogDebug("Begin transaction with isolation level {IsolationLevel}", iso);
            return _sqlConnection.BeginTransaction(iso);
        }

        private void OnInfoMessage(object sender, SqlInfoMessageEventArgs e)
        {
            if (e is null)
                return;
            _log?.LogDebug(e.Message);
            foreach (SqlError info in e.Errors)
            {
                _log?.LogError(@"{Message} Procedure:{Procedure}, Line:{LineNumber}, Server:{Server}", info.Message, info.Procedure, info.LineNumber, info.Server);
            }
        }

        public void Open()
        {
            if (string.IsNullOrWhiteSpace(_sqlConnection.ConnectionString))
                throw new ArgumentNullException("No connection string defined");
            if (_sqlConnection.State == ConnectionState.Closed)
            {
                _log?.LogDebug("Opening connection {ConnectionString}...", _sqlConnection.ConnectionString);
                _sqlConnection.Open();
            }
            if (_sqlConnection.State == ConnectionState.Broken)
            {
                _log?.LogDebug("Connection broken, {ConnectionString}...", _sqlConnection.ConnectionString);
                _sqlConnection.Close();
                _sqlConnection.Open();
            }
        }
        public async Task OpenAsync()
        {
            if (string.IsNullOrWhiteSpace(_sqlConnection.ConnectionString))
                throw new ArgumentNullException("No connection string defined");
            if (_sqlConnection.State == ConnectionState.Closed)
            {
                _log?.LogDebug("Opening connection {ConnectionString}...", _sqlConnection.ConnectionString);
                await _sqlConnection.OpenAsync();
            }
            if (_sqlConnection.State == ConnectionState.Broken)
            {
                _log?.LogDebug("Connection broken, {ConnectionString}...", _sqlConnection.ConnectionString);
                _sqlConnection.Close();
                await _sqlConnection.OpenAsync();
            }
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
