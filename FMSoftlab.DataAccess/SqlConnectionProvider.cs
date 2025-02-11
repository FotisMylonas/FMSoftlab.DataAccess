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
        public SqlConnectionProvider(string connectionString, ILogger log) : this(new ExecutionContext(connectionString), log)
        {

        }

        public IDbTransaction BeginTransaction(IsolationLevel iso)
        {
            Open();
            _log?.LogTrace("Begin transaction with isolation level {IsolationLevel}", iso);
            return _sqlConnection.BeginTransaction(iso);
        }
        public async Task<IDbTransaction> BeginTransactionAsync(IsolationLevel iso)
        {
            await OpenAsync();
            _log?.LogTrace("Begin transaction async with isolation level {IsolationLevel}", iso);
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
        private void ValidateConnection()
        {
            if (_sqlConnection is null)
                throw new ArgumentNullException("No connection defined");
            if (string.IsNullOrWhiteSpace(_sqlConnection.ConnectionString))
                throw new ArgumentNullException("No connection string defined");
        }

        public void Open()
        {
            ValidateConnection();
            if (_sqlConnection.State == ConnectionState.Closed)
            {
                _log?.LogTrace("Opening connection {ConnectionString}...", _sqlConnection.ConnectionString);
                _sqlConnection.Open();
                _log?.LogTrace("Opened connection {ConnectionString}, ServerProcessId: {ServerProcessId}, ClientConnectionId: {ClientConnectionId}...", _sqlConnection.ConnectionString, _sqlConnection.ServerProcessId, _sqlConnection.ClientConnectionId);
            }
            if (_sqlConnection.State == ConnectionState.Broken)
            {
                _log?.LogTrace("Connection is broken, will open again {ConnectionString}...", _sqlConnection.ConnectionString);
                _sqlConnection.Close();
                _sqlConnection.Open();
                _log?.LogTrace("Connection was broken, opened again {ConnectionString}, ServerProcessId: {ServerProcessId}, ClientConnectionId: {ClientConnectionId}...", _sqlConnection.ConnectionString, _sqlConnection.ServerProcessId, _sqlConnection.ClientConnectionId);
            }
        }
        public void Close()
        {
            ValidateConnection();
            if (!_ownsConnection)
            {
                _log?.LogTrace("Connection not owned, will not close");
                return;
            }
            if (_sqlConnection.State == ConnectionState.Open)
            {
                _log?.LogTrace("Closing connection {ConnectionString}...", _sqlConnection.ConnectionString);
                _sqlConnection.Close();
            }
            if (_sqlConnection.State == ConnectionState.Broken)
            {
                _log?.LogTrace("Connection broken, Closing connection, {ConnectionString}...", _sqlConnection.ConnectionString);
                _sqlConnection.Close();
            }
        }

        public async Task OpenAsync()
        {
            ValidateConnection();
            if (_sqlConnection.State == ConnectionState.Closed)
            {
                _log?.LogTrace("Opening connection {ConnectionString}...", _sqlConnection.ConnectionString);
                await _sqlConnection.OpenAsync();
                _log?.LogTrace("Opened connection {ConnectionString}, ServerProcessId: {ServerProcessId}, ClientConnectionId: {ClientConnectionId}...", _sqlConnection.ConnectionString, _sqlConnection.ServerProcessId, _sqlConnection.ClientConnectionId);
            }
            if (_sqlConnection.State == ConnectionState.Broken)
            {
                _log?.LogTrace("Connection is broken, will open again {ConnectionString}...", _sqlConnection.ConnectionString);
                _sqlConnection.Close();
                await _sqlConnection.OpenAsync();
                _log?.LogTrace("Connection was broken, opened again {ConnectionString}, ServerProcessId: {ServerProcessId}, ClientConnectionId: {ClientConnectionId}...", _sqlConnection.ConnectionString, _sqlConnection.ServerProcessId, _sqlConnection.ClientConnectionId);
            }
        }
        public void Dispose()
        {
            if (!_ownsConnection)
            {
                _log?.LogTrace("Connection not owned, will not dispose");
                return;
            }
            try
            {
                Close();
            }
            finally
            {
                _sqlConnection.Dispose();
                _log?.LogTrace("Connection disposed");
            }
        }
        public SqlConnection Connection { get { return _sqlConnection; } }
    }
}