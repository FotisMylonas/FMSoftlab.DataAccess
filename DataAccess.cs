using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;


namespace FmSoftlab.DataAccess
{
    public interface ISqlConnectionProvider : IDisposable
    {
        SqlConnection Connection { get; }
        SqlTransaction BeginTransaction(IsolationLevel iso);
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
                Console.WriteLine(info.Message);
            }
        }
        public SqlTransaction BeginTransaction(IsolationLevel iso)
        {
            _sqlConnection.Open();
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
    public interface IExecutionContext
    {
        int CommandTimeout { get; set; }
        string ConnectionString { get; set; }
        IsolationLevel IsolationLevel { get; set; }
        bool LogServerMessages { get; set; }
    }

    public class ExecutionContext : IExecutionContext
    {
        public int CommandTimeout { get; set; }
        public string ConnectionString { get; set; }
        public IsolationLevel IsolationLevel { get; set; }
        public bool LogServerMessages { get; set; }
        public ExecutionContext(string connectionString, int commandTimeout, IsolationLevel isolationLevel)
        {
            CommandTimeout=commandTimeout;
            ConnectionString =connectionString;
            IsolationLevel=isolationLevel;
        }
        public ExecutionContext(string connectionString) : this(connectionString, 30, IsolationLevel.ReadCommitted)
        {

        }
    }
    public interface IExecutionContextFactory
    {
        IExecutionContext GetShortRunning();
        IExecutionContext TenSeconds();
        IExecutionContext ThirtySeconds();
        IExecutionContext OneMinute();
        IExecutionContext FiveMinutes();
        IExecutionContext TenMinutes();
        IExecutionContext OneHour();
        IExecutionContext TwoHours();
        IExecutionContext ThreeHours();
        IExecutionContext GetLongRunning();
        IExecutionContext GetForeverRunning();
    }

    public class ExecutionContextFactory : IExecutionContextFactory
    {
        private string _connectionString;
        private readonly IsolationLevel _isolationLevel;
        private readonly int _shortRunning;
        private readonly int _longrunning;

        public ExecutionContextFactory(string connectionString, IsolationLevel isolationLevel, int shortRunning, int longrunning)
        {
            _connectionString=connectionString;
            _isolationLevel=isolationLevel;
            _shortRunning=shortRunning;
            _longrunning=longrunning;
        }
        public IExecutionContext GetShortRunning()
        {
            return new ExecutionContext(_connectionString, _shortRunning, _isolationLevel);
        }

        public IExecutionContext TenSeconds()
        {
            return new ExecutionContext(_connectionString, 10, _isolationLevel);
        }
        public IExecutionContext ThirtySeconds()
        {
            return new ExecutionContext(_connectionString, 10, _isolationLevel);
        }

        public IExecutionContext OneMinute()
        {
            return new ExecutionContext(_connectionString, 60, _isolationLevel);
        }

        public IExecutionContext FiveMinutes()
        {
            return new ExecutionContext(_connectionString, 300, _isolationLevel);
        }

        public IExecutionContext TenMinutes()
        {
            return new ExecutionContext(_connectionString, 600, _isolationLevel);
        }
        public IExecutionContext OneHour()
        {
            return new ExecutionContext(_connectionString, 3600, _isolationLevel);
        }
        public IExecutionContext TwoHours()
        {
            return new ExecutionContext(_connectionString, 7200, _isolationLevel);
        }

        public IExecutionContext ThreeHours()
        {
            return new ExecutionContext(_connectionString, 10800, _isolationLevel);
        }

        public IExecutionContext GetLongRunning()
        {
            return new ExecutionContext(_connectionString, _longrunning, _isolationLevel);
        }
        public IExecutionContext GetForeverRunning()
        {
            return new ExecutionContext(_connectionString, 0, _isolationLevel);
        }
    }

    public class SqlExecutorBase
    {
        private ILogger _log;
        public SqlExecutorBase(ILogger log)
        {
            _log=log;
        }

        public async Task Execute(IExecutionContext executionContext, string sql, DynamicParameters dyn)
        {
            await Execute(executionContext, sql, dyn, CommandType.StoredProcedure);
        }

        public async Task Execute(IExecutionContext executionContext, string sql, DynamicParameters dyn, CommandType commandType)
        {
            //_log?.LogDebug($"Execute in, ConnectionString:{executionContext.ConnectionString}, Sql:{sql}, CommandTimeout:{executionContext.CommandTimeout}, IsolationLevel:{executionContext.IsolationLevel}");
            using (ISqlConnectionProvider con = new SqlConnectionProvider(executionContext.ConnectionString, executionContext.LogServerMessages, _log))
            {
                await Execute(con, executionContext, sql, dyn, commandType);
            }
        }
        public async Task Execute(ISqlConnectionProvider connectionProvider, IExecutionContext executionContext, string sql, DynamicParameters dyn, CommandType commandType)
        {
            //_log?.LogDebug($"Execute in, ConnectionString:{executionContext.ConnectionString}, Sql:{sql}, CommandTimeout:{executionContext.CommandTimeout}, IsolationLevel:{executionContext.IsolationLevel}");
            try
            {
                using (var trans = connectionProvider.BeginTransaction(executionContext.IsolationLevel))
                {
                    await connectionProvider.Connection.ExecuteAsync(sql, dyn, commandTimeout: executionContext.CommandTimeout, transaction: trans, commandType: commandType);
                    trans.Commit();
                }
            }
            catch (Exception ex)
            {
                _log?.LogError($"{ex.Message}{ex.StackTrace}");
                throw;
            }
        }

        public async Task<T> ExecuteScalar<T>(IExecutionContext executionContext, string sql, DynamicParameters dyn, CommandType commandType)
        {
            //_log?.LogDebug($"Execute in, ConnectionString:{executionContext.ConnectionString}, Sql:{sql}, CommandTimeout:{executionContext.CommandTimeout}, IsolationLevel:{executionContext.IsolationLevel}");
            T res = default(T);
            using (ISqlConnectionProvider con = new SqlConnectionProvider(executionContext.ConnectionString, executionContext.LogServerMessages, _log))
            {
                res=await ExecuteScalar<T>(con, executionContext, sql, dyn, commandType);
            }
            return res;
        }

        public async Task<T> ExecuteScalar<T>(ISqlConnectionProvider connectionProvider, IExecutionContext executionContext, string sql, DynamicParameters dyn, CommandType commandType)
        {
            //_log?.LogDebug($"Execute in, ConnectionString:{executionContext.ConnectionString}, Sql:{sql}, CommandTimeout:{executionContext.CommandTimeout}, IsolationLevel:{executionContext.IsolationLevel}");
            T res = default(T);
            try
            {
                using (var trans = connectionProvider.BeginTransaction(executionContext.IsolationLevel))
                {
                    res=await connectionProvider.Connection.ExecuteScalarAsync<T>(sql, dyn, commandTimeout: executionContext.CommandTimeout, transaction: trans, commandType: commandType);
                    trans.Commit();
                }
            }
            catch (Exception ex)
            {
                _log?.LogError($"{ex.Message}{ex.StackTrace}");
                throw;
            }
            return res;
        }

        public async Task<IEnumerable<T>> Query<T>(IExecutionContext executionContext, string sql, DynamicParameters dyn)
        {
            return await Query<T>(executionContext, sql, dyn, CommandType.StoredProcedure);
        }
        public async Task<IEnumerable<T>> Query<T>(IExecutionContext executionContext, string sql, DynamicParameters dyn, CommandType commandType)
        {
            IEnumerable<T> result = Enumerable.Empty<T>();
            //_log?.LogDebug($"Execute in, ConnectionString:{executionContext.ConnectionString}, Sql:{sql}, CommandTimeout:{executionContext.CommandTimeout}, IsolationLevel:{executionContext.IsolationLevel}");
            using (ISqlConnectionProvider con = new SqlConnectionProvider(executionContext.ConnectionString, executionContext.LogServerMessages, _log))
            {
                result=await Query<T>(con, executionContext, sql, dyn, commandType);
            }
            return result;
        }
        public async Task<IEnumerable<T>> Query<T>(ISqlConnectionProvider connectionProvider, IExecutionContext executionContext, string sql, DynamicParameters dyn, CommandType commandType)
        {
            IEnumerable<T> result = Enumerable.Empty<T>();
            //_log?.LogDebug($"Execute in, ConnectionString:{executionContext.ConnectionString}, Sql:{sql}, CommandTimeout:{executionContext.CommandTimeout}, IsolationLevel:{executionContext.IsolationLevel}");
            try
            {
                using (var trans = connectionProvider.BeginTransaction(executionContext.IsolationLevel))
                {
                    var dbres = await connectionProvider.Connection.QueryAsync<T>(sql, dyn, commandTimeout: executionContext.CommandTimeout, transaction: trans, commandType: commandType);
                    if (dbres != null && dbres.Any())
                    {
                        result = dbres;
                    }
                    trans.Commit();
                }
            }
            catch (Exception ex)
            {
                _log?.LogError($"{ex.Message}{ex.StackTrace}");
                throw;
            }
            return result;
        }
        public async Task QueryMultiple(IExecutionContext executionContext, string sql, DynamicParameters dyn, CommandType commandType, Action<SqlMapper.GridReader> action)
        {
            //_log?.LogDebug($"Execute in, ConnectionString:{executionContext.ConnectionString}, Sql:{sql}, CommandTimeout:{executionContext.CommandTimeout}, IsolationLevel:{executionContext.IsolationLevel}");
            using (ISqlConnectionProvider con = new SqlConnectionProvider(executionContext.ConnectionString, executionContext.LogServerMessages, _log))
            {
                await QueryMultiple(con, executionContext, sql, dyn, commandType, action);
            }
        }
        public async Task QueryMultiple(ISqlConnectionProvider connectionProvider, IExecutionContext executionContext, string sql, DynamicParameters dyn, CommandType commandType, Action<SqlMapper.GridReader> action)
        {
            //_log?.LogDebug($"Execute in, ConnectionString:{executionContext.ConnectionString}, Sql:{sql}, CommandTimeout:{executionContext.CommandTimeout}, IsolationLevel:{executionContext.IsolationLevel}");
            try
            {
                using (var trans = connectionProvider.BeginTransaction(executionContext.IsolationLevel))
                {
                    SqlMapper.GridReader gridReader = await connectionProvider.Connection.QueryMultipleAsync(sql, dyn, commandTimeout: executionContext.CommandTimeout, transaction: trans, commandType: commandType);
                    if (gridReader != null)
                    {
                        action(gridReader);
                    }
                    trans.Commit();
                }
            }
            catch (Exception ex)
            {
                _log?.LogError($"{ex.Message}{ex.StackTrace}");
                throw;
            }
        }
    }

    public class SqlExecution
    {
        private readonly ISqlConnectionProvider _connectionProvider;
        private readonly string _sql;
        private readonly DynamicParameters _dyn;
        private readonly CommandType _commandType;
        private readonly IExecutionContext _executionContext;
        private readonly ILogger _log;

        public SqlExecution(IExecutionContext executionContext, string sql, DynamicParameters dyn, CommandType commandType, ILogger log)
        {
            _executionContext=executionContext;
            _sql=sql;
            _dyn=dyn;
            _commandType=commandType;
            _log=log;
        }

        public SqlExecution(IExecutionContext executionContext, string sql, DynamicParameters dyn, ILogger log)
        {
            _executionContext=executionContext;
            _sql=sql;
            _dyn=dyn;
            _commandType=CommandType.StoredProcedure;
            _log=log;
        }

        public SqlExecution(IExecutionContext executionContext, ISqlConnectionProvider connectionProvider, string sql, DynamicParameters dyn, CommandType commandType, ILogger log)
        {
            _executionContext=executionContext;
            _sql=sql;
            _dyn=dyn;
            _commandType=commandType;
            _log=log;
            _connectionProvider=connectionProvider;
        }

        public SqlExecution(IExecutionContext executionContext, ISqlConnectionProvider connectionProvider, string sql, DynamicParameters dyn, ILogger log)
        {
            _executionContext=executionContext;
            _sql=sql;
            _dyn=dyn;
            _commandType=CommandType.StoredProcedure;
            _log=log;
            _connectionProvider=connectionProvider;
        }

        public async Task Execute()
        {
            SqlExecutorBase executor = new SqlExecutorBase(_log);
            if (_connectionProvider!=null)
            {
                await executor.Execute(_connectionProvider, _executionContext, _sql, _dyn, _commandType);
            }
            else
            {
                await executor.Execute(_executionContext, _sql, _dyn, _commandType);
            }
        }

        public async Task<IEnumerable<T>> Query<T>()
        {
            SqlExecutorBase executor = new SqlExecutorBase(_log);
            IEnumerable<T> dbres = Enumerable.Empty<T>();
            if (_connectionProvider != null)
            {
                dbres=await executor.Query<T>(_connectionProvider, _executionContext, _sql, _dyn, _commandType);
            }
            else
            {
                dbres=await executor.Query<T>(_executionContext, _sql, _dyn, _commandType);
            }
            return dbres;
        }

        public async Task<T> FirstOrDefault<T>()
        {
            T res = default(T);
            SqlExecutorBase executor = new SqlExecutorBase(_log);
            IEnumerable<T> dbres = Enumerable.Empty<T>();
            if (_connectionProvider!=null)
            {
                dbres = await executor.Query<T>(_connectionProvider, _executionContext, _sql, _dyn, _commandType);
            }
            else
            {
                dbres = await executor.Query<T>(_executionContext, _sql, _dyn, _commandType);
            }
            if (dbres != null && dbres.Any())
            {
                res=dbres.FirstOrDefault();
            }
            return res;
        }
        public async Task QueryMultiple(Action<SqlMapper.GridReader> action)
        {
            SqlExecutorBase executor = new SqlExecutorBase(_log);
            if (_connectionProvider!=null)
            {
                await executor.QueryMultiple(_connectionProvider, _executionContext, _sql, _dyn, _commandType, action);
            }
            else
            {
                await executor.QueryMultiple(_executionContext, _sql, _dyn, _commandType, action);
            }
        }
        public async Task<T> ExecuteScalar<T>()
        {
            SqlExecutorBase executor = new SqlExecutorBase(_log);
            T res = default(T);
            if (_connectionProvider!=null)
            {
                res = await executor.ExecuteScalar<T>(_connectionProvider, _executionContext, _sql, _dyn, _commandType);
            }
            else
            {
                res = await executor.ExecuteScalar<T>(_executionContext, _sql, _dyn, _commandType);
            }
            return res;
        }
    }
}