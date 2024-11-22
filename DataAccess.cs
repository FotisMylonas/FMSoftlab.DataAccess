using Dapper;
using FmSoftlab.Logging;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;


namespace FMSoftlab.DataAccess
{

    public class SqlExecutorBase
    {
        private ILogger _log;
        public SqlExecutorBase(ILogger log)
        {
            _log=log;
        }
        public async Task Execute(ISqlConnectionProvider connectionProvider, IExecutionContext executionContext, string sql, DynamicParameters dyn, CommandType commandType)
        {
            //_log?.LogDebug($"Execute in, ConnectionString:{executionContext.ConnectionString}, Sql:{sql}, CommandTimeout:{executionContext.CommandTimeout}, IsolationLevel:{executionContext.IsolationLevel}");
            try
            {
                if (executionContext.BeginTransaction)
                {
                    using (var trans = connectionProvider.BeginTransaction(executionContext.IsolationLevel))
                    {
                        await connectionProvider.Connection.ExecuteAsync(sql, dyn, commandTimeout: executionContext.CommandTimeout, transaction: trans, commandType: commandType);
                        trans.Commit();
                    }
                }
                else
                {
                    await connectionProvider.Connection.ExecuteAsync(sql, dyn, commandTimeout: executionContext.CommandTimeout, commandType: commandType);
                }
            }
            catch (Exception ex)
            {
                _log?.LogAllErrors(ex);
                throw;
            }
        }
        public async Task<T> ExecuteScalar<T>(ISqlConnectionProvider connectionProvider, IExecutionContext executionContext, string sql, DynamicParameters dyn, CommandType commandType)
        {
            //_log?.LogDebug($"Execute in, ConnectionString:{executionContext.ConnectionString}, Sql:{sql}, CommandTimeout:{executionContext.CommandTimeout}, IsolationLevel:{executionContext.IsolationLevel}");
            T res = default(T);
            try
            {
                if (executionContext.BeginTransaction)
                {
                    using (var trans = connectionProvider.BeginTransaction(executionContext.IsolationLevel))
                    {
                        res=await connectionProvider.Connection.ExecuteScalarAsync<T>(sql, dyn, commandTimeout: executionContext.CommandTimeout, transaction: trans, commandType: commandType);
                        trans.Commit();
                    }
                }
                else
                {
                    res=await connectionProvider.Connection.ExecuteScalarAsync<T>(sql, dyn, commandTimeout: executionContext.CommandTimeout, commandType: commandType);
                }
            }
            catch (Exception ex)
            {
                _log?.LogAllErrors(ex);
                throw;
            }
            return res;
        }
        public async Task<IEnumerable<T>> Query<T>(ISqlConnectionProvider connectionProvider, IExecutionContext executionContext, string sql, DynamicParameters dyn, CommandType commandType)
        {
            IEnumerable<T> result = Enumerable.Empty<T>();
            //_log?.LogDebug($"Execute in, ConnectionString:{executionContext.ConnectionString}, Sql:{sql}, CommandTimeout:{executionContext.CommandTimeout}, IsolationLevel:{executionContext.IsolationLevel}");
            try
            {
                if (executionContext.BeginTransaction)
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
                else
                {
                    var dbres = await connectionProvider.Connection.QueryAsync<T>(sql, dyn, commandTimeout: executionContext.CommandTimeout, commandType: commandType);
                    if (dbres != null && dbres.Any())
                    {
                        result = dbres;
                    }
                }
                return result;
            }
            catch (Exception ex)
            {
                _log?.LogAllErrors(ex);
                throw;
            }
        }
        public async Task QueryMultiple(ISqlConnectionProvider connectionProvider, IExecutionContext executionContext, string sql, DynamicParameters dyn, CommandType commandType, Action<SqlMapper.GridReader> action)
        {
            //_log?.LogDebug($"Execute in, ConnectionString:{executionContext.ConnectionString}, Sql:{sql}, CommandTimeout:{executionContext.CommandTimeout}, IsolationLevel:{executionContext.IsolationLevel}");
            try
            {
                if (executionContext.BeginTransaction)
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
                else
                {
                    SqlMapper.GridReader gridReader = await connectionProvider.Connection.QueryMultipleAsync(sql, dyn, commandTimeout: executionContext.CommandTimeout, commandType: commandType);
                    if (gridReader != null)
                    {
                        action(gridReader);
                    }
                }
            }
            catch (Exception ex)
            {
                _log?.LogAllErrors(ex);
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
            ISqlConnectionProvider provider = _connectionProvider ?? new SqlConnectionProvider(
                       _executionContext.ConnectionString,
                       _executionContext.LogServerMessages,
                       _log);
            try
            {
                await executor.Execute(_connectionProvider, _executionContext, _sql, _dyn, _commandType);
            }
            finally
            {
                if (_connectionProvider == null)
                {
                    provider?.Dispose();
                }
            }
        }

        public async Task<IEnumerable<T>> Query<T>()
        {
            SqlExecutorBase executor = new SqlExecutorBase(_log);
            ISqlConnectionProvider provider = _connectionProvider ?? new SqlConnectionProvider(
                       _executionContext.ConnectionString,
                       _executionContext.LogServerMessages,
                       _log);
            try
            {
                return await executor.Query<T>(provider, _executionContext, _sql, _dyn, _commandType)??Enumerable.Empty<T>();
            }
            finally
            {
                if (_connectionProvider == null)
                {
                    provider?.Dispose();
                }
            }
        }
        public async Task<T> FirstOrDefault<T>()
        {
            T res = default(T);
            var dbres = await Query<T>();
            if (dbres?.Any()??false)
                res=dbres.First();
            return res;
        }
        public async Task QueryMultiple(Action<SqlMapper.GridReader> action)
        {
            SqlExecutorBase executor = new SqlExecutorBase(_log);
            ISqlConnectionProvider provider = _connectionProvider ?? new SqlConnectionProvider(
                       _executionContext.ConnectionString,
                       _executionContext.LogServerMessages,
                       _log);
            try
            {
                await executor.QueryMultiple(_connectionProvider, _executionContext, _sql, _dyn, _commandType, action);
            }
            finally
            {
                if (_connectionProvider == null)
                {
                    provider?.Dispose();
                }
            }
        }
        public async Task<T> ExecuteScalar<T>()
        {
            SqlExecutorBase executor = new SqlExecutorBase(_log);
            ISqlConnectionProvider provider = _connectionProvider ?? new SqlConnectionProvider(
                       _executionContext.ConnectionString,
                       _executionContext.LogServerMessages,
                       _log);
            try
            {
                return await executor.ExecuteScalar<T>(_connectionProvider, _executionContext, _sql, _dyn, _commandType);
            }
            finally
            {
                if (_connectionProvider == null)
                {
                    provider?.Dispose();
                }
            }
        }
    }
}