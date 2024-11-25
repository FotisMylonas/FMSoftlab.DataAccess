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
using System.Transactions;


namespace FMSoftlab.DataAccess
{

    /*public class SqlExecutorBase
    {
        private ILogger _log;
        public SqlExecutorBase(ILogger log)
        {
            _log=log;
        }
        public async Task Execute(SingleTransactionManager transactionManager, IExecutionContext executionContext, string sql, DynamicParameters dyn, CommandType commandType)
        {
            //_log?.LogDebug($"Execute in, ConnectionString:{executionContext.ConnectionString}, Sql:{sql}, CommandTimeout:{executionContext.CommandTimeout}, IsolationLevel:{executionContext.IsolationLevel}");
            try
            {
                transactionManager.BeginTransaction();
                await transactionManager.Execute(async (connection, transaction) =>
                {
                    await connection.ExecuteAsync(sql, dyn, commandTimeout: executionContext.CommandTimeout, transaction: transaction, commandType: commandType);
                    transactionManager.Commit();
                });
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
                transactionManager.BeginTransaction();
                await transactionManager.Execute(async (connection, transaction) =>
                {
                    await connection.ExecuteAsync(sql, dyn, commandTimeout: executionContext.CommandTimeout, transaction: transaction, commandType: commandType);
                    transactionManager.Commit();
                });
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
    }*/

    public class SqlExecution
    {
        private readonly ISingleTransactionManager _singleTransactionManager;
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

        public SqlExecution(IExecutionContext executionContext, ISingleTransactionManager singleTransactionManager, string sql, DynamicParameters dyn, CommandType commandType, ILogger log)
        {
            _executionContext=executionContext;
            _sql=sql;
            _dyn=dyn;
            _commandType=commandType;
            _log=log;
            _singleTransactionManager = singleTransactionManager;
        }

        public SqlExecution(IExecutionContext executionContext, ISingleTransactionManager singleTransactionManager, string sql, DynamicParameters dyn, ILogger log)
        {
            _executionContext=executionContext;
            _sql=sql;
            _dyn=dyn;
            _commandType=CommandType.StoredProcedure;
            _log=log;
            _singleTransactionManager=singleTransactionManager;
        }

        public async Task Execute()
        {
            ISingleTransactionManager tm = _singleTransactionManager ?? new SingleTransactionManager(new SqlConnectionProvider(_executionContext.ConnectionString), _executionContext, _log);
            try
            {
                await tm.Execute(_sql, _dyn, async (connection, transaction) =>
                {
                    await connection.ExecuteAsync(
                        _sql,
                        _dyn,
                        commandTimeout: _executionContext.CommandTimeout,
                        transaction: transaction,
                        commandType: _commandType);
                });
            }
            finally
            {
                if (_singleTransactionManager==null)
                {
                    tm.Dispose();
                }
            }
        }

        public async Task<IEnumerable<T>> Query<T>()
        {
            IEnumerable<T> res = Enumerable.Empty<T>();
            ISingleTransactionManager tm = _singleTransactionManager ?? new SingleTransactionManager(new SqlConnectionProvider(_executionContext.ConnectionString), _executionContext, _log);
            try
            {
                await tm.Execute(_sql, _dyn, async (connection, transaction) =>
                {
                    res = await connection.QueryAsync<T>(
                        _sql,
                        _dyn,
                        commandTimeout: _executionContext.CommandTimeout,
                        transaction: transaction,
                        commandType: _commandType);
                });
                return res;
            }
            finally
            {
                if (_singleTransactionManager==null)
                {
                    tm.Dispose();
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
            ISingleTransactionManager tm = _singleTransactionManager ?? new SingleTransactionManager(new SqlConnectionProvider(_executionContext.ConnectionString), _executionContext, _log);
            try
            {
                await tm.Execute(_sql, _dyn, async (connection, transaction) =>
                {
                    var reader = await connection.QueryMultipleAsync(
                        _sql,
                        _dyn,
                        commandTimeout: _executionContext.CommandTimeout,
                        transaction: transaction,
                        commandType: _commandType);
                    action(reader);
                });
            }
            finally
            {
                if (_singleTransactionManager==null)
                {
                    tm.Dispose();
                }
            }
        }
        public async Task<T> ExecuteScalar<T>()
        {
            T res = default(T);
            ISingleTransactionManager tm = _singleTransactionManager ?? new SingleTransactionManager(new SqlConnectionProvider(_executionContext.ConnectionString), _executionContext, _log);
            try
            {
                await tm.Execute(_sql, _dyn, async (connection, transaction) =>
                {
                    res = await connection.ExecuteScalarAsync<T>(
                        _sql,
                        _dyn,
                        commandTimeout: _executionContext.CommandTimeout,
                        transaction: transaction,
                        commandType: _commandType);
                });
                return res;
            }
            finally
            {
                if (_singleTransactionManager==null)
                {
                    tm.Dispose();
                }
            }
        }
    }
}