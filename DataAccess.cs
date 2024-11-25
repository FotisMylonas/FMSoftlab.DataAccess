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
    public class SqlExecution
    {
        private readonly bool _startsTransaction;
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
            _startsTransaction=true;
        }
        public SqlExecution(IExecutionContext executionContext, string sql, DynamicParameters dyn, ILogger log) : this(executionContext, sql, dyn, CommandType.StoredProcedure, log) { }

        public SqlExecution(IExecutionContext executionContext, ISingleTransactionManager singleTransactionManager, string sql, DynamicParameters dyn, CommandType commandType, ILogger log)
        {
            _executionContext=executionContext;
            _sql=sql;
            _dyn=dyn;
            _commandType=commandType;
            _log=log;
            _singleTransactionManager = singleTransactionManager;
            _startsTransaction=false;
        }

        public SqlExecution(IExecutionContext executionContext, ISingleTransactionManager singleTransactionManager, string sql, DynamicParameters dyn, ILogger log) : this(executionContext, singleTransactionManager, sql, dyn, CommandType.StoredProcedure, log) { }

        public async Task Execute()
        {
            ISingleTransactionManager tm = _singleTransactionManager ?? new SingleTransactionManager(new SqlConnectionProvider(_executionContext.ConnectionString), _executionContext, _log);
            try
            {
                await tm.Execute(_startsTransaction, _sql, _dyn, async (connection, transaction) =>
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
                if (_startsTransaction)
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
                await tm.Execute(_startsTransaction, _sql, _dyn, async (connection, transaction) =>
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
                if (_startsTransaction)
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
                await tm.Execute(_startsTransaction, _sql, _dyn, async (connection, transaction) =>
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
                if (_startsTransaction)
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
                await tm.Execute(_startsTransaction, _sql, _dyn, async (connection, transaction) =>
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
                if (_startsTransaction)
                {
                    tm.Dispose();
                }
            }
        }
    }
}