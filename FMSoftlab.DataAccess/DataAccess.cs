using Dapper;
using FMSoftlab.Logging;
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
    public class SqlExecution
    {
        private readonly bool _ownsTransaction;
        private readonly ISingleTransactionManager _singleTransactionManager;
        private readonly string _sql;
        private readonly object _parameters;
        private readonly CommandType _commandType;
        private readonly IExecutionContext _executionContext;
        private readonly ILogger _log;

        public SqlExecution(IExecutionContext executionContext, string sql, object parameters, CommandType commandType, ILogger log)
        {
            _executionContext=executionContext;
            _sql=sql;
            _parameters=parameters;
            _commandType=commandType;
            _log=log;
            _ownsTransaction=true;
            _singleTransactionManager=new SingleTransactionManager(executionContext, log);
        }
        public SqlExecution(IExecutionContext executionContext, string sql, object parameters, ILogger log) : this(executionContext, sql, parameters, CommandType.StoredProcedure, log) { }

        public SqlExecution(IExecutionContext executionContext, ISingleTransactionManager singleTransactionManager, string sql, object parameters, CommandType commandType, ILogger log)
        {
            _executionContext=executionContext;
            _sql=sql;
            _parameters=parameters;
            _commandType=commandType;
            _log=log;
            _singleTransactionManager = singleTransactionManager;
            _ownsTransaction=false;
        }

        public SqlExecution(IExecutionContext executionContext, ISingleTransactionManager singleTransactionManager, string sql, object parameters, ILogger log) : this(executionContext, singleTransactionManager, sql, parameters, CommandType.StoredProcedure, log) { }

        public async Task Execute()
        {
            try
            {
                await _singleTransactionManager.Execute(_ownsTransaction, _sql, _parameters, async (connection, transaction) =>
                {
                    await connection.ExecuteAsync(
                        _sql,
                        _parameters,
                        commandTimeout: _executionContext.CommandTimeout,
                        transaction: transaction,
                        commandType: _commandType);
                });
            }
            finally
            {
                if (_ownsTransaction)
                {
                    _singleTransactionManager.Dispose();
                }
            }
        }
        public async Task<IEnumerable<T>> Query<T>()
        {
            IEnumerable<T> res = Enumerable.Empty<T>();
            try
            {
                await _singleTransactionManager.Execute(_ownsTransaction, _sql, _parameters, async (connection, transaction) =>
                {
                    res = await connection.QueryAsync<T>(
                        _sql,
                        _parameters,
                        commandTimeout: _executionContext.CommandTimeout,
                        transaction: transaction,
                        commandType: _commandType);
                });
                return res;
            }
            finally
            {
                if (_ownsTransaction)
                {
                    _singleTransactionManager.Dispose();
                }
            }
        }
        public async Task<IEnumerable<dynamic>> Query()
        {
            return await Query<dynamic>();
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
            try
            {
                await _singleTransactionManager.Execute(_ownsTransaction, _sql, _parameters, async (connection, transaction) =>
                {
                    var reader = await connection.QueryMultipleAsync(
                        _sql,
                        _parameters,
                        commandTimeout: _executionContext.CommandTimeout,
                        transaction: transaction,
                        commandType: _commandType);
                    action(reader);
                });
            }
            finally
            {
                if (_ownsTransaction)
                {
                    _singleTransactionManager.Dispose();
                }
            }
        }
        public async Task<T> ExecuteScalar<T>()
        {
            T res = default(T);
            try
            {
                await _singleTransactionManager.Execute(_ownsTransaction, _sql, _parameters, async (connection, transaction) =>
                {
                    res = await connection.ExecuteScalarAsync<T>(
                        _sql,
                        _parameters,
                        commandTimeout: _executionContext.CommandTimeout,
                        transaction: transaction,
                        commandType: _commandType);
                });
                return res;
            }
            finally
            {
                if (_ownsTransaction)
                {
                    _singleTransactionManager.Dispose();
                }
            }
        }
        public async Task<object> ExecuteScalar()
        {
            return await ExecuteScalar<object>();
        }
    }
}