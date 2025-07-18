﻿using Dapper;
using FMSoftlab.Logging;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FMSoftlab.DataAccess
{
    public interface ISingleTransactionManager : IDisposable
    {
        void Commit();
        void Rollback();
        IDbTransaction BeginTransaction();
        Task<IDbTransaction> BeginTransactionAsync();
        Task Execute(bool startsTransaction, string sql, object parameters, Func<IDbConnection, IDbTransaction, Task> execute);
    }
    public class SingleTransactionManager : ISingleTransactionManager, IDisposable
    {
        private IDbTransaction _transaction;
        private ISqlConnectionProvider _connectionProvider;
        private IExecutionContext _executionContext;
        private ILogger _log;
        private readonly bool _ownsConnection;

        public SingleTransactionManager(ISqlConnectionProvider connectionProvider, IExecutionContext executionContext, ILogger log)
        {
            _connectionProvider=connectionProvider;
            _executionContext =executionContext;
            _transaction=null;
            _log =log;
            _ownsConnection=false;
        }
        public SingleTransactionManager(IExecutionContext executionContext, ILogger log)
        {
            _executionContext =executionContext;
            _transaction=null;
            _log =log;
            _connectionProvider=new SqlConnectionProvider(executionContext, _log);
            _ownsConnection=true;
        }

        public IDbTransaction BeginTransaction()
        {
            if (_transaction is null)
            {
                _transaction=_connectionProvider.BeginTransaction(_executionContext.IsolationLevel);
            }
            return _transaction;
        }
        public async Task<IDbTransaction> BeginTransactionAsync()
        {
            if (_transaction is null)
            {
                _transaction=await _connectionProvider.BeginTransactionAsync(_executionContext.IsolationLevel);
            }
            return _transaction;
        }
        public void Commit()
        {
            if (_transaction==null)
            {
                return;
            }
            try
            {
                try
                {
                    _transaction.Commit();
                    _log?.LogTrace("SingleTransactionManager, Transaction Committed!");
                }
                finally
                {
                    try
                    {
                        _transaction.Dispose();
                        _log?.LogTrace("SingleTransactionManager, Transaction disposed!");
                    }
                    finally
                    {
                        _transaction=null;
                    }
                }
            }
            catch (Exception ex)
            {
                _log?.LogAllErrors(ex);
                throw;
            }
        }
        public void Rollback()
        {
            if (_transaction==null)
            {
                return;
            }
            try
            {
                try
                {
                    _transaction.Rollback();
                    _log?.LogWarning("SingleTransactionManager, Transaction rollback!");
                }
                finally
                {
                    try
                    {
                        _transaction.Dispose();
                        _log?.LogTrace("SingleTransactionManager, Transaction disposed!");
                    }
                    finally
                    {
                        _transaction=null;
                    }
                }
            }
            catch (Exception ex)
            {
                _log?.LogAllErrors(ex);
                throw;
            }
        }
        public async Task Execute(bool newTransaction, string sql, object parameters, Func<IDbConnection, IDbTransaction, Task> execute)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return;
            try
            {
                _log?.LogTrace("SingleTransactionManager, newTransaction:{newTransaction}", newTransaction);
                if (newTransaction)
                    await BeginTransactionAsync();
                string tracesqltext = SqlHelperUtils.BuildFinalQuery(sql, parameters);
                _log?.LogTrace("SingleTransactionManager, Will execute sql, ConnectionString:{ConnectionString}, newTransaction:{newTransaction}, " +
                    "isolation level:{isolation}, ServerProcessId:{ServerProcessId}, clientconnectionid:{clientconnectionid}"+Environment.NewLine+
                    "sql:{sql}",
                    _connectionProvider.Connection.ConnectionString,
                    newTransaction,
                    _transaction?.IsolationLevel,
                    _connectionProvider.Connection.ServerProcessId,
                    _connectionProvider.Connection.ClientConnectionId,
                    tracesqltext);
                //_log?.LogDebug("{tracesqltext}", tracesqltext);
                await execute(_connectionProvider.Connection, _transaction);
                if (newTransaction)
                    Commit();
            }
            catch (Exception ex)
            {
                if (newTransaction)
                    Rollback();
                string tracesqltext = SqlHelperUtils.BuildFinalQuery(sql, parameters);
                _log?.LogAllErrors(ex, tracesqltext);
                throw;
            }
        }
        public void Dispose()
        {
            _log?.LogTrace("SingleTransactionManager, Disposing...");
            try
            {
                Rollback();
            }
            finally
            {
                if (_ownsConnection)
                {
                    _connectionProvider?.Dispose();
                }
            }
        }
    }
    public static class SqlHelperUtils
    {
        public static string BuildFinalQuery(string commandText, object parameters)
        {
            if (string.IsNullOrWhiteSpace(commandText))
                return string.Empty;

            if (parameters is null)
            {
                return commandText;
            }
            if (!(parameters is DynamicParameters sqlDynamicParams))
            {
                return commandText;
            }
            var sb = new StringBuilder();
            try
            {
                var propertyInfo = typeof(DynamicParameters).GetField("templates", BindingFlags.NonPublic | BindingFlags.Instance);
                if (propertyInfo != null)
                {
                    var templates = propertyInfo.GetValue(sqlDynamicParams) as IEnumerable<object>;
                    if (templates!=null)
                    {
                        foreach (object template in templates)
                        {
                            var properties = template.GetType().GetProperties();
                            foreach (var property in properties)
                            {
                                object value = property.GetValue(template);
                                string sqlType = GetSqlType(value);
                                string formattedValue = FormatValue(value);
                                sb.AppendLine($"DECLARE @{property.Name} {sqlType} = {formattedValue};");
                            }
                        }
                    }
                }
                foreach (var paramName in sqlDynamicParams.ParameterNames)
                {
                    object value = sqlDynamicParams.Get<object>(paramName);
                    string sqlType = GetSqlType(value);
                    string formattedValue = FormatValue(value);
                    sb.AppendLine($"DECLARE @{paramName} {sqlType} = {formattedValue};");
                }
            }
            catch (Exception)
            {

            }
            sb.AppendLine();
            sb.AppendLine(commandText);
            return sb.ToString();
        }

        private static string GetSqlType(object value)
        {
            if (value == null)
                return "NVARCHAR(MAX)";

            if (value is int)
                return "INT";
            if (value is long)
                return "BIGINT";
            if (value is short)
                return "SMALLINT";
            if (value is byte)
                return "TINYINT";
            if (value is bool)
                return "BIT";
            if (value is decimal)
                return "DECIMAL(18, 4)";
            if (value is double)
                return "FLOAT";
            if (value is float)
                return "REAL";
            if (value is DateTime)
                return "DATETIME";
            if (value is string)
                return "NVARCHAR(MAX)";

            return "NVARCHAR(MAX)"; // Fallback type
        }
        private static string FormatValue(object value)
        {
            if (value == null || value == DBNull.Value)
                return "NULL";

            if (value is string s)
                return $"'{s.Replace("'", "''")}'"; // Escape single quotes
            if (value is DateTime dt)
                return $"'{dt:yyyy-MM-dd HH:mm:ss.fff}'"; // Format DateTime
            if (value is bool b)
                return b ? "1" : "0"; // Convert boolean to SQL bit
            if (IsNumeric(value))
                return value.ToString(); // Leave numeric types as-is

            return $"'{value.ToString().Replace("'", "''")}'"; // Fallback for unknown types
        }
        private static bool IsNumeric(object value)
        {
            if (value == null)
                return false;

            return value is byte
                || value is sbyte
                || value is short
                || value is ushort
                || value is int
                || value is uint
                || value is long
                || value is ulong
                || value is float
                || value is double
                || value is decimal;
        }
    }
}