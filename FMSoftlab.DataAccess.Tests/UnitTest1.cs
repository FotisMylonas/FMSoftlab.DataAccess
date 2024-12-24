using Dapper;
using FMSoftlab.DataAccess;
using System.Data;
using ExecutionContext = FMSoftlab.DataAccess.ExecutionContext;
namespace FMSoftlab.DataAccess.Tests
{
    public class UnitTest1
    {
        private readonly int CheckValue = 123456;
        private readonly string _connectionString = @"Server=(localdb)\MSSQLLocalDB;Database=tempdb;Trusted_Connection=True;TrustServerCertificate=True";

        [Fact]
        public async Task Select_ExecuteScalar_1()
        {
            int id = 0;
            ExecutionContext context = new ExecutionContext(_connectionString);
            using (ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString))
            {
                using (SingleTransactionManager tm = new SingleTransactionManager(con, context, null))
                {
                    tm.BeginTransaction();
                    DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
                    SqlExecution execution = new SqlExecution(context, tm, "Select @Id as Id", dyn, CommandType.Text, null);
                    id = await execution.ExecuteScalar<int>();
                    tm.Rollback();
                }
            }
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Select_ExecuteScalar_2()
        {
            ExecutionContext context = new ExecutionContext(_connectionString);
            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            SqlExecution execution = new SqlExecution(context, "Select @Id as Id", dyn, CommandType.Text, null);
            int id = await execution.ExecuteScalar<int>();
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Select_Query_1()
        {
            int id = 0;
            ExecutionContext context = new ExecutionContext(_connectionString);
            using (ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString))
            {
                using (SingleTransactionManager tm = new SingleTransactionManager(con, context, null))
                {
                    DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
                    tm.BeginTransaction();
                    SqlExecution execution = new SqlExecution(context, tm, "Select @Id as Id", dyn, CommandType.Text, null);
                    id = (await execution.Query<int>()).FirstOrDefault();
                    tm.Rollback();
                }
            }
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Select_Query_2()
        {
            ExecutionContext context = new ExecutionContext(_connectionString);
            ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString);
            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            SqlExecution execution = new SqlExecution(context, "Select @Id as Id", dyn, CommandType.Text, null);
            int id = (await execution.Query<int>()).FirstOrDefault();
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Select_FirstOrDefault_1()
        {
            ExecutionContext context = new ExecutionContext(_connectionString);
            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            SqlExecution execution = new SqlExecution(context, "Select @Id as Id", dyn, CommandType.Text, null);
            int id = await execution.FirstOrDefault<int>();
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Select_FirstOrDefault_2()
        {
            int id = 0;
            ExecutionContext context = new ExecutionContext(_connectionString);
            using (ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString))
            {
                using (SingleTransactionManager tm = new SingleTransactionManager(con, context, null))
                {
                    tm.BeginTransaction();
                    DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
                    SqlExecution execution = new SqlExecution(context, tm, "Select @Id as Id", dyn, CommandType.Text, null);
                    id = await execution.FirstOrDefault<int>();
                    tm.Rollback();
                }
            }
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Select_QueryMultiple_1()
        {
            int id = 0;
            ExecutionContext context = new ExecutionContext(_connectionString);
            using (ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString))
            {
                using (SingleTransactionManager tm = new SingleTransactionManager(con, context, null))
                {
                    DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
                    tm.BeginTransaction();
                    SqlExecution execution = new SqlExecution(context, tm, "Select @Id as Id", dyn, CommandType.Text, null);
                    await execution.QueryMultiple(async (x) =>
                    {
                        var dr = await x.ReadAsync<int>(buffered:false);
                        id=dr.FirstOrDefault<int>();
                    });
                    tm.Rollback();
                }
            }
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Execute_2()
        {
            ExecutionContext context = new ExecutionContext(_connectionString);
            using (ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString))
            {
                using (SingleTransactionManager tm = new SingleTransactionManager(con, context, null))
                {
                    DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
                    tm.BeginTransaction();
                    SqlExecution execution = new SqlExecution(context, tm, "Select @Id as Id", dyn, CommandType.Text, null);
                    await execution.Execute();
                    tm.Rollback();
                }
            }
        }

        [Fact]
        public async Task Select_QueryMultiple_2()
        {
            int id = 0;
            ExecutionContext context = new ExecutionContext(_connectionString);
            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            SqlExecution execution = new SqlExecution(context, "Select @Id as Id", dyn, CommandType.Text, null);
            await execution.QueryMultiple(async (x) =>
            {
                var dr = await x.ReadAsync<int>(buffered: false);
                id=dr.FirstOrDefault<int>();
            });
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Execute_1()
        {
            int id1 = 0;
            int id2 = 0;
            ExecutionContext context = new ExecutionContext(_connectionString);
            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            using (ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString))
            {
                using (SingleTransactionManager tm = new SingleTransactionManager(con, context, null))
                {
                    tm.BeginTransaction();
                    await new SqlExecution(context, tm, "create table ##temptable(Id int);insert into ##temptable(Id) values(10);", null, CommandType.Text, null).Execute();
                    id1 = await new SqlExecution(context, tm, "select id from ##temptable", null, CommandType.Text, null).FirstOrDefault<int>();
                    await new SqlExecution(context, tm, "update t set t.id=@Id from ##temptable t;", dyn, CommandType.Text, null).Execute();
                    id2 = await new SqlExecution(context, tm, "select t.id from ##temptable t where id=@Id", dyn, CommandType.Text, null).FirstOrDefault<int>();
                    tm.Rollback();
                }
            }
            Assert.Equal(10, id1);
            Assert.Equal(CheckValue, id2);
        }

        [Fact]
        public async Task Execute_Reader()
        {
            int id1 = 0;
            ExecutionContext context = new ExecutionContext(_connectionString);
            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            using (ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString))
            {
                using (SingleTransactionManager tm = new SingleTransactionManager(con, context, null))
                {
                    tm.BeginTransaction();
                    await new SqlExecution(context, tm, "create table ##temptable(Id int);insert into ##temptable(Id) values(10);", null, CommandType.Text, null).Execute();
                    using (IDataReader read = await new SqlExecution(context, tm, "select id from ##temptable", null, CommandType.Text, null).ExecuteReader())
                    {
                        while (read.Read())
                        {
                            id1 = read.GetInt32(0);
                        }
                    }
                    tm.Rollback();
                }
            }
            Assert.Equal(10, id1);
        }

    }
}