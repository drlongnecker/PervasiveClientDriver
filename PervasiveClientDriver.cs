using System;
using System.Data;
using System.Data.Common;
using NHibernate.Dialect.Schema;
using NHibernate.Driver;
using Environment = NHibernate.Cfg.Environment;

namespace FluentNHibernate.Cfg.Db
{
    public class PervasiveClientConfiguration : PersistenceConfiguration<PervasiveClientConfiguration, PervasiveConnectionStringBuilder>
    {
        protected PervasiveClientConfiguration()
        {
            Driver<PervasiveClientDriver>();
        }

        public static PervasiveClientConfiguration Pervasive10
        {
            get { return new PervasiveClientConfiguration().Dialect<PervasiveSqlDialect>(); }
        }
    }

    public class PervasiveConnectionStringBuilder : ConnectionStringBuilder
    {
        private string _server;
        private string _database;
        private string _username;
        private string _password;
        public PervasiveConnectionStringBuilder Server(string server)
        {
            this._server = server;
            IsDirty = true;
            return this;
        }
        public PervasiveConnectionStringBuilder Database(string database)
        {
            this._database = database;
            IsDirty = true;
            return this;
        }
        public PervasiveConnectionStringBuilder UserName(string username)
        {
            this._username = username;
            IsDirty = true;
            return this;
        }
        public PervasiveConnectionStringBuilder Password(string password)
        {
            this._password = password;
            IsDirty = true;
            return this;
        }

        protected override string Create()
        {
            var connectionString = base.Create();
            if (!string.IsNullOrWhiteSpace(connectionString))
                return connectionString;

            return string.Format("Server Name={0};Database Name={1};User ID={2};Password={3}", this._server, this._database,
                          this._username, this._password);
        }
    }

    public class PervasiveSqlDialect : NHibernate.Dialect.Dialect
    {
        public PervasiveSqlDialect()
        {
            // string types
            RegisterColumnType(DbType.AnsiStringFixedLength, "CHAR(255)");
            RegisterColumnType(DbType.AnsiStringFixedLength, 8000, "CHAR($l)");
            RegisterColumnType(DbType.AnsiStringFixedLength, 2147483647, "LONGVARCHAR");
            RegisterColumnType(DbType.AnsiString, "VARCHAR(255)");
            RegisterColumnType(DbType.AnsiString, 8000, "VARCHAR($l)");
            RegisterColumnType(DbType.AnsiString, 2147483647, "LONGVARCHAR");
            RegisterColumnType(DbType.StringFixedLength, "CHAR(255)");
            RegisterColumnType(DbType.StringFixedLength, 8000, "CHAR($l)");
            RegisterColumnType(DbType.StringFixedLength, 2147483647, "LONGVARCHAR");
            RegisterColumnType(DbType.String, "VARCHAR(255)");
            RegisterColumnType(DbType.String, 8000, "VARCHAR($l)");
            RegisterColumnType(DbType.String, 2147483647, "LONGVARCHAR");

            // numeric data types
            RegisterColumnType(DbType.Boolean, "BIT");
            RegisterColumnType(DbType.Byte, "UTINYINT");
            RegisterColumnType(DbType.SByte, "TINYINT");
            RegisterColumnType(DbType.UInt16, "USMALLINT");
            RegisterColumnType(DbType.Int16, "SMALLINT");
            RegisterColumnType(DbType.UInt32, "UINTEGER");
            RegisterColumnType(DbType.Int32, "INTEGER");
            RegisterColumnType(DbType.UInt64, "UBIGINT");
            RegisterColumnType(DbType.Int64, "BIGINT");
            RegisterColumnType(DbType.Single, "REAL");
            RegisterColumnType(DbType.Double, "DOUBLE");
            RegisterColumnType(DbType.Currency, "CURRENCY");
            RegisterColumnType(DbType.Decimal, "DECIMAL(19,5)");
            RegisterColumnType(DbType.Decimal, 19, "NUMERIC($p, $s)");

            // date/time data types
            RegisterColumnType(DbType.Date, "DATE");
            RegisterColumnType(DbType.DateTime, "TIMESTAMP");
            RegisterColumnType(DbType.Time, "TIME");

            // binary types
            RegisterColumnType(DbType.Binary, "LONGVARBINARY");
            RegisterColumnType(DbType.Binary, 8000, "BINARY");

            // other data types
            RegisterColumnType(DbType.Guid, "UNIQUEIDENTIFIER");

            DefaultProperties[Environment.ConnectionDriver] = "Db.Driver.PsqlDataDriver";
        }

        public override bool SupportsIdentityColumns
        {
            get { return true; }
        }

        public override string IdentitySelectString
        {
            get { return "SELECT @@IDENTITY"; }
        }

        public override string IdentityColumnString
        {
            get { return "IDENTITY"; }
        }

        public override bool HasDataTypeInIdentityColumn
        {
            get { return false; }
        }

        public override string AddColumnString
        {
            get { return "add column"; }
        }

        public override bool QualifyIndexName
        {
            get { return false; }
        }

        /// TODO: Need to add support for retrieving the database schema from Pervasive
        //public override IDataBaseSchema GetDataBaseSchema(DbConnection connection)
        //{
        //    throw new NotSupportedException("Retrieving the database schema is currently unsupported for Pervasive");
        //}

        public override string GetDropPrimaryKeyConstraintString(string constraintName)
        {
            return " drop primary key";
        }

        public override bool SupportsTemporaryTables
        {
            get { return true; }
        }

        public override string GenerateTemporaryTableName(string baseTableName)
        {
            return "#" + baseTableName;
        }
    }

    public class PervasiveClientDriver : ReflectionBasedDriver, ISqlParameterFormatter
    {
        public PervasiveClientDriver()
            : base(
            "Pervasive.Data.SqlClient",
            "Pervasive.Data.SqlClient.PsqlConnection",
            "Pervasive.Data.SqlClient.PsqlCommand")
        {
        }

        public override bool UseNamedPrefixInSql
        {
            get { return false; }
        }

        public override bool UseNamedPrefixInParameter
        {
            get { return false; }
        }

        public override string NamedPrefix
        {
            get { return "?"; }
        }

        string ISqlParameterFormatter.GetParameterName(int index)
        {
            return "?";
        }

        public override bool SupportsMultipleOpenReaders
        {
            get { return false; }
        }
    }
}