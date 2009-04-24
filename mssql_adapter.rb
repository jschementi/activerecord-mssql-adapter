require 'active_record/connection_adapters/abstract_adapter'
require 'mscorlib'
require 'System.Data'
 
module System
  class DBNull
    def nil?
      true
    end
  end
end
 
module ActiveRecord
  
  class Base
    
    # Establishes a connection to the database that's used by all Active Record objects
    def self.mssql_connection(config) # :nodoc:
      config = config.symbolize_keys
      if config.has_key?(:connection_string)
        connection_string = config[:connection_string]
      else
        builder = System::Data::SqlClient::SqlConnectionStringBuilder.new
        builder.data_source = config[:host]
        builder.integrated_security = config[:integrated_security]
        if not builder.integrated_security
          builder.UserID = config[:username].to_s
          builder.password = config[:password].to_s
        end
        builder.initial_catalog = config[:database]
        connection_string = builder.connection_string
      end
      connection = System::Data::SqlClient::SqlConnection.new connection_string
      ConnectionAdapters::MSSQLAdapter.new(connection, logger, config)
    end
 
  end
 
  module ConnectionAdapters
    
    #
    # The IronRuby MSSQL adapter works with System.Data.SqlClient
    #
    class MSSQLAdapter < AbstractAdapter
      
      # Returns 'MSSQL' as adapter name for identification purposes.
      def adapter_name
        'MSSQL'
      end
 
      # Initializes and connects a MSSQL adapter.
      def initialize(connection, logger, config)
        super(connection, logger)
        @config = config
        @transaction = nil
        connect
      end
 
      # Is this connection alive and ready for queries?
      def active?
        @connection.state == System::Data::ConnectionState::open
      end
 
      # Close then reopen the connection.
      def reconnect!
        if active?
          disconnect
        end
        connect
      end
 
      # Close the connection.
      def disconnect!
        @connection.close rescue nil
      end
 
      def native_database_types #:nodoc:
        {
          :primary_key => "int not null identity primary key",
          :string => { :name => "varchar", :limit => 8000 },
          :text => { :name => "text" },
          :integer => { :name => "int" },
          :float => { :name => "float" },
          :decimal => { :name => "numeric" },
          :datetime => { :name => "datetime" },
          :timestamp => { :name => "datetime" },
          :time => { :name => "datetime" },
          :date => { :name => "datetime" },
          :binary => { :name => "image" },
          :boolean => { :name => "bit" }
        }
      end
 
      # Does MSSQL support migrations?
      def supports_migrations?
        true
      end
 
      # Does MSSQL support standard conforming strings?
      def supports_standard_conforming_strings?
        true
      end
 
      # Returns the configured supported identifier length supported by MSSQL,
      def table_alias_length
        @table_alias_length ||= (query("select length from systypes where name = 'sysname'")[0][0].to_i)
      end
 
      # QUOTING ==================================================
 
      # Quotes MSSQL-specific data types for SQL input.
      def quote(value, column = nil) #:nodoc:
        if value.kind_of?(String) && column && column.type == :binary
          "#{quoted_string_prefix}'#{column.class.string_to_binary(value)}'"
        elsif value.kind_of?(String) && column && column.sql_type =~ /^xml$/
          "xml '#{quote_string(value)}'"
        elsif value.kind_of?(Numeric) && column && column.sql_type =~ /^money$/
          # Not truly string input, so doesn't require (or allow) escape string syntax.
          "'#{value.to_s}'"
        elsif value.kind_of?(String) && column && column.sql_type =~ /^bit/
          case value
            when /^[01]*$/
              "B'#{value}'" # Bit-string notation
            when /^[0-9A-F]*$/i
              "X'#{value}'" # Hexadecimal notation
          end
        else
          super
        end
      end
 
      # Double any single-quote characters in the string
      def quote_string(s) #:nodoc:
        s.gsub(/'/, "''") # ' (for ruby-mode)
      end
 
      # Quotes column names for use in SQL queries.
      def quote_column_name(name) #:nodoc:
        '[' + name.to_s + ']'
      end
      
      # Quotes table names for use in SQL queries.
      def quote_table_name(name)
        '[' + name.to_s + ']'
      end
 
      # Quote date/time values for use in SQL input. Includes microseconds
      # if the value is a Time responding to usec.
      def quoted_date(value) #:nodoc:
        if value.acts_like?(:time) && value.respond_to?(:usec)
          "#{super}.#{sprintf("%06d", value.usec)}"
        else
          super
        end
      end
 
      # REFERENTIAL INTEGRITY ====================================
 
      def disable_referential_integrity(&block) #:nodoc:
          execute(tables.collect { |name| "ALTER TABLE #{quote_table_name(name)} DISABLE TRIGGER ALL" }.join(";"))
        yield
      ensure
        execute(tables.collect { |name| "ALTER TABLE #{quote_table_name(name)} ENABLE TRIGGER ALL" }.join(";"))
      end
 
      # DATABASE STATEMENTS ======================================
 
      # Executes a SELECT query and returns an array of rows. Each row is an
      # array of field values.
      def select_rows(sql, name = nil)
        select_raw(sql, name).last
      end
 
      # Executes an INSERT query and returns the new record's ID
      def insert(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil)
        table = sql.split(" ", 4)[2]
        super || last_insert_id(table, sequence_name || default_sequence_name(table, pk))
      end
 
      # Queries the database and returns the results in an Array or nil otherwise.
      def query(sql, name = nil) #:nodoc:
        #log(sql, name) do
        #TODO: @async
        select_rows sql, name
      end
 
      # Executes an SQL statement
      def execute(sql, name = nil)
        #log(sql, name) do
        # TODO: @async
        begin
          command = System::Data::SqlClient::SqlCommand.new sql, @connection
          command.transaction = @transaction
          command.execute_non_query
        rescue System::Data::SqlClient::SqlException
          raise ActiveRecord::StatementInvalid, "#{$!}"
        end
      end
 
      # Executes an UPDATE query and returns the number of affected tuples.
      def update_sql(sql, name = nil)
        super
      end
 
      def add_limit_offset!(sql, options)
        if options[:limit] and options[:offset]
          total_rows = @connection.select_all("SELECT count(*) as TotalRows from (#{sql.gsub(/\bSELECT(\s+DISTINCT)?\b/i, "SELECT#{$1} TOP 1000000000")}) tally")[0][:TotalRows].to_i
          if (options[:limit] + options[:offset]) >= total_rows
            options[:limit] = (total_rows - options[:offset] >= 0) ? (total_rows - options[:offset]) : 0
          end
          sql.sub!(/^\s*SELECT(\s+DISTINCT)?/i, "SELECT * FROM (SELECT TOP #{options[:limit]} * FROM (SELECT#{$1} TOP #{options[:limit] + options[:offset]} ")
          sql << ") AS tmp1"
          if options[:order]
            options[:order] = options[:order].split(',').map do |field|
              parts = field.split(" ")
              tc = parts[0]
              if sql =~ /\.\[/ and tc =~ /\./ # if column quoting used in query
                tc.gsub!(/\./, '\\.\\[')
                tc << '\\]'
              end
              if sql =~ /#{tc} AS (t\d_r\d\d?)/
                parts[0] = $1
              elsif parts[0] =~ /\w+\.(\w+)/
                parts[0] = $1
              end
              parts.join(' ')
            end.join(', ')
            sql << " ORDER BY #{change_order_direction(options[:order])}) AS tmp2 ORDER BY #{options[:order]}"
          else
            sql << " ) AS tmp2"
          end
        elsif sql !~ /^\s*SELECT (@@|COUNT\()/i
          sql.sub!(/^\s*SELECT(\s+DISTINCT)?/i) do
            "SELECT#{$1} TOP #{options[:limit]}"
          end unless options[:limit].nil?
        end
      end
 
      # Begins a transaction.
      def begin_db_transaction
        @transaction = @connection.begin_transaction
      end
 
      # Commits a transaction.
      def commit_db_transaction
        @transaction.commit
        @transaction = nil
      end
 
      # Aborts a transaction.
      def rollback_db_transaction
        @transaction.rollback
        @transaction = nil
      end
 
      # SCHEMA STATEMENTS ========================================
 
      # Returns the list of all tables in the schema search path or a specified schema.
      def tables(name = nil)
        select_rows(<<-SQL, name).map { |row| row[0] }
SELECT name
FROM sysobjects
WHERE type = 'U'
SQL
      end
 
      # Returns the list of all indexes for a table.
      def indexes(table_name, name = nil)
        result = query("exec sp_helpindex '#{table_name}'", name)
 
        indexes = []
        result.each do |row|
          if row[1].match('primary key') == nil
            indexes << IndexDefinition.new(table_name, row[0], row[1].match('unique') != nil, row[2].split(',').each {|x| x.strip!})
          end
        end
 
        indexes
      end
 
      # Returns the list of all column definitions for a table.
      def columns(table_name, name = nil)
        # Limit, precision, and scale are all handled by the superclass.
        column_definitions(table_name).collect do |name, type, default, notnull|
          notnull = [false, true][notnull]
          Column.new(name, default, type, notnull)
        end
      end
 
      # Sets the schema search path to a string of comma-separated schema names.
      # Names beginning with $ have to be quoted (e.g. $user => '$user').
      #
      # This should be not be called manually but set in database.yml.
      def schema_search_path=(schema_csv)
      end
 
      # Returns the active schema search path.
      def schema_search_path
        'dbo'
      end
 
      # Renames a table.
      def rename_table(name, new_name)
        execute "exec sp_rename '#{name}', '#{new_name}'"
      end
 
      # Adds a column to a table.
      def add_column(table_name, column_name, type, options = {})
        if options_include_default?(options)
          default = 'default ' + quote(options[:default])
        else
          default = ''
        end
        notnull = options[:null] == false
 
        # Add the column.
        execute("ALTER TABLE #{table_name} ADD #{quote_column_name(column_name)} #{type_to_sql(type, options[:limit])} #{notnull ? 'NOT NULL' : 'NULL'} #{default}")
      end
 
      # Changes the column of a table.
      def change_column(table_name, column_name, type, options = {})
        begin
          execute "ALTER TABLE #{table_name} ALTER COLUMN #{quote_column_name(column_name)} #{type_to_sql(type, options[:limit], options[:precision], options[:scale])}"
        rescue ActiveRecord::StatementInvalid
          # This is PostgreSQL 7.x, so we have to use a more arcane way of doing it.
          begin_db_transaction
          tmp_column_name = "#{column_name}_ar_tmp"
          add_column(table_name, tmp_column_name, type, options)
          execute "UPDATE #{table_name} SET #{quote_column_name(tmp_column_name)} = CAST(#{quote_column_name(column_name)} AS #{type_to_sql(type, options[:limit], options[:precision], options[:scale])})"
          remove_column(table_name, column_name)
          rename_column(table_name, tmp_column_name, column_name)
          commit_db_transaction
        end
 
        change_column_default(table_name, column_name, options[:default]) if options_include_default?(options)
        change_column_null(table_name, column_name, options[:null], options[:default]) if options.key?(:null)
      end
 
      # Changes the default value of a table column.
      def change_column_default(table_name, column_name, default)
        print "change_column_default to '#{default}'\n"
        execute "ALTER TABLE #{table_name} ALTER COLUMN #{quote_column_name(column_name)} SET DEFAULT #{quote(default)}"
      end
 
      def change_column_null(table_name, column_name, null, default = nil)
        unless null || default.nil?
          execute("UPDATE #{table_name} SET #{quote_column_name(column_name)}=#{quote(default)} WHERE #{quote_column_name(column_name)} IS NULL")
        end
        execute("ALTER TABLE #{table_name} ALTER #{quote_column_name(column_name)} #{null ? 'NULL' : 'NOT NULL'}")
      end
 
      # Renames a column in a table.
      def rename_column(table_name, column_name, new_column_name)
        execute "exec sp_rename '#{table_name}.#{column_name}', '#{new_column_name}'"
      end
 
      # Drops an index from a table.
      def remove_index(table_name, options = {})
        execute "DROP INDEX #{index_name(table_name, options)}"
      end
 
      # Maps logical Rails types to MSSQL-specific data types.
      def type_to_sql(type, limit = nil, precision = nil, scale = nil)
        return super unless type.to_s == 'integer'
 
        if limit.nil? || limit == 4
          'integer'
        elsif limit < 4
          'smallint'
        else
          'bigint'
        end
      end
      
      protected
        # Returns the version of the connected SQL Server.
        def mssql_version
          @mssql_version ||=
              begin
                query('SELECT @@version')[0][0] =~ /(\d+)\.(\d+)\.(\d+).(\d+)/
                [$1, $2, $3, $4]
              rescue
                [0, 0, 0, 0]
              end
        end
 
      private
 
        # Connects to SQL Server
        def connect
          @connection.open
        end
 
        # Returns the current ID of a table's sequence.
        def last_insert_id(table, sequence_name) #:nodoc:
          identity = select_value("SELECT scope_identity()")
          if identity.class == System::DBNull
            nil
          else
            System::Convert.to_int32(identity)
          end
        end
 
        # Executes a SELECT query and returns the results, performing any data type
        # conversions that are required to be performed here
        def select(sql, name = nil)
          fields, rows = select_raw(sql, name)
          result = []
          for row in rows
            row_hash = {}
            fields.each_with_index do |f, i|
              row_hash[f] = row[i]
            end
            result << row_hash
          end
          result
        end
 
        def select_raw(sql, name = nil)
          reader = nil
          begin
            command = System::Data::SqlClient::SqlCommand.new sql, @connection
   command.transaction =   @transaction
            reader = command.execute_reader
            fields = []
            schema = reader.get_schema_table
            if schema != nil
              for row in reader.get_schema_table.rows
                fields << row.item(0)
              end
            end
  
            rows = []
            while reader.read
              row = []
              for i in (1..fields.length)
                value = reader.get_value(i - 1)
                if value.class == System::String
                  value = value.to_s
                end
                row << value
              end
              rows << row
            end
            return fields, rows
          rescue System::Data::SqlClient::SqlException
            raise ActiveRecord::StatementInvalid, "#{$!}"
          ensure
            if reader != nil
              reader.close
            end
          end
        end
 
 
        # Returns the list of a table's column names, data types, and default values.
        #
        def column_definitions(table_name) #:nodoc:
          query <<-end_sql
select
c.name,
case
when t.name in ('char', 'varchar', 'nchar', 'nvarchar') then 'string'
when t.name in ('binary', 'varbinary', 'image') then 'binary'
when t.name in ('int', 'smallint', 'tinyint') then 'integer'
when t.name in ('datetime', 'smalldatetime') then 'datetime'
when t.name = 'bit' then 'boolean'
when t.name = 'numeric' and c.prec < 10 and c.scale = 0 then 'integer'
when t.name = 'numeric' then 'decimal'
when t.name = 'text' then 'text'
else t.name
end type,
d.text,
   c.isnullable
from
syscolumns c
inner join systypes t
on c.xusertype = t.xusertype
left outer join syscomments d
on c.cdefault = d.id
where
c.id = object_id('#{table_name}')
order by
c.colid
end_sql
        end
    end
  end
end