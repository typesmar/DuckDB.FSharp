namespace DuckDB.FSharp


open System
open System.Data
open System.Threading
open System.Threading.Tasks
open DuckDB.NET.Data


[<RequireQualifiedAccess>]
module Sql =

    type ExecutionTarget =
        | Connection of DuckDBConnection
        | ConnectionString of string
        | FileDataSource of path : string * readOnly : bool
        | InMemoryDataSource of name : string * sharedCache : bool

    type SqlProps =
        private
            {
                ExecutionTarget : ExecutionTarget
                SqlQuery : string list
                Parameters : (string * SqlValue) list
                IsFunction : bool
                NeedPrepare : bool
                CancellationToken : CancellationToken
            }

    let private defaultProps () =
        {
            ExecutionTarget = InMemoryDataSource ("default", true)
            SqlQuery = []
            Parameters = []
            IsFunction = false
            NeedPrepare = false
            CancellationToken = CancellationToken.None
        }

    let connect (constr : string) =
        { defaultProps () with
            ExecutionTarget = ConnectionString (constr)
        }

    let inMemory name shared con =
        { con with
            ExecutionTarget = InMemoryDataSource (name, shared)
        }

    let fileDB path readOnly con =
        { con with
            ExecutionTarget = FileDataSource (path, readOnly)
        }

    let connectFromConfig (connectionConfig : DuckDBConnectionStringBuilder) =
        connect (connectionConfig.ConnectionString)

    /// Uses an existing connection to execute SQL commands against
    let existingConnection (connection : DuckDBConnection) =
        { defaultProps () with
            ExecutionTarget = Connection connection
        }

    let cancellationToken token config =
        { config with
            CancellationToken = token
        }


    /// Configures the SQL query to execute
    let query (sql : string) props = { props with SqlQuery = [ sql ] }

    let func (sql : string) props =
        { props with
            SqlQuery = [ sql ]
            IsFunction = true
        }

    let prepare props = { props with NeedPrepare = true }

    /// Provides the SQL parameters for the query
    let parameters ls props = { props with Parameters = ls }

    // /// When using the DuckDB.FSharp.Analyzer, this function annotates the code to tell the analyzer to ignore and skip the SQL analyzer against the database.
    // let skipAnalysis (props: SqlProps) = props


    /// Creates or returns the SQL connection used to execute the SQL commands
    let createConnection (props : SqlProps) : DuckDBConnection =
        match props.ExecutionTarget with
        | Connection existingConnection -> existingConnection

        | ConnectionString connectionString ->
            let connection = new DuckDBConnection (connectionString)
            connection

        | FileDataSource (path, readOnly) ->
            let constr =
                if readOnly then
                    $"Data Source={path};ACCESS_MODE=READ_ONLY"
                else
                    $"Data Source={path}"

            let connection = new DuckDBConnection (constr)
            connection

        | InMemoryDataSource (name, sharedCache) ->
            let constr =
                if sharedCache then
                    $"Data Source=:memory:{name}?cache=shared"
                else
                    "Data Source=:memory:{name}"

            let connection = new DuckDBConnection (constr)
            connection

    let private populateRow (cmd : DuckDBCommand) (row : (string * SqlValue) list) =
        for (paramName, value) in row do
            let normalizedParameterName = paramName.Trim().TrimStart ('$', '@')

            let add (value : obj) =
                let param = DuckDBParameter (normalizedParameterName, value)
                cmd.Parameters.Add (param) |> ignore

            let addTy (value : obj) (ty : DbType) =
                let param = DuckDBParameter (normalizedParameterName, ty, value)
                cmd.Parameters.Add (param) |> ignore

            match value with
            | SqlValue.Null ->
                cmd.Parameters.Add (DuckDBParameter (normalizedParameterName, DBNull.Value))
                |> ignore

            | SqlValue.Parameter p ->
                p.ParameterName <- normalizedParameterName
                cmd.Parameters.Add (p) |> ignore

            // ── Integer types ─────────────────────────────────────
            | SqlValue.TinyInt x -> add (box x)
            | SqlValue.SmallInt x -> add (box x)
            | SqlValue.Integer x -> add (box x)
            | SqlValue.BigInt x -> add (box x)

            // ── Floating-point & decimal ─────────────────────────
            | SqlValue.Real x -> add (box x)
            | SqlValue.Double x -> add (box x)
            | SqlValue.Decimal x -> add (box x)

            // ── Boolean & bit string ─────────────────────────────
            | SqlValue.Boolean x -> add (box x)
            | SqlValue.Bit x -> add (box x)

            // ── Text types ───────────────────────────────────────
            | SqlValue.VarChar x -> add (box x)
            | SqlValue.Json x -> add (box x)

            // ── Binary ───────────────────────────────────────────
            | SqlValue.Blob x -> add (box x)

            // ── UUID ─────────────────────────────────────────────
            | SqlValue.Uuid x -> add (box x)
            | SqlValue.UuidList x -> add (box x)

            // ── Date & time types ────────────────────────────────
            | SqlValue.Date (Choice1Of2 x) -> add (box x)
            | SqlValue.Date (Choice2Of2 x) -> add (box x)
            | SqlValue.Time x -> add (box x)
            | SqlValue.TimeTz x -> add (box x)
            | SqlValue.Timestamp x -> add (box x)
            | SqlValue.TimestampTz x -> add (box x)
            | SqlValue.Interval x -> add (box x)

            // ── List / array types (DuckDB uses LIST) ────────────
            | SqlValue.VarCharList x -> add (box x)
            | SqlValue.IntegerList x -> add (box x)
            | SqlValue.SmallIntList x -> add (box x)
            | SqlValue.BigIntList x -> add (box x)
            | SqlValue.DoubleList x -> add (box x)
            | SqlValue.DecimalList x -> add (box x)


    let private populateCmd (cmd : DuckDBCommand) (props : SqlProps) =
        if props.IsFunction then
            cmd.CommandType <- CommandType.StoredProcedure

        populateRow cmd props.Parameters

    let private disposeOwnConnection (props : SqlProps) (connection : DuckDBConnection) =
        match props.ExecutionTarget with
        | ConnectionString _
        | FileDataSource _
        | InMemoryDataSource _ -> connection.Dispose ()
        | Connection _ -> ()

    let executeTransaction (queries : (string * (string * SqlValue) list list) list) (props : SqlProps) =
        if List.isEmpty queries then
            []
        else
            let connection = createConnection props

            try
                if connection.State <> ConnectionState.Open then
                    connection.Open ()

                use transaction = connection.BeginTransaction ()
                let affectedRowsByQuery = ResizeArray<int> ()

                for (sql, parameterSets) in queries do
                    if List.isEmpty parameterSets then
                        // Simple non-parameterized query
                        use cmd = connection.CreateCommand (CommandText = sql, Transaction = transaction) // <-- Add Transaction
                        let affected = cmd.ExecuteNonQuery ()
                        affectedRowsByQuery.Add affected
                    else
                        // Multiple parameter sets → execute one by one
                        let mutable totalAffected = 0

                        for paramsRow in parameterSets do
                            use cmd = connection.CreateCommand (CommandText = sql, Transaction = transaction) // <-- Add Transaction
                            populateRow cmd paramsRow
                            totalAffected <- totalAffected + cmd.ExecuteNonQuery ()

                        affectedRowsByQuery.Add totalAffected

                transaction.Commit ()
                List.ofSeq affectedRowsByQuery

            finally
                disposeOwnConnection props connection

    let executeTransactionAsync (queries : (string * (string * SqlValue) list list) list) (props : SqlProps) =
        task {
            if List.isEmpty queries then
                return []
            else
                let connection = createConnection props

                try
                    if connection.State <> ConnectionState.Open then
                        do! connection.OpenAsync (props.CancellationToken)

                    use! transaction = connection.BeginTransactionAsync (props.CancellationToken)

                    let affectedRowsByQuery = ResizeArray<int> ()

                    for (sql, parameterSets) in queries do
                        if List.isEmpty parameterSets then
                            use cmd = connection.CreateCommand (CommandText = sql, Transaction = transaction)
                            let! affected = cmd.ExecuteNonQueryAsync (props.CancellationToken)
                            affectedRowsByQuery.Add affected
                        else
                            let mutable totalAffected = 0

                            for paramsRow in parameterSets do
                                use cmd = connection.CreateCommand (CommandText = sql, Transaction = transaction)
                                populateRow cmd paramsRow
                                let! affected = cmd.ExecuteNonQueryAsync (props.CancellationToken)
                                totalAffected <- totalAffected + affected

                            affectedRowsByQuery.Add totalAffected

                    do! transaction.CommitAsync (props.CancellationToken)
                    return List.ofSeq affectedRowsByQuery

                finally
                    disposeOwnConnection props connection
        }


    let execute (read : RowReader -> 't) (props : SqlProps) : 't list =
        if List.isEmpty props.SqlQuery then
            raise
            <| MissingQueryException "No query provided to execute. Please use Sql.query"

        let connection = createConnection props

        try
            if not (connection.State.HasFlag ConnectionState.Open) then
                connection.Open ()

            let sql = List.head props.SqlQuery
            use command = connection.CreateCommand (CommandText = sql)
            do populateCmd command props

            if props.NeedPrepare then
                command.Prepare ()

            use reader = command.ExecuteReader ()
            let rowReader = RowReader (reader)
            let result = ResizeArray<'t> ()

            while reader.Read () do
                result.Add (read rowReader)

            List.ofSeq result
        finally
            disposeOwnConnection props connection

    let iter (perform : RowReader -> unit) (props : SqlProps) : unit =
        if List.isEmpty props.SqlQuery then
            raise
            <| MissingQueryException "No query provided to execute. Please use Sql.query"

        let connection = createConnection props

        try
            if not (connection.State.HasFlag ConnectionState.Open) then
                connection.Open ()

            let sql = List.head props.SqlQuery
            use command = connection.CreateCommand (CommandText = sql)

            do populateCmd command props

            if props.NeedPrepare then
                command.Prepare ()

            use reader = command.ExecuteReader ()
            let rowReader = RowReader (reader)

            while reader.Read () do
                perform rowReader
        finally
            disposeOwnConnection props connection

    let executeRow (read : RowReader -> 't) (props : SqlProps) : 't =
        if List.isEmpty props.SqlQuery then
            raise
            <| MissingQueryException "No query provided to execute. Please use Sql.query"

        let connection = createConnection props

        try
            if not (connection.State.HasFlag ConnectionState.Open) then
                connection.Open ()

            let sql = List.head props.SqlQuery
            use command = connection.CreateCommand (CommandText = sql)
            do populateCmd command props

            if props.NeedPrepare then
                command.Prepare ()

            use reader = command.ExecuteReader ()

            let rowReader = RowReader (reader)

            if reader.Read () then
                read rowReader
            else
                raise
                <| NoResultsException
                    "Expected at least one row to be returned from the result set. Instead it was empty"
        finally
            disposeOwnConnection props connection

    let iterAsync (perform : RowReader -> unit) (props : SqlProps) : Task =
        task {
            if List.isEmpty props.SqlQuery then
                raise
                <| MissingQueryException "No query provided to execute. Please use Sql.query"

            let connection = createConnection props

            try
                if not (connection.State.HasFlag ConnectionState.Open) then
                    do! connection.OpenAsync (props.CancellationToken)

                let sql = List.head props.SqlQuery
                use command = connection.CreateCommand (CommandText = sql)
                do populateCmd command props

                if props.NeedPrepare then
                    do! command.PrepareAsync (props.CancellationToken)

                use! reader = command.ExecuteReaderAsync (props.CancellationToken)
                let rowReader = RowReader (unbox<DuckDBDataReader> reader)

                while reader.Read () do
                    perform rowReader
            finally
                disposeOwnConnection props connection
        }

    let executeRowAsync (read : RowReader -> 't) (props : SqlProps) : Task<'t> =
        task {
            if List.isEmpty props.SqlQuery then
                raise
                <| MissingQueryException "No query provided to execute. Please use Sql.query"

            let connection = createConnection props

            try
                if not (connection.State.HasFlag ConnectionState.Open) then
                    do! connection.OpenAsync (props.CancellationToken)

                let sql = List.head props.SqlQuery
                use command = connection.CreateCommand (CommandText = sql)
                do populateCmd command props

                if props.NeedPrepare then
                    do! command.PrepareAsync (props.CancellationToken)

                use! reader = command.ExecuteReaderAsync props.CancellationToken
                let rowReader = RowReader (unbox<DuckDBDataReader> reader)

                if reader.Read () then
                    return read rowReader
                else
                    return!
                        raise
                        <| NoResultsException
                            "Expected at least one row to be returned from the result set. Instead it was empty"
            finally
                disposeOwnConnection props connection
        }

    /// Executes the query and returns the number of rows affected
    let executeNonQuery (props : SqlProps) : int =
        if List.isEmpty props.SqlQuery then
            raise <| MissingQueryException "No query provided to execute..."

        let connection = createConnection props

        try
            if not (connection.State.HasFlag ConnectionState.Open) then
                connection.Open ()

            let sql = List.head props.SqlQuery
            use command = connection.CreateCommand (CommandText = sql)
            populateCmd command props

            if props.NeedPrepare then
                command.Prepare ()

            command.ExecuteNonQuery ()
        finally
            disposeOwnConnection props connection

    /// Executes the query as asynchronously and returns the number of rows affected
    let executeNonQueryAsync (props : SqlProps) =
        task {
            if List.isEmpty props.SqlQuery then
                raise
                <| MissingQueryException "No query provided to execute. Please use Sql.query"

            let connection = createConnection props

            try
                if not (connection.State.HasFlag ConnectionState.Open) then
                    do! connection.OpenAsync props.CancellationToken

                let sql = List.head props.SqlQuery
                use command = connection.CreateCommand (CommandText = sql)
                populateCmd command props

                if props.NeedPrepare then
                    do! command.PrepareAsync (props.CancellationToken)

                let! affectedRows = command.ExecuteNonQueryAsync props.CancellationToken
                return affectedRows
            finally
                disposeOwnConnection props connection
        }

    /// Wraps the execution of the query in a sequence
    let toSeq (read : RowReader -> 't) (props : SqlProps) =
        seq {
            if List.isEmpty props.SqlQuery then
                raise
                <| MissingQueryException "No query provided to execute. Please use Sql.query"

            let connection = createConnection props

            try
                if not (connection.State.HasFlag ConnectionState.Open) then
                    connection.Open ()

                let sql = List.head props.SqlQuery
                use command = connection.CreateCommand (CommandText = sql)
                do populateCmd command props

                if props.NeedPrepare then
                    command.Prepare ()

                use reader = command.ExecuteReader ()
                let rowReader = RowReader (reader)

                while reader.Read () do
                    yield read rowReader
            finally
                disposeOwnConnection props connection
        }
