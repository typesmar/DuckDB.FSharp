[<AutoOpen>]
module DuckDB.FSharp.CommonExtensionsAndTypes

open System
open DuckDB.NET.Data
open System.Data

[<RequireQualifiedAccess>]
type SqlValue =
    | Parameter of DuckDBParameter
    | Null

    // ── Integer types ─────────────────────────────────────
    | TinyInt of int8 // TINYINT
    | SmallInt of int16 // SMALLINT
    | Integer of int // INTEGER
    | BigInt of int64 // BIGINT

    // ── Floating-point & decimal ─────────────────────────
    | Real of float32 // REAL / FLOAT
    | Double of double // DOUBLE
    | Decimal of decimal // DECIMAL(p,s) – also used for money-like values

    // ── Boolean & bit string ─────────────────────────────
    | Boolean of bool // BOOLEAN
    | Bit of string // BIT string literal, e.g. "10110"

    // ── Text types ───────────────────────────────────────
    | VarChar of string // VARCHAR
    | Json of string // JSON (DuckDB has native JSON, not JSONB)

    // ── Binary ───────────────────────────────────────────
    | Blob of byte[] // BLOB / BYTEA equivalent

    // ── UUID ─────────────────────────────────────────────
    | Uuid of Guid // UUID
    | UuidList of Guid[] // LIST[UUID]

    // ── Date & time types ────────────────────────────────
    | Date of Choice<DateTime, DateOnly> // DATE
    | Time of TimeOnly // TIME (without time zone)
    | TimeTz of DateTimeOffset // TIME WITH TIME ZONE
    | Timestamp of DateTime // TIMESTAMP (no zone)
    | TimestampTz of DateTime // TIMESTAMPTZ – stored as UTC
    | Interval of TimeSpan // INTERVAL

    // ── List types (DuckDB uses LIST) ────────────
    | VarCharList of string seq
    | IntegerList of int seq
    | SmallIntList of int16 seq
    | BigIntList of int64 seq
    | DoubleList of double seq
    | DecimalList of decimal seq

// ── Future / experimental ───────────────────────────
// | Point2D of struct { X: double; Y: double }   // POINT_2D struct

module internal Utils =
    let inline sqlMap (option : 'a option) (f : 'a -> SqlValue) : SqlValue =
        Option.defaultValue SqlValue.Null (Option.map f option)

    let inline sqlValueMap (option : 'a voption) (f : 'a -> SqlValue) : SqlValue =
        ValueOption.defaultValue SqlValue.Null (ValueOption.map f option)

exception MissingQueryException of string
exception NoResultsException of string
exception UnknownColumnException of string

open System
open DuckDB.NET.Data

type Sql() =
    // ── Null & raw parameter ─────────────────────────────────────
    static member dbnull = SqlValue.Null
    static member parameter (p : DuckDBParameter) = SqlValue.Parameter p

    // ── Integer types ───────────────────────────────────────────
    static member tinyInt (v : int8) = SqlValue.TinyInt v
    static member tinyIntOrNone (v : int8 option) = Utils.sqlMap v Sql.tinyInt
    static member tinyIntOrValueNone (v : int8 voption) = Utils.sqlValueMap v Sql.tinyInt

    static member smallInt (v : int16) = SqlValue.SmallInt v
    static member smallIntOrNone (v : int16 option) = Utils.sqlMap v Sql.smallInt
    static member smallIntOrValueNone (v : int16 voption) = Utils.sqlValueMap v Sql.smallInt

    static member integer (v : int) = SqlValue.Integer v
    static member integerOrNone (v : int option) = Utils.sqlMap v Sql.integer
    static member integerOrValueNone (v : int voption) = Utils.sqlValueMap v Sql.integer

    static member bigInt (v : int64) = SqlValue.BigInt v
    static member bigIntOrNone (v : int64 option) = Utils.sqlMap v Sql.bigInt
    static member bigIntOrValueNone (v : int64 voption) = Utils.sqlValueMap v Sql.bigInt

    // ── Floating-point & decimal ─────────────────────────────────
    static member real (v : float32) = SqlValue.Real v
    static member realOrNone (v : float32 option) = Utils.sqlMap v Sql.real
    static member realOrValueNone (v : float32 voption) = Utils.sqlValueMap v Sql.real

    static member double (v : double) = SqlValue.Double v
    static member doubleOrNone (v : double option) = Utils.sqlMap v Sql.double
    static member doubleOrValueNone (v : double voption) = Utils.sqlValueMap v Sql.double

    static member decimal (v : decimal) = SqlValue.Decimal v
    static member decimalOrNone (v : decimal option) = Utils.sqlMap v Sql.decimal
    static member decimalOrValueNone (v : decimal voption) = Utils.sqlValueMap v Sql.decimal

    // ── Boolean & bit string ─────────────────────────────────────
    static member boolean (v : bool) = SqlValue.Boolean v
    static member booleanOrNone (v : bool option) = Utils.sqlMap v Sql.boolean
    static member booleanOrValueNone (v : bool voption) = Utils.sqlValueMap v Sql.boolean

    static member bit (v : string) = SqlValue.Bit v
    static member bitOrNone (v : string option) = Utils.sqlMap v Sql.bit
    static member bitOrValueNone (v : string voption) = Utils.sqlValueMap v Sql.bit

    // ── Text & JSON ──────────────────────────────────────────────
    static member varChar (v : string) =
        if isNull v then
            SqlValue.VarChar String.Empty
        else
            SqlValue.VarChar v

    static member varCharOrNone (v : string option) = Utils.sqlMap v Sql.varChar
    static member varCharOrValueNone (v : string voption) = Utils.sqlValueMap v Sql.varChar

    static member json (v : string) = SqlValue.Json v
    static member jsonOrNone (v : string option) = Utils.sqlMap v Sql.json
    static member jsonOrValueNone (v : string voption) = Utils.sqlValueMap v Sql.json

    // ── Binary ───────────────────────────────────────────────────
    static member blob (v : byte[]) = SqlValue.Blob v
    static member blobOrNone (v : byte[] option) = Utils.sqlMap v Sql.blob
    static member blobOrValueNone (v : byte[] voption) = Utils.sqlValueMap v Sql.blob

    // ── UUID ─────────────────────────────────────────────────────
    static member uuid (v : Guid) = SqlValue.Uuid v
    static member uuidOrNone (v : Guid option) = Utils.sqlMap v Sql.uuid
    static member uuidOrValueNone (v : Guid voption) = Utils.sqlValueMap v Sql.uuid

    static member uuidList (v : Guid[]) = SqlValue.UuidList v
    static member uuidListOrNone (v : Guid[] option) = Utils.sqlMap v Sql.uuidList
    static member uuidListOrValueNone (v : Guid[] voption) = Utils.sqlValueMap v Sql.uuidList

    // ── Date & time ──────────────────────────────────────────────
    static member date (v : DateOnly) = SqlValue.Date (Choice2Of2 v)
    static member date (v : DateTime) = SqlValue.Date (Choice1Of2 v.Date)
    static member dateOrNone (v : DateOnly option) = Utils.sqlMap v (Sql.date)
    static member dateOrNone (v : DateTime option) = Utils.sqlMap v (Sql.date)
    static member dateOrValueNone (v : DateOnly voption) = Utils.sqlValueMap v (Sql.date)
    static member dateOrValueNone (v : DateTime voption) = Utils.sqlValueMap v (Sql.date)

    static member time (v : TimeOnly) = SqlValue.Time v
    static member timeOrNone (v : TimeOnly option) = Utils.sqlMap v Sql.time
    static member timeOrValueNone (v : TimeOnly voption) = Utils.sqlValueMap v Sql.time

    /// NOTE: The DuckDB TIMETZ type only stores time-of-day with timezone, not the full date
    static member timeTz (v : DateTimeOffset) = SqlValue.TimeTz v

    /// NOTE: The DuckDB TIMETZ type only stores time-of-day with timezone, not the full date
    static member timeTzOrNone (v : DateTimeOffset option) = Utils.sqlMap v Sql.timeTz

    /// NOTE: The DuckDB TIMETZ type only stores time-of-day with timezone, not the full date
    static member timeTzOrValueNone (v : DateTimeOffset voption) = Utils.sqlValueMap v Sql.timeTz

    static member timestamp (v : DateTime) = SqlValue.Timestamp v
    static member timestampOrNone (v : DateTime option) = Utils.sqlMap v Sql.timestamp
    static member timestampOrValueNone (v : DateTime voption) = Utils.sqlValueMap v Sql.timestamp

    // TIMESTAMPTZ — DuckDB returns UTC already → just pass through
    static member timestampTz (v : DateTime) = SqlValue.TimestampTz v
    static member timestampTz (v : DateTimeOffset) = SqlValue.TimestampTz v.UtcDateTime
    static member timestampTzOrNone (v : DateTime option) = Utils.sqlMap v Sql.timestampTz
    static member timestampTzOrNone (v : DateTimeOffset option) = Utils.sqlMap v (Sql.timestampTz)
    static member timestampTzOrValueNone (v : DateTime voption) = Utils.sqlValueMap v Sql.timestampTz
    static member timestampTzOrValueNone (v : DateTimeOffset voption) = Utils.sqlValueMap v (Sql.timestampTz)

    static member interval (v : TimeSpan) = SqlValue.Interval v
    static member intervalOrNone (v : TimeSpan option) = Utils.sqlMap v Sql.interval
    static member intervalOrValueNone (v : TimeSpan voption) = Utils.sqlValueMap v Sql.interval

    // ── List / array types (DuckDB = LIST) ───────────────────────
    static member varCharList (v : string seq) = SqlValue.VarCharList v
    static member varCharListOrNone (v : string seq option) = Utils.sqlMap v Sql.varCharList
    static member varCharListOrValueNone (v : string seq voption) = Utils.sqlValueMap v Sql.varCharList

    static member integerList (v : int seq) = SqlValue.IntegerList v
    static member integerListOrNone (v : int seq option) = Utils.sqlMap v Sql.integerList
    static member integerListOrValueNone (v : int seq voption) = Utils.sqlValueMap v Sql.integerList

    static member smallIntList (v : int16 seq) = SqlValue.SmallIntList v
    static member smallIntListOrNone (v : int16 seq option) = Utils.sqlMap v Sql.smallIntList
    static member smallIntListOrValueNone (v : int16 seq voption) = Utils.sqlValueMap v Sql.smallIntList

    static member bigIntList (v : int64 seq) = SqlValue.BigIntList v
    static member bigIntListOrNone (v : int64 seq option) = Utils.sqlMap v Sql.bigIntList
    static member bigIntListOrValueNone (v : int64 seq voption) = Utils.sqlValueMap v Sql.bigIntList

    static member doubleList (v : double seq) = SqlValue.DoubleList v
    static member doubleListOrNone (v : double seq option) = Utils.sqlMap v Sql.doubleList
    static member doubleListOrValueNone (v : double seq voption) = Utils.sqlValueMap v Sql.doubleList

    static member decimalList (v : decimal seq) = SqlValue.DecimalList v
    static member decimalListOrNone (v : decimal seq option) = Utils.sqlMap v Sql.decimalList
    static member decimalListOrValueNone (v : decimal seq voption) = Utils.sqlValueMap v Sql.decimalList


open System.Collections.Generic

module RowReaderHelpers =
    open System.IO

    /// Converts an UnmanagedMemoryStream to a byte[] by copying its contents.
    /// Safe, works with any position/length, and doesn't assume the stream is at position 0.
    let unmanagedMemoryStreamToArray (stream : UnmanagedMemoryStream) : byte[] =
        if isNull stream then
            nullArg (nameof stream)

        let length = int stream.Length
        let position = int stream.Position

        let buffer = Array.zeroCreate<byte> length

        // If not at start, seek to beginning (or copy from current position)
        if position <> 0 then
            stream.Seek (0L, SeekOrigin.Begin) |> ignore

        // Read full content
        let bytesRead = stream.Read (buffer, 0, length)

        if bytesRead <> length then
            failwithf "Failed to read all bytes: expected %d, read %d" length bytesRead

        buffer

    let getValue<'a> (reader : DuckDBDataReader, columnDict : Dictionary<string, int>, fail : string -> string -> _) =
        fun column ->
            match columnDict.TryGetValue (column) with
            | true, i -> reader.GetFieldValue<'a> (i)
            | false, _ -> fail column typeof<'a>.Name

    let inline getValueOrNone<'a>
        (
            reader : DuckDBDataReader,
            columnDict : Dictionary<string, int>,
            [<InlineIfLambda>] fail : string -> string -> _
        )
        =
        fun column ->
            match columnDict.TryGetValue (column) with
            | true, i when reader.IsDBNull (i) -> None
            | true, i -> Some (reader.GetFieldValue<'a> (i))
            | false, _ -> fail column (typeof<'a>.Name + " option")

    let inline getValueOrValueNone<'a>
        (
            reader : DuckDBDataReader,
            columnDict : Dictionary<string, int>,
            [<InlineIfLambda>] fail : string -> string -> _
        )
        =
        fun column ->
            match columnDict.TryGetValue (column) with
            | true, i when reader.IsDBNull (i) -> ValueNone
            | true, i -> ValueSome (reader.GetFieldValue<'a> (i))
            | false, _ -> fail column (typeof<'a>.Name + " voption")

open RowReaderHelpers

type RowReader(reader : DuckDBDataReader) =
    let columnDict = Dictionary<string, int> ()
    let columnTypes = Dictionary<string, string> ()

    do
        for i in 0 .. reader.FieldCount - 1 do
            let name = reader.GetName (i)
            columnDict.Add (name, i)
            columnTypes.Add (name, reader.GetDataTypeName (i))

    let fail column expectedType =
        let available =
            columnTypes
            |> Seq.map (fun kv -> sprintf "[%s: %s]" kv.Key kv.Value)
            |> String.concat ", "

        raise (
            UnknownColumnException (
                sprintf "Column '%s' not found (expected %s). Available: %s" column expectedType available
            )
        )

    // ── Integer types ───────────────────────────────────────
    member _.integer (column) =
        getValue<int> (reader, columnDict, fail) column

    member _.integerOrNone (column) =
        getValueOrNone<int> (reader, columnDict, fail) column

    member _.integerOrValueNone (column) =
        getValueOrValueNone<int> (reader, columnDict, fail) column

    member _.smallInt (column) =
        getValue<int16> (reader, columnDict, fail) column

    member _.smallIntOrNone (column) =
        getValueOrNone<int16> (reader, columnDict, fail) column

    member _.smallIntOrValueNone (column) =
        getValueOrValueNone<int16> (reader, columnDict, fail) column

    member _.tinyInt (column) =
        getValue<sbyte> (reader, columnDict, fail) column

    member _.tinyIntOrNone (column) =
        getValueOrNone<sbyte> (reader, columnDict, fail) column

    member _.tinyIntOrValueNone (column) =
        getValueOrValueNone<sbyte> (reader, columnDict, fail) column

    member _.bigInt (column) =
        getValue<int64> (reader, columnDict, fail) column

    member _.bigIntOrNone (column) =
        getValueOrNone<int64> (reader, columnDict, fail) column

    member _.bigIntOrValueNone (column) =
        getValueOrValueNone<int64> (reader, columnDict, fail) column

    // ── Floating-point & decimal ───────────────────────────
    member _.real (column) =
        getValue<float32> (reader, columnDict, fail) column

    member _.realOrNone (column) =
        getValueOrNone<float32> (reader, columnDict, fail) column

    member _.realOrValueNone (column) =
        getValueOrValueNone<float32> (reader, columnDict, fail) column

    member _.double (column) =
        getValue<double> (reader, columnDict, fail) column

    member _.doubleOrNone (column) =
        getValueOrNone<double> (reader, columnDict, fail) column

    member _.doubleOrValueNone (column) =
        getValueOrValueNone<double> (reader, columnDict, fail) column

    member _.decimal (column) =
        getValue<decimal> (reader, columnDict, fail) column

    member _.decimalOrNone (column) =
        getValueOrNone<decimal> (reader, columnDict, fail) column

    member _.decimalOrValueNone (column) =
        getValueOrValueNone<decimal> (reader, columnDict, fail) column

    // ── Boolean & bit string ───────────────────────────────
    member _.boolean (column) =
        getValue<bool> (reader, columnDict, fail) column

    member _.booleanOrNone (column) =
        getValueOrNone<bool> (reader, columnDict, fail) column

    member _.booleanOrValueNone (column) =
        getValueOrValueNone<bool> (reader, columnDict, fail) column

    member _.bit (column) =
        getValue<string> (reader, columnDict, fail) column // DuckDB returns BIT as string "10110"

    member _.bitOrNone (column) =
        getValueOrNone<string> (reader, columnDict, fail) column

    // ── Text & JSON ───────────────────────────────────────
    member _.varChar (column) =
        getValue<string> (reader, columnDict, fail) column

    member _.varCharOrNone (column) =
        getValueOrNone<string> (reader, columnDict, fail) column

    member _.varCharOrValueNone (column) =
        getValueOrValueNone<string> (reader, columnDict, fail) column

    member _.text (column) =
        getValue<string> (reader, columnDict, fail) column

    member _.textOrNone (column) =
        getValueOrNone<string> (reader, columnDict, fail) column

    member _.json (column) =
        getValue<string> (reader, columnDict, fail) column

    member _.jsonOrNone (column) =
        getValueOrNone<string> (reader, columnDict, fail) column

    // ── Binary ─────────────────────────────────────────────
    member _.blob (column) =
        match columnDict.TryGetValue (column) with
        | true, i ->
            let value = reader.GetValue (i)

            match value with
            | :? System.IO.UnmanagedMemoryStream as stream -> unmanagedMemoryStreamToArray stream
            | _ ->
                fail column "BLOB"
                [||]
        | false, _ ->
            fail column "BLOB"
            [||]

    member _.blobOrNone (column) =
        match columnDict.TryGetValue (column) with
        | true, i when reader.IsDBNull (i) -> None
        | true, i ->
            let value = reader.GetValue (i)

            match value with
            | :? System.IO.UnmanagedMemoryStream as stream -> Some (unmanagedMemoryStreamToArray stream)
            | _ ->
                fail column "BLOB option"
                None
        | false, _ ->
            fail column "BLOB option"
            None

    member _.blobOrValueNone (column) =
        match columnDict.TryGetValue (column) with
        | true, i when reader.IsDBNull (i) -> ValueNone
        | true, i ->
            let value = reader.GetValue (i)

            match value with
            | :? System.IO.UnmanagedMemoryStream as stream -> ValueSome (unmanagedMemoryStreamToArray stream)
            | _ ->
                fail column "BLOB voption"
                ValueNone
        | false, _ ->
            fail column "BLOB voption"
            ValueNone

    // ── UUID ───────────────────────────────────────────────
    member _.uuid (column) =
        getValue<Guid> (reader, columnDict, fail) column

    member _.uuidOrNone (column) =
        getValueOrNone<Guid> (reader, columnDict, fail) column

    member _.uuidOrValueNone (column) =
        getValueOrValueNone<Guid> (reader, columnDict, fail) column

    // ── Date & Time ───────────────────────────────────────
    member _.date (column) =
        getValue<DateOnly> (reader, columnDict, fail) column

    member _.dateOrNone (column) =
        getValueOrNone<DateOnly> (reader, columnDict, fail) column

    member _.time (column) =
        getValue<TimeOnly> (reader, columnDict, fail) column

    member _.timeOrNone (column) =
        getValueOrNone<TimeOnly> (reader, columnDict, fail) column

    /// NOTE: The DuckDB TIMETZ type only stores time-of-day with timezone, not the full date
    member _.timeTz (column) =
        getValue<DateTimeOffset> (reader, columnDict, fail) column

    /// NOTE: The DuckDB TIMETZ type only stores time-of-day with timezone, not the full date
    member _.timeTzOrNone (column) =
        getValueOrNone<DateTimeOffset> (reader, columnDict, fail) column

    /// NOTE: The DuckDB TIMETZ type only stores time-of-day with timezone, not the full date
    member _.timestamp (column) =
        getValue<DateTime> (reader, columnDict, fail) column

    member _.timestampOrNone (column) =
        getValueOrNone<DateTime> (reader, columnDict, fail) column

    // TIMESTAMPTZ — DuckDB returns it already as UTC DateTime (Kind = Utc)
    member _.timestampTz (column) =
        getValue<DateTime> (reader, columnDict, fail) column // Kind = Utc

    member _.timestampTzOrNone (column) =
        getValueOrNone<DateTime> (reader, columnDict, fail) column

    member _.timestampTzOffset (column) =
        getValue<DateTimeOffset> (reader, columnDict, fail) column // Alternative: full offset

    member _.timestampTzOffsetOrNone (column) =
        getValueOrNone<DateTimeOffset> (reader, columnDict, fail) column

    member _.interval (column) =
        getValue<TimeSpan> (reader, columnDict, fail) column

    member _.intervalOrNone (column) =
        getValueOrNone<TimeSpan> (reader, columnDict, fail) column

    // ── List types ─────────────────
    member _.integerList (column) =
        getValue<List<int>> (reader, columnDict, fail) column
        |> fun list -> list.ToArray ()

    member _.integerListOrNone (column) =
        getValueOrNone<List<int>> (reader, columnDict, fail) column
        |> Option.map (fun list -> list.ToArray ())

    member _.smallIntList (column) =
        getValue<List<int16>> (reader, columnDict, fail) column
        |> fun list -> list.ToArray ()

    member _.smallIntListOrNone (column) =
        getValueOrNone<List<int16>> (reader, columnDict, fail) column
        |> Option.map (fun list -> list.ToArray ())

    member _.bigIntList (column) =
        getValue<List<int64>> (reader, columnDict, fail) column
        |> fun list -> list.ToArray ()

    member _.bigIntListOrNone (column) =
        getValueOrNone<List<int64>> (reader, columnDict, fail) column
        |> Option.map (fun list -> list.ToArray ())

    member _.doubleList (column) =
        getValue<List<double>> (reader, columnDict, fail) column
        |> fun list -> list.ToArray ()

    member _.doubleListOrNone (column) =
        getValueOrNone<List<double>> (reader, columnDict, fail) column
        |> Option.map (fun list -> list.ToArray ())

    member _.decimalList (column) =
        getValue<List<decimal>> (reader, columnDict, fail) column
        |> fun list -> list.ToArray ()

    member _.decimalListOrNone (column) =
        getValueOrNone<List<decimal>> (reader, columnDict, fail) column
        |> Option.map (fun list -> list.ToArray ())

    member _.varCharList (column) =
        getValue<List<string>> (reader, columnDict, fail) column
        |> fun list -> list.ToArray ()

    member _.varCharListOrNone (column) =
        getValueOrNone<List<string>> (reader, columnDict, fail) column
        |> Option.map (fun list -> list.ToArray ())

    member _.uuidList (column) =
        getValue<List<Guid>> (reader, columnDict, fail) column
        |> fun list -> list.ToArray ()

    member _.uuidListOrNone (column) =
        getValueOrNone<List<Guid>> (reader, columnDict, fail) column
        |> Option.map (fun list -> list.ToArray ())

    member _.uuidListOrValueNone (column) =
        getValueOrValueNone<List<Guid>> (reader, columnDict, fail) column
        |> ValueOption.map (fun list -> list.ToArray ())

    // ── Raw access ─────────────────────────────────────────
    member _.reader = reader
    member _.fieldCount = reader.FieldCount
    member _.getName i = reader.GetName i
    member _.getDataTypeName i = reader.GetDataTypeName i
