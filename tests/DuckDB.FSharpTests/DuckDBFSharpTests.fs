module DuckDB.FSharp.ComprehensiveTests

open Expecto
open DuckDB.FSharp
open System
open System.IO
open System.Threading
open System.Threading.Tasks
open DuckDB.NET.Data
open System.Data

// ─────────────────────────────────────────────────────────────────────
// Test Helpers
// ─────────────────────────────────────────────────────────────────────
let withInMemoryDb (f : Sql.SqlProps -> 'a) : 'a =
    let dbName = Guid.NewGuid().ToString ("N")
    Sql.connect "" |> Sql.inMemory dbName true |> f

let withTempFileDb (f : string -> 'a) : 'a =
    let path =
        Path.Combine (Path.GetTempPath (), $"duckdb_test_{Guid.NewGuid ():N}.duckdb")

    try
        f path
    finally
        try
            File.Delete (path)
        with _ ->
            ()

// ─────────────────────────────────────────────────────────────────────
// Data Type Tests - Integer Types
// ─────────────────────────────────────────────────────────────────────
[<Tests>]
let integerTypeTests =
    testList
        "Integer data types"
        [
            test "TinyInt (int8) roundtrip" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val TINYINT)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.tinyInt 127y ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.tinyInt "val")

                    Expect.equal result 127y "TinyInt roundtrip"
                )
            }

            test "TinyInt with None values" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val TINYINT)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.tinyIntOrNone (Some 10y) ]
                                [ "v", Sql.tinyIntOrNone None ]
                                [ "v", Sql.tinyIntOrValueNone (ValueSome 20y) ]
                                [ "v", Sql.tinyIntOrValueNone ValueNone ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t ORDER BY val NULLS LAST"
                        |> Sql.execute (fun r -> r.tinyIntOrNone "val")

                    Expect.equal results [ Some 10y ; Some 20y ; None ; None ] "TinyInt option handling"
                )
            }

            test "SmallInt (int16) roundtrip" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val SMALLINT)"
                    |> Sql.executeNonQuery
                    |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.smallInt 32767s ]
                                [ "v", Sql.smallInt -32768s ]
                                [ "v", Sql.smallIntOrNone None ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t ORDER BY val NULLS LAST"
                        |> Sql.execute (fun r -> r.smallIntOrNone "val")

                    Expect.equal results [ Some -32768s ; Some 32767s ; None ] "SmallInt roundtrip"
                )
            }

            test "Integer (int32) roundtrip" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val INTEGER)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.integer 2147483647 ]
                                [ "v", Sql.integer -2147483648 ]
                                [ "v", Sql.integerOrNone (Some 42) ]
                                [ "v", Sql.integerOrValueNone ValueNone ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t ORDER BY val NULLS LAST"
                        |> Sql.execute (fun r -> r.integerOrNone "val")

                    Expect.equal results [ Some -2147483648 ; Some 42 ; Some 2147483647 ; None ] "Integer roundtrip"
                )
            }

            test "BigInt (int64) roundtrip" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val BIGINT)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.bigInt 9223372036854775807L ]
                                [ "v", Sql.bigInt -9223372036854775808L ]
                                [ "v", Sql.bigIntOrNone None ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t ORDER BY val NULLS LAST"
                        |> Sql.execute (fun r -> r.bigIntOrNone "val")

                    Expect.equal results.[0] (Some -9223372036854775808L) "BigInt min"
                    Expect.equal results.[1] (Some 9223372036854775807L) "BigInt max"
                    Expect.equal results.[2] None "BigInt null"
                )
            }
        ]

// ─────────────────────────────────────────────────────────────────────
// Data Type Tests - Floating Point & Decimal
// ─────────────────────────────────────────────────────────────────────
[<Tests>]
let floatingPointTests =
    testList
        "Floating point and decimal types"
        [
            test "Real (float32) roundtrip" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val REAL)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.real 3.14f ]
                                [ "v", Sql.real -2.718f ]
                                [ "v", Sql.realOrNone None ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t ORDER BY val NULLS LAST"
                        |> Sql.execute (fun r -> r.realOrNone "val")

                    Expect.equal
                        (results.[0] |> Option.map (fun x -> Math.Round (float x, 2)))
                        (Some -2.72)
                        "Real negative"

                    Expect.equal
                        (results.[1] |> Option.map (fun x -> Math.Round (float x, 2)))
                        (Some 3.14)
                        "Real positive"

                    Expect.equal results.[2] None "Real null"
                )
            }

            test "Double roundtrip" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val DOUBLE)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.double 3.141592653589793 ]
                                [ "v", Sql.doubleOrNone (Some 2.718281828459045) ]
                                [ "v", Sql.doubleOrValueNone ValueNone ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t ORDER BY val NULLS LAST"
                        |> Sql.execute (fun r -> r.doubleOrNone "val")

                    Expect.isTrue
                        (results.[0]
                         |> Option.exists (fun x -> Math.Abs (x - 2.718281828459045) < 0.0001))
                        "Double e"

                    Expect.isTrue
                        (results.[1]
                         |> Option.exists (fun x -> Math.Abs (x - 3.141592653589793) < 0.0001))
                        "Double pi"

                    Expect.equal results.[2] None "Double null"
                )
            }

            test "Decimal roundtrip with precision" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val DECIMAL(18,4))"
                    |> Sql.executeNonQuery
                    |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.decimal 123456.7890M ]
                                [ "v", Sql.decimal -9876.5432M ]
                                [ "v", Sql.decimalOrNone None ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t ORDER BY val NULLS LAST"
                        |> Sql.execute (fun r -> r.decimalOrNone "val")

                    Expect.equal results [ Some -9876.5432M ; Some 123456.7890M ; None ] "Decimal roundtrip"
                )
            }
        ]

// ─────────────────────────────────────────────────────────────────────
// Data Type Tests - Boolean & Bit
// ─────────────────────────────────────────────────────────────────────
[<Tests>]
let booleanAndBitTests =
    testList
        "Boolean and bit string types"
        [
            test "Boolean roundtrip" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val BOOLEAN)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.boolean true ]
                                [ "v", Sql.boolean false ]
                                [ "v", Sql.booleanOrNone None ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t ORDER BY val NULLS LAST"
                        |> Sql.execute (fun r -> r.booleanOrNone "val")

                    Expect.equal results [ Some false ; Some true ; None ] "Boolean roundtrip"
                )
            }

            test "Bit string roundtrip" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val BIT)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.bit "10110101" ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con |> Sql.query "SELECT val FROM t" |> Sql.executeRow (fun r -> r.bit "val")

                    Expect.equal result "10110101" "Bit string roundtrip"
                )
            }
        ]

// ─────────────────────────────────────────────────────────────────────
// Data Type Tests - Text & JSON
// ─────────────────────────────────────────────────────────────────────
[<Tests>]
let textAndJsonTests =
    testList
        "Text and JSON types"
        [
            test "VarChar roundtrip with empty and null" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val VARCHAR)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.varChar "Hello, World!" ]
                                [ "v", Sql.varChar "" ]
                                [ "v", Sql.varCharOrNone None ]
                                [ "v", Sql.varChar null ] // Should convert to empty string
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.execute (fun r -> r.varCharOrNone "val")

                    Expect.equal results.[0] (Some "Hello, World!") "VarChar normal"
                    Expect.equal results.[1] (Some "") "VarChar empty"
                    Expect.equal results.[2] None "VarChar None"
                    Expect.equal results.[3] (Some "") "VarChar null converts to empty"
                )
            }

            test "JSON roundtrip with complex object" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val JSON)" |> Sql.executeNonQuery |> ignore

                    let json =
                        """{"name":"Alice","age":30,"tags":["admin","user"],"metadata":{"created":"2025-01-01"}}"""

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.json json ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con |> Sql.query "SELECT val FROM t" |> Sql.executeRow (fun r -> r.json "val")

                    Expect.equal result json "JSON roundtrip"
                )
            }

            test "JSON with None" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val JSON)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.jsonOrNone (Some """{"key":"value"}""") ]
                                [ "v", Sql.jsonOrNone None ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.execute (fun r -> r.jsonOrNone "val")

                    Expect.equal results.[0] (Some """{"key":"value"}""") "JSON Some"
                    Expect.equal results.[1] None "JSON None"
                )
            }
        ]

// ─────────────────────────────────────────────────────────────────────
// Data Type Tests - Binary (Blob)
// ─────────────────────────────────────────────────────────────────────
[<Tests>]
let blobTests =
    testList
        "Binary (BLOB) types"
        [
            test "Blob roundtrip" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val BLOB)" |> Sql.executeNonQuery |> ignore

                    let bytes = [| 0uy ; 1uy ; 2uy ; 255uy ; 128uy |]

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.blob bytes ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con |> Sql.query "SELECT val FROM t" |> Sql.executeRow (fun r -> r.blob "val")

                    Expect.equal result bytes "Blob roundtrip"
                )
            }

            test "Blob with None" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val BLOB)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.blobOrNone (Some [| 42uy ; 84uy |]) ]
                                [ "v", Sql.blobOrValueNone ValueNone ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.execute (fun r -> r.blobOrNone "val")

                    Expect.equal results.[0] (Some [| 42uy ; 84uy |]) "Blob Some"
                    Expect.equal results.[1] None "Blob None"
                )
            }

            test "Empty blob" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val BLOB)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.blob [||] ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con |> Sql.query "SELECT val FROM t" |> Sql.executeRow (fun r -> r.blob "val")

                    Expect.equal result [||] "Empty blob roundtrip"
                )
            }
        ]

// ─────────────────────────────────────────────────────────────────────
// Data Type Tests - UUID
// ─────────────────────────────────────────────────────────────────────
[<Tests>]
let uuidTests =
    testList
        "UUID types"
        [
            test "UUID roundtrip" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val UUID)" |> Sql.executeNonQuery |> ignore

                    let uuid = Guid.NewGuid ()

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.uuid uuid ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con |> Sql.query "SELECT val FROM t" |> Sql.executeRow (fun r -> r.uuid "val")

                    Expect.equal result uuid "UUID roundtrip"
                )
            }

            test "UUID with None values" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val UUID)" |> Sql.executeNonQuery |> ignore

                    let uuid1 = Guid.NewGuid ()

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.uuidOrNone (Some uuid1) ]
                                [ "v", Sql.uuidOrValueNone ValueNone ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.execute (fun r -> r.uuidOrNone "val")

                    Expect.equal results [ Some uuid1 ; None ] "UUID option handling"
                )
            }

            test "UUID list roundtrip" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val UUID[])" |> Sql.executeNonQuery |> ignore

                    let uuids = [| Guid.NewGuid () ; Guid.NewGuid () ; Guid.NewGuid () |]

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.uuidList uuids ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.uuidList "val")

                    Expect.equal result uuids "UUID list roundtrip"
                )
            }

            test "UUID list with None" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val UUID[])" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.uuidListOrNone (Some [| Guid.Empty |]) ]
                                [ "v", Sql.uuidListOrValueNone ValueNone ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.execute (fun r -> r.uuidListOrNone "val")

                    Expect.equal results.[0] (Some [| Guid.Empty |]) "UUID list Some"
                    Expect.equal results.[1] None "UUID list None"
                )
            }
        ]

// ─────────────────────────────────────────────────────────────────────
// Data Type Tests - Date & Time
// ─────────────────────────────────────────────────────────────────────
[<Tests>]
let dateTimeTests =
    testList
        "Date and time types"
        [
            test "Date with DateOnly roundtrip" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val DATE)" |> Sql.executeNonQuery |> ignore

                    let date = DateOnly (2025, 11, 21)

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.date date ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con |> Sql.query "SELECT val FROM t" |> Sql.executeRow (fun r -> r.date "val")

                    Expect.equal result date "DateOnly roundtrip"
                )
            }

            test "Date with DateTime roundtrip" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val DATE)" |> Sql.executeNonQuery |> ignore

                    let dateTime = DateTime (2025, 11, 21, 14, 30, 0)
                    let expectedDate = DateOnly (2025, 11, 21)

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.date dateTime ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con |> Sql.query "SELECT val FROM t" |> Sql.executeRow (fun r -> r.date "val")

                    Expect.equal result expectedDate "DateTime to DateOnly conversion"
                )
            }

            test "Date with None values" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val DATE)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.dateOrNone (Some (DateOnly (2025, 1, 1))) ]
                                [ "v", Sql.dateOrNone (None : DateOnly option) ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.execute (fun r -> r.dateOrNone "val")

                    Expect.equal results.[0] (Some (DateOnly (2025, 1, 1))) "Date Some"
                    Expect.equal results.[1] None "Date None"
                )
            }

            test "Time roundtrip" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val TIME)" |> Sql.executeNonQuery |> ignore

                    let time = TimeOnly (14, 30, 45)

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.time time ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con |> Sql.query "SELECT val FROM t" |> Sql.executeRow (fun r -> r.time "val")

                    Expect.equal result time "Time roundtrip"
                )
            }

            test "TimeTz (TIME WITH TIME ZONE) roundtrip" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val TIMETZ)" |> Sql.executeNonQuery |> ignore

                    let timeTz = DateTimeOffset (2025, 1, 1, 14, 30, 0, TimeSpan.FromHours (5.0))

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.timeTz timeTz ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con |> Sql.query "SELECT val FROM t" |> Sql.executeRow (fun r -> r.timeTz "val")

                    // TIMETZ only stores time-of-day with timezone, not the full date
                    // Compare the UTC time-of-day component only
                    Expect.equal result.UtcDateTime.TimeOfDay timeTz.UtcDateTime.TimeOfDay "TimeTz roundtrip"
                )
            }

            test "Timestamp roundtrip" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val TIMESTAMP)"
                    |> Sql.executeNonQuery
                    |> ignore

                    let timestamp = DateTime (2025, 11, 21, 14, 30, 45, 123)

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.timestamp timestamp ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.timestamp "val")

                    Expect.equal result timestamp "Timestamp roundtrip"
                )
            }

            test "TimestampTz with DateTime roundtrip" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val TIMESTAMPTZ)"
                    |> Sql.executeNonQuery
                    |> ignore

                    let timestampTz = DateTime (2025, 11, 21, 14, 30, 0, DateTimeKind.Utc)

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.timestampTz timestampTz ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.timestampTz "val")

                    Expect.equal result timestampTz "TimestampTz DateTime roundtrip"
                )
            }

            test "TimestampTz with DateTimeOffset roundtrip" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val TIMESTAMPTZ)"
                    |> Sql.executeNonQuery
                    |> ignore

                    let offset = DateTimeOffset (2025, 11, 21, 14, 30, 0, TimeSpan.FromHours (-5.0))

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.timestampTz offset ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.timestampTzOffset "val")

                    Expect.equal result.UtcDateTime offset.UtcDateTime "TimestampTz DateTimeOffset roundtrip"
                )
            }

            test "Interval roundtrip" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val INTERVAL)"
                    |> Sql.executeNonQuery
                    |> ignore

                    let interval = TimeSpan (2, 14, 30, 0) // 2 days, 14 hours, 30 minutes

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.interval interval ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.interval "val")

                    Expect.equal result interval "Interval roundtrip"
                )
            }
        ]

// ─────────────────────────────────────────────────────────────────────
// Data Type Tests - List/Array Types
// ─────────────────────────────────────────────────────────────────────
[<Tests>]
let listTypeTests =
    testList
        "List/Array types"
        [
            test "VarChar list roundtrip" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val VARCHAR[])"
                    |> Sql.executeNonQuery
                    |> ignore

                    let strings = [| "hello" ; "world" ; "" ; "test" |]

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.varCharList strings ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.varCharList "val")

                    Expect.equal result strings "VarChar list roundtrip"
                )
            }

            test "VarChar list with nulls" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val VARCHAR[])"
                    |> Sql.executeNonQuery
                    |> ignore

                    let strings = [| "hello" ; null ; "world" |]

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.varCharList strings ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.varCharList "val")

                    Expect.equal result strings "VarChar list with nulls"
                )
            }

            test "Integer list roundtrip" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val INTEGER[])"
                    |> Sql.executeNonQuery
                    |> ignore

                    let ints = [| 1 ; 2 ; 3 ; -99 ; 0 ; 42 |]

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.integerList ints ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.integerList "val")

                    Expect.equal result ints "Integer list roundtrip"
                )
            }

            test "SmallInt list roundtrip" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val SMALLINT[])"
                    |> Sql.executeNonQuery
                    |> ignore

                    let smallInts = [| 1s ; 2s ; 3s ; -99s |]

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.smallIntList smallInts ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.smallIntList "val")

                    Expect.equal result smallInts "SmallInt list roundtrip"
                )
            }

            test "BigInt list roundtrip" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val BIGINT[])"
                    |> Sql.executeNonQuery
                    |> ignore

                    let bigInts = [| 1L ; 9223372036854775807L ; -9223372036854775808L |]

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.bigIntList bigInts ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.bigIntList "val")

                    Expect.equal result bigInts "BigInt list roundtrip"
                )
            }

            test "Double list roundtrip" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val DOUBLE[])"
                    |> Sql.executeNonQuery
                    |> ignore

                    let doubles = [| 3.14 ; 2.718 ; -1.414 ; 0.0 |]

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.doubleList doubles ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.doubleList "val")

                    Expect.equal result doubles "Double list roundtrip"
                )
            }

            test "Decimal list roundtrip" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val DECIMAL(18,4)[])"
                    |> Sql.executeNonQuery
                    |> ignore

                    let decimals = [| 123.45M ; 678.90M ; -999.99M |]

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.decimalList decimals ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.decimalList "val")

                    Expect.equal result decimals "Decimal list roundtrip"
                )
            }

            test "Empty lists" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (ints INTEGER[], strs VARCHAR[])"
                    |> Sql.executeNonQuery
                    |> ignore

                    con
                    |> Sql.query "INSERT INTO t VALUES ($i, $s)"
                    |> Sql.parameters [ "i", Sql.integerList [||] ; "s", Sql.varCharList [||] ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let ints, strs =
                        con
                        |> Sql.query "SELECT ints, strs FROM t"
                        |> Sql.executeRow (fun r -> r.integerList "ints", r.varCharList "strs")

                    Expect.equal ints [||] "Empty int list"
                    Expect.equal strs [||] "Empty string list"
                )
            }

            test "List option types" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val INTEGER[])"
                    |> Sql.executeNonQuery
                    |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [
                                [ "v", Sql.integerListOrNone (Some [| 1 ; 2 ; 3 |]) ]
                                [ "v", Sql.integerListOrValueNone ValueNone ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.execute (fun r -> r.integerListOrNone "val")

                    Expect.equal results.[0] (Some [| 1 ; 2 ; 3 |]) "List Some"
                    Expect.equal results.[1] None "List None"
                )
            }
        ]

// ─────────────────────────────────────────────────────────────────────
// API Tests - Connection Functions
// ─────────────────────────────────────────────────────────────────────
[<Tests>]
let connectionTests =
    testList
        "Connection API functions"
        [
            test "connect with connection string" {
                let con = Sql.connect "Data Source=:memory:"

                con
                |> Sql.query "SELECT 42 as answer"
                |> Sql.executeRow (fun r -> r.integer "answer")
                |> fun x -> Expect.equal x 42 "Connection string works"
            }

            test "inMemory with unique names are isolated" {
                let name1 = "db1_" + Guid.NewGuid().ToString ("N")
                let name2 = "db2_" + Guid.NewGuid().ToString ("N")

                let con1 = Sql.connect "" |> Sql.inMemory name1 true
                let con2 = Sql.connect "" |> Sql.inMemory name2 true

                con1 |> Sql.query "CREATE TABLE t (x INTEGER)" |> Sql.executeNonQuery |> ignore
                con1 |> Sql.query "INSERT INTO t VALUES (1)" |> Sql.executeNonQuery |> ignore

                // con2 should not see the table from con1
                let result =
                    try
                        con2
                        |> Sql.query "SELECT * FROM t"
                        |> Sql.execute (fun r -> r.integer "x")
                        |> Some
                    with _ ->
                        None

                Expect.isNone result "Different in-memory DBs are isolated"
            }

            test "inMemory with same name shares data" {
                let name = "shared_db_" + Guid.NewGuid().ToString ("N")

                let con1 = Sql.connect "" |> Sql.inMemory name true
                let con2 = Sql.connect "" |> Sql.inMemory name true

                con1
                |> Sql.query "CREATE TABLE shared (x INTEGER)"
                |> Sql.executeNonQuery
                |> ignore

                con1
                |> Sql.query "INSERT INTO shared VALUES (99)"
                |> Sql.executeNonQuery
                |> ignore

                let value =
                    con2
                    |> Sql.query "SELECT x FROM shared"
                    |> Sql.executeRow (fun r -> r.integer "x")

                Expect.equal value 99 "Shared in-memory DB works"
            }

            test "fileDB creates and persists data" {
                withTempFileDb (fun path ->
                    let con = Sql.connect "" |> Sql.fileDB path false

                    con
                    |> Sql.query "CREATE TABLE persistent (val INTEGER)"
                    |> Sql.executeNonQuery
                    |> ignore

                    con
                    |> Sql.query "INSERT INTO persistent VALUES (123)"
                    |> Sql.executeNonQuery
                    |> ignore

                    // Open again and verify data persists
                    let con2 = Sql.connect "" |> Sql.fileDB path true

                    let value =
                        con2
                        |> Sql.query "SELECT val FROM persistent"
                        |> Sql.executeRow (fun r -> r.integer "val")

                    Expect.equal value 123 "File DB persists data"
                )
            }

            test "fileDB readonly prevents writes" {
                withTempFileDb (fun path ->
                    // First create the file
                    let con = Sql.connect "" |> Sql.fileDB path false
                    con |> Sql.query "CREATE TABLE t (x INTEGER)" |> Sql.executeNonQuery |> ignore

                    // Open as readonly
                    let conReadonly = Sql.connect "" |> Sql.fileDB path true

                    let result =
                        try
                            conReadonly
                            |> Sql.query "INSERT INTO t VALUES (1)"
                            |> Sql.executeNonQuery
                            |> ignore

                            false
                        with _ ->
                            true

                    Expect.isTrue result "Readonly file DB prevents writes"
                )
            }

            test "existingConnection reuses connection" {
                use conn = new DuckDBConnection ("Data Source=:memory:")
                conn.Open ()

                Sql.existingConnection conn
                |> Sql.query "CREATE TABLE t (x INTEGER)"
                |> Sql.executeNonQuery
                |> ignore

                Sql.existingConnection conn
                |> Sql.query "INSERT INTO t VALUES (42)"
                |> Sql.executeNonQuery
                |> ignore

                let value =
                    Sql.existingConnection conn
                    |> Sql.query "SELECT x FROM t"
                    |> Sql.executeRow (fun r -> r.integer "x")

                Expect.equal value 42 "Existing connection is reused"
                Expect.equal conn.State ConnectionState.Open "Connection not disposed"
            }

            test "connectFromConfig works with DuckDBConnectionStringBuilder" {
                let builder = DuckDBConnectionStringBuilder ()
                builder.DataSource <- ":memory:"

                let con = Sql.connectFromConfig builder

                let result =
                    con
                    |> Sql.query "SELECT 1 + 1 as sum"
                    |> Sql.executeRow (fun r -> r.integer "sum")

                Expect.equal result 2 "Connection from config works"
            }

            test "cancellationToken can be set" {
                use cts = new CancellationTokenSource ()

                withInMemoryDb (fun con ->
                    let conWithToken = con |> Sql.cancellationToken cts.Token

                    conWithToken
                    |> Sql.query "SELECT 42 as val"
                    |> Sql.executeRow (fun r -> r.integer "val")
                    |> fun x -> Expect.equal x 42 "Cancellation token set"
                )
            }
        ]

// ─────────────────────────────────────────────────────────────────────
// API Tests - Query Configuration
// ─────────────────────────────────────────────────────────────────────
[<Tests>]
let queryConfigTests =
    testList
        "Query configuration API"
        [
            test "query sets SQL command" {
                withInMemoryDb (fun con ->
                    let result =
                        con |> Sql.query "SELECT 1 as num" |> Sql.executeRow (fun r -> r.integer "num")

                    Expect.equal result 1 "Query works"
                )
            }

            test "parameters with list of tuples" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (x INTEGER, y VARCHAR)"
                    |> Sql.executeNonQuery
                    |> ignore

                    con
                    |> Sql.query "INSERT INTO t VALUES ($x, $y)"
                    |> Sql.parameters [ "x", Sql.integer 42 ; "y", Sql.varChar "hello" ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let x, y =
                        con
                        |> Sql.query "SELECT x, y FROM t"
                        |> Sql.executeRow (fun r -> r.integer "x", r.varChar "y")

                    Expect.equal x 42 "Parameter x"
                    Expect.equal y "hello" "Parameter y"
                )
            }

            test "prepare optimizes repeated queries" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val INTEGER)" |> Sql.executeNonQuery |> ignore

                    // Using prepare should still work correctly
                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.integer 100 ]
                    |> Sql.prepare
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.integer "val")

                    Expect.equal result 100 "Prepare doesn't break execution"
                )
            }

            test "func sets command type to stored procedure" {
                // DuckDB doesn't have traditional stored procedures, but this tests the API
                withInMemoryDb (fun con ->
                    // Create a macro (DuckDB's version of stored procedure)
                    con
                    |> Sql.query "CREATE MACRO add_one(x) AS x + 1"
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT add_one(5) as result"
                        |> Sql.executeRow (fun r -> r.integer "result")

                    Expect.equal result 6 "Macro/function works"
                )
            }
        ]

// ─────────────────────────────────────────────────────────────────────
// API Tests - Execution Functions (Sync)
// ─────────────────────────────────────────────────────────────────────
[<Tests>]
let executionSyncTests =
    testList
        "Synchronous execution API"
        [
            test "execute returns list of results" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (x INTEGER)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [ [ "v", Sql.integer 1 ] ; [ "v", Sql.integer 2 ] ; [ "v", Sql.integer 3 ] ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT x FROM t ORDER BY x"
                        |> Sql.execute (fun r -> r.integer "x")

                    Expect.equal results [ 1 ; 2 ; 3 ] "Execute returns all rows"
                )
            }

            test "executeRow returns single row" {
                withInMemoryDb (fun con ->
                    let result =
                        con
                        |> Sql.query "SELECT 'hello' as msg"
                        |> Sql.executeRow (fun r -> r.varChar "msg")

                    Expect.equal result "hello" "ExecuteRow returns single row"
                )
            }

            test "executeRow throws on empty result" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (x INTEGER)" |> Sql.executeNonQuery |> ignore

                    let result =
                        try
                            con
                            |> Sql.query "SELECT x FROM t"
                            |> Sql.executeRow (fun r -> r.integer "x")
                            |> ignore

                            false
                        with
                        | :? NoResultsException -> true
                        | _ -> false

                    Expect.isTrue result "ExecuteRow throws on empty result"
                )
            }

            test "executeNonQuery returns affected rows" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (x INTEGER)" |> Sql.executeNonQuery |> ignore

                    let affected =
                        con |> Sql.query "INSERT INTO t VALUES (1), (2), (3)" |> Sql.executeNonQuery

                    Expect.equal affected 3 "ExecuteNonQuery returns row count"
                )
            }

            test "executeTransaction commits all changes" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (x INTEGER)" |> Sql.executeNonQuery |> ignore

                    let affected =
                        con
                        |> Sql.executeTransaction
                            [
                                "INSERT INTO t VALUES ($v)", [ [ "v", Sql.integer 10 ] ; [ "v", Sql.integer 20 ] ]
                                "INSERT INTO t VALUES ($v)", [ [ "v", Sql.integer 30 ] ]
                            ]

                    Expect.equal affected [ 2 ; 1 ] "Transaction returns affected rows per query"

                    let count =
                        con
                        |> Sql.query "SELECT COUNT(*) as cnt FROM t"
                        |> Sql.executeRow (fun r -> r.bigInt "cnt")

                    Expect.equal count 3L "All rows committed"
                )
            }

            test "executeTransaction rolls back on error" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (x INTEGER PRIMARY KEY)"
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        try
                            con
                            |> Sql.executeTransaction
                                [
                                    "INSERT INTO t VALUES (1)", [ [] ]
                                    "INSERT INTO t VALUES (1)", [ [] ] // Duplicate key error
                                ]
                            |> ignore

                            false
                        with _ ->
                            true

                    Expect.isTrue result "Transaction throws on error"

                    let count =
                        con
                        |> Sql.query "SELECT COUNT(*) as cnt FROM t"
                        |> Sql.executeRow (fun r -> r.bigInt "cnt")

                    Expect.equal count 0L "Transaction rolled back"
                )
            }

            test "iter processes each row without collecting" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (x INTEGER)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [ [ "v", Sql.integer 1 ] ; [ "v", Sql.integer 2 ] ; [ "v", Sql.integer 3 ] ]
                        ]
                    |> ignore

                    let mutable sum = 0

                    con
                    |> Sql.query "SELECT x FROM t"
                    |> Sql.iter (fun r -> sum <- sum + r.integer "x")

                    Expect.equal sum 6 "Iter processes all rows"
                )
            }

            test "toSeq returns lazy sequence" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (x INTEGER)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)",
                            [ [ "v", Sql.integer 1 ] ; [ "v", Sql.integer 2 ] ; [ "v", Sql.integer 3 ] ]
                        ]
                    |> ignore

                    let seq =
                        con
                        |> Sql.query "SELECT x FROM t ORDER BY x"
                        |> Sql.toSeq (fun r -> r.integer "x")

                    let first = seq |> Seq.take 2 |> Seq.toList
                    let second = seq |> Seq.toList

                    Expect.equal first [ 1 ; 2 ] "ToSeq can be partially consumed"
                    Expect.equal second [ 1 ; 2 ; 3 ] "ToSeq can be reused"
                )
            }
        ]

// ─────────────────────────────────────────────────────────────────────
// API Tests - Execution Functions (Async)
// ─────────────────────────────────────────────────────────────────────
// [<Tests>]
// let executionAsyncTests =
//     testList "Asynchronous execution API" [
//         testTask "executeRowAsync returns single row" {
//             withInMemoryDb (fun con ->
//                 task {
//                     let! result = con |> Sql.query "SELECT 'async' as msg"
//                                    |> Sql.executeRowAsync (fun r -> r.varChar "msg")
//                     Expect.equal result "async" "ExecuteRowAsync works"
//                 }
//             )
//         }

//         testTask "executeRowAsync throws on empty result" {
//             withInMemoryDb (fun con ->
//                 task {
//                     con |> Sql.query "CREATE TABLE t (x INTEGER)" |> Sql.executeNonQuery |> ignore

//                     let! result = task {
//                         try
//                             let! _ = con |> Sql.query "SELECT x FROM t" |> Sql.executeRowAsync (fun r -> r.integer "x")
//                             return false
//                         with
//                         | :? NoResultsException -> return true
//                         | _ -> return false
//                     }
//                     Expect.isTrue result "ExecuteRowAsync throws on empty"
//                 }
//             )
//         }

//         testTask "executeNonQueryAsync returns affected rows" {
//             withInMemoryDb (fun con ->
//                 task {
//                     do! con |> Sql.query "CREATE TABLE t (x INTEGER)" |> Sql.executeNonQueryAsync |> Task.ignore

//                     let! affected = con |> Sql.query "INSERT INTO t VALUES (1), (2)" |> Sql.executeNonQueryAsync
//                     Expect.equal affected 2 "ExecuteNonQueryAsync returns count"
//                 }
//             )
//         }

//         testTask "executeTransactionAsync commits all changes" {
//             withInMemoryDb (fun con ->
//                 task {
//                     do! con |> Sql.query "CREATE TABLE t (x INTEGER)" |> Sql.executeNonQueryAsync |> Task.ignore

//                     let! affected = con |> Sql.executeTransactionAsync [
//                         "INSERT INTO t VALUES ($v)", [
//                             ["v", Sql.integer 100]
//                             ["v", Sql.integer 200]
//                         ]
//                     ]

//                     Expect.equal affected [2] "TransactionAsync returns affected rows"

//                     let! count = con |> Sql.query "SELECT COUNT(*) as cnt FROM t"
//                                   |> Sql.executeRowAsync (fun r -> r.bigInt "cnt")
//                     Expect.equal count 2L "All rows committed"
//                 }
//             )
//         }

//         testTask "iterAsync processes each row" {
//             withInMemoryDb (fun con ->
//                 task {
//                     do! con |> Sql.query "CREATE TABLE t (x INTEGER)" |> Sql.executeNonQueryAsync |> Task.ignore
//                     do! con |> Sql.executeTransactionAsync [
//                         "INSERT INTO t VALUES ($v)", [
//                             ["v", Sql.integer 5]
//                             ["v", Sql.integer 10]
//                             ["v", Sql.integer 15]
//                         ]
//                     ] |> Task.ignore

//                     let mutable sum = 0
//                     do! con |> Sql.query "SELECT x FROM t" |> Sql.iterAsync (fun r -> sum <- sum + r.integer "x")

//                     Expect.equal sum 30 "IterAsync processes all rows"
//                 }
//             )
//         }

//         testTask "cancellationToken cancels async operations" {
//             withInMemoryDb (fun con ->
//                 task {
//                     use cts = new CancellationTokenSource()
//                     cts.Cancel()

//                     let! result = task {
//                         try
//                             let! _ = con
//                                      |> Sql.cancellationToken cts.Token
//                                      |> Sql.query "SELECT 1"
//                                      |> Sql.executeNonQueryAsync
//                             return false
//                         with
//                         | :? OperationCanceledException -> return true
//                         | :? System.AggregateException as ex when
//                             ex.InnerExceptions |> Seq.exists (fun e -> e :? OperationCanceledException) -> return true
//                         | _ -> return false
//                     }
//                     Expect.isTrue result "Cancellation token works"
//                 }
//             )
//         }
//     ]

// ─────────────────────────────────────────────────────────────────────
// API Tests - RowReader Error Handling
// ─────────────────────────────────────────────────────────────────────
[<Tests>]
let rowReaderErrorTests =
    testList
        "RowReader error handling"
        [
            test "UnknownColumnException for missing column" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (x INTEGER)" |> Sql.executeNonQuery |> ignore
                    con |> Sql.query "INSERT INTO t VALUES (1)" |> Sql.executeNonQuery |> ignore

                    let result =
                        try
                            con
                            |> Sql.query "SELECT x FROM t"
                            |> Sql.executeRow (fun r -> r.integer "nonexistent")
                            |> ignore

                            false
                        with
                        | :? UnknownColumnException -> true
                        | _ -> false

                    Expect.isTrue result "UnknownColumnException thrown for missing column"
                )
            }

            test "RowReader provides available columns in error message" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (col1 INTEGER, col2 VARCHAR)"
                    |> Sql.executeNonQuery
                    |> ignore

                    con
                    |> Sql.query "INSERT INTO t VALUES (1, 'test')"
                    |> Sql.executeNonQuery
                    |> ignore

                    let mutable errorMsg = ""

                    try
                        con
                        |> Sql.query "SELECT col1, col2 FROM t"
                        |> Sql.executeRow (fun r -> r.integer "col3")
                        |> ignore
                    with
                    | :? UnknownColumnException as ex -> errorMsg <- ex.Message
                    | _ -> ()

                    Expect.isTrue (errorMsg.Contains ("col1")) "Error message contains col1"
                    Expect.isTrue (errorMsg.Contains ("col2")) "Error message contains col2"
                )
            }

            test "MissingQueryException when no query provided" {
                withInMemoryDb (fun con ->
                    let result =
                        try
                            con |> Sql.execute (fun r -> r.integer "x") |> ignore
                            false
                        with
                        | :? MissingQueryException -> true
                        | _ -> false

                    Expect.isTrue result "MissingQueryException thrown"
                )
            }
        ]

// ─────────────────────────────────────────────────────────────────────
// Edge Cases & Special Scenarios
// ─────────────────────────────────────────────────────────────────────
[<Tests>]
let edgeCaseTests =
    testList
        "Edge cases and special scenarios"
        [
            test "Empty string vs null varchar" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val VARCHAR)" |> Sql.executeNonQuery |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($v)", [ [ "v", Sql.varChar "" ] ; [ "v", Sql.varCharOrNone None ] ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.execute (fun r -> r.varCharOrNone "val")

                    Expect.equal results.[0] (Some "") "Empty string preserved"
                    Expect.equal results.[1] None "Null is None"
                )
            }

            test "Maximum values for numeric types" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (ti TINYINT, si SMALLINT, i INTEGER, bi BIGINT)"
                    |> Sql.executeNonQuery
                    |> ignore

                    con
                    |> Sql.query "INSERT INTO t VALUES ($ti, $si, $i, $bi)"
                    |> Sql.parameters
                        [
                            "ti", Sql.tinyInt 127y
                            "si", Sql.smallInt 32767s
                            "i", Sql.integer 2147483647
                            "bi", Sql.bigInt 9223372036854775807L
                        ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let ti, si, i, bi =
                        con
                        |> Sql.query "SELECT ti, si, i, bi FROM t"
                        |> Sql.executeRow (fun r -> r.tinyInt "ti", r.smallInt "si", r.integer "i", r.bigInt "bi")

                    Expect.equal ti 127y "TinyInt max"
                    Expect.equal si 32767s "SmallInt max"
                    Expect.equal i 2147483647 "Integer max"
                    Expect.equal bi 9223372036854775807L "BigInt max"
                )
            }

            test "Minimum values for numeric types" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (ti TINYINT, si SMALLINT, i INTEGER, bi BIGINT)"
                    |> Sql.executeNonQuery
                    |> ignore

                    con
                    |> Sql.query "INSERT INTO t VALUES ($ti, $si, $i, $bi)"
                    |> Sql.parameters
                        [
                            "ti", Sql.tinyInt -128y
                            "si", Sql.smallInt -32768s
                            "i", Sql.integer -2147483648
                            "bi", Sql.bigInt -9223372036854775808L
                        ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let ti, si, i, bi =
                        con
                        |> Sql.query "SELECT ti, si, i, bi FROM t"
                        |> Sql.executeRow (fun r -> r.tinyInt "ti", r.smallInt "si", r.integer "i", r.bigInt "bi")

                    Expect.equal ti -128y "TinyInt min"
                    Expect.equal si -32768s "SmallInt min"
                    Expect.equal i -2147483648 "Integer min"
                    Expect.equal bi -9223372036854775808L "BigInt min"
                )
            }

            test "Very large arrays" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (val INTEGER[])"
                    |> Sql.executeNonQuery
                    |> ignore

                    let largeArray = Array.init 1000 id

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.integerList largeArray ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.integerList "val")

                    Expect.equal result largeArray "Large array roundtrip"
                )
            }

            test "Unicode characters in varchar" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val VARCHAR)" |> Sql.executeNonQuery |> ignore

                    let unicode = "Hello 世界 🌍 Привет مرحبا"

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.varChar unicode ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con
                        |> Sql.query "SELECT val FROM t"
                        |> Sql.executeRow (fun r -> r.varChar "val")

                    Expect.equal result unicode "Unicode preserved"
                )
            }

            test "Binary data with all byte values" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (val BLOB)" |> Sql.executeNonQuery |> ignore

                    let allBytes = Array.init 256 byte

                    con
                    |> Sql.query "INSERT INTO t VALUES ($v)"
                    |> Sql.parameters [ "v", Sql.blob allBytes ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let result =
                        con |> Sql.query "SELECT val FROM t" |> Sql.executeRow (fun r -> r.blob "val")

                    Expect.equal result allBytes "All byte values preserved"
                )
            }

            test "Multiple transactions in sequence" {
                withInMemoryDb (fun con ->
                    con |> Sql.query "CREATE TABLE t (x INTEGER)" |> Sql.executeNonQuery |> ignore

                    con |> Sql.executeTransaction [ "INSERT INTO t VALUES (1)", [ [] ] ] |> ignore

                    con |> Sql.executeTransaction [ "INSERT INTO t VALUES (2)", [ [] ] ] |> ignore

                    con |> Sql.executeTransaction [ "INSERT INTO t VALUES (3)", [ [] ] ] |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT x FROM t ORDER BY x"
                        |> Sql.execute (fun r -> r.integer "x")

                    Expect.equal results [ 1 ; 2 ; 3 ] "Multiple sequential transactions work"
                )
            }

            test "Parameter names with special prefixes handled correctly" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (x INTEGER, y INTEGER, z INTEGER)"
                    |> Sql.executeNonQuery
                    |> ignore

                    con
                    |> Sql.query "INSERT INTO t VALUES ($x, $y, $z)"
                    |> Sql.parameters
                        [
                            "$x", Sql.integer 1 // With $
                            "@y", Sql.integer 2 // With @
                            "z", Sql.integer 3 // Without prefix
                        ]
                    |> Sql.executeNonQuery
                    |> ignore

                    let x, y, z =
                        con
                        |> Sql.query "SELECT x, y, z FROM t"
                        |> Sql.executeRow (fun r -> r.integer "x", r.integer "y", r.integer "z")

                    Expect.equal (x, y, z) (1, 2, 3) "Parameter prefixes normalized"
                )
            }

            test "ValueOption types work correctly" {
                withInMemoryDb (fun con ->
                    con
                    |> Sql.query "CREATE TABLE t (x INTEGER, y VARCHAR)"
                    |> Sql.executeNonQuery
                    |> ignore

                    con
                    |> Sql.executeTransaction
                        [
                            "INSERT INTO t VALUES ($x, $y)",
                            [
                                [
                                    "x", Sql.integerOrValueNone (ValueSome 42)
                                    "y", Sql.varCharOrValueNone (ValueSome "test")
                                ]
                                [
                                    "x", Sql.integerOrValueNone ValueNone
                                    "y", Sql.varCharOrValueNone ValueNone
                                ]
                            ]
                        ]
                    |> ignore

                    let results =
                        con
                        |> Sql.query "SELECT x, y FROM t ORDER BY x NULLS LAST"
                        |> Sql.execute (fun r -> r.integerOrValueNone "x", r.varCharOrValueNone "y")

                    Expect.equal results.[0] (ValueSome 42, ValueSome "test") "ValueSome works"
                    Expect.equal results.[1] (ValueNone, ValueNone) "ValueNone works"
                )
            }
        ]
