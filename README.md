# DuckDB.FSharp

Thin F#-friendly layer for DuckDB, inspired by [Npgsql.FSharp](https://github.com/Zaid-Ajaj/Npgsql.FSharp).

Provides a functional, type-safe, and ergonomic API for working with DuckDB databases in F#, including connection management, parameterized queries, and flexible reader mappings.


## Installation

```bash
dotnet add package DuckDB.FSharp
```

## Quick Start

DuckDB connection strings are simple paths to a database file or `:memory:` for an in-memory database.

```fsharp
open DuckDB.FSharp

let connectionString = ":memory:"  // or "mydb.db" for a file-based database

connectionString
|> Sql.connect
|> Sql.query "SELECT 1 AS value"
|> Sql.execute (fun read -> read.int "value")
```

## Usage Examples

### Reading results as a list of records

```fsharp
type User = {
    Id: int
    Username: string
    Email: string option
}

let getUsers (connectionString: string) : List<User> =
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT user_id, username, email FROM users"
    |> Sql.execute (fun read ->
        {
            Id = read.int "user_id"
            Username = read.text "username"
            Email = read.textOrNone "email"
        })
```

### Asynchronous execution

```fsharp
let getUsersAsync (connectionString: string) : Async<List<User>> =
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT user_id, username, email FROM users"
    |> Sql.executeAsync (fun read ->
        {
            Id = read.int "user_id"
            Username = read.text "username"
            Email = read.textOrNone "email"
        })
```

### Parameterized queries

```fsharp
let getActiveUsers (connectionString: string) (isActive: bool) =
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT * FROM users WHERE is_active = @isActive"
    |> Sql.parameters [ "@isActive", Sql.bool isActive ]
    |> Sql.execute (fun read ->
        {
            Id = read.int "user_id"
            Username = read.text "username"
            Email = read.textOrNone "email"
        })
```

### Reading a single row (scalar values)

```fsharp
let getUserCount (connectionString: string) : int64 =
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT COUNT(*) AS count FROM users"
    |> Sql.executeRow (fun read -> read.int64 "count")
```

### Transactions (multiple statements)

```fsharp
connectionString
|> Sql.connect
|> Sql.executeTransaction [
    "INSERT INTO users (username, email) VALUES (@name1, @email1), (@name2, @email2)", [
        [ "@name1", Sql.text "alice" ; "@email1", Sql.text "alice@example.com" ]
        [ "@name2", Sql.text "bob"   ; "@email2", Sql.textOrNone None ]
    ]
    "UPDATE settings SET version = @version", [
        [ "@version", Sql.int 2 ]
    ]
]
```

## Features

- Pure functional pipeline style (inspired by Npgsql.FSharp)
- Parameterized queries with type-safe parameter helpers
- Support for nullable columns via `textOrNone`, `intOrNone`, etc.
- Synchronous and asynchronous execution
- Transaction support
- Comprehensive unit tests covering connections, queries, parameters, readers, and edge cases


## Status
[![.NET](https://github.com/typesmar/DuckDB.FSharp/actions/workflows/dotnet.yml/badge.svg)](https://github.com/typesmar/DuckDB.FSharp/actions/workflows/dotnet.yml)


## Contributing

Contributions are welcome! Feel free to open issues or PRs on GitHub.

## License

MIT License (see LICENSE file)

## Support & Freelance

If DuckDB.FSharp is useful in your projects:

- ⭐ Star the repo to show support
- Consider [sponsoring on GitHub](https://github.com/sponsors/typesmar) ❤️

I'm also available for freelance F# work, especially data/analytics projects with DuckDB.
Feel free to reach out on X: [@typesmar](https://x.com/typesmar)

