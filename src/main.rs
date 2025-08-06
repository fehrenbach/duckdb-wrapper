use duckdb::{Connection, Error};
use polars_core::utils::accumulate_dataframes_vertical_unchecked;
use polars_io::{SerWriter, json::JsonFormat};
use std::io::{self, Read, Write};

enum Command {
    Execute,
    Query,
}

impl std::str::FromStr for Command {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "execute" => Ok(Command::Execute),
            "query" => Ok(Command::Query),
            _ => Err(format!("Unknown command: {}", s)),
        }
    }
}

// We write errors to stdout, so clients to don't have to read from both stdout
// and stderr.
fn print_error(err: Error) {
    io::stdout().write_all(b"error\n").unwrap();
    let err_string = err.to_string();
    let msg = err_string.as_bytes();
    io::stdout()
        .write_all(msg.len().to_string().as_bytes())
        .unwrap();
    io::stdout().write_all(b"\n").unwrap();
    io::stdout().write_all(msg).unwrap();
    io::stdout().flush().unwrap();
}

fn read_command() -> Command {
    let mut input_line = String::new();
    io::stdin().read_line(&mut input_line).unwrap();
    input_line.trim().parse::<Command>().unwrap()
}

fn read_query() -> String {
    let mut input_line = String::new();
    io::stdin().read_line(&mut input_line).unwrap();
    let num = input_line.trim().parse::<usize>().unwrap();

    let mut buffer = vec![0; num];
    io::stdin().read_exact(buffer.as_mut_slice()).unwrap();
    String::from_utf8(buffer.iter().map(|&c| c as u8).collect()).unwrap()
}

fn handle_execute(conn: &Connection, query: &str) {
    match conn.execute_batch(query) {
        Ok(_) => println!("execute"),
        Err(err) => print_error(err),
    }
}

fn handle_query(conn: &Connection, query: &str) {
    let mut stmt = match conn.prepare(query) {
        Ok(stmt) => stmt,
        Err(err) => {
            print_error(err);
            return;
        }
    };

    let pl = match stmt.query_polars([]) {
        Ok(pl) => pl,
        Err(err) => {
            print_error(err);
            return;
        }
    };

    let mut pl = pl.peekable();
    // accumulate_dataframes_vertical_unchecked throws if empty...
    // so we peek at the first element.
    if pl.peek().is_none() {
        print!("query\n2\n[]");
    } else {
        let mut df = accumulate_dataframes_vertical_unchecked(pl);
        let mut out_buffer = Vec::new();
        polars_io::json::JsonWriter::new(&mut out_buffer)
            .with_json_format(JsonFormat::Json)
            .finish(&mut df)
            .unwrap();
        println!("query");
        println!("{}", out_buffer.len());
        io::stdout().write_all(&out_buffer).unwrap();
    }
    io::stdout().flush().unwrap();
}

/// Reads commands from stdin, runs them, and returns results as JSON on stdout.
///
/// The protocol is as follows:
/// - First, the command, either "execute" or "query", followed by a newline.
/// - Second, the length of the query, in (utf8) bytes, followed by a newline.
/// - Then the statement(s) or query to execute. This must have the previously indicated number of bytes.
/// - In case of "execute", we respond with "execute\n" and no further data.
/// - In case of "query", we respond with "query\n", then the number of bytes in the result followed by a newline. Then the result as JSON.
/// - In case of an error, we respond with "error\n", then the number of bytes in the error message, followed by the error message itself.
fn main() {
    let conn = Connection::open(":memory:").unwrap();
    loop {
        // I/O errors and parsing errors represent wrapper bugs and will panic.
        // DuckDB/SQL level errors should be handled gracefully by forwarding them to the client.

        let command = read_command();
        let query = read_query();

        match command {
            Command::Execute => handle_execute(&conn, &query),
            Command::Query => handle_query(&conn, &query),
        }
    }
}
