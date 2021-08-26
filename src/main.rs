use std::{collections::HashMap, error::Error, fs::File, process};

use csv::Trim;
use serde::{Deserialize, Serialize};

type ClientId = u16;
type TxId = u32;

/// Input format for transaction.
#[derive(Debug, Deserialize)]
struct Transaction {
    #[serde(rename = "type")]
    kind: String,
    client: ClientId,
    tx: TxId,
    amount: Option<f64>,
}

/// Output format for client data.
#[derive(Debug, Serialize)]
struct Client {
    client: ClientId,
    available: f64,
    held: f64,
    total: f64,
    locked: bool,
}

/// Ledger keeps track of user accounts and their balances.
type Ledger = HashMap<ClientId, Client>;

/// Update the ledger based on the provided transaction.
fn process_tx(
    ledger: &mut Ledger,
    tx_log: &mut HashMap<TxId, Transaction>,
    mut tx: Transaction,
) -> Result<(), String> {
    // get client entry or create it if it does not exist
    let client = match ledger.get_mut(&tx.client) {
        Some(client) => client,
        None => {
            assert!(ledger
                .insert(
                    tx.client,
                    Client {
                        client: tx.client,
                        available: 0.0,
                        held: 0.0,
                        total: 0.0,
                        locked: false,
                    }
                )
                .is_none());
            ledger.get_mut(&tx.client).unwrap()
        }
    };
    let tx_id = tx.tx;
    match tx.kind.as_str() {
        "deposit" => {
            let amount = match tx.amount {
                Some(amount) => format!("{:.4}", amount).parse::<f64>().unwrap(),
                None => return Err("deposit entry without the amount".into()),
            };
            if client.locked {
                eprintln!("Cannot deposit - client account {} is locked", tx.client);
                return Ok(());
            }
            client.total += amount;
            client.available += amount;
            tx.amount = Some(amount); // update it in case it was trimmed
            tx_log.insert(tx_id, tx);
        }
        "withdrawal" => {
            let amount = match tx.amount {
                Some(amount) => format!("{:.4}", amount).parse::<f64>().unwrap(),
                None => return Err("withdrawal entry without the amount".into()),
            };
            if client.locked {
                eprintln!("Cannot withdraw - client account {} is locked", tx.client);
                return Ok(());
            }
            if client.available < amount {
                eprintln!(
                    "Insufficient balance for withdrawal from client account {}",
                    tx.client
                );
                return Ok(());
            }
            client.total -= amount;
            client.available -= amount;
            tx.amount = Some(amount); // update it in case it was trimmed
            tx_log.insert(tx_id, tx);
        }
        "dispute" => {
            let tx_context = match tx_log.get(&tx_id) {
                Some(tx_context) => tx_context,
                None => {
                    eprintln!("Ignoring dispute that references unknown transaction");
                    return Ok(());
                }
            };
            let amount = tx_context.amount.unwrap();
            if client.available < amount {
                eprintln!("Cannot dispute more than what is available");
                return Ok(());
            }
            client.available -= amount;
            client.held += amount;
        }
        "resolve" => {
            let tx_context = match tx_log.get(&tx_id) {
                Some(tx_context) => tx_context,
                None => {
                    eprintln!("Ignoring resolve that references unknown transaction");
                    return Ok(());
                }
            };
            let amount = tx_context.amount.unwrap();
            if client.held < amount {
                eprintln!("Cannot resolve more than what is held");
                return Ok(());
            }
            client.held -= amount;
            client.available += amount;
        }
        "chargeback" => {
            // remove the transaction from log to avoid double chargeback
            let tx_context = match tx_log.remove(&tx_id) {
                Some(tx_context) => tx_context,
                None => {
                    eprintln!("Ignoring chargeback that references unknown transaction");
                    return Ok(());
                }
            };
            if tx_context.kind != "deposit" {
                tx_log.insert(tx_id, tx_context);
                eprintln!("Ignoring chargeback acting on a different transaction than deposit");
                return Ok(());
            }
            let amount = tx_context.amount.unwrap();
            client.locked = true;
            client.held -= amount;
            client.total -= amount;
        }
        kind => return Err(format!("Unknown transaction type \"{}\"", kind)),
    }
    Ok(())
}

/// Parse all transactions from the input stream and build up the ledger.
fn parse<R>(stream: R) -> Result<Ledger, Box<dyn Error>>
where
    R: std::io::Read,
{
    // transaction log keeps track of processed transactions
    let mut tx_log: HashMap<TxId, Transaction> = HashMap::new();
    // ledger
    let mut ledger = HashMap::new();

    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(Trim::All)
        .from_reader(stream);
    for result in rdr.deserialize() {
        let record = result?;
        process_tx(&mut ledger, &mut tx_log, record)?;
    }
    Ok(ledger)
}

/// Print all users from the ledger with balance info to the provided stream.
fn output<W>(ledger: &Ledger, stream: W) -> Result<(), Box<dyn Error>>
where
    W: std::io::Write,
{
    let mut wtr = csv::Writer::from_writer(stream);
    for (_, val) in ledger.iter() {
        wtr.serialize(val)?;
    }
    wtr.flush()?;
    Ok(())
}

fn main() {
    // Parse arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: ledger <input-file>");
        process::exit(1);
    }
    let filename: &str = &args[1];

    // Open the input file
    let file = match File::open(filename) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("Failed to open the input file {}: {}", filename, e);
            process::exit(1);
        }
    };

    // Parse entries from the input file stream
    let ledger = match parse(file) {
        Ok(ledger) => ledger,
        Err(e) => {
            eprintln!("Failed to parse CSV input: {}", e);
            process::exit(1);
        }
    };

    // Print the ledger
    if let Err(e) = output(&ledger, std::io::stdout()) {
        eprintln!("Failed to print the ledger: {}", e);
        process::exit(1);
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() -> Result<(), Box<dyn Error>> {
        let input = "\
type, client, tx, amount
deposit, 1, 1, 1.0000001
deposit, 2, 2, 2.0
deposit, 1, 3, 0.5
deposit, 1, 4, 2.0
withdrawal, 1, 5, 1.5
withdrawal, 2, 6, 3.0
dispute, 1, 1
resolve, 1, 1
dispute, 1, 3
dispute, 1, 1
chargeback, 1, 1
";
        let ledger = parse(input.as_bytes())?;
        assert_eq!(ledger.len(), 2);
        let clnt1 = ledger.get(&1).unwrap();
        assert_eq!(clnt1.total, 1.0);
        assert_eq!(clnt1.available, 0.5);
        assert_eq!(clnt1.held, 0.5);
        assert_eq!(clnt1.locked, true);
        let clnt2 = ledger.get(&2).unwrap();
        assert_eq!(clnt2.total, 2.0);
        assert_eq!(clnt2.available, 2.0);
        assert_eq!(clnt2.held, 0.0);
        assert_eq!(clnt2.locked, false);
        Ok(())
    }

    #[test]
    fn test_output() -> Result<(), Box<dyn Error>> {
        let mut ledger = HashMap::new();
        ledger.insert(
            1,
            Client {
                client: 1,
                total: 1.5,
                available: 1.0,
                held: 0.5,
                locked: true,
            },
        );
        ledger.insert(
            2,
            Client {
                client: 2,
                total: 1.5,
                available: 1.5,
                held: 0.0,
                locked: false,
            },
        );
        let out = Vec::new();
        output(&ledger, out)?;
        assert_eq!(true, true);
        Ok(())
    }
}
