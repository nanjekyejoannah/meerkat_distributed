use runtime::repl;
use tokio;
use std::io;
use std::env;
mod tests;

pub mod comm;
pub mod frontend;
pub mod runtime;


use std::fs;



#[tokio::main]
pub async fn main() {
    // println!("Current working directory: {:?}", env::current_dir().unwrap());
    // let filename = "src/tests/tests/test1.meerkat";
    // tests::automate_test::repl_on_test_file(filename).await?;
    // Ok(())
    runtime::repl::repl().await;
}