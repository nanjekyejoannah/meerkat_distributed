use std::collections::{HashMap, HashSet};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::{
    frontend::{ 
        meerast::{Decl, Expr, ReplInput, SglStmt, Stmt},
        parse::ReplInputParser,
        typecheck::{self, FreshMetaGenerator, FreshTyvarGenerator, Type},
    },
    runtime::{ 
        manager::Manager,
        message::{CodeUpdate, WorkerKind},
        message::Val,
        transaction::{Txn, TxnId, WriteToName},
    },
};

pub async fn repl() {
    let mut manager = Manager::new();
    let repl_parser = ReplInputParser::new();
    let mut sigma_m: HashMap<String, Type> = HashMap::new();
    let mut sigma_v: HashMap<String, Type> = HashMap::new();
    let mut pub_access: HashMap<String, bool> = HashMap::new();
    let mut gen_fresh_meta = FreshMetaGenerator::new("default", 0);
    let mut gen_fresh_tyvar = FreshTyvarGenerator::new("default", 0);

    loop {
        let mut stdout = tokio::io::stdout();
        let stdin = tokio::io::stdin();

        let mut curr_val_env: HashMap<String, _> = HashMap::new();
        // println!("System Env {:?}", manager.system_configuration);  // Debug
        for (name, _) in manager.system_configuration.iter() { 
            let val_of_name = Manager::retrieve_val(&manager, name);
            curr_val_env.insert(name.clone(), val_of_name);
        }

        // Display current environment
        let _ = stdout
            .write_all(b"\x1b[32mcurrent environment\x1b[0m\n")
            .await
            .expect("Failed to write to stdout");

        // Display values from the environment 
        for (name, val_opt) in curr_val_env.iter() {
            match val_opt {
                Some(Val::Int(val)) => {
                    let _ = stdout
                        .write_all(format!("\x1b[32m{}: Int({})\x1b[0m\n", name, val).as_bytes())
                        .await
                        .expect("Failed to write to stdout");
                }
                Some(Val::Bool(val)) => {
                    let _ = stdout
                        .write_all(format!("\x1b[32m{}: Bool({})\x1b[0m\n", name, val).as_bytes())
                        .await
                        .expect("Failed to write to stdout");
                }
                Some(Val::Action(expr)) => {
                    let _ = stdout
                        .write_all(
                            format!("\x1b[32m{}: Action({:?})\x1b[0m\n", name, expr).as_bytes(),
                        )
                        .await
                        .expect("Failed to write to stdout");
                }
                Some(Val::Lambda(expr)) => {
                    let _ = stdout
                        .write_all(
                            format!("\x1b[32m{}: Lambda({:?})\x1b[0m\n", name, expr).as_bytes(),
                        )
                        .await
                        .expect("Failed to write to stdout");
                }
                None => {
                    let _ = stdout
                        .write_all(format!("\x1b[32m{}: None\x1b[0m\n", name).as_bytes())
                        .await
                        .expect("Failed to write to stdout");
                }
            }
        }

        // Display prompt
        let _ = stdout
            .write_all(b"\x1b[32;1m\xCE\xBB> \x1b[0m")
            .await
            .expect("Failed to write prompt");
        let _ = stdout.flush().await.unwrap();

        // Read input
        let reader = tokio::io::BufReader::new(stdin);
        let mut lines = reader.lines();
        let command_string = lines
            .next_line()
            .await
            .expect("Failed to read line")
            .unwrap_or_default();

        // Parse input
        let command_ast = match repl_parser.parse(&command_string) {
            Ok(ast) => { 
                ast
            }
            Err(_) => {
                let _ = stdout
                    .write_all(b"\x1b[31msyntax error\x1b[0m\n")
                    .await
                    .expect("Failed to write error");
                continue;
            }
        };

        match command_ast {
            ReplInput::Exit => std::process::exit(0),
            ReplInput::Service(_) => panic!(),
            ReplInput::Open(_) => panic!(),
            ReplInput::Close => panic!(),

            ReplInput::Decl(decls) => {
                for decl in decls {
                    match typecheck::check_decl(
                        &mut sigma_v,
                        &mut sigma_m,
                        &mut pub_access,
                        &mut gen_fresh_meta,
                        &mut gen_fresh_tyvar,
                        &decl,
                    ) {
                        Ok(_) => {
                            match decl {
                                Decl::VarDecl { name, val } => {
                                    // Create code update for variable declaration
                                    let mut nodes_to_modify = HashSet::new();
                                    nodes_to_modify.insert(name.clone());

                                    let code_update = CodeUpdate {
                                        nodes_to_modify,
                                        new_code: vec![(name.clone(), val.clone())],
                                    };

                                    manager
                                        .worker_kind_env
                                        .insert(name.clone(), WorkerKind::Var);

                                    if let Err(e) = manager.handle_code_update(code_update).await {
                                        let _ = stdout
                                            .write_all(format!("\x1b[31m{}\x1b[0m\n", e).as_bytes())
                                            .await;
                                    }
                                }

                                Decl::DefDecl { name, val, .. } => {
                                    let mut nodes_to_modify = HashSet::new();
                                    nodes_to_modify.insert(name.clone());

                                    let code_update = CodeUpdate {
                                        nodes_to_modify,
                                        new_code: vec![(name.clone(), val.clone())],
                                    };

                                    manager
                                        .worker_kind_env
                                        .insert(name.clone(), WorkerKind::Def);

                                    if let Err(e) = manager.handle_code_update(code_update).await {
                                        let _ = stdout
                                            .write_all(format!("\x1b[31m{}\x1b[0m\n", e).as_bytes())
                                            .await;
                                    }
                                }

                                _ => {
                                    let _ = stdout
                                        .write_all(b"\x1b[31munsupported declaration type\x1b[0m\n")
                                        .await;
                                }
                            }
                        }
                        Err(_) => {
                            let _ = stdout
                                .write_all(b"\x1b[31mtype error\x1b[0m\n")
                                .await
                                .expect("Failed to write error");
                            continue;
                        }
                    }
                }
            },

            ReplInput::Do(stmt) => {
                match stmt {
                    Stmt::Stmt { sgl_stmts } => {
                        for sgl_stmt in sgl_stmts {
                            match sgl_stmt {
                                SglStmt::Ass { dst, src } => {
                                    let txn = Txn {
                                        id: TxnId::new(),
                                        writes: vec![WriteToName {
                                            name: match dst {
                                                Expr::IdExpr { ident } => ident,
                                                _ => panic!(),
                                            },
                                            expr: src,
                                        }],
                                    };
                                    // if let Err(e) = manager.handle_transaction(&txn).await {
                                    //     let _ = stdout
                                    //         .write_all(format!("\x1b[31mTransaction error: {}\x1b[0m\n", e).as_bytes())
                                    //         .await;
                                    //     continue;
                                    // }
                                }
                                _ => {
                                    let _ = stdout
                                        .write_all(b"\x1b[31munsupported statement type\x1b[0m\n")
                                        .await;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
