use std::fmt::Display;
use std::collections::{HashMap, HashSet};

use std::fs::File;
use std::io::{self, BufRead, BufReader};

use regex::Regex;

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

impl Display for Val {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Val::Int(n) => write!(f, "Int({:?})", n),
            Val::Bool(b) => write!(f, "Bool({:?}", b),
            Val::Action(expr) => write!(f, "Action({:?}", expr),
            Val::Lambda(expr) => write!(f, "Lambda({:?}", expr),
        }
    }
}

pub async fn repl_on_test_file(filename: &str) -> io::Result<()> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    let assert_regex = Regex::new(r"^//@assert (\w+) = (\d+)$")
        .expect("regex pattern incorrect for assert in test files");

    let mut assertions: HashMap<String, i32> = HashMap::new();
    let mut source_code: Vec<String> = Vec::new();
    let mut start_collect_code = false;

    for line in reader.lines() {
        let line = line?;
        // first process all assertions
        if let Some(caps) = assert_regex.captures(&line) {
            let var_name = caps[1].to_string();
            let value = caps[2].parse::<i32>().unwrap();
            assertions.insert(var_name, value);
        } else {
            start_collect_code = true; // start collecting code after assertions
        }

        if start_collect_code {
            source_code.push(line);
        }
    }

    let val_env_res = repl_core(source_code).await;
    if let Ok(val_env) = val_env_res {
        for (val, expected) in assertions.iter() {
            if let Some(v_opt) = val_env.get(val) {
                if let Some(v) = v_opt {
                    // todo!() extend to other type of values
                    match v {
                        Val::Int(n) if *n != *expected => {
                            println!("Wrong value of {:?} 
                                stored in environment, expected {:?}, find {:?}",
                                val, *expected, *n
                            )
                        },
                        Val::Bool(_) => todo!(),
                        Val::Action(_) => todo!(),
                        Val::Lambda(_) => todo!(),
                        _ => {
                            println!("Correct value of {:?}", val)
                        }
                    }
                } else {
                    println!("Cannot find {:?} in environment", val);
                }
            }
        }        
    } else { println!("Error from intepreter {:?}", val_env_res.unwrap_err()); }

    println!("success");
    Ok(())
}


pub async fn repl_core(lines: Vec<String>) -> Result<HashMap<String, Option<Val>>, String> {

    let mut manager = Manager::new();
    let repl_parser = ReplInputParser::new();
    let mut sigma_m: HashMap<String, Type> = HashMap::new();
    let mut sigma_v: HashMap<String, Type> = HashMap::new();
    let mut pub_access: HashMap<String, bool> = HashMap::new();
    let mut gen_fresh_meta = FreshMetaGenerator::new("default", 0);
    let mut gen_fresh_tyvar = FreshTyvarGenerator::new("default", 0);

    for line in lines.iter() {
        // Parse input
        let line_ast = match repl_parser.parse(&line) {
            Ok(ast) => ast,
            Err(_) => { return Err("line cannot be parsed".to_string()); }
        };

        match line_ast {
            ReplInput::Exit => panic!(),
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
                                        return Err(e.to_string());
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
                                        return Err(e.to_string());
                                    }
                                }

                                _ => {}
                            }
                        }
                        Err(e) => return Err(e.to_string())
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
                                    let _ = manager.handle_transaction(&txn).await;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
    }

    let mut curr_val_env: HashMap<String, _> = HashMap::new();
    // println!("System Env {:?}", manager.system_configuration);  // Debug
    for (name, _) in manager.system_configuration.iter() { 
        let val_of_name = Manager::retrieve_val(&manager, name);
        curr_val_env.insert(name.clone(), val_of_name);
    }
        
    Ok(curr_val_env)

}

