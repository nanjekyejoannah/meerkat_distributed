use inline_colorization::*;
use serde::{Deserialize, Serialize};
use serde_json;
use std::{collections::{HashMap, HashSet}, error::Error, net::SocketAddrV4, vec};
use regex::Regex;
use tokio::{
    self,
    net::TcpListener,
    sync::mpsc::{self, Sender, Receiver},
    time::{self, Duration},
};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use tokio_tungstenite::WebSocketStream;
use crate::runtime::message::Val;
use dashmap::DashMap;

use std::sync::Arc;
use tokio::task::yield_now;
use tokio::io::AsyncWriteExt;
use crate::runtime::manager::{Manager, CodeUpdate, WorkerKind};

use crate::runtime::transaction::{Txn, TxnId, WriteToName};
use crate::
    frontend::{ 
        meerast::{Decl, Expr, ReplInput, SglStmt, Stmt, Binop,Uop},
        parse::ReplInputParser,
        typecheck::{self, FreshMetaGenerator, FreshTyvarGenerator, Type},
    };
#[derive(Serialize, Deserialize, Debug)]
struct ClientInitMsg {
    user_id: i32,
}

#[derive(Serialize, Deserialize, Debug)]
struct Server2ClientMsg {
    env: String,
    err: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Client2ServerMsg {
    input: String,
    user_id: i32,
    timestamp: u128,
}

#[derive(Debug)]
struct ListenerCommMsg {
    stream: WebSocketStream<tokio::net::TcpStream>,
    user_id: i32,
}

const BUFFER_SIZE: usize = 1024;

pub struct Communication {
    pub client_stream_map: HashMap<i32, WebSocketStream<tokio::net::TcpStream>>,
    pub manager: Manager, // Use Manager instance for evaluation
}



impl Communication {
    pub async fn process_remote(&mut self) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:2025").await?;
        let (sndr, mut rcvr): (Sender<ListenerCommMsg>, Receiver<ListenerCommMsg>) =
            mpsc::channel(BUFFER_SIZE);

        tokio::spawn(async move {
            while let Ok((stream, addr)) = listener.accept().await {
                println!("{color_blue}Connection with client {:?} established{color_reset}", addr);

                match accept_async(stream).await {
                    Ok(ws_stream) => {
                        let (mut sender, mut receiver) = ws_stream.split();

                        if let Some(Ok(Message::Text(init_text))) = receiver.next().await {
                            if let Ok(mut init_msg) = serde_json::from_str::<ClientInitMsg>(&init_text) {
                                let user_id = init_msg.user_id;
                                
                                println!("User {} connected!", user_id);

                                let init_reply_msg = Server2ClientMsg {
                                    env: "WebSocket connected".to_string(),
                                    err: None,
                                };

                                let init_reply_json = serde_json::to_string(&init_reply_msg).unwrap();
                                
                               // sender.send(Message::Text(init_reply_json)).await.unwrap();

                                let conn_msg = ListenerCommMsg {
                                    stream: sender.reunite(receiver).unwrap(),
                                    user_id,
                                };

                                sndr.send(conn_msg).await.unwrap();
                                
                            }
                        }
                    }
                    Err(e) => {
                        println!("{color_red}WebSocket upgrade failed: {:?}{color_reset}", e);
                    }
                }
            }
        });
        self.handle_existing_connections(&mut rcvr).await;
        Ok(())
    }

    async fn handle_existing_connections(&mut self, rcvr: &mut Receiver<ListenerCommMsg>) {
        let mut to_be_removed: Vec<i32> = vec![];
        let mut stdout = tokio::io::stdout();
        let repl_parser = ReplInputParser::new();
        let mut sigma_m: HashMap<String, Type> = HashMap::new();
        let mut sigma_v: HashMap<String, Type> = HashMap::new();
        let mut pub_access: HashMap<String, bool> = HashMap::new();
        let mut gen_fresh_meta = FreshMetaGenerator::new("default", 0);
        let mut gen_fresh_tyvar = FreshTyvarGenerator::new("default", 0);
        let mut position: HashMap<String, Vec<f64>> = HashMap::new(); 
        let man = &mut self.manager;
        loop {
            tokio::select! {
                Some(listener_msg) = rcvr.recv() => {
                    self.client_stream_map.insert(listener_msg.user_id, listener_msg.stream);
                   
                }
                _ = time::sleep(Duration::from_millis(500)) => {
                  //  println!("Handle existing connections...");
                  
                   
            
                    for (id, stream) in self.client_stream_map.iter_mut() { 
                        let mut stream = std::pin::pin!(stream);
                        tokio::select! {
                            biased; 
            
                            result = stream.next() => {
                                match result {
                                    Some(Ok(Message::Text(msg))) => {
                                        println!("Received from {id}: {msg}");
                                        let client_msg: Client2ServerMsg = serde_json::from_str(&msg).unwrap();
                                        // var x=3;994.5/131.1875
                                        let parts: Vec<&str> = client_msg.input.splitn(2, ';').collect();
                                        let input = parts.get(0).unwrap_or(&"").trim(); // "var x=3"
                                        let positions = parts.get(1).unwrap_or(&"").trim(); 
                                        
                                        
                                        let command_ast = match repl_parser.parse(&input) {
                                            Ok(ast) => ast,
                                            Err(_) => {
                                                let error_msg = Server2ClientMsg {
                                                    env: String::new(),
                                                    err: Some("Syntax Error".to_string()),
                                                };
                                                let error_json = serde_json::to_string(&error_msg).unwrap();
                                                stream.send(Message::Text(error_json)).await.unwrap();
                                                Communication::send_environment_info(&mut *stream, man, &position).await;
                                                continue; 
                                            }
                                        };
                                        println!("Parsed AST: {:?}", command_ast);
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
                                                                    let values: Vec<f64> = positions.split('/').filter_map(|v| v.trim().parse::<f64>().ok()).collect();
                                                                    position.insert(name.clone(), values);
                                                                    println!("Position: {:?}", position);
                                                                    let mut nodes_to_modify = HashSet::new();
                                                                    nodes_to_modify.insert(name.clone());
                            
                                                                    let code_update = CodeUpdate {
                                                                        nodes_to_modify,
                                                                        new_code: vec![(name.clone(), val.clone())],
                                                                    };
                            
                                                                    man
                                                                        .worker_kind_env
                                                                        .insert(name.clone(), WorkerKind::Var);
                                                                    
                                                                    if let Err(e) = man.handle_code_update(code_update).await {
                                                                        let _ = stdout
                                                                            .write_all(format!("\x1b[31m{}\x1b[0m\n", e).as_bytes())
                                                                            .await;
                                                                    }
                                                                }
                            
                                                                Decl::DefDecl { name, val, .. } => {
                                                                    let values: Vec<f64> = positions.split('/').filter_map(|v| v.trim().parse::<f64>().ok()).collect();
                                                                    position.insert(name.clone(), values);
                                                                    println!("Position: {:?}", position);
                                                                    let mut nodes_to_modify = HashSet::new();
                                                                    nodes_to_modify.insert(name.clone());
                            
                                                                    let code_update = CodeUpdate {
                                                                        nodes_to_modify,
                                                                        new_code: vec![(name.clone(), val.clone())],
                                                                    };
                                                                  
                                                                    man
                                                                        .worker_kind_env
                                                                        .insert(name.clone(), WorkerKind::Def);
                                                                    
                                                                   
                                                                    if let Err(e) = man.handle_code_update(code_update).await {
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
                                                                    
                                                                     /*if let Err(e) = self.manager.handle_transaction(&txn).await {
                                                                         let _ = stdout
                                                                             .write_all(format!("\x1b[31mTransaction error: {}\x1b[0m\n", e).as_bytes())
                                                                             .await;
                                                                        continue;
                                                                     }*/
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
                                       
            
                                        Communication::send_environment_info(&mut *stream, man,&position).await;
                                    }
                                    Some(Ok(_)) => {
                                        println!("Received non-text message from client {id}");
                                    }
                                    Some(Err(e)) => {
                                        println!("Error with client {}: {:?}", id, e);
                                        to_be_removed.push(*id);
                                    }
                                    None => {
                                        println!("Connection closed for client {id}");
                                        to_be_removed.push(*id);
                                    }
                                }
                            }
            
                            _ = yield_now() => {
                               // println!("Yielding to other tasks...");
                            }
                        }
                    }
            
                    for i in &to_be_removed {
                        println!("Removing client {}", i);
                        self.client_stream_map.remove(&i);
                    }
                
                }
            }
        }
    }
    async fn send_environment_info(
        stream:  &mut WebSocketStream<tokio::net::TcpStream>,
        man: &Manager,
        position: &HashMap<String, Vec<f64>>,
    ) {
        let mut curr_val_env: HashMap<String, String> = HashMap::new();
        let env_message = "Current Environment:".to_string();
        let env_json = serde_json::to_string(&env_message).unwrap();
    
        // Send the environment message
        stream.send(Message::Text(env_json)).await.unwrap();
    
        // Collect environment values into a HashMap
        for (name, _) in man.system_configuration.iter() {
            let val_of_name = Manager::retrieve_val(man, name); // No changes needed here
            let val_str = match val_of_name {
                Some(Val::Int(val)) => format!("Int({})", val),
                Some(Val::Bool(val)) => format!("Bool({})", val),
                Some(Val::Action(expr)) => format!("Action({:?})", expr),
                Some(Val::Lambda(expr)) => format!("Lambda({:?})", expr),
                None => "None".to_string(),
            };
    
            let expr = man.system_configuration.get(name.as_str());
            let def_str = match expr {
                Some(e) => Communication::expr_to_string(e),
                None => "N/A".to_string(),
            };
    
            let regex = Regex::new(r"\d+$").unwrap();
            let stripped_name = regex.replace(&name, "").to_string();
            let type_var = man.worker_kind_env.get(&stripped_name);
            let kind_str = match type_var {
                Some(kind) => format!("{:?}", kind),
                None => "Unknown".to_string(),
            };
            let position_str = position
                .get(stripped_name.as_str())
                .filter(|v| v.len() == 2)
                .map(|v| format!("{}/{}", v[0], v[1]))
                .unwrap_or_else(|| "0/0".to_string());

            curr_val_env.insert(
                name.clone(),
                format!("{}-{}-{}-{}", val_str, kind_str, def_str,position_str),
            );
        }
    
        // Create response message
        let reply_msg = Server2ClientMsg {
            env: serde_json::to_string(&curr_val_env).unwrap(),
            err: None,
        };
    
        // Serialize to JSON
        let reply_json = serde_json::to_string(&reply_msg).unwrap();
    
        // Send message to client over WebSocket
        stream.send(Message::Text(reply_json)).await.unwrap();
    }
    pub fn expr_to_string(expr: &Expr) -> String {
        match expr {
            Expr::IdExpr { ident } => {
                let regex = regex::Regex::new(r"\d+$").unwrap();
                regex.replace(ident, "").to_string()
            },
            Expr::IntConst { val } => val.to_string(),
            Expr::BoolConst { val } => val.to_string(),
            Expr::BopExpr { opd1, opd2, bop } => {
                let left = Communication::expr_to_string(opd1);
                let right = Communication::expr_to_string(opd2);
                let op = match bop {
                    Binop::Add => "+",
                    Binop::Sub => "-",
                    Binop::Mul => "*",
                    Binop::Div => "/",
                    Binop::And => "&&",
                    Binop::Or => "||",
                    Binop::Eq => "==",
                   // Binop::Neq => "!=",
                    Binop::Lt => "<",
                    //Binop::Lte => "<=",
                    Binop::Gt => ">",
                    //Binop::Gte => ">=",
                };
                format!("({} {} {})", left, op, right)
            }
            Expr::UopExpr { opd, uop } => {
                let val = Communication::expr_to_string(opd);
                let op = match uop {
                    Uop::Neg => "-",
                    Uop::Not => "!",
                };
                format!("({}{})", op, val)
            }
            Expr::IfExpr { cond, then, elze } => {
                format!(
                    "if {} then {} else {}",
                    Communication::expr_to_string(cond),
                    Communication::expr_to_string(then),
                    Communication::expr_to_string(elze)
                )
            }
            Expr::Lambda { pars, body } => {
                let params = pars.iter().map(Communication::expr_to_string).collect::<Vec<_>>().join(", ");
                format!("\\{} -> {}", params, Communication::expr_to_string(body))
            }
            Expr::Apply { fun, args } => {
                let f = Communication::expr_to_string(fun);
                let args_str = args
                    .iter()
                    .map(|arg| Communication::expr_to_string(arg))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", f, args_str)
            }
            Expr::Action { stmt } => {
                format!("action({:?})", stmt)
            }
            Expr::Member { srv_name, member } => {
                format!("{}.{:?}", srv_name, member)
            }
        }
    }
    
}
