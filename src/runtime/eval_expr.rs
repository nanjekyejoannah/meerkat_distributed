use std::{
    collections::{HashMap, HashSet},
    iter,
};

use crate::{
    frontend::meerast::{Binop, Expr, SglStmt, Stmt, Uop},
    runtime::message::Val,
};


pub fn evaluate_expr(expr: &Expr, names_to_values: &HashMap<String, Option<Val>>) -> Option<Val> {
   
    match expr {
        Expr::IdExpr { ident } => {
            return match names_to_values.get(ident) {
                Some(Some(v)) => Some(v.clone()),
                _ => None,
            };
        }
        Expr::IntConst { val } => Some(Val::Int(val.clone())),
        Expr::BoolConst { val } => Some(Val::Bool(val.clone())),
        Expr::Action { stmt: _ } => Some(Val::Action(expr.clone())),
        Expr::Member {
            srv_name: _,
            member: _,
        } => panic!(),
        Expr::Apply { fun, args } => {
            let opt_substed_body = subst_pars_of_apply_for_args(fun, args, names_to_values);
            let substed_body = match opt_substed_body {
                Some(bd) => bd,
                None => return None,
            };
            evaluate_expr(&substed_body, names_to_values)
        }
        Expr::BopExpr { opd1, opd2, bop } => {
            let opt_val1 = evaluate_expr(opd1, names_to_values);
            let opt_val2 = evaluate_expr(opd2, names_to_values);
            let (val1, val2) = match (opt_val1, opt_val2) {
                (Some(v1), Some(v2)) => (v1, v2),
                _ => {
                    return None;
                }
            };
            Some(evaluate_binop_vals(&val1, &val2, bop))
        }
        Expr::UopExpr { opd, uop } => {
            let opt_val = evaluate_expr(opd, names_to_values);
            let val = match opt_val {
                Some(v) => v,
                None => return None,
            };
            Some(evaluate_uop_vals(&val, uop))
        }
        Expr::IfExpr { cond, then, elze } => {
            let opt_cond = evaluate_expr(cond, names_to_values);
            let evaled_cond = match opt_cond {
                Some(Val::Bool(b)) => b,
                _ => {
                    return None;
                }
            };
            if evaled_cond {
                evaluate_expr(then, names_to_values)
            } else {
                evaluate_expr(elze, names_to_values)
            }
        }
        Expr::Lambda { pars: _, body: _ } => Some(Val::Lambda(expr.clone())),
    }
}

fn subst_pars_of_apply_for_args(
    fun: &Expr,
    args: &Vec<Expr>,
    names_to_values: &HashMap<String, Option<Val>>,
) -> Option<Expr> {
    let fun_val = evaluate_expr(fun, names_to_values);
    let fun_val = match fun_val {
        Some(v) => v,
        None => {
            return None;
        }
    };
    let fun = &match fun_val {
        Val::Int(_) => panic!(),
        Val::Bool(_) => panic!(),
        Val::Action(_) => panic!(),
        Val::Lambda(e) => e,
    };
    let pars = match fun {
        Expr::Lambda { pars: ps, body: _ } => ps,
        Expr::IdExpr { ident } => {
            let fun_lambda = match names_to_values.get(ident) {
                Some(val) => match val {
                    Some(Val::Lambda(lam)) => lam,
                    _ => panic!(),
                },
                None => panic!(),
            };
            match fun_lambda {
                Expr::Lambda { pars: ps, body: _ } => ps,
                _ => panic!(),
            }
        }
        _ => panic!(),
    };
    let body = match fun {
        Expr::Lambda { pars: _, body: bd } => bd,
        Expr::IdExpr { ident } => {
            let fun_lambda = match names_to_values.get(ident) {
                Some(val) => match val {
                    Some(Val::Lambda(lam)) => lam,
                    _ => panic!(),
                },
                None => panic!(),
            };
            match fun_lambda {
                Expr::Lambda { pars: _, body: bd } => bd,
                _ => panic!(),
            }
        }
        _ => panic!(),
    };
    let mut par_arg_map: HashMap<String, Expr> = HashMap::new();
    for (par, arg) in iter::zip(pars.iter(), args.iter()) {
        let par_ident = match par {
            Expr::IdExpr { ident } => ident.clone(),
            _ => panic!(),
        };
        par_arg_map.insert(par_ident, arg.clone());
    }
    let mut substed_expr = *body.clone();
    subst(&mut substed_expr, &par_arg_map);
    Some(substed_expr)
}

fn evaluate_binop_vals(val1: &Val, val2: &Val, bop: &Binop) -> Val {
    match (val1, val2) {
        (Val::Int(i), Val::Int(j)) => match bop {
            Binop::Add => Val::Int(i + j),
            Binop::Sub => Val::Int(i - j),
            Binop::Mul => Val::Int(i * j),
            Binop::Div => Val::Int(i / j),
            Binop::Eq => Val::Bool(i == j),
            Binop::Lt => Val::Bool(i < j),
            Binop::Gt => Val::Bool(i > j),
            Binop::And | Binop::Or => panic!(),
        },
        (Val::Bool(b1), Val::Bool(b2)) => match bop {
            Binop::Add
            | Binop::Sub
            | Binop::Mul
            | Binop::Div
            | Binop::Eq
            | Binop::Lt
            | Binop::Gt => panic!(),
            Binop::And => Val::Bool(*b1 && *b2),
            Binop::Or => Val::Bool(*b1 || *b2),
        },
        _ => panic!(),
    }
}

fn evaluate_uop_vals(val: &Val, uop: &Uop) -> Val {
    match val {
        Val::Int(i) => match uop {
            Uop::Neg => Val::Int(-i),
            Uop::Not => panic!(),
        },
        Val::Bool(b) => match uop {
            Uop::Neg => panic!(),
            Uop::Not => Val::Bool(!b),
        },
        _ => panic!(),
    }
}

fn subst(expr: &mut Expr, ident_expr_map: &HashMap<String, Expr>) {
    match expr {
        Expr::IdExpr { ident } => {
            let ent = ident_expr_map.get(ident);
            match ent {
                Some(e) => {
                    *expr = e.clone();
                }
                None => {}
            }
        }
        Expr::IntConst { val: _ } => {}
        Expr::BoolConst { val: _ } => {}
        Expr::Action { stmt } => {
            let sgls = match stmt {
                Stmt::Stmt { sgl_stmts } => sgl_stmts,
            };
            for sgl_stmt in sgls.iter_mut() {
                match sgl_stmt {
                    SglStmt::Do { act } => {
                        subst(act, ident_expr_map);
                    }
                    SglStmt::Ass { dst: _, src } => {
                        subst(src, ident_expr_map);
                    }
                }
            }
        }
        Expr::Member {
            srv_name: _,
            member: _,
        } => panic!(),
        Expr::Apply { fun, args } => {
            subst(fun, ident_expr_map);
            for arg in args.iter_mut() {
                subst(arg, ident_expr_map);
            }
        }
        Expr::BopExpr { opd1, opd2, bop: _ } => {
            subst(opd1, ident_expr_map);
            subst(opd2, ident_expr_map);
        }
        Expr::UopExpr { opd, uop: _ } => {
            subst(opd, ident_expr_map);
        }
        Expr::IfExpr { cond, then, elze } => {
            subst(cond, ident_expr_map);
            subst(then, ident_expr_map);
            subst(elze, ident_expr_map);
        }
        Expr::Lambda { pars, body } => {
            let par_names: HashSet<String> = pars
                .iter()
                .map(|p| match p {
                    Expr::IdExpr { ident } => ident.clone(),
                    _ => panic!(),
                })
                .collect();
            let mut body_map: HashMap<String, Expr> = HashMap::new();
            for (ident, arg_expr) in ident_expr_map.iter() {
                if !par_names.contains(ident) {
                    body_map.insert(ident.clone(), arg_expr.clone());
                }
            }
            subst(body, &body_map);
        }
    }
}
