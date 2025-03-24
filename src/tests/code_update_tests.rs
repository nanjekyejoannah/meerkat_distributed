use super::*;  
use std::collections::HashSet;
use tokio::test;
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::{
    frontend::meerast::{Expr, Binop},
    runtime::{
        lock::{LockType, LockKind},
        message::{Message,WorkerKind, Val, CodeUpdate},
        manager::{Manager, ManagerError },
        transaction::{Txn, TxnId, WriteToName},
    },
};

const BUFFER_SIZE: usize = 1024;

#[test]
async fn test_basic_code_update() {
    let mut manager = Manager::new();
    manager.managed_nodes.insert("x".to_string());

    let mut nodes_to_modify = HashSet::new();
    nodes_to_modify.insert("x".to_string());

    let mut new_code = Vec::new();
    new_code.push(("x".to_string(), Expr::IntConst { val: 42 }));

    let update = CodeUpdate {
        nodes_to_modify,
        new_code,
    };

    let result = manager.handle_code_update(update).await;
    assert!(result.is_ok());

    // Check that the configuration was updated
    assert!(manager.system_configuration.contains_key("x0"));
    if let Some(expr) = manager.system_configuration.get("x0") {
        assert_eq!(*expr, Expr::IntConst { val: 42 });
    }
}

/*
// assert f == 5

var x = 2
var y = 3
def f = x + y
*/

#[tokio::test]
async fn test_basic_code_update_with_multiple_managers() {
    let mut manager1 = Manager::new();
    let mut manager2 = Manager::new();
    
    // Set manager IDs
    manager1.manager_id = Some("manager1".to_string());
    manager2.manager_id = Some("manager2".to_string());
    
    // Set up peer channels
    let (sender1_to_2, receiver2_from_1) = mpsc::channel(BUFFER_SIZE);
    let (sender2_to_1, receiver1_from_2) = mpsc::channel(BUFFER_SIZE);
    
    // Set up peer relationships
    manager1.peer_managers.insert("manager2".to_string(), sender1_to_2);
    manager2.peer_managers.insert("manager1".to_string(), sender2_to_1);
    
    // Initialize node ownership
    manager1.managed_nodes.insert("x".to_string());
    manager2.managed_nodes.insert("y".to_string());
    
    // Set up node manager mapping
    manager1.node_manager_map.insert("y".to_string(), "manager2".to_string());
    manager2.node_manager_map.insert("x".to_string(), "manager1".to_string());
    
    // First, initialize "x" with a value of 10
    let mut nodes_to_modify_x = HashSet::new();
    nodes_to_modify_x.insert("x".to_string());
    
    let mut new_code_x = Vec::new();
    new_code_x.push(("x".to_string(), Expr::IntConst { val: 10 }));
    
    let update_x = CodeUpdate {
        nodes_to_modify: nodes_to_modify_x,
        new_code: new_code_x,
    };
    
    // Apply the update to set x = 10
    let result_x = manager1.handle_code_update(update_x).await;
    assert!(result_x.is_ok());
    
    // Verify x is set to 10
    assert!(manager1.system_configuration.contains_key("x0"));
    
    // Now create an update for "y" that depends on "x"
    let mut nodes_to_modify_y = HashSet::new();
    nodes_to_modify_y.insert("y".to_string());
    
    let mut new_code_y = Vec::new();
    // Create expression for y = x + 5
    new_code_y.push((
        "y".to_string(),
        Expr::BopExpr {
            opd1: Box::new(Expr::IdExpr { ident: "x".to_string() }),
            opd2: Box::new(Expr::IntConst { val: 5 }),
            bop: Binop::Add
        }
    ));
    
    let update_y = CodeUpdate {
        nodes_to_modify: nodes_to_modify_y,
        new_code: new_code_y,
    };
    
    // Apply update to create y = x + 5
    let result_y = manager2.handle_code_update(update_y).await;
    assert!(result_y.is_ok());
    
    // Verify that manager2 has the updated configuration for y
    assert!(manager2.system_configuration.contains_key("y0"));
    if let Some(expr) = manager2.system_configuration.get("y0") {
        // Verify the expression is y = x + 5
        match expr {
            Expr::BopExpr { opd1, opd2, bop } => {
                // Fix: Use a reference pattern to avoid moving out of a shared reference
                assert!(matches!(**opd1, Expr::IdExpr { ident: ref id } if id == "x0"));
                assert!(matches!(**opd2, Expr::IntConst { val } if val == 5));
                assert!(matches!(bop, Binop::Add));
            },
            _ => panic!("Expected BopExpr for y"),
        }
    }
    
    // Verify that manager2 is aware of the dependency on x
    assert!(manager2.dependency_graph.contains_key("y0"));
    if let Some(deps) = manager2.dependency_graph.get("y0") {
        assert!(deps.contains("x0"));
    }
    
    // Verify that manager1's node is in manager2's node_manager_map
    assert_eq!(manager2.node_manager_map.get("x"), Some(&"manager1".to_string()));
}

#[test]
async fn test_cyclic_dependency_detection() {
    let mut manager = Manager::new();

    let mut nodes_to_modify = HashSet::new();
    nodes_to_modify.insert("a".to_string());
    nodes_to_modify.insert("b".to_string());

    let mut new_code = Vec::new();
    new_code.push(("a".to_string(), Expr::IdExpr { ident: "b".to_string() }));
    new_code.push(("b".to_string(), Expr::IdExpr { ident: "a".to_string() }));

    let update = CodeUpdate {
        nodes_to_modify,
        new_code,
    };

    let result = manager.handle_code_update(update).await;
    assert!(matches!(result, Err(ManagerError::CyclicDependency(_))));
}

#[test]
async fn test_cyclic_dependency_detection2() {
    let mut manager = Manager::new();
    
    // Set up manager IDs and node ownership
    manager.manager_id = Some("manager1".to_string());
    manager.managed_nodes.insert("a".to_string());
    manager.node_manager_map.insert("b".to_string(), "manager2".to_string());
    manager.node_manager_map.insert("c".to_string(), "manager3".to_string());

    // Create a cycle across managers: a -> b -> c -> a
    let mut nodes_to_modify = HashSet::new();
    nodes_to_modify.insert("a".to_string());
    
    let mut new_code = Vec::new();
    new_code.push(("a".to_string(), Expr::IdExpr { ident: "b".to_string() }));
    
    // Add existing dependencies to simulate cross-manager cycle
    manager.dependency_graph.insert("b".to_string(), {
        let mut deps = HashSet::new();
        deps.insert("c".to_string());
        deps
    });
    
    manager.dependency_graph.insert("c".to_string(), {
        let mut deps = HashSet::new();
        deps.insert("a".to_string());
        deps
    });

    let update = CodeUpdate {
        nodes_to_modify,
        new_code,
    };

    let result = manager.handle_code_update(update).await;
    
    // Verify that the cross-manager cycle is detected
    println!("{:?}",result);
    match result {
        Err(ManagerError::CyclicDependency(msg)) => {
            assert!(msg.contains("manager2"));
            assert!(msg.contains("manager3"));
            assert!(msg.contains("local"));
        },
        _ => panic!("Expected CyclicDependency error with cross-manager cycle details"),
    }
}

#[test]
async fn test_cyclic_dependency_detection3() {
    let mut manager1 = Manager::new();
    let mut manager2 = Manager::new();
    let mut manager3 = Manager::new();
    
    // Set manager IDs
    manager1.manager_id = Some("manager1".to_string());
    manager2.manager_id = Some("manager2".to_string());
    manager3.manager_id = Some("manager3".to_string());
    
    // Set up peer channels
    let (sender1_to_2, receiver2_from_1) = mpsc::channel(BUFFER_SIZE);
    let (sender2_to_3, receiver3_from_2) = mpsc::channel(BUFFER_SIZE);
    let (sender3_to_1, receiver1_from_3) = mpsc::channel(BUFFER_SIZE);
    
    // Set up peer relationships
    manager1.peer_managers.insert("manager2".to_string(), sender1_to_2);
    manager2.peer_managers.insert("manager3".to_string(), sender2_to_3);
    manager3.peer_managers.insert("manager1".to_string(), sender3_to_1);
    
    // Initialize node ownership
    manager1.managed_nodes.insert("a".to_string());
    manager2.managed_nodes.insert("b".to_string());
    manager3.managed_nodes.insert("c".to_string());
    
    // Set up node manager mapping
    manager1.node_manager_map.insert("b".to_string(), "manager2".to_string());
    manager1.node_manager_map.insert("c".to_string(), "manager3".to_string());
    
    // Create update that would form a cycle across managers
    let mut nodes_to_modify = HashSet::new();
    nodes_to_modify.insert("a".to_string());
    
    let mut new_code = Vec::new();
    new_code.push(("a".to_string(), Expr::IdExpr { ident: "b".to_string() }));
    
    // Add existing dependencies to simulate cross-manager cycle
    manager1.dependency_graph.insert("b".to_string(), {
        let mut deps = HashSet::new();
        deps.insert("c".to_string());
        deps
    });
    
    manager1.dependency_graph.insert("c".to_string(), {
        let mut deps = HashSet::new();
        deps.insert("a".to_string());
        deps
    });
    
    let update = CodeUpdate {
        nodes_to_modify,
        new_code,
    };
    
    // Attempt update and verify cycle detection
    let result = manager1.handle_code_update(update).await;
    match result {
        Err(ManagerError::CyclicDependency(msg)) => {
            assert!(msg.contains("manager1"));
            assert!(msg.contains("manager2"));
            assert!(msg.contains("manager3"));
        },
        _ => panic!("Expected cyclic dependency error"),
    }
}

#[test]
async fn test_version_increment() {
    let mut manager = Manager::new();
    manager.managed_nodes.insert("x".to_string()); 

    // First update
    let mut nodes_to_modify = HashSet::new();
    nodes_to_modify.insert("x".to_string());

    let mut new_code = Vec::new();
    new_code.push(("x".to_string(), Expr::IntConst { val: 42 }));

    let update = CodeUpdate {
        nodes_to_modify: nodes_to_modify.clone(),
        new_code,
    };

    let result = manager.handle_code_update(update).await;
    assert!(result.is_ok());

    // Second update
    let mut new_code = Vec::new();
    new_code.push(("x".to_string(), Expr::IntConst { val: 43 }));

    let update = CodeUpdate {
        nodes_to_modify,
        new_code,
    };

    let result = manager.handle_code_update(update).await;
    assert!(result.is_ok());

    // Check that versions are handled correctly
    assert!(manager.system_configuration.contains_key("x1"));
    if let Some(expr) = manager.system_configuration.get("x1") {
        assert_eq!(*expr, Expr::IntConst { val: 43 });
    }
}

#[test]
async fn test_dependency_tracking() {
    let mut manager = Manager::new();
    manager.managed_nodes.insert("x".to_string());
    manager.managed_nodes.insert("y".to_string());

    let mut nodes_to_modify = HashSet::new();
    nodes_to_modify.insert("x".to_string());
    nodes_to_modify.insert("y".to_string());

    let mut new_code = Vec::new();
    new_code.push(("x".to_string(), Expr::IntConst { val: 42 }));
    new_code.push(("y".to_string(), Expr::IdExpr { ident: "x".to_string() }));

    let update = CodeUpdate {
        nodes_to_modify,
        new_code,
    };

    let result = manager.handle_code_update(update).await;
    assert!(result.is_ok());

    // Check dependency graph
    if let Some(deps) = manager.dependency_graph.get("y0") {
        assert!(deps.contains("x0"));
    }

    // Check reverse dependencies
    if let Some(rev_deps) = manager.reverse_dependencies.get("x0") {
        assert!(rev_deps.contains("y0"));
    }     
    // defworkers:
    assert!(manager.senders_to_workers.contains_key("y0"));
    assert_eq!(manager.worker_kind_env.get("y0"), Some(&WorkerKind::Def));

}

#[test]
async fn test_code_update_cleanup() {
    let mut manager = Manager::new();
    manager.managed_nodes.insert("x".to_string());

    // First update
    let mut nodes_to_modify = HashSet::new();
    nodes_to_modify.insert("x".to_string());

    let mut new_code = Vec::new();
    new_code.push(("x".to_string(), Expr::IntConst { val: 42 }));

    let update = CodeUpdate {
        nodes_to_modify: nodes_to_modify.clone(),
        new_code,
    };

    let result = manager.handle_code_update(update).await;
    assert!(result.is_ok());

    // Modify x and verify old version is cleaned up
    let mut new_code = Vec::new();
    new_code.push(("x".to_string(), Expr::IntConst { val: 43 }));

    let update = CodeUpdate {
        nodes_to_modify,
        new_code,
    };

    let result = manager.handle_code_update(update).await;
    assert!(result.is_ok());

    // Check that old version is removed
    assert!(!manager.system_configuration.contains_key("x0"));
    assert!(manager.system_configuration.contains_key("x1"));
}

#[test]
async fn test_dependency_update() {
    let mut manager = Manager::new();  
    manager.managed_nodes.insert("a".to_string());
    manager.managed_nodes.insert("b".to_string());
    manager.managed_nodes.insert("c".to_string());

    let mut nodes_to_modify = HashSet::new();
    nodes_to_modify.extend(vec!["a", "b", "c"].into_iter().map(String::from));

    let mut new_code = Vec::new();
    new_code.push(("a".to_string(), Expr::IntConst { val: 1 }));
    new_code.push(("b".to_string(), Expr::IdExpr { ident: "a".to_string() }));
    new_code.push(("c".to_string(), Expr::IdExpr { ident: "b".to_string() }));

    let update = CodeUpdate {
        nodes_to_modify: nodes_to_modify.clone(),
        new_code,
    };

    let result = manager.handle_code_update(update).await;
    assert!(result.is_ok(), "Update failed with error: {:?}", result.err());

    // Verify dependency chain
    if let Some(deps) = manager.dependency_graph.get("c0") {
        assert!(deps.contains("b0"));
    }
    if let Some(deps) = manager.dependency_graph.get("b0") {
        assert!(deps.contains("a0"));
    }

    // Update middle node and verify dependencies
    let mut nodes_to_modify = HashSet::new();
    nodes_to_modify.insert("b".to_string());

    let mut new_code = Vec::new();
    new_code.push(("b".to_string(), Expr::IntConst { val: 2 }));

    let update = CodeUpdate {
        nodes_to_modify,
        new_code,
    };

    let result = manager.handle_code_update(update).await;
    assert!(result.is_ok(), "Update failed with error: {:?}", result.err());

    // Verify updated dependencies
    if let Some(deps) = manager.dependency_graph.get("b1") {
        assert!(deps.is_empty());
    }
}

#[test]
async fn test_upgrade_lock_acquisition() {
    let mut manager = Manager::new();
    manager.managed_nodes.insert("x".to_string());

    // Create VarWorker first
    manager.create_varworker("x").await;

    // Create initial node
    let mut nodes_to_modify = HashSet::new();
    nodes_to_modify.insert("x".to_string());

    let mut new_code = Vec::new();
    new_code.push(("x".to_string(), Expr::IntConst { val: 42 }));

    let update = CodeUpdate {
        nodes_to_modify: nodes_to_modify.clone(),
        new_code,
    };

    let result = manager.handle_code_update(update).await;
    assert!(result.is_ok());

    // Verify lock state after update
    let locks = manager.node_locks.get("x").unwrap();
    assert!(locks.is_empty(), "Locks should be released after update");

    // Attempt to acquire lock directly
    let txn_id = TxnId::new();
    let result = manager.acquire_upgrade_lock("x", txn_id.clone()).await;
    assert!(result.is_ok(), "Should be able to acquire lock");

    // Verify lock is held
    let locks = manager.node_locks.get("x").unwrap();
    assert_eq!(locks.len(), 1);
    assert!(matches!(locks[0], LockType::Upgrade(_)));

    // Try another update while lock is held
    let mut new_code = Vec::new();
    new_code.push(("x".to_string(), Expr::IntConst { val: 43 }));

    let update = CodeUpdate {
        nodes_to_modify,
        new_code,
    };

    // This should abort existing transactions and succeed
    let result = manager.handle_code_update(update).await;
    assert!(result.is_ok());
}


#[test]
async fn test_upgrade_lock_priority() {
    let mut manager = Manager::new();    
    manager.managed_nodes.insert("x".to_string());

    // Initialize a node
    let mut nodes_to_modify = HashSet::new();
    nodes_to_modify.insert("x".to_string());

    let mut new_code = Vec::new();
    new_code.push(("x".to_string(), Expr::IntConst { val: 1 }));

    let update = CodeUpdate {
        nodes_to_modify: nodes_to_modify.clone(),
        new_code,
    };

    let result = manager.handle_code_update(update).await;
    assert!(result.is_ok());

    // First acquire read locks with earlier transaction IDs
    let read_txn = TxnId::new();
    let write_txn = TxnId::new();

    let sender1 = manager.senders_to_workers.get("x").unwrap().clone();
    let sender2 = manager.senders_to_workers.get("x").unwrap().clone();

    // Request read and write locks
    sender1.send(Message::VarLockRequest {
        lock_kind: LockKind::Read,
        txn: Txn { id: read_txn.clone(), writes: vec![] },
    }).await.unwrap();
    
    sender2.send(Message::VarLockRequest {
        lock_kind: LockKind::Write,
        txn: Txn { id: write_txn.clone(), writes: vec![] },
    }).await.unwrap();

    // Wait a moment for the lock requests to be processed
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Drain any pending messages before proceeding
    while let Ok(Some(_)) = tokio::time::timeout(
        Duration::from_millis(10),
        manager.receiver_from_workers.recv()
    ).await {}

    // Now request an upgrade lock with a later transaction ID
    let update_txn = TxnId::new();
    let result = manager.acquire_upgrade_lock("x", update_txn.clone()).await;
    assert!(result.is_ok(), "Upgrade lock should preempt existing read and write locks");

    // Verify all other transactions were aborted
    let aborted_txns = manager.active_transactions.clone();
    assert!(!aborted_txns.contains_key(&read_txn), "Read transaction should be aborted");
    assert!(!aborted_txns.contains_key(&write_txn), "Write transaction should be aborted");

    // Verify only the upgrade lock remains
    let locks = manager.node_locks.get("x").unwrap();
    assert_eq!(locks.len(), 1, "Should only have the upgrade lock");
    assert!(matches!(locks[0], LockType::Upgrade(ref id) if id == &update_txn));
}
 
#[test]
async fn test_concurrent_updates() {
    let manager = Arc::new(Mutex::new(Manager::new())); 
    
    // Initialize "x" as a managed node
    {
        let mut mgr = manager.lock().await;
        mgr.managed_nodes.insert("x".to_string());
    }
    // Spawn multiple concurrent updates
    let update_task1 = {
        let manager = Arc::clone(&manager);
        {
            let mut manager = manager.lock().await;
            manager.managed_nodes.insert("x".to_string());
        }
        tokio::spawn(async move {
            let mut nodes_to_modify = HashSet::new();
            nodes_to_modify.insert("x".to_string());

            let mut new_code = Vec::new();
            new_code.push(("x".to_string(), Expr::IntConst { val: 1 }));

            let update = CodeUpdate {
                nodes_to_modify,
                new_code,
            };

            let mut manager = manager.lock().await;
            manager.handle_code_update(update).await
        })
    };

    let update_task2 = {
        let manager = Arc::clone(&manager);
        tokio::spawn(async move {
            // Add small delay to ensure tasks overlap
            tokio::time::sleep(Duration::from_millis(10)).await;

            let mut nodes_to_modify = HashSet::new();
            nodes_to_modify.insert("x".to_string());

            let mut new_code = Vec::new();
            new_code.push(("x".to_string(), Expr::IntConst { val: 2 }));

            let update = CodeUpdate {
                nodes_to_modify,
                new_code,
            };

            let mut manager = manager.lock().await;
            manager.handle_code_update(update).await
        })
    };

    // Wait for both updates to complete
    let (result1, result2) = tokio::join!(update_task1, update_task2);

    // Both updates should complete successfully
    assert!(result1.unwrap().is_ok());
    assert!(result2.unwrap().is_ok()); 

    // Verify final state
    let manager = manager.lock().await;
    let versions: Vec<_> = manager.system_configuration
        .keys()
        .filter(|k| k.starts_with("x"))
        .collect();
    assert_eq!(versions.len(), 1, "Should only have one version after concurrent updates");

    // Verify final value
    if let Some(expr) = manager.system_configuration.get(versions[0]) {
        match expr {
            Expr::IntConst { val } => {
                assert!(*val == 1 || *val == 2, "Final value should be from one of the updates");
            },
            _ => panic!("Unexpected expression type"),
        }
    }
}

#[test]
async fn test_distributed_updates() {
    let manager = Arc::new(Mutex::new(Manager::new()));

    // Initialize workers
    {
        let mut manager = manager.lock().await;
        for name in ["x", "y", "z"] {
            manager.create_varworker(name).await;
        }
    }

    // Create three update tasks
    let task1 = {
        let manager = Arc::clone(&manager);
        tokio::spawn(async move {
            let mut nodes_to_modify = HashSet::new();
            nodes_to_modify.extend(vec!["x", "y", "z"].into_iter().map(String::from));

            let mut new_code = Vec::new();
            new_code.push(("x".to_string(), Expr::IntConst { val: 0 }));
            new_code.push(("y".to_string(), Expr::IdExpr { ident: "x".to_string() }));
            new_code.push(("z".to_string(), Expr::IdExpr { ident: "y".to_string() }));

            let update = CodeUpdate {
                nodes_to_modify,
                new_code,
            };

            let mut manager = manager.lock().await;
            manager.handle_code_update(update).await
        })
    };

    let task2 = {
        let manager = Arc::clone(&manager);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;

            let mut nodes_to_modify = HashSet::new();
            nodes_to_modify.extend(vec!["x", "y", "z"].into_iter().map(String::from));

            let mut new_code = Vec::new();
            new_code.push(("x".to_string(), Expr::IntConst { val: 1 }));
            new_code.push(("y".to_string(), Expr::IdExpr { ident: "x".to_string() }));
            new_code.push(("z".to_string(), Expr::IdExpr { ident: "y".to_string() }));

            let update = CodeUpdate {
                nodes_to_modify,
                new_code,
            };

            let mut manager = manager.lock().await;
            manager.handle_code_update(update).await
        })
    };

    let task3 = {
        let manager = Arc::clone(&manager);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;

            let mut nodes_to_modify = HashSet::new();
            nodes_to_modify.extend(vec!["x", "y", "z"].into_iter().map(String::from));

            let mut new_code = Vec::new();
            new_code.push(("x".to_string(), Expr::IntConst { val: 2 }));
            new_code.push(("y".to_string(), Expr::IdExpr { ident: "x".to_string() }));
            new_code.push(("z".to_string(), Expr::IdExpr { ident: "y".to_string() }));

            let update = CodeUpdate {
                nodes_to_modify,
                new_code,
            };

            let mut manager = manager.lock().await;
            manager.handle_code_update(update).await
        })
    };

    // Wait for all tasks to complete
    let (result1, result2, result3) = tokio::join!(task1, task2, task3);

    // Verify all updates completed successfully
    assert!(result1.unwrap().is_ok());
    assert!(result2.unwrap().is_ok());
    assert!(result3.unwrap().is_ok());

    // Verify consistency
    let manager = manager.lock().await;
    // Ensure dependency chain is maintained
    if let Some(deps) = manager.dependency_graph.get("z0") {
        assert!(deps.contains("y0"));
    }
    if let Some(deps) = manager.dependency_graph.get("y0") {
        assert!(deps.contains("x0"));
    }
}

#[tokio::test]
async fn test_concurrent_distributed_updates() {
    let manager1 = Arc::new(Mutex::new(Manager::new()));
    let manager2 = Arc::new(Mutex::new(Manager::new()));
    
    // Set manager IDs
    {
        let mut mgr1 = manager1.lock().await;
        let mut mgr2 = manager2.lock().await;
        mgr1.manager_id = Some("manager1".to_string());
        mgr2.manager_id = Some("manager2".to_string());
    }
    
    // Set up peer channels
    let (sender1_to_2, receiver2_from_1) = mpsc::channel(BUFFER_SIZE);
    let (sender2_to_1, receiver1_from_2) = mpsc::channel(BUFFER_SIZE);
    
    // Set up peer relationships
    {
        let mut mgr1 = manager1.lock().await;
        let mut mgr2 = manager2.lock().await;
        mgr1.peer_managers.insert("manager2".to_string(), sender1_to_2);
        mgr2.peer_managers.insert("manager1".to_string(), sender2_to_1);
    }
    
    // Initialize node ownership
    {
        let mut mgr1 = manager1.lock().await;
        let mut mgr2 = manager2.lock().await;
        mgr1.managed_nodes.insert("a".to_string());
        mgr2.managed_nodes.insert("b".to_string());
    }
    
    // Set up node manager mapping
    {
        let mut mgr1 = manager1.lock().await;
        let mut mgr2 = manager2.lock().await;
        mgr1.node_manager_map.insert("b".to_string(), "manager2".to_string());
        mgr2.node_manager_map.insert("a".to_string(), "manager1".to_string());
    }
    
    // Create concurrent update tasks
    let update_task1 = {
        let manager1 = Arc::clone(&manager1);
        tokio::spawn(async move {
            let mut nodes_to_modify = HashSet::new();
            nodes_to_modify.insert("a".to_string());
            
            let mut new_code = Vec::new();
            new_code.push(("a".to_string(), Expr::IntConst { val: 1 }));
            
            let update = CodeUpdate {
                nodes_to_modify,
                new_code,
            };
            
            let mut manager = manager1.lock().await;
            manager.handle_code_update(update).await
        })
    };
    
    let update_task2 = {
        let manager2 = Arc::clone(&manager2);
        tokio::spawn(async move {
            let mut nodes_to_modify = HashSet::new();
            nodes_to_modify.insert("b".to_string());
            
            let mut new_code = Vec::new();
            new_code.push(("b".to_string(), Expr::IntConst { val: 2 }));
            
            let update = CodeUpdate {
                nodes_to_modify,
                new_code,
            };
            
            let mut manager = manager2.lock().await;
            manager.handle_code_update(update).await
        })
    };
    
    // Wait for both updates to complete
    let (result1, result2) = tokio::join!(update_task1, update_task2);
    
    // Both updates should complete successfully
    assert!(result1.unwrap().is_ok());
    assert!(result2.unwrap().is_ok());
    
    // Verify final state
    {
        let manager1 = manager1.lock().await;
        let manager2 = manager2.lock().await;
        assert!(manager1.system_configuration.contains_key("a0"));
        assert!(manager2.system_configuration.contains_key("b0"));
    }
}

#[tokio::test]
async fn test_multimanager_code_update_with_cross_dependency() {
    use crate::runtime::message::{Message, CodeUpdate};
    use crate::frontend::meerast::Expr;
    use tokio::sync::mpsc;

    // Create two manager instances.
    let mut manager1 = Manager::new();
    let mut manager2 = Manager::new();

    // Set unique manager IDs.
    manager1.manager_id = Some("manager1".to_string());
    manager2.manager_id = Some("manager2".to_string());

    // Define ownership: manager1 owns node "X", manager2 owns node "Y".
    manager1.managed_nodes.insert("X".to_string());
    manager2.managed_nodes.insert("Y".to_string());

    // For cross-manager updates, manager1 maps node "Y" to manager2,
    // and manager2 maps node "X" to manager1.
    manager1.node_manager_map.insert("Y".to_string(), "manager2".to_string());
    manager2.node_manager_map.insert("X".to_string(), "manager1".to_string());

    // Create peer communication channels.
    // tx_1to2: messages from manager1 to manager2.
    // tx_2to1: messages from manager2 to manager1.
    let (tx_1to2, mut rx_1to2) = mpsc::channel(10);
    let (tx_2to1, mut rx_2to1) = mpsc::channel(10);

    // Set the peer manager senders.
    manager1.peer_managers.insert("manager2".to_string(), tx_1to2.clone());
    manager2.peer_managers.insert("manager1".to_string(), tx_2to1.clone());

    // Spawn a task for manager2 to process incoming distributed updates.
    let mut manager2_clone = manager2;
    tokio::spawn(async move {
        while let Some(msg) = rx_1to2.recv().await {
            let _ = manager2_clone.handle_message(msg).await;
        }
    });

    // Forward ACK messages from manager2 into manager1’s receiver.
    let manager1_sender = manager1.sender_to_manager.clone();
    tokio::spawn(async move {
        while let Some(ack_msg) = rx_2to1.recv().await {
            let _ = manager1_sender.send(ack_msg).await;
        }
    });

    // Create a CodeUpdate:
    // - Node A is updated with a literal value: 5.
    // - Node B is updated with an expression: A + 6.
    let update = CodeUpdate {
        nodes_to_modify: ["X".to_string(), "Y".to_string()].iter().cloned().collect(),
        new_code: vec![
            // Expression for node A: literal 5.
            ("X".to_string(),  Expr::IntConst { val: 5 }),
            // Expression for node B: binary operation: (A) + (6).
            ("Y".to_string(), Expr::BopExpr {
                opd1: Box::new(Expr::IdExpr { ident: "X".to_string() }),
                opd2: Box::new(Expr::IntConst { val: 6 }),
                bop: Binop::Add,
            }),
        ],
    };

    // Execute the distributed code update from manager1.
    let result = manager1.handle_code_update(update).await;

    // Assert that the update succeeds.
    assert!(result.is_ok());
}

