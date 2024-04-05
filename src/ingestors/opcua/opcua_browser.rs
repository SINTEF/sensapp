use crate::config::opcua::OpcuaAutoDiscovery;
use anyhow::{bail, Result};
use opcua::{
    client::prelude::*,
    sync::RwLock,
    types::{BrowseDescription, BrowseDescriptionResultMask, BrowseDirection, NodeId},
};
use regex::Regex;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::Arc,
};

#[derive(Debug)]
struct BrowsingState {
    max_nodes: usize,
    filter_regex: Option<Regex>,
    namespace: u16,
    discover_across_namespaces: bool,
    skip_variables_with_children: bool,
    visited: RefCell<HashSet<NodeId>>,
    // The boolean is true if the variable should be kept,
    // It is false when the variable should be excluded, because
    // it has some sub nodes (children).
    found_variables: RefCell<HashMap<NodeId, bool>>,
}

fn browse(
    node_id: NodeId,
    session: Arc<RwLock<Session>>,
    levels_of_depth_left: usize,
    state: Rc<BrowsingState>,
) -> Result<()> {
    if levels_of_depth_left == 0 {
        bail!("Max recursive depth reached");
    }
    {
        let visited_read = state.visited.borrow();
        if visited_read.contains(&node_id) {
            return Ok(());
        }
    }
    {
        let mut visited_write = state.visited.borrow_mut();
        visited_write.insert(node_id.clone());
    }

    println!("Visiting node: {}", node_id);

    let session_read = session.read();
    let result_mask = (BrowseDescriptionResultMask::RESULT_MASK_BROWSE_NAME
        | BrowseDescriptionResultMask::RESULT_MASK_NODE_CLASS)
        .bits();
    let browse_results = session_read.browse(&[
        BrowseDescription {
            node_id: node_id.clone(),
            browse_direction: BrowseDirection::Forward,
            reference_type_id: ReferenceTypeId::Organizes.into(),
            include_subtypes: true,
            node_class_mask: 0,
            result_mask,
        },
        BrowseDescription {
            node_id: node_id.clone(),
            browse_direction: BrowseDirection::Forward,
            reference_type_id: ReferenceTypeId::Aggregates.into(),
            include_subtypes: true,
            node_class_mask: 0,
            result_mask,
        },
        BrowseDescription {
            node_id: node_id.clone(),
            browse_direction: BrowseDirection::Forward,
            reference_type_id: ReferenceTypeId::HasSubtype.into(),
            include_subtypes: true,
            node_class_mask: 0,
            result_mask,
        },
    ])?;

    let mut node_ids_to_browse_again: Vec<NodeId> = Vec::new();

    match browse_results {
        None => {
            bail!("No results found");
        }
        Some(results) => {
            if results.len() != 3 {
                bail!("Not the right number of results found");
            }

            for result in results {
                if let Some(refs) = &result.references {
                    // If the current node has children and is a variable, we may want to ignore it
                    if state.skip_variables_with_children && !refs.is_empty() {
                        let mut found_variables = state.found_variables.borrow_mut();
                        if let Some(true) = found_variables.get(&node_id) {
                            // This variable has children, we should not keep it
                            println!("Excluding variable: {}", node_id);
                            found_variables.insert(node_id.clone(), false);
                        }
                    }

                    for reference in refs {
                        let browse_name = reference.browse_name.name.as_ref();
                        // Skip references that are in another namespace, if not allowed
                        if !state.discover_across_namespaces
                            && reference.node_id.node_id.namespace != state.namespace
                        {
                            continue;
                        }

                        // Filter out the references based on their browsing name, if the option
                        // is enabled.
                        if let Some(filter_regex) = &state.filter_regex {
                            if filter_regex.is_match(browse_name) {
                                continue;
                            }
                        }

                        // Add the reference to the list of nodes to browse.
                        node_ids_to_browse_again.push(reference.node_id.node_id.clone());

                        // Save the variable !
                        if reference.node_class == NodeClass::Variable {
                            let mut found_variables = state.found_variables.borrow_mut();
                            if found_variables.len() >= state.max_nodes {
                                bail!("Max number of nodes reached: {} Perhaps the OPCUA server has a loop in its organisation.", state.max_nodes);
                            }
                            println!("Found variable: {}", reference.node_id.node_id);
                            found_variables.insert(reference.node_id.node_id.clone(), true);
                        }
                    }
                }
            }
        }
    }

    //let current_node_id = node_id;
    for node_id in node_ids_to_browse_again {
        browse(
            node_id,
            session.clone(),
            levels_of_depth_left - 1,
            state.clone(),
        )?;
    }

    Ok(())
}

pub fn opcua_browser(
    session: Arc<RwLock<Session>>,
    namespace: u16,
    config: OpcuaAutoDiscovery,
) -> Result<Vec<NodeId>> {
    let start = match config.start_node {
        Some(node) => NodeId::new(namespace, node),
        None => NodeId::root_folder_id(),
    };

    let excluded_nodes = config
        .excluded_nodes
        .into_iter()
        .map(|identifier| NodeId::new(namespace, identifier))
        .collect::<HashSet<NodeId>>();

    let level_of_depth_left = config.max_depth;

    let filter_regex = match config.node_browse_name_exclude_regex {
        Some(regex) => Some(Regex::new(&regex)?),
        None => None,
    };

    let browse_state = Rc::new(BrowsingState {
        max_nodes: config.max_nodes,
        filter_regex,
        namespace,
        discover_across_namespaces: config.discover_across_namespaces,
        skip_variables_with_children: config.skip_variables_with_children,
        visited: RefCell::new(excluded_nodes),
        found_variables: RefCell::new(HashMap::new()),
    });

    browse(
        start,
        session.clone(),
        level_of_depth_left,
        browse_state.clone(),
    )?;

    let found_variables_node_ids = browse_state
        .found_variables
        .clone()
        .into_inner()
        .into_iter()
        .filter_map(|(node_id, keep)| if keep { Some(node_id) } else { None })
        .collect::<Vec<NodeId>>();

    match config.variable_identifier_include_regex {
        Some(regex) => {
            let regex = Regex::new(&regex)?;
            let found_variables_node_ids = found_variables_node_ids
                .into_iter()
                .filter(|node_id| regex.is_match(&node_id.identifier.to_string()))
                .collect::<Vec<NodeId>>();
            Ok(found_variables_node_ids)
        }
        None => Ok(found_variables_node_ids),
    }
}
