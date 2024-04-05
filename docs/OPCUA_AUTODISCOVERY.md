# OPCUA Auto Discovery with SensApp.

Unlike MQTT, OPCUA does not have a subscription mechanism allowing wildcards. One must specify all the nodes to monitor on the OPCUA server.

This is not always very convenient, so to streamline the deployment of SensApp, SensApp supports the auto-discovery of OPCUA variable nodes.

SensApp will browse the OPCUA server and automatically monitors the variables and collect the data from the OPCUA server.

## Configuration

```toml
[opcua.subscriptions.autodiscovery]

# Enable or disable the auto-discovery of OPCUA nodes. Enabled by default if the configuration is present.
enabled = true

# Start node for the auto-discovery. It will be the root node by default.
# Some server contain a LOT of nodes under the root node, so you may prefer to start
# from another node. It uses the same
# syntax as the identifiers in the OPCUA subscrptions.
start_node = "DataBlocksGlobal"

# List of node identifiers that are excluded from the auto-discovery.
# They will not be monitored or navigated through.
# By default, no nodes are excluded.
excluded_nodes = [
    "DataBlocksGlobal/SomeNode",
    "DataBlocksGlobal/AnotherNode"
]

# Maintaining a list of excluded nodes can be cumbersome too. You can use a regular expression to exclude nodes based on their
# browse name.
# By default, no nodes are excluded.
node_browse_name_exclude_regex = "^(Icon|History)$"

# Sometimes you will discover many variables that you do not want to monitor.
# You can use a regular expression to include only the variables that have an identifier that match the regular expression.
# If it's not a string, it uses the string representation of the identifier.
# By default, all the variables are included.
variable_identifier_include_regex = ".*Temperature.*"

# The auto-discovery starts from the namespace fo the subscription or the root node, but it can discover nodes in other namespaces if the OPCUA server is configured with links accross namespaces.
# This may not be desirable, so you can ignore the nodes from other namespaces.
# Enabled by default.
discover_accross_namespaces = true

# Some OPCUA servers will organise some variables under a parent node
# that is also a variable, of type object.
# This is nice, but if you don't want to monitor the parent node as well as the children, you can use this option.
# It is enabled by default.
skip_variables_with_children = true

# Maximum discovery depth.
# 32 by default
max_depth = 32

# Maximum number of nodes to discover.
# 1024 by default
max_nodes = 1024

```

## In Production

In production, you may not want to rely on the auto-discovery. Also, the auto-discovery is done only once at the start of the subscription. It will not monitor for new nodes that are added to the OPCUA server, or nodes that are removed.

Moreover, the auto-discovery is not very fast, and it may take a little while before the data is being collected.

Still, this may work well for your use case. As a reminder, this software is provided as-is without any warranty.
