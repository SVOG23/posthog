# Key on this canonical event only — also matching the legacy `mcp_tool_call` alias
# (frozen, pre-2026-06-16 history) would double-count every call.
MCP_TOOL_CALL_EVENT = "$mcp_tool_call"
MCP_MISSING_CAPABILITY_EVENT = "$mcp_missing_capability"
# Emitted per tools/list response; carries $mcp_listed_tool_names (the advertised catalog).
MCP_TOOLS_LIST_EVENT = "$mcp_tools_list"
