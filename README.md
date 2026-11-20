# agentic-ebay-store

Autonomous eBay store operations built on **Prefect** and **FastMCP**.

An existing ERP MCP server (Intunex) is wrapped in a FastMCP gateway alongside an
eBay store MCP server, and a tool-using agent runs every part of the store through
durable Prefect flows. Anything that touches money, buyers or listings goes through
a policy engine and, when the risk warrants it, a human approval gate.

> Work in progress — see the commit history for how it was built.
