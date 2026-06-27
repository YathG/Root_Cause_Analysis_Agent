from langgraph.graph import StateGraph, END
from rca_agent.agent.state import PipelineState
from rca_agent.agent.nodes import find_incident, investigate, move_upstream, route

def build_graph() -> StateGraph:
    """
    Constructs and compiles the LangGraph state machine workflow for RCA.
    """
    builder = StateGraph(PipelineState)

    # 1. Add nodes
    builder.add_node("find_incident", find_incident)
    builder.add_node("investigate", investigate)
    builder.add_node("move_upstream", move_upstream)

    # 2. Setup entrypoint
    builder.set_entry_point("find_incident")

    # 3. Add edges
    builder.add_edge("find_incident", "investigate")
    
    # Conditional edge from investigate based on route logic
    builder.add_conditional_edges(
        "investigate",
        route,
        {
            "move_upstream": "move_upstream",
            "__end__": END
        }
    )
    
    # Edge back to investigate after shifting upstream
    builder.add_edge("move_upstream", "investigate")

    # Compile the graph
    return builder.compile()

# Compile single instance
graph = build_graph()
