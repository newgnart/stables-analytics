#!/usr/bin/env python
"""
Shared state and configuration for multi-agent workflow.
"""

from typing import Any, Dict, List, Optional
from langgraph.graph import MessagesState
from langchain_openai import ChatOpenAI

# Constants
MAX_REPLANS = 3
reasoning_llm = ChatOpenAI(
    model="o3",
    model_kwargs={"response_format": {"type": "json_object"}},
)

llm = ChatOpenAI(model="gpt-4o")


# State Definition
class State(MessagesState):
    """
    Custom state class with specific keys for the multi-agent workflow.

    Inherits from MessagesState which provides a 'messages' field that keeps
    track of the list of messages shared among agents.
    """

    user_query: Optional[str]  # The user's original query
    enabled_agents: Optional[
        List[str]
    ]  # Makes the system modular on which agents to include
    plan: Optional[
        List[Dict[int, Dict[str, Any]]]
    ]  # Steps in the plan to achieve the goal
    current_step: int  # Current step in the plan
    agent_query: Optional[
        str
    ]  # Tells the next agent exactly what to do at the current step
    last_reason: Optional[str]  # Explains the executor's decision for traceability
    replan_flag: Optional[bool]  # Indicates that the planner should revise the plan
    replan_attempts: Optional[
        Dict[int, Dict[int, int]]
    ]  # Replan attempts per step number
