from langchain_core.tools import tool
from langchain_experimental.utilities import PythonREPL
from typing import Annotated
from langchain_openai import ChatOpenAI
from scripts.knowledge.vector_search import search_qdrant_vectordb
from scripts.utils.database_client import get_qdrant_client
from langfuse import observe
import os

# Configure matplotlib to use non-GUI backend (fixes macOS threading issues)
import matplotlib
matplotlib.use('Agg')

# create a python repl tool for importing in the lessons
repl = PythonREPL()


@tool
@observe(name="python_repl_tool")
def python_repl_tool(
    code: Annotated[str, "The python code to execute to generate your chart."],
):
    """Use this to execute python code. You will be used to execute python code
    that generates charts. Charts should be saved to files using plt.savefig() instead of plt.show().
    Only print the chart once.
    This is visible to the user."""
    try:
        result = repl.run(code)
    except BaseException as e:
        return f"Failed to execute. Error: {repr(e)}"
    result_str = f"Successfully executed:\n```python\n{code}\n```\nStdout: {result}"
    return (
        result_str + "\n\nIf you have completed all tasks, respond with FINAL ANSWER."
    )


reasoning_llm = ChatOpenAI(
    model="o3",
    model_kwargs={"response_format": {"type": "json_object"}},
)


@tool
@observe(name="retrieve_knowledge_tool")
def retrieve_knowledge_tool(query: str, limit: int = 2) -> str:
    """
    Search the knowledge base for relevant information about Open Stablecoin Index.
    """

    try:
        results = search_qdrant_vectordb(
            query=query,
            collection_name=os.getenv("QDRANT_KNOWLEDGE_COLLECTION"),
            limit=limit,
            qdrant_client=get_qdrant_client(),
        )

        if not results:
            output = "No relevant knowledge found for the query."

            return output

        # Format as readable text
        output_lines = [f"Found {len(results)} relevant knowledge chunk(s):\n"]

        for idx, result in enumerate(results, 1):
            output_lines.append(f"Content:\n{result.get('content', 'N/A')}\n")

        output = "\n".join(output_lines)

        return output

    except Exception as e:
        error_msg = f"Error retrieving knowledge: {str(e)}"

        return error_msg
