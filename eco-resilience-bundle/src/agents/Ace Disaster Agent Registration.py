# Databricks notebook source
# MAGIC %pip install databricks-agents unitycatalog-ai[databricks]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install -U mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import subprocess
subprocess.check_call(["pip", "install", "-q", "openai"])

import mlflow
mlflow.openai.autolog()

# COMMAND ----------

import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentResponse, ChatContext
from databricks.sdk import WorkspaceClient
from unitycatalog.ai.core.databricks import DatabricksFunctionClient
from openai import OpenAI
import json
import uuid
from typing import Optional

SYSTEM_PROMPT = """You are Ace, an autonomous Chief Risk Officer for Australian
small businesses navigating disaster recovery.

You have two tools available:
  - get_business_risk_assessment(abn): assesses risk for a verified ABN.
    Call this whenever the user provides an ABN.
  - query_nema_guidelines(question): searches NEMA DRFA policy documents and
    returns relevant chunks with source PDF and page number. Call this for
    any policy/eligibility/evidence question, and cite the source PDF and
    page number in your final answer.

You may call multiple tools in sequence if a complete answer requires both
identity-grounded risk context AND policy citations. Explain your reasoning
clearly based on tool outputs and never fabricate citations."""

TOOL_REGISTRY = {
    "get_business_risk_assessment": "eco_resilience.gold.get_business_risk_assessment",
    "query_nema_guidelines":        "eco_resilience.silver.query_nema_guidelines",
}

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_business_risk_assessment",
            "description": "Assess business risk for a given ABN during a disaster event",
            "parameters": {
                "type": "object",
                "properties": {
                    "abn": {"type": "string", "description": "Australian Business Number"}
                },
                "required": ["abn"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_nema_guidelines",
            "description": (
                "Search NEMA Disaster Recovery Funding Arrangements (DRFA) policy "
                "documents using natural-language semantic search. Returns up to 5 "
                "relevant text chunks with source PDF filename and page number. "
                "Use this for any question about DRFA categories, eligibility, "
                "evidence requirements, or recovery assistance rules."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": (
                            "Natural-language question about NEMA DRFA rules, "
                            "e.g. 'What evidence is needed for Category C "
                            "reconstruction claims?'"
                        ),
                    }
                },
                "required": ["question"],
            },
        },
    },
]


class AceDisasterAgent(ChatAgent):
    def __init__(self):
        self._client = None
        self._workspace_client = None
        self._function_client = None

    @property
    def client(self):
        """Lazy-init OpenAI client (safe for serialization)."""
        if self._client is None:
            self._workspace_client = WorkspaceClient()
            self._client = OpenAI(
                base_url=f"{self._workspace_client.config.host}/serving-endpoints",
                api_key=self._workspace_client.config.token,
            )
        return self._client

    @property
    def function_client(self):
        """Lazy-init UC function client."""
        if self._function_client is None:
            if self._workspace_client is None:
                self._workspace_client = WorkspaceClient()
            self._function_client = DatabricksFunctionClient(client=self._workspace_client)
        return self._function_client

    def _call_tool(self, function_name: str, arguments: dict) -> str:
        """Execute a Unity Catalog function via the TOOL_REGISTRY."""
        full_name = TOOL_REGISTRY.get(function_name)
        if full_name is None:
            return json.dumps({"error": f"Unknown tool: {function_name}"})
        result = self.function_client.execute_function(
            function_name=full_name,
            parameters=arguments,
        )
        if result.value is not None:
            return str(result.value)
        elif result.error is not None:
            return json.dumps({"error": str(result.error)})
        else:
            return json.dumps({"error": "Function returned no result"})

    def _sanitize_message(self, msg) -> dict:
        """Convert an OpenAI message to a dict with only Databricks-supported fields."""
        sanitized = msg.model_dump(exclude_none=True)
        # Remove fields not supported by Databricks Foundation Model API
        sanitized.pop("annotations", None)
        sanitized.pop("function_call", None)
        return sanitized

    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict] = None,
    ) -> ChatAgentResponse:
        # Build conversation with system prompt
        chat_messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        chat_messages += [{"role": m.role, "content": m.content} for m in messages]

        # Agentic loop: keep calling the LLM with tools bound until it stops
        # requesting tool calls (or we hit the iteration cap).
        MAX_ITERS = 10
        assistant_msg = None
        for _ in range(MAX_ITERS):
            response = self.client.chat.completions.create(
                model="databricks-meta-llama-3-3-70b-instruct",
                messages=chat_messages,
                tools=TOOLS,
            )
            assistant_msg = response.choices[0].message

            if not assistant_msg.tool_calls:
                break

            chat_messages.append(self._sanitize_message(assistant_msg))
            for tool_call in assistant_msg.tool_calls:
                args = json.loads(tool_call.function.arguments)
                result = self._call_tool(tool_call.function.name, args)
                chat_messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": result,
                })

        # Ensure content is never None
        content = assistant_msg.content if assistant_msg.content is not None else "I was unable to generate a response. Please try again."

        return ChatAgentResponse(
            messages=[{"role": "assistant", "content": content, "id": str(uuid.uuid4())}]
        )


ace_agent = AceDisasterAgent()
mlflow.models.set_model(AceDisasterAgent())

# COMMAND ----------

import os
import mlflow
from mlflow.models.resources import (
    DatabricksFunction,
    DatabricksServingEndpoint,
    DatabricksTable,
    DatabricksVectorSearchIndex,
)
from databricks import agents

# Set Unity Catalog as the model registry
mlflow.set_registry_uri("databricks-uc")

# Disable autolog during model logging to avoid interference
mlflow.openai.autolog(disable=True)

# Provide token for OpenAI client validation during log_model
os.environ["OPENAI_API_KEY"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

model_name = "eco_resilience.gold.ace_disaster_agent"
agent_path = "/Workspace/Shared/eco_resilience/Agents/agent.py"

# Declare resources the agent needs at serving time
resources = [
    DatabricksServingEndpoint(endpoint_name="databricks-meta-llama-3-3-70b-instruct"),
    DatabricksFunction(function_name="eco_resilience.gold.get_business_risk_assessment"),
    DatabricksFunction(function_name="eco_resilience.silver.query_nema_guidelines"),
    DatabricksVectorSearchIndex(index_name="eco_resilience.bronze.drfa_chunks_index"),
    DatabricksTable(table_name="eco_resilience.bronze.drfa_chunks"),
]

# Log the agent using code-based logging
with mlflow.start_run():
    logged_model = mlflow.pyfunc.log_model(
        name="agent",
        python_model=agent_path,
        resources=resources,
        pip_requirements=[
            "mlflow",
            "databricks-sdk",
            "unitycatalog-ai[databricks]",
            "openai",
        ],
    )

# Register to Unity Catalog as a model first before deploy it to the endpoint
registered_model = mlflow.register_model(
    model_uri=logged_model.model_uri,
    name=model_name,
)

print(f"Agent registered as {model_name} version {registered_model.version}")

# Deploy the agent to the serveing endpoint with the latest version.
agents.deploy(
    model_name=model_name,
    model_version=registered_model.version,
)

# COMMAND ----------

