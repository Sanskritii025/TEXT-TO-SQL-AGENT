import os
import psycopg2
from typing import TypedDict
from langgraph.graph import StateGraph, END, START
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_chroma import Chroma
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

# --- CONFIGURATION ---
# Replace with your actual credentials
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",        # Default is usually postgres
    "password": "Sanskriti@25",    # Your actual password
    "host": "localhost",
    "port": "5432"
}

# --- CONNECT TO KNOWLEDGE BASE ---
vector_db = Chroma(
    persist_directory="./sales_memory",
    embedding_function=OpenAIEmbeddings()
)

# --- DEFINE STATE ---
class AgentState(TypedDict):
    question: str
    schema: str
    evidence: str
    sql_query: str
    data_result: str
    final_answer: str

# --- 2. THE CONTEXT (UPDATED TO MATCH YOUR DB EXACTLY) ---
def get_schema():
    """
    Connects to the DB and asks 'What tables do you have?'
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # This SQL query asks Postgres for its own structure
    cur.execute("""
        SELECT table_name, column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        ORDER BY table_name, ordinal_position;
    """)
    rows = cur.fetchall()
    
    # Format the output into a readable string
    schema_str = ""
    current_table = ""
    for table, col, dtype in rows:
        if table != current_table:
            schema_str += f"\nTABLE: {table}\n"
            current_table = table
        schema_str += f" - {col} ({dtype})\n"
        
    cur.close()
    conn.close()
    return schema_str
    """
    Returns the exact PostgreSQL Schema. 
    CRITICAL: 'opportunities' table uses 'amount', NOT 'value'.
    """
    return """
    You are a Postgres SQL Agent for a Sales Operations database.
    
    TABLES:
    1. territories (territory_id, region, manager_name)
    2. sales_reps (sales_rep_id, name, territory_id)
    3. accounts (account_id, account_name, region, account_type)
    
    4. opportunities (
        opportunity_id, 
        account_id, 
        stage (Enum: 'Prospecting', 'Negotiation', 'Closed Won'), 
        amount (NUMERIC - This is the deal value), 
        close_date
    )
    
    5. quotes (quote_id, opportunity_id, quote_amount, status)
    6. sales_orders (sales_order_id, opportunity_id, total_amount, status)
    7. approval_requests (approval_id, quote_id, status, approver_role)
    
    RELATIONSHIPS:
    - opportunities.account_id -> accounts.account_id
    - sales_orders.opportunity_id -> opportunities.opportunity_id
    """

# --- 3. THE NODES ---

def retrieve_node(state: AgentState):
    query = state['question']
    print(f"🔎 Looking up rules for: '{query}'...")
    results = vector_db.similarity_search(query, k=2)
    found_evidence = "\n".join([doc.page_content for doc in results])
    print(f"📄 Found Evidence:\n{found_evidence}")
    return {"evidence": found_evidence, "schema": get_schema()}

# Structured Output to force valid SQL
class SQLOutput(BaseModel):
    reasoning: str = Field(description="Reasoning for the query")
    sql: str = Field(description="The PostgreSQL query")

def generate_sql_node(state: AgentState):
    llm = ChatOpenAI(model="gpt-4o", temperature=0)
    structured_llm = llm.with_structured_output(SQLOutput)
    
    template = """
    You are a Postgres SQL Agent. Write a query to answer the user's question.

    SCHEMA: {schema}
    BUSINESS RULES: {evidence}

    --- EXAMPLES OF CORRECT SQL ---
    Q: "How many leads?"
    SQL: SELECT count(*) FROM opportunities WHERE stage IN ('Prospecting', 'Qualification');

    Q: "Total revenue?"
    SQL: SELECT sum(total_amount) FROM sales_orders WHERE status != 'Cancelled';
    --------------------------------

    USER QUESTION: {question}
    """
    prompt = ChatPromptTemplate.from_template(template)
    response = (prompt | structured_llm).invoke({
        "schema": state['schema'],
        "evidence": state['evidence'],
        "question": state['question']
    })
    
    return {"sql_query": response.sql}

def execute_sql_node(state: AgentState):
    print(f"📝 Executing SQL: {state['sql_query']}")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(state['sql_query'])
        rows = cur.fetchall()
        
        if not rows:
            return {"data_result": "No data found matching criteria."}
            
        # Get column names
        colnames = [desc[0] for desc in cur.description]
        result_str = f"Columns: {colnames}\nData: {rows}"
        
        cur.close()
        conn.close()
        return {"data_result": result_str}
    except Exception as e:
        return {"data_result": f"SQL Error: {str(e)}"}

def answer_node(state: AgentState):
    llm = ChatOpenAI(model="gpt-4o", temperature=0)
    
    template = """
    User Question: {question}
    SQL Data: {data_result}
    
    Answer the question simply based on the data.
    """
    prompt = ChatPromptTemplate.from_template(template)
    response = (prompt | llm).invoke(state)
    
    return {"final_answer": response.content}

# --- 4. BUILD GRAPH ---
workflow = StateGraph(AgentState)
workflow.add_node("retrieve", retrieve_node)
workflow.add_node("generate_sql", generate_sql_node)
workflow.add_node("execute_sql", execute_sql_node)
workflow.add_node("answer", answer_node)

workflow.add_edge(START, "retrieve")
workflow.add_edge("retrieve", "generate_sql")
workflow.add_edge("generate_sql", "execute_sql")
workflow.add_edge("execute_sql", "answer")
workflow.add_edge("answer", END)

app = workflow.compile()

# --- 5. RUN IT ---
if __name__ == "__main__":
    user_q = "Which customers churned last month? ?"
    
    print(f" User asking: {user_q}")
    result = app.invoke({"question": user_q})
    
    print(f"\n Final Answer: {result['final_answer']}")