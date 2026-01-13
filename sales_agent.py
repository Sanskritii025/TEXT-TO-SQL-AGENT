import os
import time
import psycopg2
from typing import TypedDict
from langgraph.graph import StateGraph, END, START
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_chroma import Chroma
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.markdown import Markdown
from rich.syntax import Syntax
from rich.prompt import Prompt
from rich.status import Status


console = Console()

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "Sanskriti@25",  
    "host": "localhost",
    "port": "5432"
}

vector_db = Chroma(
    persist_directory="./sales_memory",
    embedding_function=OpenAIEmbeddings()
)


class AgentState(TypedDict):
    question: str
    schema: str
    evidence: str
    sql_query: str
    data_result: str    
    rows: list           
    columns: list        
    final_answer: str


def get_schema():
    """Dynamically fetches schema from Postgres."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name, column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            ORDER BY table_name, ordinal_position;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        schema_str = ""
        current_table = ""
        for table, col, dtype in rows:
            if table != current_table:
                schema_str += f"\nTABLE: {table}\n"
                current_table = table
            schema_str += f" - {col} ({dtype})\n"
        return schema_str
    except Exception:
        return "Error fetching schema."


def retrieve_node(state: AgentState):
    query = state['question']
    results = vector_db.similarity_search(query, k=2)
    found_evidence = "\n".join([doc.page_content for doc in results])
    return {"evidence": found_evidence, "schema": get_schema()}

class SQLOutput(BaseModel):
    reasoning: str = Field(description="Why you wrote this query")
    sql: str = Field(description="The PostgreSQL query")

def generate_sql_node(state: AgentState):
    llm = ChatOpenAI(model="gpt-4o-nano", temperature=0)
    structured_llm = llm.with_structured_output(SQLOutput)
    
    template = """
    You are a Postgres SQL Agent.
    
    SCHEMA: {schema}
    BUSINESS RULES: {evidence}
    
    Write a valid SQL query to answer: {question}
    """
    prompt = ChatPromptTemplate.from_template(template)
    response = (prompt | structured_llm).invoke({
        "schema": state['schema'],
        "evidence": state['evidence'],
        "question": state['question']
    })
    
    return {"sql_query": response.sql}

def execute_sql_node(state: AgentState):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(state['sql_query'])
        
        if cur.description:
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            result_str = f"Columns: {colnames}\nData: {rows}"
        else:
            rows = []
            colnames = []
            result_str = "Query executed successfully. No rows returned."
            
        cur.close()
        conn.close()
        return {"data_result": result_str, "rows": rows, "columns": colnames}
    except Exception as e:
        return {"data_result": f"SQL Error: {str(e)}", "rows": [], "columns": []}

def answer_node(state: AgentState):
    llm = ChatOpenAI(model="gpt-4o-nano", temperature=0)
    response = llm.invoke(f"Question: {state['question']}\nData: {state['data_result']}\nAnswer professionally:")
    return {"final_answer": response.content}

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

def print_header():
    console.clear()
    console.print(Panel.fit(
        "[bold white]SALES OPS INTELLIGENCE AGENT[/bold white]\n"
        "[dim]v1.2 | Powered by RAG & PostgreSQL[/dim]",
        style="bold blue",
        subtitle="Type 'exit' to quit"
    ))

def print_sql_panel(sql):
    syntax = Syntax(sql, "sql", theme="monokai", line_numbers=False)
    console.print(Panel(syntax, title="[bold yellow]Generated SQL[/]", border_style="yellow"))

def print_data_table(columns, rows):
    if not rows:
        return
    
    table = Table(show_header=True, header_style="bold magenta", border_style="dim")
    
   
    for col in columns:
        table.add_column(col)
    
   
    for row in rows[:10]:
        table.add_row(*[str(cell) for cell in row])
        
    if len(rows) > 10:
        console.print(f"[dim]Showing top 10 of {len(rows)} rows...[/dim]")
    
    console.print(table)

if __name__ == "__main__":
    print_header()
    
    while True:
        try:
            console.print()
            user_q = Prompt.ask("[bold green]Ask a question[/]")
            
            if user_q.lower() in ["exit", "quit", "q"]:
                console.print("[bold red]Shutting down system...[/]")
                break
            
            
            with console.status("[bold cyan]Analyzing request & checking policies...", spinner="dots"):
              
                state = app.invoke({"question": user_q})
                
          
            print_sql_panel(state['sql_query'])
            
           
            if "SQL Error" in state['data_result']:
                console.print(f"[bold red] {state['data_result']}[/]")
            else:
              
                print_data_table(state.get('columns', []), state.get('rows', []))
            
           
            console.print(Panel(Markdown(state['final_answer']), title="[bold green]Insight[/]", border_style="green"))

        except Exception as e:
            console.print(f"[bold red]System Error: {str(e)}[/]")