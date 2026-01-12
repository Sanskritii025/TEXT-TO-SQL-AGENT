# setup_knowledge.py (The Automated Version)
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings
from langchain_community.document_loaders import TextLoader # <--- NEW

# Instead of typing a list, we load the file
loader = TextLoader("./company_policies.txt")
docs = loader.load()

# Save to DB (Same as before)
vector_db = Chroma.from_documents(
    documents=docs,
    embedding=OpenAIEmbeddings(),
    persist_directory="./sales_memory"
)
print("✅ I have read your policy document and memorized it.")
