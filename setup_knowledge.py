
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings
from langchain_community.document_loaders import TextLoader # <--- NEW

loader = TextLoader("./company_policies.txt")
docs = loader.load()

vector_db = Chroma.from_documents(
    documents=docs,
    embedding=OpenAIEmbeddings(),
    persist_directory="./sales_memory"
)
print(" I have read your policy document and memorized it.")
