import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from neo4j import GraphDatabase
import weaviate
from sentence_transformers import SentenceTransformer
from urllib.parse import urlparse

# --- Configuration & Initialization ---
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password1234")
WEAVIATE_URL = os.getenv("WEAVIATE_URL", "http://localhost:8080")
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL_NAME", "all-MiniLM-L6-v2")
WEAVIATE_CLASS_NAME = "Document"

app = FastAPI(title="Enterprise Knowledge Graph API")

# --- Global Resources (managed with lifespan) ---
neo4j_driver = None
weaviate_client = None
embedding_model = None

@app.on_event("startup")
def startup_event():
    global neo4j_driver, weaviate_client, embedding_model
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    parsed_url = urlparse(WEAVIATE_URL)
    weaviate_client = weaviate.connect_to_local(host=parsed_url.hostname, port=parsed_url.port)
    
    embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    print("API resources loaded.")

@app.on_event("shutdown")
def shutdown_event():
    if neo4j_driver:
        neo4j_driver.close()
    if weaviate_client:
        weaviate_client.close()
    print("API resources released.")

# --- Pydantic Models for API ---
class QueryRequest(BaseModel):
    query: str
    limit: int = 5

class GraphQueryRequest(BaseModel):
    entity_name: str
    limit: int = 10

class RAGResponse(BaseModel):
    answer_prompt: str
    retrieved_context: list[dict]

# --- API Endpoints ---
@app.get("/")
def read_root():
    return {"status": "Knowledge Graph API is running"}

@app.post("/query/semantic")
def semantic_search(request: QueryRequest):
    """Performs vector similarity search in Weaviate. This logic remains the same."""
    try:
        query_vector = embedding_model.encode(request.query).tolist()
        
        documents_collection = weaviate_client.collections.get(WEAVIATE_CLASS_NAME)
        
        response = documents_collection.query.near_vector(
            near_vector=query_vector,
            limit=request.limit
        )
        
        results = []
        for item in response.objects:
            results.append(item.properties)
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query/graph")
def graph_search(request: GraphQueryRequest):
    """
    Finds entities and their related documents based on the new, richer LLM-generated graph.
    """
    # CORRECTED QUERY:
    # - It no longer looks for a generic ":Entity" label. It looks for any node that has an "id" property.
    # - The relationship is now ":CONTAINS_ENTITY" from our ingestion script.
    # - It returns the specific labels of the found entity (e.g., ["Person"]).
    query = """
    MATCH (d:Document)-[:CONTAINS_ENTITY]->(e)
    WHERE toLower(e.id) CONTAINS toLower($entity_name)
    RETURN e.id AS entity, labels(e) AS type, collect(d.id) AS mentioned_in_docs
    LIMIT $limit
    """
    with neo4j_driver.session() as session:
        result = session.run(query, entity_name=request.entity_name, limit=request.limit)
        return [record.data() for record in result]

@app.post("/query/rag", response_model=RAGResponse)
def retrieval_augmented_generation(request: QueryRequest):
    """Simulates a RAG process. This logic remains the same."""
    retrieved_docs = semantic_search(QueryRequest(query=request.query, limit=3))
    
    if not retrieved_docs:
        return RAGResponse(answer_prompt="Could not find relevant context.", retrieved_context=[])

    context_str = "\n\n---\n\n".join([f"Document ID: {doc.get('doc_id')}\nContent: {doc.get('content')}" for doc in retrieved_docs])
    
    prompt = f"""
Based on the following context from our internal knowledge base, please answer the user's question.

[CONTEXT]
{context_str}

[USER QUESTION]
{request.query}

Answer:
"""
    return RAGResponse(answer_prompt=prompt, retrieved_context=retrieved_docs)