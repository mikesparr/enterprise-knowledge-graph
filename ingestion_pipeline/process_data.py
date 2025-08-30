import os
import json
import time
import requests
import weaviate
import weaviate.classes as wvc
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from urllib.parse import urlparse

# --- LangChain Imports ---
from langchain_ollama.llms import OllamaLLM
from langchain_experimental.graph_transformers.llm import LLMGraphTransformer
from langchain_core.documents import Document
from langchain_neo4j import Neo4jGraph

# --- Configuration ---
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password1234")
WEAVIATE_URL = os.getenv("WEAVIATE_URL", "http://localhost:8080")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
DATA_SOURCE_PATH = os.getenv("DATA_SOURCE_PATH", "data/source/initial_documents.json")
WEAVIATE_CLASS_NAME = "Document"

# --- Model Loading ---
print("--- Loading embedding model into memory ---")
EMBEDDING_MODEL = 'all-MiniLM-L6-v2'
embedding_model = SentenceTransformer(EMBEDDING_MODEL)
print("--- Embedding model loaded successfully ---")


def wait_for_service(check_function, service_name, timeout=120):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            if check_function():
                print(f"--- {service_name} is ready. ---")
                return True
        except Exception:
            print(f"Waiting for {service_name}...")
        time.sleep(5)
    raise Exception(f"{service_name} was not ready in time.")

def check_ollama(base_url):
    try:
        response = requests.get(base_url)
        return response.status_code == 200
    except requests.exceptions.ConnectionError:
        return False

def setup_weaviate_schema(client: weaviate.WeaviateClient):
    if client.collections.exists(WEAVIATE_CLASS_NAME):
        print(f"--- Collection '{WEAVIATE_CLASS_NAME}' already exists. ---")
        return
    print(f"--- Creating collection '{WEAVIATE_CLASS_NAME}'... ---")
    client.collections.create(
        name=WEAVIATE_CLASS_NAME,
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
        properties=[
            wvc.config.Property(name="doc_id", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="content", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="source", data_type=wvc.config.DataType.TEXT),
        ]
    )
    print(f"--- Collection '{WEAVIATE_CLASS_NAME}' created. ---")

def process_documents_with_llm(neo4j_graph: Neo4jGraph, weaviate_client: weaviate.WeaviateClient, documents_data):
    print("--- Initializing LLM and Graph Transformer ---")
    llm = OllamaLLM(model="llama3:8b", base_url=OLLAMA_BASE_URL)
    llm_transformer = LLMGraphTransformer(
        llm=llm,
        allowed_nodes=["Person", "Organization", "Project", "Product", "Technology", "Concept"],
        allowed_relationships=["WORKS_FOR", "WORKS_ON", "MANAGES", "LAUNCHED", "USES", "ASSOCIATED_WITH"]
    )
    print("--- LLM and Transformer ready. ---")
    
    documents_collection = weaviate_client.collections.get(WEAVIATE_CLASS_NAME)
    with documents_collection.batch.dynamic() as batch:
        for doc_data in tqdm(documents_data, desc="Processing Documents with LLM"):
            doc = Document(
                page_content=doc_data.get("content", ""),
                metadata={"doc_id": doc_data.get("id"), "source": doc_data.get("source")}
            )
            try:
                graph_documents = llm_transformer.convert_to_graph_documents([doc])
                
                if graph_documents:
                    neo4j_graph.add_graph_documents(graph_documents)

                    neo4j_graph.query("""
                        MERGE (d:Document {id: $doc_id})
                        ON CREATE SET d.content = $content, d.source = $source
                        WITH d
                        UNWIND $entity_ids AS entity_id
                        MATCH (e {id: entity_id})
                        MERGE (d)-[:CONTAINS_ENTITY]->(e)
                    """, {
                        "doc_id": doc_data.get("id"),
                        "content": doc_data.get("content"),
                        "source": doc_data.get("source"),
                        "entity_ids": [node.id for node in graph_documents[0].nodes]
                    })

                vector = embedding_model.encode(doc_data.get("content", "")).tolist()
                properties = {"doc_id": doc_data.get("id"), "content": doc_data.get("content"), "source": doc_data.get("source")}
                batch.add_object(properties=properties, vector=vector)
            except Exception as e:
                print(f"\n[ERROR] An error occurred while processing document {doc_data.get('id')}: {e}")
                continue
    print("--- LLM-powered Neo4j and Weaviate population complete. ---")

def main():
    print("--- Main function starting. ---")
    weaviate_client = None
    try:
        neo4j_graph = Neo4jGraph(url=NEO4J_URI, username=NEO4J_USER, password=NEO4J_PASSWORD)
        
        parsed_url = urlparse(WEAVIATE_URL)
        weaviate_client = weaviate.connect_to_local(host=parsed_url.hostname, port=parsed_url.port)
        print("--- Database connections established. ---")

        wait_for_service(lambda: neo4j_graph.query("RETURN 1"), "Neo4j")
        wait_for_service(lambda: weaviate_client.is_live(), "Weaviate")
        wait_for_service(lambda: check_ollama(OLLAMA_BASE_URL), "Ollama API")
        
        setup_weaviate_schema(weaviate_client)
        with open(DATA_SOURCE_PATH, 'r') as f:
            documents = json.load(f)
        process_documents_with_llm(neo4j_graph, weaviate_client, documents)
        print("--- Ingestion pipeline finished successfully. ---")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if weaviate_client:
            weaviate_client.close()

if __name__ == "__main__":
    main()