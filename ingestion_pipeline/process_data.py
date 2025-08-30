import os
import json
import time
import weaviate
import weaviate.classes as wvc
import spacy
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from urllib.parse import urlparse

# --- Configuration ---
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password1234")
WEAVIATE_URL = os.getenv("WEAVIATE_URL", "http://localhost:8080")
DATA_SOURCE_PATH = os.getenv("DATA_SOURCE_PATH", "data/source/initial_documents.json")
LAKE_OUTPUT_PATH = "data/lake/processed_entities.json"
WEAVIATE_CLASS_NAME = "Document" # Weaviate class names must be capitalized

# --- SCRIPT EXECUTION STARTS HERE ---
print("--- Loading NLP and embedding models into memory ---")
EMBEDDING_MODEL = 'all-MiniLM-L6-v2' 
nlp = spacy.load("en_core_web_sm")
model = SentenceTransformer(EMBEDDING_MODEL)
print("--- Models loaded successfully ---")


def wait_for_service(check_function, service_name, timeout=120):
    """Waits for a service to be ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            if check_function():
                print(f"--- {service_name} is ready. ---")
                return True
        except Exception as e:
            print(f"Waiting for {service_name}...")
        time.sleep(5)
    raise Exception(f"{service_name} was not ready in time.")

def check_neo4j(driver):
    try:
        with driver.session() as session:
            session.run("RETURN 1")
        return True
    except:
        return False

def setup_weaviate_schema(client: weaviate.WeaviateClient):
    """Creates the Document collection in Weaviate using v4 syntax."""
    if client.collections.exists(WEAVIATE_CLASS_NAME):
        print(f"--- Collection '{WEAVIATE_CLASS_NAME}' already exists. ---")
        return
    
    print(f"--- Creating collection '{WEAVIATE_CLASS_NAME}'... ---")
    client.collections.create(
        name=WEAVIATE_CLASS_NAME,
        # FINAL CORRECTION: The correct parameter for setting the vectorizer module is 'vectorizer_config'.
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
        properties=[
            wvc.config.Property(name="doc_id", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="content", data_type=wvc.config.DataType.TEXT),
            wvc.config.Property(name="source", data_type=wvc.config.DataType.TEXT),
        ]
    )
    print(f"--- Collection '{WEAVIATE_CLASS_NAME}' created. ---")


def process_documents(neo4j_driver, weaviate_client: weaviate.WeaviateClient, documents):
    """Processes documents and populates databases using v4 syntax."""
    all_entities = []
    
    documents_collection = weaviate_client.collections.get(WEAVIATE_CLASS_NAME)
    
    with neo4j_driver.session() as session:
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (d:Document) REQUIRE d.id IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (e:Entity) REQUIRE e.name IS UNIQUE")
        
        with documents_collection.batch.dynamic() as batch:
            for doc in tqdm(documents, desc="Processing Documents"):
                content = doc.get("content", "")
                doc_id = doc.get("id", "")

                spacy_doc = nlp(content)
                entities = []
                for ent in spacy_doc.ents:
                    if ent.label_ in ["PERSON", "ORG", "PRODUCT", "WORK_OF_ART"]:
                        entities.append({"name": ent.text, "type": ent.label_})
                
                unique_entities = { (e['name'], e['type']) for e in entities }
                all_entities.append({"doc_id": doc_id, "entities": list(unique_entities)})

                cypher_entities = [{"name": name, "type": type} for name, type in unique_entities]

                session.execute_write(
                    lambda tx: tx.run("""
                        MERGE (d:Document {id: $doc_id})
                        SET d.content = $content, d.source = $source
                        WITH d
                        UNWIND $entities as entity
                        MERGE (e:Entity {name: entity.name})
                        ON CREATE SET e.type = entity.type
                        MERGE (d)-[:MENTIONS]->(e)
                    """, doc_id=doc_id, content=content, source=doc.get("source"), entities=cypher_entities)
                )
                
                vector = model.encode(content).tolist()
                properties = { "doc_id": doc_id, "content": content, "source": doc.get("source") }
                batch.add_object(properties=properties, vector=vector)

    print("--- Neo4j and Weaviate population complete. ---")
    
    os.makedirs(os.path.dirname(LAKE_OUTPUT_PATH), exist_ok=True)
    with open(LAKE_OUTPUT_PATH, 'w') as f:
        json.dump(all_entities, f, indent=2)
    print(f"--- Processed entity data saved to '{LAKE_OUTPUT_PATH}' ---")

def main():
    print("--- Main function starting. ---")
    
    neo4j_driver = None
    weaviate_client = None
    try:
        print("--- Connecting to databases... ---")
        neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        parsed_url = urlparse(WEAVIATE_URL)
        weaviate_client = weaviate.connect_to_local(host=parsed_url.hostname, port=parsed_url.port)
        print("--- Database connections established. ---")

        wait_for_service(lambda: check_neo4j(neo4j_driver), "Neo4j")
        wait_for_service(lambda: weaviate_client.is_live(), "Weaviate")
        
        setup_weaviate_schema(weaviate_client)
        
        with open(DATA_SOURCE_PATH, 'r') as f:
            documents = json.load(f)
        
        process_documents(neo4j_driver, weaviate_client, documents)
        
        print("--- Ingestion pipeline finished successfully. ---")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if neo4j_driver:
            neo4j_driver.close()
        if weaviate_client:
            weaviate_client.close()

print("--- Script definitions loaded, preparing to run main() ---")
if __name__ == "__main__":
    main()