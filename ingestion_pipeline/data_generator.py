import json
import argparse
from faker import Faker
import random
import uuid

fake = Faker()

def generate_documents(count):
    """Generates a list of random documents."""
    documents = []
    
    # Create a pool of fake entities to increase connections
    people = [fake.name() for _ in range(count // 2)]
    orgs = [fake.company() for _ in range(count // 3)]
    projects = [f"{fake.word().capitalize()} Project" for _ in range(count // 2)]
    products = [f"{fake.word().capitalize()}System" for _ in range(count // 3)]
    
    for i in range(count):
        doc_id = str(uuid.uuid4())
        
        # Construct a sentence with random entities
        p1 = random.choice(people)
        p2 = random.choice(people)
        org = random.choice(orgs)
        proj = random.choice(projects)
        prod = random.choice(products)
        
        templates = [
            f"A new memo from {org} confirms that {p1} will be leading the {proj}.",
            f"The {prod} development team, including {p1} and {p2}, resolved a critical bug.",
            f"Financial report from {org} highlights the success of the {prod}.",
            f"Meeting notes for {proj}: {p1} presented the latest updates.",
            f"{p2} from {org} is now assigned to the {proj} team."
        ]
        
        content = random.choice(templates)
        
        doc = {
            "id": doc_id,
            "content": content,
            "source": random.choice(["Email Archive", "Slack Channel", "Confluence"]),
            "timestamp": fake.iso8601()
        }
        documents.append(doc)
        
    return documents

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate random documents for the knowledge graph.")
    parser.add_argument("--count", type=int, default=100, help="Number of documents to generate.")
    parser.add_argument("--output", type=str, default="data/source/generated_docs.json", help="Output JSON file path.")
    
    args = parser.parse_args()
    
    print(f"Generating {args.count} documents...")
    docs = generate_documents(args.count)
    
    with open(args.output, 'w') as f:
        json.dump(docs, f, indent=2)
        
    print(f"Successfully saved generated documents to {args.output}")