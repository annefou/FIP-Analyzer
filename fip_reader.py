#!/usr/bin/env python3
"""
FIP Reader - Parse FAIR Implementation Profiles and display FAIR-enabling resources
by FAIR principle in a human-readable format.

This script reads .trig files containing FIP metadata, fetches the full declarations
from the nanopublication network, and organizes them by FAIR principle.
"""

import sys
import json
from pathlib import Path

try:
    from rdflib import Graph, ConjunctiveGraph, Namespace, URIRef, Literal
    from rdflib.namespace import RDF, RDFS, DCTERMS
except ImportError:
    print("Installing rdflib...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "rdflib", "--break-system-packages", "-q"])
    from rdflib import Graph, ConjunctiveGraph, Namespace, URIRef, Literal
    from rdflib.namespace import RDF, RDFS, DCTERMS

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

try:
    import requests
except ImportError:
    print("Installing requests...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "--break-system-packages", "-q"])
    import requests

# Define namespaces
FIP = Namespace("https://w3id.org/fair/fip/terms/")
FAIR = Namespace("https://w3id.org/fair/principles/terms/")
SCHEMA = Namespace("https://schema.org/")
NP = Namespace("http://www.nanopub.org/nschema#")
NPX = Namespace("http://purl.org/nanopub/x/")

# FAIR Principles mapping with human-readable descriptions
FAIR_PRINCIPLES = {
    "F1": "F1 - Globally unique and persistent identifiers",
    "F2": "F2 - Data described with rich metadata",
    "F3": "F3 - Metadata include identifier of data",
    "F4": "F4 - Metadata registered in searchable resource",
    "A1": "A1 - Retrievable by identifier using standard protocol",
    "A1.1": "A1.1 - Protocol is open, free, universally implementable",
    "A1.2": "A1.2 - Protocol allows authentication/authorization",
    "A2": "A2 - Metadata accessible even when data unavailable",
    "I1": "I1 - Knowledge representation language used",
    "I2": "I2 - FAIR vocabularies used",
    "I3": "I3 - Qualified references to other data",
    "R1": "R1 - Richly described with accurate attributes",
    "R1.1": "R1.1 - Clear and accessible data usage license",
    "R1.2": "R1.2 - Detailed provenance",
    "R1.3": "R1.3 - Meet domain-relevant community standards",
}

# FIP question types mapping
FIP_QUESTIONS = {
    "FIP-Question-F1-D": ("F1", "Data", "What globally unique, persistent, resolvable identifiers do you use for datasets?"),
    "FIP-Question-F1-MD": ("F1", "Metadata", "What globally unique, persistent, resolvable identifiers do you use for metadata records?"),
    "FIP-Question-F2-D": ("F2", "Data", "Which metadata schemas do you use for findability?"),
    "FIP-Question-F2-MD": ("F2", "Metadata", "Which metadata schemas do you use for describing metadata?"),
    "FIP-Question-F3-D": ("F3", "Data", "What is the technology that links the persistent identifiers of your data to the metadata description?"),
    "FIP-Question-F3-MD": ("F3", "Metadata", "What is the technology linking metadata identifiers?"),
    "FIP-Question-F4-D": ("F4", "Data", "In which search engines are your datasets indexed?"),
    "FIP-Question-F4-MD": ("F4", "Metadata", "In which search engines are your metadata records indexed?"),
    "FIP-Question-A1-D": ("A1", "Data", "Which standardized communication protocols do you use for datasets?"),
    "FIP-Question-A1-MD": ("A1", "Metadata", "Which standardized communication protocols do you use for metadata?"),
    "FIP-Question-A1.1-D": ("A1.1", "Data", "Which authentication & authorization technique do you use for datasets?"),
    "FIP-Question-A1.1-MD": ("A1.1", "Metadata", "Which authentication & authorization technique do you use for metadata?"),
    "FIP-Question-A1.2-D": ("A1.2", "Data", "Which authentication & authorization technique do you use for datasets?"),
    "FIP-Question-A1.2-MD": ("A1.2", "Metadata", "Which authentication & authorization technique do you use for metadata?"),
    "FIP-Question-A2": ("A2", "Metadata", "Which metadata longevity plan do you use?"),
    "FIP-Question-I1-D": ("I1", "Data", "Which knowledge representation languages do you use for datasets?"),
    "FIP-Question-I1-MD": ("I1", "Metadata", "Which knowledge representation languages do you use for metadata?"),
    "FIP-Question-I2-D": ("I2", "Data", "Which structured vocabularies do you use for datasets?"),
    "FIP-Question-I2-MD": ("I2", "Metadata", "Which structured vocabularies do you use for metadata?"),
    "FIP-Question-I3-D": ("I3", "Data", "Which models/formats do you use for qualified references between datasets?"),
    "FIP-Question-I3-MD": ("I3", "Metadata", "Which models/formats do you use for qualified references in metadata?"),
    "FIP-Question-R1-D": ("R1", "Data", "Which metadata schemas do you use for rich description?"),
    "FIP-Question-R1-MD": ("R1", "Metadata", "Which metadata schemas do you use for rich metadata description?"),
    "FIP-Question-R1.1-D": ("R1.1", "Data", "Which license do you use for datasets?"),
    "FIP-Question-R1.1-MD": ("R1.1", "Metadata", "Which license do you use for metadata?"),
    "FIP-Question-R1.2-D": ("R1.2", "Data", "Which metadata schemas do you use for provenance?"),
    "FIP-Question-R1.2-MD": ("R1.2", "Metadata", "Which metadata schemas do you use for metadata provenance?"),
    "FIP-Question-R1.3-D": ("R1.3", "Data", "Which community-endorsed standards do you follow for data?"),
    "FIP-Question-R1.3-MD": ("R1.3", "Metadata", "Which community-endorsed standards do you follow for metadata?"),
}


def parse_fip_header(filepath: str) -> dict:
    """Parse the FIP header .trig file to extract basic metadata."""
    g = ConjunctiveGraph()
    g.parse(filepath, format="trig")
    
    fip_info = {
        "label": None,
        "description": None,
        "version": None,
        "declared_by": None,
        "declaration_index": None,
        "creators": [],
        "created": None,
        "wizard_source": None,
    }
    
    # Iterate through all named graphs to find the data
    for context in g.contexts():
        for s, p, o in context:
            p_str = str(p)
            o_str = str(o)
            
            # Get label
            if "label" in p_str.lower():
                fip_info["label"] = o_str
            
            # Get description
            if "description" in p_str.lower():
                fip_info["description"] = o_str
            
            # Get version
            if "version" in p_str.lower():
                fip_info["version"] = o_str
            
            # Get declared-by
            if "declared-by" in p_str.lower():
                fip_info["declared_by"] = o_str
            
            # Get declaration index
            if "declaration-index" in p_str.lower():
                fip_info["declaration_index"] = o_str
            
            # Get creators (ORCID)
            if "creator" in p_str.lower() and "orcid.org" in o_str:
                fip_info["creators"].append(o_str)
            
            # Get creation date
            if "created" in p_str.lower() and isinstance(o, Literal):
                fip_info["created"] = o_str
            
            # Get wizard source
            if "wasDerivedFrom" in p_str and "fip/wizard" in o_str:
                fip_info["wizard_source"] = o_str
    
    return fip_info


def fetch_nanopub(uri: str, format_type: str = "trig") -> ConjunctiveGraph:
    """Fetch a nanopublication from the network."""
    # Try different endpoints
    np_id = uri.split('/')[-1]
    endpoints = [
        f"{uri}.{format_type}",
        f"https://np.petapico.org/{np_id}.{format_type}",
        f"https://server.np.trustyuri.net/{np_id}.{format_type}",
        f"https://w3id.org/np/{np_id}.{format_type}",
    ]
    
    headers = {"Accept": "application/trig"}
    
    for endpoint in endpoints:
        try:
            resp = requests.get(endpoint, headers=headers, timeout=30)
            if resp.status_code == 200:
                g = ConjunctiveGraph()
                g.parse(data=resp.text, format="trig")
                return g
        except Exception as e:
            continue
    
    return None


def read_fip_from_json(json_filepath: str) -> tuple:
    """Read FIP declarations from a JSON export from FIP Wizard."""
    with open(json_filepath, 'r') as f:
        data = json.load(f)
    
    fip_info = {
        "label": data.get("name", "Unknown FIP"),
        "description": data.get("description", ""),
        "version": data.get("version", "1.0.0"),
        "creators": data.get("creators", []),
        "declared_by": data.get("community", ""),
        "created": data.get("created", ""),
        "declaration_index": "",
        "wizard_source": data.get("uuid", ""),
    }
    
    declarations = []
    
    # Parse replies/declarations from JSON structure
    for reply in data.get("replies", []):
        question_path = reply.get("path", "")
        
        # Try to extract principle from path
        principle_match = None
        for q_key, (principle, dtype, _) in FIP_QUESTIONS.items():
            if q_key.lower() in question_path.lower():
                principle_match = (principle, dtype)
                break
        
        if not principle_match:
            continue
        
        # Extract resources from the reply
        answer = reply.get("answer", {})
        if isinstance(answer, dict):
            items = answer.get("items", [])
            for item in items:
                if isinstance(item, dict):
                    resource_name = item.get("label") or item.get("name") or item.get("id", "Unknown")
                    resource_uri = item.get("uri") or item.get("url") or ""
                    
                    declarations.append({
                        "question_id": question_path,
                        "resource_label": resource_name,
                        "resource_uri": resource_uri,
                        "resource_type": "current",
                        "principle": principle_match[0],
                        "data_type": principle_match[1],
                    })
    
    return fip_info, declarations


def organize_by_principle_from_json(declarations: list) -> dict:
    """Organize declarations by FAIR principle from JSON data."""
    organized = {}
    
    for principle_key in ["F1", "F2", "F3", "F4", "A1", "A1.1", "A1.2", "A2", 
                          "I1", "I2", "I3", "R1", "R1.1", "R1.2", "R1.3"]:
        organized[principle_key] = {"data": [], "metadata": []}
    
    for decl in declarations:
        principle = decl.get("principle")
        dtype = decl.get("data_type", "data").lower()
        
        if principle and principle in organized:
            resource_info = {
                "label": decl.get("resource_label", "Unknown"),
                "uri": decl.get("resource_uri"),
                "type": decl.get("resource_type", "current"),
            }
            
            if dtype == "data":
                organized[principle]["data"].append(resource_info)
            else:
                organized[principle]["metadata"].append(resource_info)
    
    return organized


def extract_declarations_from_index(index_graph: ConjunctiveGraph, debug: bool = False) -> list:
    """Extract declaration URIs from the index nanopublication."""
    declarations = []
    
    # Common predicates used in nanopub indexes (full URIs)
    index_predicate_uris = [
        "http://purl.org/nanopub/x/includesElement",  # This is the one used by FIP Wizard!
        "http://purl.org/nanopub/x/hasElement",
        "http://purl.org/nanopub/x/includes",
        "https://w3id.org/fair/fip/terms/has-declaration",
    ]
    
    if debug:
        print("\n   DEBUG: All triples in index nanopublication:")
        for context in index_graph.contexts():
            ctx_id = str(context.identifier)
            # Only show assertion graph content (not head, provenance, pubinfo)
            if "assertion" in ctx_id.lower() or "Head" not in ctx_id:
                print(f"\n   Graph: {ctx_id}")
                for s, p, o in context:
                    print(f"      {s}")
                    print(f"        --[{p}]-->")
                    print(f"        {o}")
                    print()
    
    # Iterate through all named graphs
    for context in index_graph.contexts():
        for s, p, o in context:
            p_str = str(p)
            o_str = str(o)
            
            # Check if predicate matches any known index predicate
            if p_str in index_predicate_uris:
                declarations.append(o_str)
                if debug:
                    print(f"   ✓ Found declaration: {o_str[:70]}...")
    
    return list(set(declarations))


def parse_declaration(decl_graph: ConjunctiveGraph, debug: bool = False) -> dict:
    """Parse a single FIP declaration to extract the resource and question."""
    declaration = {
        "question": None,
        "question_id": None,
        "resource_label": None,
        "resource_uri": None,
        "resource_type": "current",
    }
    
    # FIP-specific predicates
    fip_predicates = {
        "refers-to-question": "https://w3id.org/fair/fip/terms/refers-to-question",
        "declares-current-use-of": "https://w3id.org/fair/fip/terms/declares-current-use-of",
        "declares-planned-use-of": "https://w3id.org/fair/fip/terms/declares-planned-use-of",
        "declares-planned-replacement-of": "https://w3id.org/fair/fip/terms/declares-planned-replacement-of",
    }
    
    # Collect all labels for lookup
    labels = {}
    
    # Iterate through all named graphs
    for context in decl_graph.contexts():
        for s, p, o in context:
            p_str = str(p)
            o_str = str(o)
            s_str = str(s)
            
            # Collect labels
            if "label" in p_str.lower():
                labels[s_str] = o_str
            
            # Look for question reference
            if "refers-to-question" in p_str.lower() or p_str == fip_predicates["refers-to-question"]:
                declaration["question"] = o_str
                # Extract question ID from URI (e.g., "F1-D", "F2-MD", etc.)
                if "FIP-Question-" in o_str:
                    q_id = o_str.split("FIP-Question-")[-1]
                    declaration["question_id"] = q_id
                elif "/F" in o_str or "/A" in o_str or "/I" in o_str or "/R" in o_str:
                    q_id = o_str.split("/")[-1]
                    declaration["question_id"] = q_id
            
            # Look for declared resource (current use)
            if "declares-current-use-of" in p_str.lower() or p_str == fip_predicates["declares-current-use-of"]:
                declaration["resource_uri"] = o_str
                declaration["resource_type"] = "current"
            
            # Look for planned use
            if "declares-planned-use-of" in p_str.lower() or p_str == fip_predicates["declares-planned-use-of"]:
                declaration["resource_uri"] = o_str
                declaration["resource_type"] = "planned"
            
            # Look for planned replacement
            if "declares-planned-replacement-of" in p_str.lower() or p_str == fip_predicates["declares-planned-replacement-of"]:
                declaration["resource_uri"] = o_str
                declaration["resource_type"] = "replacement"
    
    # Try to get label for the resource
    if declaration["resource_uri"]:
        if declaration["resource_uri"] in labels:
            declaration["resource_label"] = labels[declaration["resource_uri"]]
        else:
            # Try to extract name from URI
            uri = declaration["resource_uri"]
            if "#" in uri:
                declaration["resource_label"] = uri.split("#")[-1].replace("-", " ").replace("_", " ")
            elif "/" in uri:
                last_part = uri.split("/")[-1]
                if last_part:
                    declaration["resource_label"] = last_part.replace("-", " ").replace("_", " ")
    
    if debug and declaration["question_id"]:
        print(f"   Parsed: {declaration['question_id']} -> {declaration.get('resource_label', 'Unknown')}")
    
    return declaration


def organize_by_principle(declarations: list) -> dict:
    """Organize declarations by FAIR principle."""
    organized = {}
    
    for principle_key in ["F1", "F2", "F3", "F4", "A1", "A1.1", "A1.2", "A2", 
                          "I1", "I2", "I3", "R1", "R1.1", "R1.2", "R1.3"]:
        organized[principle_key] = {"data": [], "metadata": []}
    
    for decl in declarations:
        if not decl.get("question_id"):
            continue
        
        q_id = decl["question_id"]
        
        # Parse question ID format: "F1-D", "F1-MD", "A1.1-D", "R1.2-MD", etc.
        # The principle is before the dash, the type (D or MD) is after
        if "-" in q_id:
            parts = q_id.rsplit("-", 1)
            principle = parts[0]
            dtype = parts[1] if len(parts) > 1 else "D"
        else:
            # Fallback: try to extract principle from the ID
            principle = q_id
            dtype = "D"
        
        # Normalize principle (e.g., "A1.1" -> "A1.1")
        if principle in organized:
            resource_info = {
                "label": decl.get("resource_label") or decl.get("resource_uri", "Unknown"),
                "uri": decl.get("resource_uri"),
                "type": decl.get("resource_type", "current"),
            }
            
            if dtype.upper() == "MD":
                organized[principle]["metadata"].append(resource_info)
            else:
                organized[principle]["data"].append(resource_info)
    
    return organized


def print_fip_report(fip_info: dict, organized: dict):
    """Print a human-readable FIP report."""
    print("=" * 80)
    print(f"FAIR IMPLEMENTATION PROFILE: {fip_info.get('label', 'Unknown')}")
    print("=" * 80)
    print()
    
    if fip_info.get("description"):
        print(f"Description: {fip_info['description']}")
    if fip_info.get("version"):
        print(f"Version: {fip_info['version']}")
    if fip_info.get("creators"):
        print(f"Creators: {', '.join(fip_info['creators'])}")
    if fip_info.get("declared_by"):
        print(f"Declared by: {fip_info['declared_by']}")
    
    print()
    print("-" * 80)
    print("FAIR-ENABLING RESOURCES BY PRINCIPLE")
    print("-" * 80)
    
    for principle_key, principle_name in FAIR_PRINCIPLES.items():
        resources = organized.get(principle_key, {"data": [], "metadata": []})
        
        has_resources = resources["data"] or resources["metadata"]
        
        if has_resources:
            print()
            print(f"\n📋 {principle_name}")
            print("   " + "-" * 60)
            
            if resources["data"]:
                print("   📊 For DATA:")
                for res in resources["data"]:
                    status = " (planned)" if res["type"] == "planned" else ""
                    print(f"      • {res['label']}{status}")
                    if res.get("uri"):
                        print(f"        URI: {res['uri']}")
            
            if resources["metadata"]:
                print("   📝 For METADATA:")
                for res in resources["metadata"]:
                    status = " (planned)" if res["type"] == "planned" else ""
                    print(f"      • {res['label']}{status}")
                    if res.get("uri"):
                        print(f"        URI: {res['uri']}")
    
    print()
    print("=" * 80)


def read_fip_from_file(filepath: str, fetch_remote: bool = True, debug: bool = False):
    """Main function to read and display a FIP from a .trig file."""
    print(f"\n🔍 Reading FIP from: {filepath}")
    print("-" * 40)
    
    # Step 1: Parse the FIP header
    fip_info = parse_fip_header(filepath)
    
    print(f"✅ Found FIP: {fip_info.get('label', 'Unknown')}")
    print(f"   Declaration index: {fip_info.get('declaration_index', 'Not found')}")
    
    declarations = []
    
    if fetch_remote and fip_info.get("declaration_index"):
        print(f"\n📡 Fetching declarations from nanopublication network...")
        
        # Step 2: Fetch the declaration index
        index_graph = fetch_nanopub(fip_info["declaration_index"])
        
        if index_graph:
            print(f"✅ Fetched declaration index")
            
            # Step 3: Extract declaration URIs
            decl_uris = extract_declarations_from_index(index_graph, debug=debug)
            print(f"   Found {len(decl_uris)} declaration references")
            
            if len(decl_uris) == 0 and not debug:
                print("\n⚠️  No declarations found in index. Run with --debug to see the index content:")
                print(f"   python fip_reader.py {filepath} --fetch --debug")
            
            # Step 4: Fetch each declaration
            for i, decl_uri in enumerate(decl_uris[:50]):  # Limit to 50 for performance
                decl_graph = fetch_nanopub(decl_uri)
                if decl_graph:
                    decl = parse_declaration(decl_graph, debug=debug)
                    if decl.get("question_id"):
                        declarations.append(decl)
                    elif debug:
                        print(f"\n   DEBUG: Declaration {decl_uri} has no question_id")
                print(f"\r   Fetching declarations: {i+1}/{min(len(decl_uris), 50)}", end="")
            
            print()
        else:
            print("⚠️  Could not fetch declaration index from network")
            print()
            print("   This may be due to network restrictions. Alternatives:")
            print("   1. Export the FIP as JSON from FIP Wizard and use:")
            print(f"      python fip_reader.py <exported_file.json>")
            print()
            print("   2. Access the FIP directly in your browser:")
            if fip_info.get('wizard_source'):
                print(f"      https://fip.fair-wizard.com/projects/{fip_info['wizard_source'].split('/')[-1]}")
            print()
    
    # Step 5: Organize and display
    organized = organize_by_principle(declarations)
    print_fip_report(fip_info, organized)
    
    return fip_info, declarations


def read_fip_local_only(filepath: str):
    """Read FIP header without fetching remote declarations."""
    print(f"\n🔍 Reading FIP header from: {filepath}")
    print("-" * 40)
    
    fip_info = parse_fip_header(filepath)
    
    print()
    print("=" * 80)
    print(f"FAIR IMPLEMENTATION PROFILE: {fip_info.get('label', 'Unknown')}")
    print("=" * 80)
    print()
    print(f"📋 Description: {fip_info.get('description', 'N/A')}")
    print(f"📌 Version: {fip_info.get('version', 'N/A')}")
    print(f"👥 Creators: {', '.join(fip_info.get('creators', ['N/A'])) or 'N/A'}")
    print(f"🏢 Declared by: {fip_info.get('declared_by', 'N/A')}")
    print(f"📅 Created: {fip_info.get('created', 'N/A')}")
    print(f"🔗 Declaration Index: {fip_info.get('declaration_index', 'N/A')}")
    print(f"🧙 FIP Wizard Source: {fip_info.get('wizard_source', 'N/A')}")
    print()
    print("=" * 80)
    print()
    print("ℹ️  To fetch full declarations from the network, use:")
    print(f"   python fip_reader.py {filepath} --fetch")
    print()
    
    return fip_info


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("FIP Reader - Parse FAIR Implementation Profiles")
        print("=" * 50)
        print()
        print("Usage: python fip_reader.py <fip_file> [options]")
        print()
        print("Supported file types:")
        print("  .trig    - TriG format (nanopublication)")
        print("  .json    - JSON export from FIP Wizard")
        print()
        print("Options:")
        print("  --fetch    Fetch full declarations from nanopublication network")
        print("  --debug    Show detailed debug output (use with --fetch)")
        print("  --local    Only read local file header (default for .trig)")
        print()
        print("Examples:")
        print("  python fip_reader.py FIESTA_Bio_FIP.trig")
        print("  python fip_reader.py FIESTA_Bio_FIP.trig --fetch")
        print("  python fip_reader.py FIESTA_Bio_FIP.trig --fetch --debug")
        print("  python fip_reader.py fip_export.json")
        print()
        print("Note: To export from FIP Wizard, go to your FIP page and use")
        print("      the export function to download the JSON file.")
        sys.exit(1)
    
    filepath = sys.argv[1]
    fetch_remote = "--fetch" in sys.argv
    debug_mode = "--debug" in sys.argv
    
    if not Path(filepath).exists():
        print(f"Error: File not found: {filepath}")
        sys.exit(1)
    
    # Check file type
    if filepath.endswith(".json"):
        print(f"\n🔍 Reading FIP from JSON: {filepath}")
        print("-" * 40)
        try:
            fip_info, declarations = read_fip_from_json(filepath)
            organized = organize_by_principle_from_json(declarations)
            print_fip_report(fip_info, organized)
        except Exception as e:
            print(f"Error reading JSON file: {e}")
            sys.exit(1)
    elif fetch_remote:
        read_fip_from_file(filepath, fetch_remote=True, debug=debug_mode)
    else:
        read_fip_local_only(filepath)
