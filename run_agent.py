import sys

# Ensure UTF-8 output encoding for Windows terminals
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from rca_agent.agent import graph

def main():
    print("====================================================")
    print("Starting DataOps Root Cause Analysis LangGraph Agent")
    print("====================================================")
    
    try:
        # Run graph
        result = graph.invoke({})
        
        print("\n" + "=" * 30 + " FINAL DIAGNOSIS " + "=" * 30)
        print(result.get("diagnosis", "No diagnosis returned."))
        print("=" * 77)
        
    except Exception as e:
        print(f"\nError running agent: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
