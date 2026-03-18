from hybrid_rag import HybridRAG
rag = HybridRAG()
questions = [
    "Hvornår skal min bil synes?",
    "Hvad sker der hvis jeg kører over for rødt lys?",
    "Hvordan bruger jeg identitetstegnebogen?",
    "Hvilke regler gælder for digital signatur?"
]
for idx, q in enumerate(questions, 1):
    print(f"\nSpørgsmål {idx}: {q}")
    top_paragraphs = rag.search(q, top_k=5)
    answer = rag.rerank_with_ollama(q, top_paragraphs)
    print(f"Svar:\n{answer}")
