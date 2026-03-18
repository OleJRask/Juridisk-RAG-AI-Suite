# ...existing code...
# Streamlit UI entry point for legal QA
import streamlit as st
from hybrid_rag import HybridRAG

st.set_page_config(page_title="Juridisk RAG (Hybrid)", page_icon="\u2696\ufe0f", layout="centered")
st.title("Juridisk RAG \u2013 Stil dit juridiske sp\u00f8rgsm\u00e5l")

if "history" not in st.session_state:
	st.session_state["history"] = []

rag = HybridRAG()

with st.form("qa_form", clear_on_submit=True):
	question = st.text_area("Stil et juridisk sp\u00f8rgsm\u00e5l", key="question", height=80)
	submitted = st.form_submit_button("Svar")

if submitted and question.strip():
	with st.spinner("Finder svar ..."):
		top_paragraphs = rag.search(question, top_k=5)
		answer = rag.rerank_with_ollama(question, top_paragraphs)
		# Extract sources from top_paragraphs
		sources = []
		for p in top_paragraphs:
			law_name = p[5] if len(p) > 5 else ''
			url = p[6] if len(p) > 6 else ''
			paragraph = p[2] if len(p) > 2 else ''
			section = p[3] if len(p) > 3 else ''
			if law_name and url and paragraph:
				sources.append({"law_name": law_name, "paragraph": paragraph, "section": section, "url": url})
		st.session_state["history"].append({"question": question, "answer": answer, "sources": sources})
		st.success("Svar klar!")
		st.markdown(f"**Svar:**\n{answer}")
		if sources:
			st.markdown("**Kilder:**")
			for src in sources:
				st.markdown(f"- [{src['law_name']} {src['paragraph']}]({src['url']})")
		st.markdown("---")
		st.markdown("**Feedback:**")
		feedback = st.radio("Var svaret brugbart?", ["Ja", "Nej", "Delvist"], key=f"feedback_{len(st.session_state['history'])}")
		if feedback:
			rag.log_feedback(question, answer, feedback)
			st.info(f"Tak for din feedback: {feedback}")

if st.session_state["history"]:
	with st.expander("Tidligere sp\u00f8rgsm\u00e5l"):
		for item in reversed(st.session_state["history"]):
			st.write(f"**Sp\u00f8rgsm\u00e5l:** {item['question']}")
			st.write(f"**Svar:** {item['answer']}")
			if item['sources']:
				st.markdown("**Kilder:**")
				for src in item['sources']:
					st.markdown(f"- [{src['law_name']} {src['paragraph']}]({src['url']})")
			st.markdown("---")
# ...existing code...
# Streamlit UI entry point for legal QA
# (copy your current juridisk_rag_streamlit_new.py here)
