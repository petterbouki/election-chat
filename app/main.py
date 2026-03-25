"""
app/main.py — Interface Streamlit Elections CI 2025
"""

import sys, os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

from agent.sql_agent import SQLAgent, Intent
from agent.chart import make_chart, suggest_chart_type

st.set_page_config(
    page_title="Elections CI 2025 — Chat",
    page_icon="🗳️",
    layout="wide",
    initial_sidebar_state="collapsed",  # Mobile : sidebar fermée par défaut
)

DB_PATH = os.getenv("DB_PATH", "data/elections.duckdb")

st.markdown("""
<style>
/* Mobile responsive */
@media (max-width: 768px) {
    [data-testid="stSidebar"] { display: none; }
    .block-container { padding: 1rem 0.5rem !important; }
}

/* Bouton copier */
.copy-btn {
    background: none; border: 1px solid var(--color-border-secondary);
    border-radius: 6px; padding: 4px 10px; cursor: pointer;
    font-size: 12px; color: var(--color-text-secondary);
    margin-top: 4px;
}
.copy-btn:hover { background: var(--color-background-secondary); }

/* Questions rapides */
.quick-label { font-size: 13px; color: var(--color-text-secondary); margin-bottom: 4px; }
            
/* Cache les boutons Streamlit Cloud */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
header { visibility: hidden; }
[data-testid="stToolbar"] { display: none; }
[data-testid="stDecoration"] { display: none; }
.viewerBadge_container__r5tak { display: none; }            
</style>
""", unsafe_allow_html=True)

# ─── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("Elections Législatives CI 2025")
    #st.markdown("**Assemblée Nationale — 27 décembre 2025**")
   # st.divider()
   # st.markdown("**Questions suggérées**")
    #suggestions = [
    #    "Combien de sièges a remporté le RHDP ?",
     #   "Top 10 candidats par score",
     #   "Taux de participation par région",
     #   "Histogramme des élus par parti",
     #   "Qui a gagné à Agboville ?",
     #   "Les élus d'Abidjan",
     #   "Quelle circonscription a le plus fort taux ?",
      #  "Quels partis ont remporté des sièges ?",
    #]
    #for s in suggestions:
    #    if st.button(s, key=f"sug_{s[:20]}", use_container_width=True):
    #        st.session_state["pending_question"] = s

    #st.divider()
    if st.button("Effacer la conversation", use_container_width=True):
        st.session_state["messages"] = []
        st.session_state["chart_counter"] = 0
        if "agent" in st.session_state:
            st.session_state["agent"].reset_history()
        st.rerun()

    show_sql   = st.toggle("Afficher le SQL généré", value=False)
    show_debug = st.toggle("Mode debug", value=False)

# ─── Agent ───────────────────────────────────────────────────────────────────
@st.cache_resource
def load_agent(db_path: str) -> SQLAgent:
    return SQLAgent(db_path)

if "agent" not in st.session_state:
    if not Path(DB_PATH).exists():
        st.error(f"Base introuvable : `{DB_PATH}`")
        st.stop()
    st.session_state["agent"] = load_agent(DB_PATH)

if "messages" not in st.session_state:
    st.session_state["messages"] = []

if "chart_counter" not in st.session_state:
    st.session_state["chart_counter"] = 0

agent: SQLAgent = st.session_state["agent"]

# ─── Helpers ─────────────────────────────────────────────────────────────────

def build_narrative(result: dict) -> str:
    narrative = result.get("narrative", "")
    route = result.get("route", "sql")

    if route in ("system", "clarification"):
        return narrative

    if route == "rag":
        if narrative.count("|") > 5 or narrative.startswith("id :"):
            ctx = result.get("rag_context", "")
            if ctx:
                return ctx
        return narrative

    df = result.get("data")
    if df is None or df.empty:
        return narrative

    if len(df) == 1 and len(df.columns) == 1:
        val = df.iloc[0, 0]
        if isinstance(val, float) and not pd.isna(val) and val == int(val):
            val = int(val)
        q = result.get("question", "").lower()
        if "rhdp" in q and ("siège" in q or "siege" in q):
            return f"Le RHDP a remporté **{val} sièges**."
        elif "siège" in q or "siege" in q:
            return f"**{val} sièges** au total."
        elif "taux" in q or "participation" in q:
            return f"Taux de participation moyen : **{val}%**."
        elif "combien" in q:
            return f"**{val}**"
        else:
            return f"**{val}**\n\n{narrative}".strip()
    elif len(df) == 1:
        parts = []
        for i in range(len(df.columns)):
            v = df.iloc[0, i]
            if isinstance(v, float) and not pd.isna(v) and v == int(v):
                v = int(v)
            parts.append(f"**{df.columns[i]}** : {v}")
        return " | ".join(parts)

    return narrative


def should_show_chart(df, intent, requested_type):
    if requested_type:
        return requested_type
    if df is None or df.empty or len(df) <= 1:
        return None
    if intent in (Intent.CHART, Intent.AGGREGATION, Intent.RANKING):
        return suggest_chart_type(df)
    return None


def display_result(result: dict, show_sql: bool, show_debug: bool):
    error = result.get("error")
    df = result.get("data")
    narrative = build_narrative(result)

    # Message d'erreur seulement si erreur ET pas de données ET pas de narrative
    if error and (df is None or df.empty) and not narrative:
        st.warning(
            "Je n'ai pas pu traiter cette demande. "
            "Essayez de reformuler votre question en précisant "
            "une circonscription, un parti ou un candidat."
        )
        if show_debug:
            st.caption(f"Erreur technique: {error}")
        return

    # Affiche la narrative
    if narrative:
        st.markdown(narrative)
        if result.get("route") not in ("system", "welcome"):
            with st.expander("Copier la réponse", expanded=False):
                st.code(narrative, language=None)

    if show_debug and result.get("intent"):
        st.caption(f"Intent: `{result['intent']}` | Route: `{result.get('route','?')}` | {result.get('elapsed_ms',0):.0f}ms")

    if show_sql and result.get("sql"):
        with st.expander("SQL généré", expanded=False):
            st.code(result["sql"], language="sql")

    route = result.get("route", "sql")
    if df is not None and not df.empty and route == "sql":
        if len(df) > 1 or len(df.columns) > 1:
            st.dataframe(df, use_container_width=True,
                         height=min(400, len(df) * 35 + 50))

        chart_type = should_show_chart(df, result.get("intent",""), result.get("chart_type"))
        if chart_type:
            figure = make_chart(df, chart_type=chart_type,
                                title=result.get("question","")[:60])
            if figure:
                st.session_state["chart_counter"] += 1
                st.plotly_chart(
                    figure,
                    use_container_width=True,
                    key=f"chart_{st.session_state['chart_counter']}"
                )

# ─── Titre ───────────────────────────────────────────────────────────────────
st.title("Explorer les résultats des élections Legislatives CI 2025")
#st.caption("Elections législatives ivoiriennes — 27 décembre 2025")

# ─── Message de bienvenue ────────────────────────────────────────────────────
if not st.session_state["messages"]:
    welcome_result = {
        "question": "", "intent": "welcome", "route": "system",
        "sql": None, "data": None, "chart_type": None,
        "error": None, "elapsed_ms": 0, "rag_context": None,
        "narrative": """Bienvenue, je suis  **Electia** !

 Votre assistant Intelligent pour explorer les résultats de ces **élections **.

Je peux vous aider sur :
- Les **résultats par parti** — sièges, voix, pourcentages
- Les **candidats élus** par circonscription ou région
- Les **statistiques** — participation, bulletins nuls, suffrages


**Exemples pour commencer :**
> *"Combien de sièges a remporté le RHDP ?"*
> *"Qui a gagné à Agboville ?"*
> *"Taux de participation par région"*


Posez votre première question ci-dessous""",
    }
    st.session_state["messages"].append({
        "role": "assistant",
        "content": welcome_result["narrative"],
        "result": welcome_result,
    })

# ─── Historique ──────────────────────────────────────────────────────────────
for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        if msg["role"] == "user":
            st.write(msg["content"])
        else:
            display_result(msg.get("result", {}), show_sql, show_debug)

# ─── Input ───────────────────────────────────────────────────────────────────
question = st.chat_input("Posez votre question sur les élections...")

# Questions rapides
st.markdown('<p class="quick-label">Questions rapides :</p>', unsafe_allow_html=True)
cols = st.columns(3)
quick_questions = [
    "Combien de sièges a le RHDP ?",
    "Top 10 candidats par score",
    "Qui a gagné à Agboville ?",
    "Taux de participation par région",
    "Histogramme des élus par parti",
    "Parle moi d'Abidjan",
]
for i, q in enumerate(quick_questions):
    with cols[i % 3]:
        if st.button(q, key=f"quick_{i}", use_container_width=True):
            question = q

if not question:
    question = st.session_state.pop("pending_question", None)

if question:
    st.session_state["messages"].append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.write(question)

    with st.chat_message("assistant"):
        with st.spinner("Analyse en cours..."):
            result = agent.ask(question)
        result["question"] = question
        display_result(result, show_sql, show_debug)

    st.session_state["messages"].append({
        "role": "assistant",
        "content": result.get("narrative", ""),
        "result": result,
    })
    st.rerun()