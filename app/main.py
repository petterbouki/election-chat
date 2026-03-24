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
from startup import init_db
init_db()

from agent.sql_agent import SQLAgent, Intent
from agent.chart import make_chart, suggest_chart_type

st.set_page_config(
    page_title="Elections Legislatives CI 2025 ",
    page_icon="🗳️",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = os.getenv("DB_PATH", "data/elections.duckdb")

st.markdown("""
<style>
.stat-box { background:#1D9E75; color:white; border-radius:12px; padding:16px;
            text-align:center; font-size:2rem; font-weight:700; margin:8px 0; }
/* Auto-scroll */
.main .block-container {
    padding-bottom: 200px;
}
</style>
<script>
// Auto-scroll vers le bas
function scrollToBottom() {
    const messages = document.querySelectorAll('[data-testid="stChatMessage"]');
    if (messages.length > 0) {
        messages[messages.length - 1].scrollIntoView({ behavior: 'smooth', block: 'end' });
    }
}
setTimeout(scrollToBottom, 500);
</script>
""", unsafe_allow_html=True)

# ─── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🗳️ Elections Legislatives CI 2025")
    st.markdown("")
   # st.divider()
    #st.markdown("**Questions suggérées**")
    
   # suggestions = [
    #    "Combien de sièges a remporté le RHDP ?",
     #   "Top 10 candidats par score",
      #  "Taux de participation par région",
       # "Histogramme des élus par parti",
      #  "Qui a gagné à Agboville ?",
       # "Les élus d'Abidjan",
       # "Quelle circonscription a le plus fort taux ?",
       # "Quels partis ont remporté des sièges ?",
   # ]
    #for s in suggestions:
     #   if st.button(s, key=f"sug_{s[:20]}", use_container_width=True):
      #      st.session_state["pending_question"] = s

    st.divider()
    if st.button("🔄 Effacer la conversation", use_container_width=True):
        st.session_state["messages"] = []
        if "agent" in st.session_state:
            st.session_state["agent"].reset_history()
        st.rerun()

    show_sql   = st.toggle("Afficher le SQL généré", value=True)
    show_debug = st.toggle("Mode debug (intent)", value=False)

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
    if result.get("error"):
        st.error(result["error"])
        return

    narrative = build_narrative(result)
    if narrative:
        st.markdown(narrative)

    if show_debug and result.get("intent"):
        route = result.get("route", "?")
        st.caption(f"Intent: `{result['intent']}` | Route: `{route}` | {result.get('elapsed_ms',0):.0f}ms")

    if show_sql and result.get("sql"):
        with st.expander("SQL généré", expanded=False):
            st.code(result["sql"], language="sql")

    figure = None
    df = result.get("data")
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
                st.plotly_chart(figure, use_container_width=True)

    result["figure"] = figure


# ─── Titre ───────────────────────────────────────────────────────────────────
st.title("🗳️ Chat avec les résultats électoraux")
#st.caption("Élections législatives ivoiriennes  2025")

# ─── Message de bienvenue ────────────────────────────────────────────────────
if not st.session_state["messages"]:
    welcome_result = {
        "question": "", "intent": "welcome", "route": "system",
        "sql": None, "data": None, "chart_type": None,
        "error": None, "elapsed_ms": 0, "rag_context": None,
        "narrative": """ Bienvenue sur ELECTIA 👋

Votre assistant intelligent pour analyser les élections législatives ivoiriennes de 2025.

Je peux vous aider sur :
-  Les **résultats par parti** — sièges, voix, pourcentages
-  Les **statistiques** — participation, bulletins nuls, suffrages
-  Les **analyses locales** — Abidjan, Bouaké, Agboville...

**Quelques exemples pour commencer :**
> *"Combien de sièges a remporté le RHDP ?"*
> *"Qui a gagné à Agboville ?"*
> *"Taux de participation par région"*


Posez votre première question ci-dessous """,
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

# ─── Input + Questions rapides ───────────────────────────────────────────────
# IMPORTANT : chat_input doit être avant les boutons pour éviter les conflits
question = st.chat_input("Posez votre question sur les élections...")

# Questions rapides sous le prompt
# st.markdown("#####  Questions rapides")
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
            question = q  # Utilise directement sans rerun

# Récupère aussi depuis la sidebar
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

    # Force le scroll vers le bas
    st.markdown("""
    <script>
      setTimeout(function() {
      const messages = document.querySelectorAll('[data-testid="stChatMessage"]');
      if (messages.length > 0) {
        messages[messages.length - 1].scrollIntoView({ behavior: 'smooth', block: 'end' });
      }
      }, 300);
    </script>
    """, unsafe_allow_html=True)

    st.rerun()