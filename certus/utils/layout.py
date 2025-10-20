import streamlit as st

def render_header(title: str, subtitle: str = ""):
    st.title(title)
    if subtitle:
        st.write(subtitle)
    st.markdown("")

def render_kpis(kpis: dict):
    cols = st.columns(len(kpis))
    for col, (label, value) in zip(cols, kpis.items()):
        with col:
            st.metric(label, value)

def section(title: str, help_text: str | None = None):
    st.markdown(f"### {title}")
    if help_text:
        st.caption(help_text)

def footer():
    st.markdown("---")
    st.caption("© Certus • TrendLab Intelligence Engine")
