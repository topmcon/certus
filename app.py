# app.py — minimal Streamlit launcher for pages/
import streamlit as st

st.set_page_config(page_title="Certus", layout="wide")
st.title("Certus")
st.write("Use the sidebar to open **Markets**.")
st.info("If the Markets page doesn’t appear, ensure the file exists at pages/01_Markets.py")