import streamlit as st
from nestify_agent_mvp import nestify_agent
from property_card import show_property_card

st.set_page_config(
    page_title="Nestify AI",
    page_icon="🏠",
    layout="wide"
)

st.title("🏠 Nestify AI Real Estate Assistant")

st.write("Find your dream property using AI")

# Chat input
query = st.text_input("Ask Nestify about properties")

if st.button("Search"):

    if query:

        with st.spinner("Searching properties..."):

            response, df = nestify_agent(query)

        st.subheader("🤖 Nestify AI")

        st.write(response)

        st.subheader("🏠 Available Properties")

        if df.empty:

            st.warning("No properties found")

        else:

            for _, row in df.iterrows():

                property = row.to_dict()

                show_property_card(property)