import streamlit as st

def show_property_card(property):

    st.markdown(
        f"""
        <div style="
            border:1px solid #ddd;
            border-radius:10px;
            padding:15px;
            margin-bottom:15px;
            background-color:#fafafa;
        ">
        
        <h4>🏠 {property['bedrooms']} BHK Apartment</h4>
        
        <b>📍 Location:</b> {property['location']}, {property['city']} <br>
        <b>💰 Price:</b> ₹{int(property['price_total']):,} <br>
        <b>📐 Area:</b> {property['area_sqft']} sqft <br>
        <b>🏗 Age:</b> {property['property_age']} <br>
        
        </div>
        """,
        unsafe_allow_html=True
    )