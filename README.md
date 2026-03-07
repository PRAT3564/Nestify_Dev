# 🏠 Nestify AI – Intelligent Real Estate Search Agent

Nestify AI is an **AI-powered real estate search assistant** that allows users to find properties using **natural language queries**.

Instead of using traditional filters, users can simply type queries like:

> "Find 2BHK apartments in Mumbai under ₹80L"

Nestify converts the request into **database queries using an LLM**, retrieves relevant property data, and displays the results through an interactive UI.

⚡ **Current Status:**  
Nestify currently has a **working MVP (Minimum Viable Product)**.  
Many new features and improvements are planned in upcoming updates.

---

# 🚀 Features (MVP)

### 🧠 Natural Language Property Search
Users can search for properties using conversational queries.

Example:

```text
Find 2BHK in Mumbai South West under 88L
```

---

### 🤖 AI Query Understanding
The AI model converts user queries into SQL queries using the property database schema.

Example:

User Query:

```text
Find 2BHK in Gurgaon under 36000000
```

Generated SQL:

```sql
SELECT *
FROM unified_properties
WHERE city = 'Gurgaon'
AND bedrooms = 2
AND price_total <= 36000000
LIMIT 5;
```

---

### 🏠 Property Card UI
Search results are displayed in a clean property card layout including:

- 📍 Location
- 💰 Price
- 📐 Area
- 🛏 Bedrooms
- 🛋 Furnishing
- 🏗 Property Age

---

### ⚡ Fast AI Inference
Nestify uses **Groq LLM API** for fast responses.

---

# 🏗 MVP Architecture

```
User
 ↓
Nestify UI (Streamlit)
 ↓
Nestify AI Agent
 ↓
Groq LLM (Query → SQL)
 ↓
MySQL Database
 ↓
Property Results
 ↓
Groq LLM (Results → Natural Language)
 ↓
Property Cards UI
```

---

# 🧰 MVP Tech Stack

| Layer | Technology |
|------|-------------|
| Frontend UI | Streamlit |
| AI Model | Groq LLM (Llama 3.3) |
| Backend Logic | Python |
| Database | MySQL |
| ORM | SQLAlchemy |
| Data Processing | Pandas |

---

# 📂 Project Structure

```
Nestify/
│
├── nestify_agent_mvp.py     # Core AI agent pipeline
├── nestify_ui.py            # Streamlit UI
├── property_card.py         # Property card component
└── README.md
```

---

# ⚙️ Installation

### 1️⃣ Clone the Repository

```
git clone https://github.com/PRAT3564/Nestify_Dev.git
```

---

### 3️⃣ Configure Groq API

Add your API key in the agent file or as an environment variable:

```python
client = Groq(api_key="YOUR_GROQ_API_KEY")
```

---

### 4️⃣ Configure MySQL Database

Update the database connection string:

```python
DB_URI = f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
```

---

### 5️⃣ Run the Application

Start the UI:

```
streamlit run nestify_ui.py
```

Open in browser:

```
http://localhost:8501
```

---

# 🗄 Database Schema

Example table used by Nestify:

### Table: `unified_properties`

| Column | Type |
|------|------|
| property_id | INT |
| city | VARCHAR |
| location | VARCHAR |
| bedrooms | INT |
| bathrooms | INT |
| area_sqft | FLOAT |
| price_total | FLOAT |
| furnish | VARCHAR |
| property_age | VARCHAR |
| amenities | TEXT |
| features | TEXT |
| listing_source | VARCHAR |

---

# 💡 Example Queries

Users can ask:

```
Find 3BHK in Gurgaon
```

```
Show 2BHK flat, society name and location in Kolkata
```

Nestify will:

1. Understand the request  
2. Generate SQL  
3. Query the database  
4. Display results in property cards  

---

# ⚠️ Current Limitations (MVP)

Since this is an MVP:

- SQL is generated directly by the LLM
- Limited property dataset
- No authentication system
- No property images yet
- No map-based search

---

# 🔮 Upcoming Features

Planned upgrades for Nestify:

### 🗺 Back-End API Call
Integration with **SPRING BOOT Backend to avoid direct interaction with DB**

### 🧠 Vector Semantic Search
Allow queries like:

```
3BHK homes near tech parks with schools AND Metro nearby
```

### 📊 AI Property Ranking
Ranking properties based on:

- price value
- amenities
- area growth
- demand

### 🏙 Neighborhood Intelligence
Provide insights such as:

- investment score
- safety rating
- nearby schools
- commute times

### 📱 Advanced UI
Future UI improvements:

- property images
- map view
- filters
- recommendation engine

---

# 🤝 Contributing

Contributions are welcome.

Steps:

1. Fork the repository  
2. Create a feature branch  
3. Submit a pull request  

# 👨‍💻 Author

**Nestify AI**

An experimental AI-powered real estate search assistant currently in **MVP stage**, with many exciting features coming soon.
