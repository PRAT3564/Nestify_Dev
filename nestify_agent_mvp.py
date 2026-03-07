import os
import re
from groq import Groq
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
load_dotenv() 

# -----------------------------
# 1️ Configure Groq API
# -----------------------------
client = Groq(api_key=os.getenv("GROQ_API"))

MODEL = os.getenv("GROQ_MODEL")


# -----------------------------
# 2️ MySQL Database Connection
# -----------------------------
db_connection = f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

engine = create_engine(db_connection)


# -----------------------------
# 3️ Database Schema Prompt
# -----------------------------
SCHEMA = """
Table: unified_properties

Columns:
property_id INT
city VARCHAR
location VARCHAR
bedrooms INT
bathrooms INT
area_sqft FLOAT
price_total FLOAT
furnish VARCHAR
property_age VARCHAR
amenities TEXT
features TEXT
listing_source VARCHAR
"""


# -----------------------------
# 4️ LLM Helper
# -----------------------------
def ask_llm(prompt):

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": "You are a real estate AI assistant."},
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )

    return response.choices[0].message.content

def clean_sql(sql):

    sql = re.sub(r"```sql", "", sql)
    sql = re.sub(r"```", "", sql)

    return sql.strip()


# -----------------------------
# 5️ Convert User Query → SQL
# -----------------------------
def generate_sql(user_query):

    prompt = f"""
You are a real estate database assistant.

Convert the user request into a SQL query.

Database schema:
{SCHEMA}

Rules:
- Use MySQL syntax
- Only return SQL
- Table name is unified_properties
- Limit results to 5

User request:
{user_query}
"""

    sql_query = clean_sql(ask_llm(prompt)).strip()

    return sql_query


# -----------------------------
# 6️ Execute SQL
# -----------------------------
def run_sql(query):

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    return df


# -----------------------------
# 7️ Format Results for User
# -----------------------------
def format_response(user_query, df):

    if df.empty:
        return "No properties found matching your request."

    prompt = f"""
User query:
{user_query}

Property results:
{df.to_string()}

Explain the results like a helpful real estate agent.
"""

    return ask_llm(prompt)


# -----------------------------
# 8️ Nestify Agent Pipeline
# -----------------------------
def nestify_agent(user_query):

    print("\nUser:", user_query)

    sql = generate_sql(user_query)

    print("\nGenerated SQL:", sql)

    results = run_sql(sql)

    answer = format_response(user_query, results)

    return answer, results


# -----------------------------
# 9️ Chat Loop
# -----------------------------
if __name__ == "__main__":

    while True:

        query = input("\nAsk Nestify: ")

        if query.lower() in ["exit", "quit"]:
            break

        response = nestify_agent(query)

        print("\nNestify:", response)