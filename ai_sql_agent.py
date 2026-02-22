import re
import snowflake.connector
from groq import Groq
from tabulate import tabulate

# ==============================
# CONFIG
# ==============================

GROQ_API_KEY = ""
MODEL_NAME = "meta-llama/llama-4-maverick-17b-128e-instruct"

SNOWFLAKE_CONFIG = {
    "user": "",
    "password": "",
    "account": "",
    "warehouse": "",
    "database": "",
    "schema": ""
}

# ==============================
# INIT
# ==============================

client = Groq(api_key=GROQ_API_KEY)

conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cursor = conn.cursor()

active_table = None
table_schema = None

# Rolling conversational memory
query_memory = []
memory_limit = 3  # default (will ask user)

# ==============================
# UTIL FUNCTIONS
# ==============================

def get_table_schema(table):
    cursor.execute(f"DESC TABLE {table}")
    rows = cursor.fetchall()
    return "\n".join([f"{r[0]} ({r[1]})" for r in rows])


def extract_sql(text):
    text = text.replace("```sql", "").replace("```", "").strip()

    match = re.search(r"(SELECT[\s\S]+?;)", text, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    match = re.search(r"(SELECT[\s\S]+)", text, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    return None


def call_llm(messages):
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
        temperature=0
    )
    return response.choices[0].message.content


def build_memory_context():
    global query_memory

    if not query_memory:
        return ""

    context = "\nPrevious Query History:\n"

    for idx, item in enumerate(query_memory):
        context += f"""
Query {idx+1}:
User Question: {item['question']}
SQL Used: {item['sql']}
Result Preview:
{item['result']}
"""

    return context


def generate_sql(user_question):
    global table_schema, active_table

    memory_context = build_memory_context()

    system_prompt = f"""
You are a Snowflake SQL generator.

STRICT RULES:
- Use ONLY this table: {active_table}
- Only generate SELECT queries
- No explanations
- No markdown
- Use exact column names from schema
- Add LIMIT 50 unless user specifies limit
- Use previous query history if user refers to previous result

Table Schema:
{table_schema}

{memory_context}
"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_question}
    ]

    response = call_llm(messages)
    return extract_sql(response)


def store_in_memory(question, sql, rows, columns):
    global query_memory, memory_limit

    preview = tabulate(rows[:5], headers=columns, tablefmt="grid")

    query_memory.append({
        "question": question,
        "sql": sql,
        "result": preview
    })

    # Maintain rolling memory
    if len(query_memory) > memory_limit:
        query_memory.pop(0)


def execute_sql(sql, user_question):
    global query_memory

    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]

        if rows:
            print("\nQuery Results:\n")
            print(tabulate(rows, headers=columns, tablefmt="grid"))

            # Store this query in rolling memory
            store_in_memory(user_question, sql, rows, columns)

        else:
            print("\nQuery executed successfully. No rows returned.")

        return True, None

    except Exception as e:
        print("\nSQL Error:", e)
        return False, str(e)


def fix_sql(sql, error_message):
    global table_schema, active_table

    fix_prompt = f"""
The following SQL caused an error:

SQL:
{sql}

Snowflake Error:
{error_message}

Fix the SQL.
Rules:
- Only SELECT query
- Use table {active_table}
- Use correct column names
- No explanation
- No markdown

Schema:
{table_schema}
"""

    messages = [{"role": "system", "content": fix_prompt}]
    response = call_llm(messages)
    return extract_sql(response)


# ==============================
# MAIN LOOP
# ==============================

print("\nAI SQL Agent Started.")
print("Type 'exit' anytime to stop.\n")

# Ask memory size
try:
    memory_limit = int(input("How many last query results to store? "))
except:
    memory_limit = 3

running = True

while running:

    country = input("\nEnter country code (2 letters) or type 'exit': ").strip().upper()

    if country.lower() == "exit":
        break

    if not re.match(r"^[A-Z]{2}$", country):
        print("Invalid country code.\n")
        continue

    active_table = f"{country}_DATA"

    try:
        table_schema = get_table_schema(active_table)
    except Exception:
        print(f"Table {active_table} not found.\n")
        continue

    print(f"\nCountry set to: {active_table}")

    while True:

        question = input("\nWhat would you like to query? (or type 'change' / 'exit'): ").strip()

        if question.lower() == "exit":
            running = False
            break

        if question.lower() == "change":
            break

        sql = generate_sql(question)

        if not sql:
            print("Could not generate SQL.")
            continue

        print("\nGenerated SQL:\n", sql)

        success, error_msg = execute_sql(sql, question)

        if not success:
            corrected_sql = fix_sql(sql, error_msg)

            if corrected_sql:
                print("\nCorrected SQL:\n", corrected_sql)
                execute_sql(corrected_sql, question)
            else:
                print("Could not auto-fix SQL.")

        print("\nDone.")

    if not running:
        break


# ==============================
# CLEAN SHUTDOWN
# ==============================

print("\nShutting down agent...")
cursor.close()
conn.close()

query_memory.clear()
active_table = None
table_schema = None

print("Memory cleared. Agent stopped.")
