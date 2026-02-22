
# AI-Powered Snowflake SQL Agent with Automated ELT Pipeline

This project implements an end-to-end automated ELT pipeline in Snowflake integrated with an AI-powered SQL agent that enables users to query data using natural language. The system ingests semi-structured JSON data, performs incremental transformations using Snowflake Streams and Tasks, and allows users to retrieve insights without writing SQL manually.

The project demonstrates modern data engineering practices combined with Generative AI to improve data accessibility, automate workflows, and accelerate analytics.

---

# Architecture Overview

Data flows from Snowflake Stage into raw tables, then incrementally processes through Streams and Tasks into final analytical tables. The AI agent connects to Snowflake and generates SQL queries based on user input.

Pipeline flow:

AWS S3 / Snowflake Stage → Raw JSON Table → Stream → Merge Task → Final Table → Analysis Table → AI SQL Agent → User Insights

---

# Key Features

Automated ingestion of semi-structured JSON data into Snowflake raw tables.

Implemented incremental data processing using Snowflake Streams and Tasks to ensure efficient and reliable updates.

Built automated merge pipelines to upsert new and updated records into analytical tables.

Developed an AI-powered SQL agent using Python and Groq LLM to convert natural language into Snowflake SQL queries.

Enabled automatic query execution and result retrieval without manual SQL writing.

Implemented conversational memory, allowing contextual follow-up questions.

Improved data accessibility and reduced manual data retrieval effort significantly.

---

# Technologies Used

Snowflake for data warehousing and pipeline orchestration

Python for automation and AI agent development

Snowflake Streams and Tasks for incremental processing

AWS S3 and Snowflake Stage for data ingestion

Groq LLM for natural language to SQL generation

Snowflake Connector for Python for database connectivity

SQL for transformations and analytics

Tabulate library for formatted output display

---

# Project Structure

```
ai-snowflake-sql-agent/

├── sql/
│   ├── snowflake_queries.sql
│
├── src/
│   └── ai_sql_agent.py
│
├── requirements.txt
└── README.md
```

---

# How It Works

Step 1: JSON data is ingested into Snowflake raw table using COPY INTO from Stage.

Step 2: Snowflake Stream tracks incremental changes in the raw table.

Step 3: Snowflake Task automatically merges changes into the final table.

Step 4: Analytical tables are created for business insights.

Step 5: The AI agent accepts natural language input from the user.

Step 6: Groq LLM converts natural language into optimized Snowflake SQL queries.

Step 7: Queries are executed automatically and results are displayed.

---

# Example Usage

User input:

Show companies registered after 2020

AI generates SQL:

SELECT *
FROM KE_DATA
WHERE REGISTRATION_DATE > '2020-01-01';

Results are fetched and displayed automatically.

---

# Business Impact

Automated ELT pipeline handling large-scale data efficiently

Reduced manual SQL query effort by over 70 percent

Enabled near real-time data availability using incremental processing

Improved productivity by enabling non-technical users to access data using natural language

Demonstrated integration of Generative AI with modern data engineering workflows

---

# Setup Instructions

Install dependencies:

```
pip install -r requirements.txt
```

Configure Snowflake credentials in the Python script:

```
user
password
account
warehouse
database
schema
```

Run the AI agent:

```
python src/ai_sql_agent.py
```

---

# Skills Demonstrated

Snowflake

Data Engineering

ELT Pipeline Development

Snowflake Streams and Tasks

Python Automation

SQL Optimization

Generative AI Integration

Natural Language Processing Applications

Cloud Data Pipelines

---

# Author

Manoj Korukonda
Data Analyst | Snowflake | Python | SQL | AWS | Generative AI | Power BI | MongoDB

---

