# 🚀 Omnichannel Marketing Data Warehouse (dbt + BigQuery)

An end-to-end Data Warehouse solution designed to analyze Omnichannel Marketing performance and provide RAG-ready data for AI Agents.

## 🏗️ Architecture & Data Lineage
The project follows the **Medallion Architecture** implemented within Google BigQuery:
- **Raw Layer:** As-is transactional data from CRM, TikTok Ads, and Meta Ads.
- **Staging Layer:** Data cleaning, explicit type casting, and schema standardization.
- **Intermediate Layer:** Complex business logic execution (SCD Type 2 for Creator history using Window Functions).
- **Mart Layer:** Optimized Star Schema (Fact/Dim) designed for BI tools and LLM consumption.

## 🌟 Technical Highlights (Optimizer Mindset)
- **Cost Optimization:** Implemented `Incremental Materialization` with `Partitioning` & `Clustering` on BigQuery, reducing data scan volume by up to 90%.
- **Dimension Modeling (SCD Type 2):** Managed Creator lifecycle and follower history using `LEAD()` window functions to ensure accurate point-in-time reporting.
- **Data Quality Assurance:** Integrated 50+ automated data tests (`unique`, `not_null`, `accepted_values`, `relationships`) to maintain referential integrity and a "Single Source of Truth."
- **AI-Native Engineering:** Developed a specialized `dim_creator_knowledge_base` that flattens metrics and internal reviews into structured Text Blobs, ready for Vector Embedding and RAG (Retrieval-Augmented Generation) applications.

## 🛠️ Tech Stack
- **Data Warehouse:** Google BigQuery
- **Transformation:** dbt Core (CLI)
- **CI/CD & Version Control:** GitHub
- **Visualization:** Looker Studio
- **Language:** SQL (Jinja), Python (vEnv)

## 📊 Dashboard
[[Omnichannel Marketing Dashboard](https://datastudio.google.com/s/rmVsMyUfYgI)]
