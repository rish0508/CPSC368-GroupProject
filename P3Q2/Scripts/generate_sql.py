"""
CPSC 368 Phase 3 - SQL Generation Script
Reads clean CSVs and produces a .sql file with:
  - DROP TABLE statements (reverse dependency order)
  - CREATE TABLE statements with PK, FK, NOT NULL constraints
  - INSERT statements for all data
  - COMMIT
"""

import pandas as pd
import math

# ============================================================
# Load clean CSVs
# ============================================================
productions = pd.read_csv('/Users/afifmv/Documents/Classes/2025/2025W2/CPSC 368/Project/Clean Datasets/clean_productions.csv')
production_countries = pd.read_csv('/Users/afifmv/Documents/Classes/2025/2025W2/CPSC 368/Project/Clean Datasets/clean_production_countries.csv')
countries = pd.read_csv('/Users/afifmv/Documents/Classes/2025/2025W2/CPSC 368/Project/Clean Datasets/clean_countries.csv')

def escape_sql(val):
    """Escape single quotes for Oracle SQL strings."""
    if pd.isna(val):
        return "NULL"
    return str(val).replace("'", "''")

# ============================================================
# Build the .sql file
# ============================================================
lines = []

# -- DROP TABLES (reverse dependency: junction first, then parents)
lines.append("-- Drop tables in reverse dependency order")
lines.append("DROP TABLE production_countries CASCADE CONSTRAINTS;")
lines.append("DROP TABLE productions CASCADE CONSTRAINTS;")
lines.append("DROP TABLE countries CASCADE CONSTRAINTS;")
lines.append("PURGE RECYCLEBIN;")
lines.append("")

# -- CREATE TABLE: countries
lines.append("-- Countries table (World Bank population data)")
lines.append("""CREATE TABLE countries (
    country_name    VARCHAR2(100)   PRIMARY KEY,
    country_code    VARCHAR2(10)    NOT NULL,
    population      NUMBER(15)      NOT NULL,
    region          VARCHAR2(50)    NOT NULL,
    income_group    VARCHAR2(30)    NOT NULL,
    CONSTRAINT uq_country_code UNIQUE (country_code)
);""")
lines.append("")

# -- CREATE TABLE: productions
lines.append("-- Productions table (Netflix movies and TV shows)")
lines.append("""CREATE TABLE productions (
    show_id         NUMBER(10)      PRIMARY KEY,
    title           VARCHAR2(200)   NOT NULL,
    type            VARCHAR2(20)    NOT NULL,
    release_year    NUMBER(4)       NOT NULL,
    language        VARCHAR2(10)    NOT NULL,
    genres          VARCHAR2(200)   NOT NULL,
    popularity      NUMBER(12,3)    NOT NULL,
    vote_count      NUMBER(10)      NOT NULL,
    vote_average    NUMBER(5,3)     NOT NULL,
    CONSTRAINT chk_type CHECK (type IN ('Movie', 'TV Show'))
);""")
lines.append("")

# -- CREATE TABLE: production_countries (junction table)
lines.append("-- Junction table linking productions to producing countries")
lines.append("""CREATE TABLE production_countries (
    show_id         NUMBER(10)      NOT NULL,
    country_name    VARCHAR2(100)   NOT NULL,
    PRIMARY KEY (show_id, country_name),
    FOREIGN KEY (show_id) REFERENCES productions(show_id),
    FOREIGN KEY (country_name) REFERENCES countries(country_name)
);""")
lines.append("")

# ============================================================
# INSERT statements: countries
# ============================================================
lines.append("-- Insert countries")
for _, row in countries.iterrows():
    name = escape_sql(row['country_name'])
    code = escape_sql(row['country_code'])
    pop = int(row['population'])
    region = escape_sql(row['region'])
    income = escape_sql(row['income_group'])
    lines.append(
        f"INSERT INTO countries VALUES ('{name}', '{code}', {pop}, '{region}', '{income}');"
    )
lines.append("")

# ============================================================
# INSERT statements: productions
# ============================================================
lines.append("-- Insert productions")
for _, row in productions.iterrows():
    sid = int(row['show_id'])
    title = escape_sql(row['title'])
    ptype = escape_sql(row['type'])
    year = int(row['release_year'])
    lang = escape_sql(row['language'])
    genres = escape_sql(row['genres'])
    pop = row['popularity']
    vc = int(row['vote_count'])
    va = row['vote_average']
    lines.append(
        f"INSERT INTO productions VALUES ({sid}, '{title}', '{ptype}', {year}, "
        f"'{lang}', '{genres}', {pop}, {vc}, {va});"
    )
lines.append("")

# ============================================================
# INSERT statements: production_countries
# ============================================================
lines.append("-- Insert production_countries")
for _, row in production_countries.iterrows():
    sid = int(row['show_id'])
    name = escape_sql(row['country_name'])
    lines.append(
        f"INSERT INTO production_countries VALUES ({sid}, '{name}');"
    )
lines.append("")

# -- COMMIT
lines.append("COMMIT;")

# ============================================================
# Write to file
# ============================================================
sql_path = './phase3_setup.sql'
with open(sql_path, 'w') as f:
    f.write('\n'.join(lines))

print(f"Generated {sql_path}")
print(f"  Total lines: {len(lines)}")
print(f"  Countries INSERTs: {len(countries)}")
print(f"  Productions INSERTs: {len(productions)}")
print(f"  Production_countries INSERTs: {len(production_countries)}")
