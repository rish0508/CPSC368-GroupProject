"""
CPSC 368 Phase 3 - Data Cleaning Script
Cleans Netflix movies/TV shows and World Bank population data,
producing three clean CSVs ready for Oracle insertion.
"""

import pandas as pd

# ============================================================
# 1. LOAD RAW DATA
# ============================================================
movies = pd.read_csv('/Users/afifmv/Documents/Classes/2025/2025W2/CPSC 368/Project/Datasets/archive-2/netflix_movies_detailed_up_to_2025.csv')

shows = pd.read_csv('/Users/afifmv/Documents/Classes/2025/2025W2/CPSC 368/Project/Datasets/archive-2/netflix_tv_shows_detailed_up_to_2025.csv')

pop_raw = pd.read_csv('/Users/afifmv/Documents/Classes/2025/2025W2/CPSC 368/Project/Datasets/API_SP.POP.TOTL_DS2_en_csv_v2_61/API_SP.POP.TOTL_DS2_en_csv_v2_61.csv', skiprows=4)

meta = pd.read_csv('/Users/afifmv/Documents/Classes/2025/2025W2/CPSC 368/Project/Datasets/API_SP.POP.TOTL_DS2_en_csv_v2_61/Metadata_Country_API_SP.POP.TOTL_DS2_en_csv_v2_61.csv', encoding='utf-8-sig')

print(f"Raw movies: {len(movies)}, Raw shows: {len(shows)}")

# ============================================================
# 2. NETFLIX COUNTRY NAME -> WORLD BANK NAME MAPPING
# ============================================================
# Maps Netflix country names to World Bank "Country Name" values.
# Countries not in this dict already match exactly.
COUNTRY_NAME_MAP = {
    'Bahamas': 'Bahamas, The',
    'Congo': 'Congo, Rep.',
    "Cote D'Ivoire": "Cote d'Ivoire",
    'Czech Republic': 'Czechia',
    'Egypt': 'Egypt, Arab Rep.',
    'Hong Kong': 'Hong Kong SAR, China',
    'Iran': 'Iran, Islamic Rep.',
    'Macao': 'Macao SAR, China',
    'Macedonia': 'North Macedonia',
    'Russia': 'Russian Federation',
    'Slovakia': 'Slovak Republic',
    'South Korea': 'Korea, Rep.',
    'Turkey': 'Turkiye',
    'United States of America': 'United States',
    'Venezuela': 'Venezuela, RB',
    'Vietnam': 'Viet Nam',
}

# These Netflix territories have no World Bank population data — will be dropped
DROPPED_TERRITORIES = {
    'Faeroe Islands', 'Guadaloupe', 'Netherlands Antilles',
    'Palestinian Territory', 'Puerto Rico', 'St. Pierre and Miquelon',
    'Taiwan', 'US Virgin Islands', 'United States Minor Outlying Islands',
    "Lao People's Democratic Republic",  # small production count
}

# ============================================================
# 3. COMBINE AND FILTER NETFLIX DATA
# ============================================================
# Keep only rows with country info and at least 1 vote
combined = pd.concat([movies, shows], ignore_index=True)
combined = combined[combined['country'].notna() & (combined['vote_count'] > 0)].copy()
print(f"After removing nulls/zero votes: {len(combined)}")

# Select top 1200 by vote_count for Oracle quota compliance
combined = combined.nlargest(950, 'vote_count').copy()
print(f"After top-1200 filter: {len(combined)}")

# ============================================================
# 4. CLEAN PRODUCTIONS TABLE
# ============================================================
# Select and rename columns (keeping under 10 columns)
productions = combined[[
    'show_id', 'title', 'type', 'release_year',
    'language', 'genres', 'popularity', 'vote_count', 'vote_average'
]].copy()

# Standardize type values
productions['type'] = productions['type'].str.strip()

# Standardize language codes to lowercase
productions['language'] = productions['language'].str.strip().str.lower()

# Clean genres: take just the first genre to keep it simple, or keep as-is
# Keeping full genre string for richer queries
productions['genres'] = productions['genres'].fillna('Unknown')

# Ensure no duplicate show_ids
productions = productions.drop_duplicates(subset='show_id')
print(f"Productions table: {len(productions)} rows, {len(productions.columns)} columns")

# ============================================================
# 5. BUILD PRODUCTION_COUNTRIES JUNCTION TABLE
# ============================================================
rows = []
for _, row in combined.iterrows():
    countries = [c.strip() for c in row['country'].split(',')]
    for country in countries:
        # Skip territories without population data
        if country in DROPPED_TERRITORIES:
            continue
        # Map to World Bank name
        standardized = COUNTRY_NAME_MAP.get(country, country)
        rows.append({
            'show_id': row['show_id'],
            'country_name': standardized
        })

production_countries = pd.DataFrame(rows).drop_duplicates()
print(f"Production_countries table: {len(production_countries)} rows")

# ============================================================
# 6. BUILD COUNTRIES TABLE
# ============================================================
# Get the unique countries we actually need
needed_countries = set(production_countries['country_name'].unique())

# Filter World Bank data to actual countries (not aggregates)
actual_codes = meta[meta['Region'].notna()]['Country Code'].tolist()
pop_actual = pop_raw[pop_raw['Country Code'].isin(actual_codes)].copy()

# Use 2023 population (most recent complete year)
countries = pop_actual[pop_actual['Country Name'].isin(needed_countries)][[
    'Country Name', 'Country Code', '2023'
]].copy()
countries.columns = ['country_name', 'country_code', 'population']

# Add region and income group from metadata
meta_clean = meta[['Country Code', 'Region', 'IncomeGroup']].copy()
meta_clean.columns = ['country_code', 'region', 'income_group']
countries = countries.merge(meta_clean, on='country_code', how='left')

# Check for missing matches
matched_countries = set(countries['country_name'].unique())
missing = needed_countries - matched_countries
if missing:
    print(f"WARNING - Countries in productions but not in population data: {missing}")

# Drop any production_countries rows that reference missing countries
production_countries = production_countries[
    production_countries['country_name'].isin(matched_countries)
]

print(f"Countries table: {len(countries)} rows, {len(countries.columns)} columns")

# ============================================================
# 7. FINAL VALIDATION
# ============================================================
print("\n=== FINAL TABLE SIZES ===")
print(f"  productions:          {len(productions)} rows x {len(productions.columns)} cols")
print(f"  production_countries: {len(production_countries)} rows x {len(production_countries.columns)} cols")
print(f"  countries:            {len(countries)} rows x {len(countries.columns)} cols")

# Referential integrity check
prod_ids = set(productions['show_id'])
junc_ids = set(production_countries['show_id'])
country_names = set(countries['country_name'])
junc_countries = set(production_countries['country_name'])

print(f"\n=== REFERENTIAL INTEGRITY ===")
print(f"  Junction show_ids not in productions: {len(junc_ids - prod_ids)}")
print(f"  Junction countries not in countries table: {len(junc_countries - country_names)}")

# ============================================================
# 8. EXPORT CLEAN CSVs
# ============================================================
output_dir = './data'
productions.to_csv(f'{output_dir}/clean_productions.csv', index=False)
production_countries.to_csv(f'{output_dir}/clean_production_countries.csv', index=False)
countries.to_csv(f'{output_dir}/clean_countries.csv', index=False)

print(f"\nClean CSVs written to {output_dir}/")
print("  - clean_productions.csv")
print("  - clean_production_countries.csv")
print("  - clean_countries.csv")

# Quick preview
print("\n=== SAMPLE: productions ===")
print(productions.head(3).to_string())
print("\n=== SAMPLE: production_countries ===")
print(production_countries.head(6).to_string())
print("\n=== SAMPLE: countries ===")
print(countries.head(5).to_string())
