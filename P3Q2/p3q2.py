import oracledb
import pandas as pd
import matplotlib.pyplot as plt

# 1. Connect to Oracle
conn = oracledb.connect(
    user="ora_avarikko",
    password="a79093555",
    dsn="dbhost.students.cs.ubc.ca:1522/stu"
)

# 2. Run query and load into DataFrame
query = """
SELECT 
    c.country_name,
    c.population,
    COUNT(p.show_id) AS num_productions,
    ROUND(AVG(p.vote_average), 3) AS avg_rating,
    CASE 
        WHEN AVG(p.vote_average) >= 8 THEN 'High'
        WHEN AVG(p.vote_average) >= 6 THEN 'Medium'
        ELSE 'Low'
    END AS rating_category
FROM productions p
JOIN production_countries pc ON p.show_id = pc.show_id
JOIN countries c ON pc.country_name = c.country_name
GROUP BY c.country_name, c.population
ORDER BY c.population DESC
"""

df = pd.read_sql(query, conn)

# 3. Visualize — scatter plot: population vs avg rating
fig, ax = plt.subplots(figsize=(10, 6))
scatter = ax.scatter(df['POPULATION'], df['AVG_RATING'], s=df['NUM_PRODUCTIONS'] * 2, alpha=0.6)
ax.set_xscale('log')
ax.set_xlabel('Population (log scale)')
ax.set_ylabel('Average Vote Rating')
ax.set_title('Country Population vs Average Production Rating')

# Label some interesting points
for _, row in df.iterrows():
    # if row['NUM_PRODUCTIONS'] > 50 or row['AVG_RATING'] > 7.8:
        ax.annotate(row['COUNTRY_NAME'], (row['POPULATION'], row['AVG_RATING']), fontsize=6)

ax.legend(title='Bubble size - No. of Productions', loc='lower right', framealpha=0.9)

plt.tight_layout()
plt.savefig('population_vs_rating.png')
plt.show()



query = """
SELECT 
    c.country_name,
    c.population,
    COUNT(p.show_id) AS num_productions,
    SUM(p.vote_count) AS total_votes,
    ROUND(SUM(p.vote_count) / (c.population / 1000000), 3) AS votes_per_million,
    ROUND(AVG(p.vote_average), 3) AS avg_rating
FROM productions p
JOIN production_countries pc ON p.show_id = pc.show_id
JOIN countries c ON pc.country_name = c.country_name
GROUP BY c.country_name, c.population
ORDER BY votes_per_million DESC
"""

df = pd.read_sql(query, conn)

fig, ax = plt.subplots(figsize=(12, 6))
scatter = ax.scatter(df['POPULATION'], df['VOTES_PER_MILLION'], s=df['NUM_PRODUCTIONS'] * 2, alpha=0.6)
ax.set_xscale('log')
ax.set_xlabel('Population (log scale)')
ax.set_ylabel('Votes per Million People')
ax.set_title('Country Population vs Normalized Engagement (Votes per Capita)')

for _, row in df.iterrows():
    ax.annotate(row['COUNTRY_NAME'], (row['POPULATION'], row['VOTES_PER_MILLION']),
                fontsize=6, zorder=5, xytext=(5, 5), textcoords='offset points')

ax.legend(title='Bubble size - Number of productions', loc='upper right', framealpha=0.9)

plt.tight_layout()
plt.savefig('normalized_engagement.png')
plt.show()