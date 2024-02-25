import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
db_name = "/home/arteweyl/estudo/aulas/GDP/world_economies.db"
csv_path = "/home/arteweyl/estudo/aulas/GDP/countries_by_GDP.csv"


def extract(url, table_attribs):
    page_html = requests.get(url).text
    page_data = BeautifulSoup(page_html, "html.parser")
    tables = page_data.find_all("tbody")
    rows = tables[2].find_all("tr")
    df = pd.DataFrame(columns=table_attribs)

    for row in rows[3:]:
        cols = row.find_all("td")
        row_data = {
            table_attribs[0]: cols[0].contents[2].string,
            table_attribs[1]: cols[2].contents[0],
        }
        df1 = pd.DataFrame(row_data, index=[0])
        df = pd.concat([df, df1], ignore_index=True)

    return df


def transform(df):

    # clean the data
    df1 = df[df[df.columns[1]] != "—"][df.columns[1]].str.replace(",", "").astype(float)
    mean = df1.median()
    df[df.columns[1]] = (
        df[df.columns[1]].str.replace("—", f"{mean}").str.replace(",", "")
    )

    # Turn to Billion
    df.columns = ["Country", "GDP_in_Billion"]
    df[df.columns[1]] = df[df.columns[1]].astype(float)
    df[df.columns[1]] = (df[df.columns[1]] / 1000).round(2)

    return df


def load(df, csv_path, db_name):
    df.to_csv(csv_path)

    conn = sqlite3.connect(db_name)
    df.to_sql("Countries_by_GDP", conn, if_exists="replace", index=False)
    conn.close()


def run_query(query, db_name):
    conn = sqlite3.connect(db_name)
    query = pd.read_sql(query, conn)
    conn.close()
    print(query)


def log_progress(message):
    print(message)


df = extract(url, ["Country", "GDP_in_Millions"])
log_progress("Done with Extraction")
transform(df)
log_progress("Done with Transformation")
load(df, csv_path, db_name)
log_progress("Done with Load")
run_query("SELECT * from Countries_by_GDP WHERE GDP_in_Billion >=19373.59", db_name)
log_progress("done with query")
