import requests
import json
import pandas as pd
import pyodbc
import time

# configurations:
CONN = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=(localdb)\MSSQLLocalDB;'
                      'Database=mydb;'
                      'username=localhost;'
                      'TrustedConnection=yes;')


def process_dataframe(counter):
    url = f'https://reqres.in/api/users?page={counter}'
    try:
        # HTTP GET
        request = requests.get(url)

        data = json.loads(request.text)

        # Convert json string into DataFrame
        dataframe = pd.DataFrame(data["data"])

        # Do simple data transformations
        dataframe["name"] = dataframe["first_name"] + ' ' + dataframe["last_name"]
        concat_df = dataframe.loc[:, ~dataframe.columns.isin(["first_name", "last_name"])]

    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

    return concat_df


def create_table(conn):
    # Initiate cursor and execute statement
    cursor = conn.cursor()
    cursor.execute("""  DROP TABLE [mydb].[dbo].[employees];
                        CREATE TABLE [mydb].[dbo].[employees]
                        (
                        id integer,
                        email varchar(max),
                        name varchar(max),
                        avatar varchar(max)
                        )
                        """)
    conn.commit()


def insert_data(conn, dataframe):
    # Insert data into table
    for row, value in dataframe.iterrows():
        cursor = conn.cursor()
        cursor.execute("""INSERT INTO  [mydb].[dbo].[employees] (id, email, name, avatar)
                           values (?,?,?,?)""",
                       value.id, value.email, value["name"], value.avatar)
        conn.commit()


def main():
    dataframes = []
    # main loop to extract data from HTTP GET twice, append the DFs into a list
    # and concat the items from the list
    for count in range(1, 3):
        result = process_dataframe(count)
        dataframes.append(result)
    union_df = pd.concat(dataframes)
    create_table(CONN)
    insert_data(CONN, union_df)


if __name__ == '__main__':
    main()
