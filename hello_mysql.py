# Creating connection to MySQL
import mysql.connector as mysql

# Import DB Info
from db_connect import MY_HOST, MY_USER, MY_PASS

def main():
  print("MySQL Example")

  # Define db and cursor object
  db = None # To satisfy the warning monster
  cur = None 

  # Use try and catch to check MySQL connection
  try:
    db = mysql.connect(host=MY_HOST, user=MY_USER, password=MY_PASS, database='mydb')

    # prepared statement to sanitize and avoid sql injection
    cur = db.cursor(prepared=True)

    print("Connected")

  except mysql.Error as err:
    print(f"Could not connect to MySQL: {err}")
    exit(1)

  try:

    cur.execute('DROP TABLE IF EXISTS hello')

    # Create a table
    sql_create = '''
      CREATE TABLE IF NOT EXISTS hello (
        id SERIAL PRIMARY KEY,
        a VARCHAR(16),
        b VARCHAR(16),
        c VARCHAR(16)
      )
    '''

    cur.execute(sql_create)
    print("table created")

  except mysql.Error as err:
    print(f"Could not create a table: {err}")
    exit(1)

  try:
    # insert rows into table using executemany
    row_data = (
      ('one', 'two', 'three'),
      ('four', 'five', 'six'),
      ('seven', 'eight', 'nine')
    )

    print("inserting rows")
    # use "?" is called bind variable => essentially template placeholder for value to be used in query
    cur.executemany("INSERT INTO hello (a, b, c) VALUES (?, ?, ?)", row_data)
    count = cur.rowcount
    cur.executemany("INSERT INTO hello (a, b, c) VALUES (?, ?, ?)", row_data)
    count = cur.rowcount
    cur.executemany("INSERT INTO hello (a, b, c) VALUES (?, ?, ?)", row_data)
    count = cur.rowcount
    print(f"{count} rows added")

    db.commit()

  except mysql.Error as err:
    print(f"Could not add rows: {err}")
    exit(1)

  try:
    # count rows using SELECT COUNT(*)
    cur.execute("SELECT COUNT(*) FROM hello")
    count = cur.fetchone()[0]
    print(f"there are {count} rows in the table")

    # get column names by selecting one row
    cur.execute("SELECT * FROM hello LIMIT 1")
    cur.fetchall()
    colnames = cur.column_names
    print(f"column names are: {colnames}")

    # fetch rows using iterator
    print('\nusing iterator')
    cur.execute("SELECT * FROM hello LIMIT 5")
    for row in cur:
        print(row)

    # fetch rows using dictionary workaround
    print('\ndictionary workaround')
    cur.execute("SELECT * FROM hello LIMIT 5")
    for row in cur:
        # use zip function to zip together tuple of columns name and rows
        rd = dict(zip(colnames, row))
        # print as tuple or dictionary
        print(f"as tuple: {tuple(row)}, as dict: id:{rd['id']} a:{rd['a']}, b:{rd['b']}, c:{rd['c']}")

    # fetch rows in groups of 5 using fetchmany
    print('\ngroups of 5 using fetchmany')
    cur.execute("SELECT * FROM hello")
    rows = cur.fetchmany(5)
    while rows:
      for r in rows:
        print(r)
      print("====== ====== ======")
      rows = cur.fetchmany(5)

    # drop table and close connection
    print('\ndrop table and close connection')
    cur.execute("DROP TABLE IF EXISTS hello")
    cur.close()
    db.close()

  except mysql.Error as err:
    print(f"mysql error ({err})")
    exit(1)


if __name__ == '__main__':
  main()
