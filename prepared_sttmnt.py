# Creating connection to MySQL
import mysql.connector as mysql

# Import DB Info
from db_connect import MY_HOST, MY_USER, MY_PASS

def main():
  db = mysql.connect(host=MY_HOST, user=MY_USER, password=MY_PASS, database='mydb')

  # prepared statement to sanitize and avoid sql injection
  cur = db.cursor(prepared=True)

  cur.execute('DROP TABLE IF EXISTS temp')
  cur.execute('CREATE TABLE IF NOT EXISTS temp (a TEXT, b TEXT, c TEXT)')
  cur.execute("INSERT INTO temp VALUES ('one', 'two', 'three')")
  cur.execute("INSERT INTO temp VALUES ('four', 'five', 'six')")
  cur.execute("INSERT INTO temp VALUES ('seven', 'eight', 'nine')")

  cur.execute("SELECT * FROM temp")
  for row in cur:
    print(row)

  # use "?" is called bind variable => essentially template placeholder for value to be used in query
  query = "SELECT * FROM temp WHERE a = ?"
  # query with tuple parameter
  cur.execute(query, ('four',)) 

  for row in cur:
    print(f"result is {row}")

  cur.execute("DROP TABLE IF EXISTS temp")
  cur.close()
  db.close()


if __name__ == '__main__':
  main()
