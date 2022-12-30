import mariadb
import credentials

def connect():
  try:
    conn = mariadb.connect(
      user=credentials.dbUser,
      password=credentials.dbPassword,
      host=credentials.dbHost,
      port=credentials.dbPort,
      database=credentials.dbName
    )
  except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)
  return conn

def selectAllStatic(query):
  conn = connect()
  cur = conn.cursor(buffered=True)
  cur.execute(query)
  results = cur.fetchall()
  cur.close()
  conn.close()
  return results

def selectAll(query, params):
  conn = connect()
  cur = conn.cursor(buffered=True)
  cur.execute(query, params)
  results = cur.fetchall()
  cur.close()
  conn.close()
  return results

def selectOne(query, params):
  conn = connect()
  cur = conn.cursor(buffered=True)
  cur.execute(query, params)
  result = cur.fetchone()
  cur.close()
  conn.close()
  return result

def insertMany(query, params):
  conn = connect()
  cur = conn.cursor(buffered=True)
  cur.executemany(query, params)
  conn.commit()
  cur.close()
  conn.close()