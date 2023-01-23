import mariadb
import pymysql
import credentials
from sqlalchemy import create_engine
import sys
import logging

def connect():
  try:
    conn = mariadb.connect(
      user=credentials.dbUser,
      password=credentials.dbPassword,
      host=credentials.dbHost,
      port=credentials.dbPort,
      database=credentials.dbName
    )
  except pymysql.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)
  cur = conn.cursor(buffered=True)
  return cur, conn

def disconnect(cur, conn):
  logging.debug(cur.statement)
  cur.close()
  conn.close()

def selectAllStatic(query):
  cur, conn = connect()
  cur.execute(query)
  results = cur.fetchall()
  disconnect(cur, conn)
  return results

def selectAll(query, params):
  cur, conn = connect()
  cur.execute(query, params)
  results = cur.fetchall()
  disconnect(cur, conn)
  return results

def selectOneStatic(query):
  cur, conn = connect()
  cur.execute(query)
  result = cur.fetchone()
  disconnect(cur, conn)
  return result

def selectOne(query, params):
  cur, conn = connect()
  cur.execute(query, params)
  result = cur.fetchone()
  disconnect(cur, conn)
  return result

def insertMany(query, params):
  cur, conn = connect()
  cur.executemany(query, params)
  conn.commit()
  disconnect(cur, conn)

def getEngine():
  return create_engine(f"mysql+pymysql://{credentials.dbUser}:{credentials.dbPassword}@{credentials.dbHost}:{credentials.dbPort}/{credentials.dbName}")