import psycopg2 

def connection():
    return  psycopg2.connect(
        host="localhost",
        database="E-commerce",
        user="postgres",
        password="123456")