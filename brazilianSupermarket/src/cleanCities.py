from dbConnect import connection

def cleanCities():
    print("here")
    conn = connection()
    cur = conn.cursor()
    cur.execute("Delete from city")
    conn.commit()
    cur.close()