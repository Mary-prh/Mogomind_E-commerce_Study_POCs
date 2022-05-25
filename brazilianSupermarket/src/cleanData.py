from dbConnect import connection

def cleanData():
    print("here")
    conn = connection()
    cur = conn.cursor()
    cur.execute("Delete from sales_category")
    conn.commit()
    cur.close()