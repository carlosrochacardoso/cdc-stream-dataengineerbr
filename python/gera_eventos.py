import mariadb
import sys
import time
import datetime
import argparse
from faker import Faker

parser = argparse.ArgumentParser(description = "Gera N eventos de insert update e delete para db crm, tabela contacts, em uma instância MariaDB")
parser.add_argument("-n", "--num",
                    help="Número de transações geradas para cada comando (insert, update, select)",
                    default=5, dest="n")

opts = parser.parse_args(sys.argv[1:])

print("Gerando eventos para a tabela de contatos...")

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user="root",
        password="example",
        host="127.0.0.1",
        port=3306,
        database="crm"

    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()

#Init Faker
fake = Faker('pt_BR')

try:
    n = int(opts.n)
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)

for i in range(n):
    c = fake.simple_profile() 
    try: 
        cur.execute("INSERT INTO contacts (username,name,sex,address,mail,birthdate) VALUES (?, ?, ?, ?, ?, ?)", (c['username'],c['name'],c['sex'],c['address'],c['mail'],c['birthdate'])) 
    except mariadb.Error as e: 
        print(f"Error: {e}")
        sys.exit(1)
    
    time.sleep(3)

try: 
    cur.execute("UPDATE contacts SET lastupdate = ?", (datetime.date.today(),)) 
    cur.execute("DELETE FROM contacts") 
except mariadb.Error as e: 
    print(f"Error: {e}")
    sys.exit(1)

print("Eventos gerados com sucesso!")