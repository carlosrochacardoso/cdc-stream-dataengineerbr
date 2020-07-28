import mariadb
import sys
import time
import datetime
import argparse
from faker import Faker

parser = argparse.ArgumentParser(description = "Gera N eventos de insert update e delete para db crm, tabela contacts, em uma instância MariaDB", conflict_handler="resolve")
parser.add_argument("-n", "--num", help="Número de contatos que serão inseridos para gerar eventos no MariaDB", default=5, dest="n")
parser.add_argument("-h", "--host", dest="host", help="Endereço de rede para o MariaDB", default="localhost")
parser.add_argument("-P", "--port", dest="port", help="Porta de conexão com o MariaDB", default="3306")
parser.add_argument("-u", "--user", dest="user", help="Usuário do MariaDB", default="")
parser.add_argument("-p", "--password", dest="password", help="Password do MariaDB", default="")
parser.add_argument("-d", "--db", dest="database", help="Nome do db", default="")

opts = parser.parse_args(sys.argv[1:])

try:
    n = int(opts.n)
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=opts.user,
        password=opts.password,
        host=opts.host,
        port=int(opts.port),
        database=opts.database
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()

#Init Faker
fake = Faker('pt_BR')

print("Gerando eventos para a tabela de contatos...")

for i in range(n):
    c = fake.simple_profile() 
    try: 
        cur.execute("INSERT INTO contacts (username,name,sex,address,mail,birthdate) VALUES (?, ?, ?, ?, ?, ?)", (c['username'],c['name'],c['sex'],c['address'],c['mail'],c['birthdate'])) 
    except mariadb.Error as e: 
        print(f"Error: {e}")
        sys.exit(1)
    
    time.sleep(2)

try: 
    cur.execute("UPDATE contacts SET lastupdate = NOW()") 
    cur.execute("DELETE FROM contacts") 
except mariadb.Error as e: 
    print(f"Error: {e}")
    sys.exit(1)

print("Eventos gerados com sucesso!")