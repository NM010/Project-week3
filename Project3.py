from sqlalchemy import create_engine
import pandas as pd
import mysql.connector
conn = mysql.connector.connect(user="root", password="inter", host="127.0.0.1", database="ecommerce")
cursor = conn.cursor()

db_connection_str = 'mysql+pymysql://root:inter@127.0.0.1/ecommerce'
db_connection = create_engine(db_connection_str)

prodotto = pd.read_sql("prodotto", db_connection)
pagamento = pd.read_sql("pagamento", db_connection)
indirizzo = pd.read_sql("indirizzo", db_connection)
livello = pd.read_sql("livello", db_connection)
ordine = pd.read_sql("ordine", db_connection)
prezzo = pd.read_sql("prezzo", db_connection)
utente = pd.read_sql("utente", db_connection)
spedizione = pd.read_sql("spedizione", db_connection)


def query_constructor(select, frm, join1=None, join2=None, where=None, groupby=None, orderby=None):
    stmt = 'select %s from %s ' % (select, frm)
    if join1 is not None:
        stmt = stmt + 'join %s ' % join1
    if join2 is not None:
        stmt = stmt + 'join %s ' % join2
    if where is not None:
        stmt = stmt + 'where %s ' % where
    if groupby is not None:
        stmt = stmt + 'group by %s ' % groupby
    if orderby is not None:
        stmt = stmt + 'order by %s' % orderby
    stmt = stmt + ';'
    return stmt


def dataframe(qsql):
    q = pd.read_sql(qsql, db_connection)
    return q


#Rimanenze di magazzino per riordino prodotti
qsql_1 = query_constructor("nome", "prodotto", None, None, "quantita = 0")
cursor.execute(qsql_1)
print(cursor.fetchall())
q_pandas1 = dataframe(qsql_1)
print(q_pandas1)


#Prodotti che costano tra 1 e 10 per attività promozionale (JOIN)
qsql_2 = query_constructor("nome", "prodotto", "prezzo on prodotto.pid = prezzo.pid", None, "prezzo.valore > 20 and prezzo.valore < 90")
cursor.execute(qsql_2)
print(cursor.fetchall())
q_pandas2 = dataframe(qsql_2)
print(q_pandas2)



#Tutte le marche in vendita sull'ecommerce
qsql_3 = query_constructor("nome", "marca")
cursor.execute(qsql_3)
print(cursor.fetchall())
q_pandas3 = dataframe(qsql_3)
print(q_pandas3)



#Dettaglio spedizione prodotti in stato spediti (JOIN)
qsql_4 = query_constructor("time as dett_prod_sped", "ordine", "stato on ordine.stid = stato.stid", None, "ordine.stid =2")
cursor.execute(qsql_4)
print(cursor.fetchall())
q_pandas4 = dataframe(qsql_4)
print(q_pandas4)



#Analisi statistica sugli articoli (AVG-MAX-MIN)
qsql_5 = query_constructor("avg(valore), max(valore), min(valore)", "prezzo")
cursor.execute(qsql_5)
print(cursor.fetchall())
q_pandas5 = dataframe(qsql_5)
print(q_pandas5)



#Ordino gli utenti per CAP (ORDER BY)
qsql_6 = query_constructor("cap,citta, nome, cognome", "indirizzo", None, None, "cap")
cursor.execute(qsql_6)
print(cursor.fetchall())
q_pandas6 = dataframe(qsql_6)
print(q_pandas6)


#Quanti prodotti ci sono disponibili in listino (COUNT)
qsql_7 = query_constructor("count(pid)", "prodotto", None, None, "pid = 4000001")
cursor.execute(qsql_7)
print(cursor.fetchall())
q_pandas7 = dataframe(qsql_7)
print(q_pandas7)


#User che non hanno aperto la newsletter
qsql_8 = query_constructor("email, user, nome, cognome", "utente", None, None, "newsletter = 0")
cursor.execute(qsql_8)
print(cursor.fetchall())
q_pandas8 = dataframe(qsql_8)
print(q_pandas8)


#Cerco valori nulli p.iva per il CRM (IS NULL)
qsql_9 = query_constructor("nome, cognome, piva", "utente", None, None, "piva IS NULL")
cursor.execute(qsql_9)
print(cursor.fetchall())
q_pandas9 = dataframe(qsql_9)
print(q_pandas9)


#Lista podotti con cid maggiore-uguale a 70
qsql_10 = query_constructor("pid, nome", "prodotto", None, None, "cid >= 70")
cursor.execute(qsql_10)
print(cursor.fetchall())
q_pandas10 = dataframe(qsql_10)
print(q_pandas10)





