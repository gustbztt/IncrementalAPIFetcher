import pyodbc

# Conecta ao Dremio
driver = "insert your driver connector"
host = 'insert the ip of the host'
port = 'insert the port'
uid = 'user_id'
pwd = 'password'

def connect_to_dremio(driver, host, port, uid, pwd):
    # essa função serve pra conectar ao dremio através dos parâmetros desejados
    cnxn = pyodbc.connect("Driver={};ConnectionType=Direct;HOST={};PORT={};AuthenticationType=Plain;UID={};PWD={}".format(driver,host,port,uid,pwd),autocommit=True,UseEncryption=False)
    return cnxn

# usando a função para se conectar ao drêmio
cnxn = connect_to_dremio(driver, host, port, uid, pwd)
