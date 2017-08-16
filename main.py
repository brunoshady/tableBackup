import psycopg2
import time

print "backupTabela [versao 1.07082017]"
print "(c) 2017 Bruno Baehr\n"
print "Este aplicativo cria uma tabela de backup com"
print "os registros recuperados da tabela corrompida.\n"

def main():
    host = raw_input("Server [localhost]:")
    if host == "":
        host = 'localhost'
    dbname = raw_input("Database [autosystem]:")
    if dbname == "":
        dbname = 'autosystem'
    port = raw_input("Port [5432]:")
    if port == "":
        port = 5432
    user = raw_input("Username [postgres]:")
    if user == "":
        user = 'postgres'
    password = raw_input("Senha [postgres]:")
    if password == "":
        password = 'postgres' 
    table = raw_input("Tabela Corrompida [movto]:")
    if table == "":
        table = 'movto'
    backuptable = raw_input("Tabela Backup [%s_%s_bkp]:" % (table,time.strftime("%d%m%Y_%H%M")))
    if backuptable == "":
        backuptable = table + '_' + time.strftime("%d%m%Y_%H%M") + '_bkp'

    print "Conectando em '%s'..." % host
    errors = []
    success = []
    try:
        with psycopg2.connect("host=%s port=%s dbname=%s user=%s password=%s" % (host,port,dbname,user,password)) as conn:
            print "Conectado."
            with conn.cursor() as cur:
                print "Criando tabela '%s'..." % backuptable
                cur.execute("SELECT * INTO %s FROM %s LIMIT 0" % (backuptable, table))
                conn.commit()
                print "Tabela criada."

                startTime = time.time()

                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'" % table)

                colunas = []
                rows = cur.fetchall()
                for row in rows:
                    for coluna in row:
                        colunas.append(coluna)

                print "Inserindo registros..."
                
                count = 0
                counterrors = 0
                
                while True:
                    count = count + 1
                    try:
                        cur.execute("SELECT ctid, * FROM %s LIMIT 1 OFFSET %i" % (table,count))
                        rows = cur.fetchall()
                        
                        valores = []
                        if cur.rowcount <> 0:
                            for row in rows:                    
                                for valor in row:
                                    if valor <> row[0]:
                                        if valor is None:
                                            valores.append('NULL')
                                        elif type(valor) not in (int,float,long):
                                            valores.append("'" + str(valor) + "'")
                                        else:
                                            valores.append(valor)

                            try:
                                cur.execute("INSERT INTO %s (%s) VALUES (%s)" % (backuptable, ",".join(colunas), ",".join([str(x) for x in valores])))
                                conn.commit()
                                success.append(row[0])
                            except (Exception, psycopg2.DatabaseError) as error:
                                cur.execute("ROLLBACK")
                                print "Erro ao inserir o registro de ctid %s." % row[0]
                                print error

                            valores = []
                        else:
                            print "caiu no break"
                            break
                    except:
                        counterrors = counterrors + 1
                        if counterrors >= 101:
                            break
                        pass
  
        print "Finalizado em %d minutos." % ((time.time() - startTime)/60)
        print "Registros inseridos: " + str(len(success))
        for error in errors:
            print "ctid " + error
        retorno = raw_input("\nDeseja executar novamente? [s/n]:" )
        if (retorno == 's') or (retorno =='S'):
            print "\n"
            main()    
        
        
    except (Exception, psycopg2.DatabaseError) as error:
        print "\n" + str(error)
        main()

if __name__ == '__main__':
    main()
        
