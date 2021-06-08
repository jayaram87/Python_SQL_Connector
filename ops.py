from log import Logger
import os
import mysql.connector as connection
import csv

class Db:

    def __init__(self, host, user, pwd, db, table):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.table = table

    def __connection(self):
        try:
            conn = connection.connect(host=self.host, user=self.user, passwd=self.pwd, database=self.db, auth_plugin='mysql_native_password' ,use_pure=True)
        except:
            try:
                conn = connection.connect(host=self.host, user=self.user, passwd=self.pwd, auth_plugin='mysql_native_password', use_pure=True)
                query = 'SHOW DATABASES;'
                cursor = conn.cursor()
                cursor.execute(query)
                if self.db not in [i[0] for i in cursor.fetchall()]:
                    query = 'CREATE DATABASE {}'.format(self.db)
                    cursor = conn.cursor()
                    cursor.execute(query)
                    conn.close()
                    print(f'{self.db} created')
                else:
                    print("Unable to create a db and connect")
                conn = connection.connect(host=self.host, user=self.user, passwd=self.pwd, auth_plugin='mysql_native_password', use_pure=True)
            except Exception as e:
                Logger('log1.log').logger('ERROR', e)
                print('unable to connect')
        finally:
            if conn.is_connected():
                Logger('log1.log').logger('INFO', 'connection created')
                return conn


    def __clean_query(self, inputs, querytype):
        if querytype == 'CREATE TABLE':
            if 'id' not in inputs.keys():
                inputs['id'] = 'INT(10) auto_increment PRIMARY KEY'
            elif 'int' in inputs['id'].lower():
                inputs['id'] = 'INT(10) auto_increment PRIMARY KEY'
            for k, v in inputs.items():
                if 'varchar' == v.lower():
                    inputs[k] = 'VARCHAR(20)'
                elif 'int' == v.lower():
                    inputs[k] = 'INT(10)'
            return inputs
        elif querytype == 'INSERT ROW':
            cols, values = '', ''
            for k, v in inputs.items():
                if type(v) == int or type(v) == float:
                    cols += ' ,'+k
                    values += " ,"+str(v)
                else:
                    cols += ' ,' + k
                    values += " ,'" + v + "'"
            return cols, values
        elif querytype == 'UPDATE ROW':
            sets, wheres = '', ''
            for k,v in inputs.items():
                if k == 'set':
                    for k1, v1 in v.items():
                        if type(v1) == int or type(v1) == float:
                            sets += " ,"+k1+"="+str(v1)
                        else:
                            sets += " ," + k1 + "=" + "'" + v1 + "'"
                elif k == 'where':
                    for k1, v1 in v.items():
                        if type(v1) == int or type(v1) == float:
                            wheres += " ,"+k1+"="+str(v1)
                        else:
                            wheres += " ," + k1 + "=" + "'" + v1 + "'"
            return sets, wheres


    def create_table(self, attributes):
        """
        :param attributes: dictionary object, format: {'col_name':'value type'} eg:{'id': 'INT', 'firstname': 'varchar'}
        :return: creates a sql table and logs it
        """
        conn = self.__connection()
        cursor = conn.cursor()
        cursor.execute('USE {}'.format(self.db))
        cursor = conn.cursor()
        cursor.execute('SHOW TABLES;')
        if self.table not in [i[0] for i in cursor.fetchall()]:
            try:
                cursor = conn.cursor()
                query = self.__clean_query(attributes, 'CREATE TABLE')
                vals = ', '.join([k+' '+v for k,v in attributes.items()])
                query = f'CREATE TABLE {self.table}'+'({})'.format(vals)
                cursor.execute(query)
                cursor.close()
            except Exception as e:
                Logger('log1.log').logger('ERROR', e)
                print('incorrect sql query')
        else:
            Logger('log1.log').logger('INFO', 'table {a} already exists in {b}'.format(a=self.table, b=self.db))


    def insert_row(self, attributes):
        """
        :param attributes: column names and values in a key value pair {firstname: jay} needs
        to match the schema of the table, id is auto incremented
        :return: inserts a row into a table
        """
        conn = self.__connection()
        cursor = conn.cursor()
        cursor.execute(f'USE {self.db}')
        """cursor.execute(f'SHOW TABLES')
        if self.table not in [i[0] for i in cursor.fetchall()]:
            cols = {}
            for i in attributes:
                if type(attributes[i]) == int or type(attributes[i]) == float:
                    cols[i] = 'INT'
                else:
                    cols[i] = 'VARCHAR'
            print(cols)
            self.create_table(cols)"""
        try:
            cursor = conn.cursor()
            """cursor.execute(f'DESCRIBE {self.table}')
            if attributes.keys() is [i[0] for i in cursor.fetchall()]:
                cursor = conn.cursor()
                cols, values = self.__clean_query(attributes)
                print(cols)
                print(values)
            """
            cols, values = self.__clean_query(attributes, 'INSERT ROW')
            cols = '('+cols[2:]+')'
            values = '('+values[2:]+')'
            query = "INSERT INTO "+self.table+" "+cols+" values"+values
            cursor.execute(query)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            Logger('log1.log').logger('ERROR', e)
            print("please check the query format")


    def update_row(self, attributes):
        """
        :param attributes: dict obj format {'set':{'attr':'value'}, 'where': {'attr': 'value'}}
        :return: update the table and commits to the db
        """
        conn = self.__connection()
        cursor = conn.cursor()
        cursor.execute(f'USE {self.db}')
        try:
            cursor = conn.cursor()
            sets, wheres = self.__clean_query(attributes, 'UPDATE ROW')
            sets = sets[2:]
            wheres = wheres[2:]
            query = "UPDATE "+self.table+" SET "+sets+" WHERE "+wheres
            cursor.execute(query)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            Logger('log1.log').logger('ERROR', e)
            print("please check the query format")


    def delete_row(self, attributes):
        """
        :param attributes: dict obj format {'attr' = value}
        :return: deletes the row and commits to the db
        """
        conn = self.__connection()
        cursor = conn.cursor()
        cursor.execute(f'USE {self.db}')
        try:
            cursor = conn.cursor()
            if type(list(attributes.values())[0]) == int or type(list(attributes.values())[0]) == float:
                val = str(list(attributes.keys())[0])+"="+str(list(attributes.values())[0])
            else:
                val = str(list(attributes.keys())[0]) + "='" + str(list(attributes.values())[0]) + "'"
            query = "DELETE FROM "+self.table+' WHERE '+val
            cursor.execute(query)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            Logger('log1.log').logger('ERROR', e)
            print('delete query format incorrect')


    def bulk_insert(self, filename):
        """
        :param filename: filename with extension(cvs), file should have just the values with , seperated. with col names
        :return: bulk inserts from the file and commits to the db
        """
        conn = self.__connection()
        cursor = conn.cursor()
        cursor.execute(f'USE {self.db}')
        try:
            cursor = conn.cursor()
            with open(os.path.join(os.getcwd(),filename), 'r+') as f:
                header = next(f)
                header = ', '.join([j.replace('"','') for j in header.split(',')])[:-1]
                csvdata =csv.reader(f, delimiter='\n')
                for z, line in enumerate(csvdata):
                    for i in line:
                        query = [int(j) if type(j) == int else j.replace('"','') for j in i.split(',')]
                        query = ','.join([str('"')+j+str('"') if not j.isdigit() else j for j in query])
                        cursor.execute("INSERT INTO {} ({}) values ({})".format(self.table, header, query))
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            Logger('log1.log').logger('ERROR', e)
            print('query format incorrect')


    def download_table(self, filename):
        conn = self.__connection()
        cursor = conn.cursor()
        cursor.execute(f'USE {self.db}')
        try:
            cursor = conn.cursor()
            cursor.execute(f'Describe {self.table}')
            cols = cursor.fetchall()
            cols = [i for i in zip(*cols)][0]
            with open(filename, 'w+', newline='') as f:
                cursor = conn.cursor()
                reader = csv.writer(f)
                reader.writerow(cols)
                cursor.execute(f'SELECT * from {self.table}')
                data = cursor.fetchall()
                for row in data:
                    reader.writerow([r for r in row])
            conn.close()
        except Exception as e:
            Logger('log1.log').logger('ERROR', e)


