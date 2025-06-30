# -*- coding: utf-8 -*-

import csv
import time
import MySQLdb as Database
import ipaddress

# http://mysql-python.sourceforge.net/MySQLdb.html#connection-objects
# https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlconnection-ping.html
class dbapp():
    '''
    mysql handler django db

    
        import MySQLdb as Database
        conn = self.get_db_connection(dbset='mng')
        with conn.cursor(Database.cursors.DictCursor) as cursor:
            cursor.execute(rq, prms)
            # if get_last_id:
            if not iid:
                iid = cursor.lastrowid
        conn.commit()
        conn.close()

        
        # https://docs.djangoproject.com/en/dev/topics/db/multi-db/
        def get_db_connection(self, dbset):
            import MySQLdb as Database
            dbname = settings.DATABASES[dbset]['NAME']
            user = settings.DATABASES[dbset]['USER']
            passwd = settings.DATABASES[dbset]['PASSWORD']
            host = settings.DATABASES[dbset]['HOST']
            port = settings.DATABASES[dbset]['PORT']
            init_command = settings.DATABASES[dbset]['init_command']
            conn = Database.connect(
                user=user,
                passwd=passwd,
                host=host,
                db=dbname,
                port=int(port),
                init_command=init_command,
                # use_unicode=self.use_unicode,
                charset='utf8',
            )
            return conn
    '''

    dbname = None
    user = None
    passwd = None
    host = None
    port = None
    init_command = None
    use_unicode = None
    set_character_set = None
    DictCursor = None
    cursor = ''
    db = ''  # same as cursor for compatibility (deprecated from 2025)

    def __init__(self, dbset='web', DictCursor=False, settings=None, alt=False):
        '''
        dbname: web|scrap
        DictCursor=True - to give col names with result
        alt=False|True - if True -> SET FOREIGN_KEY_CHECKS = 0;
        '''
        # if not settings:
        #     try: from .settings import settings
        #     except: pass
        #     try: from settings import settings
        #     except: pass
        if settings:
            self.dbname = settings.DATABASES[dbset]['NAME']
            self.user = settings.DATABASES[dbset]['USER']
            self.passwd = settings.DATABASES[dbset]['PASSWORD']
            self.host = settings.DATABASES[dbset]['HOST']
            self.port = settings.DATABASES[dbset]['PORT']
            self.init_command = settings.DATABASES[dbset]['init_command']
            if alt:
                self.init_command = settings.DATABASES[dbset]['init_command_alt']
            self.use_unicode = settings.DATABASES[dbset]['use_unicode']
            self.set_character_set = settings.DATABASES[dbset]['set_character_set']
            self.DictCursor = DictCursor
            self.open()
            self.close()
            # self.conn = Database.connect(user=self.user, passwd=self.passwd, host=self.host, db=self.dbname, port=int(self.port), init_command=self.init_command, use_unicode=self.use_unicode)
            # self.conn.set_character_set(self.set_character_set)
            # if DictCursor:
            #     self.cursor = self.conn.cursor(Database.cursors.DictCursor)
            # else:
            #     self.cursor = self.conn.cursor()


    def get_connection(self):
        '''
        check connection & connect if possible using some approches
        '''

        # check if connection is opened
        if self.conn.open:
            # then check if connected
            i = 0
            num = 30
            while i < num:
                i += 1
                try:
                    self.conn.ping(True)
                    return True
                except:
                    time.sleep(10)
                    continue

            return False
        
        # try to open
        else:
            i = 0
            num = 30
            while i < num:
                i += 1
                try:
                    self.open()
                except:
                    time.sleep(10)
                    continue
                if self.conn.open:
                    return True
            return False

    def open(self):
        i = 0
        num = 100
        while i < num:
            i += 1
            try:
                self.conn = Database.connect(user=self.user, passwd=self.passwd, host=self.host, db=self.dbname, port=int(self.port), init_command=self.init_command, use_unicode=self.use_unicode)
                break
            except:
                time.sleep(10)
                continue

            return False

        self.conn.set_character_set(self.set_character_set)

        if self.DictCursor:
            self.cursor = self.conn.cursor(Database.cursors.DictCursor)
        else:
            self.cursor = self.conn.cursor()
        self.db = self.cursor

    def close(self, logger=None):

        # if not self.conn or not self.conn.open:
        if not self.conn:
            return False

        # if not self.cursor:
        #     return False

        try:
            self.cursor.close()
            self.conn.close()
            return True

        except Exception as e:
            if logger: logger.error(f"sql connection closing err: {e}")
        
        return False

    def close0(self):
        if not self.get_connection(): return False
        if not self.cursor: return False
        self.conn.close()

    def commit(self, close=True):
        if not self.get_connection(): return False
        if not self.cursor: return False
        if not self.conn: return False
        if not self.cursor.connection.open: return False
        self.conn.commit()
        if close:
            # self.conn.close()
            self.close()

    # def commit0(self, close=True):
    #     if not self.get_connection(): return False
    #     if not self.cursor: return False
    #     self.conn.commit()
    #     if close:
    #         # self.conn.close()
    #         self.close()

    def check_db(self, create=None, logger=None):
        '''
        used only in lns project
        make table if it is not exist yet: contents
        status:   0 - not scraped, 1 - scraped
        '''

        if not self.get_connection(): return False
        if not self.cursor: return False
        if create:
            self.cursor.execute(create)
        else:
            self.cursor.execute('CREATE TABLE IF NOT EXISTS queue (id INT(11) AUTO_INCREMENT PRIMARY KEY, url_id INT(11) NOT NULL, url VARCHAR(255), company_id INT(11) NOT NULL, menu_type INT(11), url_type INT(11), open_hours VARCHAR(64), lng VARCHAR(3), status BOOLEAN NOT NULL DEFAULT 0)')
        self.commit()
        # self.close()

    def raw(self, raw, prms=None, commit=True):
        '''
        raw sql request
        '''
        if not raw: return False
        if not self.get_connection(): return False
        if not self.cursor: return False
        if not self.cursor.connection.open: return False
        res = None
        if prms: self.cursor.execute(raw, prms)
        else: self.cursor.execute(raw)
        if 'SELECT' in raw:
            res = self.cursor.fetchall()
            self.close()
            commit = False
        elif 'INSERT' in raw or 'UPDATE' in raw:
            res = self.cursor.lastrowid

        if commit:
            self.commit()
            # self.close()
        if res: return res
        else: return ()

    def put_multi_fast(self, table, cols, rows, duplicates=None, commit=True):
        '''
            # example:
            cols = ('col1', 'col2') or ('col1',)  # only tuple
            rows = [(a1 , b1),(a2 , b2),(a3 , b3)]
            duplicates = "name=name, ticker_id=ticker_id"  # they should be made at the table creation (name_ticker_id)
            put_multi_fast('table',('col1', 'col2'), rows)
        '''
        
        if not self.get_connection(): return False
        if not self.cursor: return False

        # '(%s, %s)'
        if type(rows[0]) == str:
            patt = '(%s)'
        else:
            patts = ['%s' for _ in rows[0]]
            patt = '(' + ','.join(patts) + ')'

        rq = 'INSERT INTO %s (%s) VALUES %s' % (
            table,
            ','.join(cols),
            ','.join(patt for _ in rows)
        )
        if duplicates:
            rq += ' ON DUPLICATE KEY UPDATE ' + duplicates + ';'

        if type(rows[0]) == str:
            values = [_ for _ in rows]
        else:
            values = [_ for r in rows for _ in r]

        self.cursor.execute(rq, values)
        if commit:
            self.commit()
            # self.close()

    def put_multi(self, table_name, columns, rows, commit=True):
        '''
        insert list into table in db with single condition, where:
            rows - list of tuple [(1,'abc'), (2,'qwe') ...]
            in order of columns in db
            columns = []
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        cols = self.get_cols_multi(columns)
        for r in rows:
            self.cursor.execute('INSERT INTO ' + table_name + ' (' + ', '.join(columns) + ') VALUES (' + cols + ')', tuple(r))  # json.dumps(rows)
        if commit:
            self.commit()
            # self.close()

    def put_list(self, table_name, rows, check_field=None, opt=False, commit=True):
        '''
        insert list into table in db with single condition, where:
            rows - list of dictionaries [{'column': 'value', ...}, ...]
            check_field - field in which checking (if the value is already exist), 
                        - as string
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        col, col_values = self.get_cols(rows)
        existed_values = []
        if check_field:
            if type(check_field) == list:
                check_field_str = ','.join(check_field)
            else:
                check_field_str = check_field
            self.cursor.execute('SELECT ' + check_field_str + ' FROM ' + table_name)
            existed_values = self.cursor.fetchall()
        for row in rows:
            col, col_values = self.get_cols([row])
            if not opt: 
                row_select = {}
                if type(check_field) == list:
                    for ch_f in check_field:
                        row_select_key = ch_f
                        row_select_value = row[ch_f]
                        row_select[row_select_key] = row_select_value
                else:
                    row_select = {check_field: row[check_field]}

                if check_field and row_select in existed_values: continue
                self.cursor.execute('INSERT INTO ' + table_name + ' ('+col+') VALUES ('+col_values+')', tuple(row.values()))
            elif opt == 'update':
                try: self.cursor.execute('INSERT INTO ' + table_name + ' ('+col+') VALUES (' + col_values + ')', tuple(row.values()))
                except:
                    if check_field:
                        row_select = {}
                        if type(check_field) == list:
                            for ch_f in check_field:
                                row_select_key = ch_f
                                row_select_value = row[ch_f]
                                row_select[row_select_key] = row_select_value
                        else:
                            row_select_key = check_field
                            row_select_value = row[check_field]
                            row_select = {row_select_key: row_select_value}
                    else:
                        row_select = row
                    col_name_cond, col_value_cond = self.get_cols_for_select([row_select])
                    self.cursor.execute('SELECT id FROM ' + table_name + ' WHERE ' + col_name_cond, col_value_cond)
                    rid = self.cursor.fetchall()
                    if rid:
                        rid = rid[0]['id']
                        self.update_single(table_name, row, {'id': rid})
                    else:
                        if commit:
                            self.close()
                        return False

            elif opt == 'ignore': self.cursor.execute('INSERT INTO ' + table_name + ' ('+col+') VALUES ('+col_values+') ON DUPLICATE KEY IGNORE', tuple(row.values()))

        if commit:
            self.commit()
            # self.close()

    def put_single(self, table_name, row, check_field=None, opt=False, logger=None, commit=True):
        '''
        insert row into table in db, where:
            row is dictionary {'column': 'value', ...}
            check_field is a field with which compare if it is already there
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        rid = None
        try:
            col, col_values = self.get_cols([row])
            if check_field:
                self.cursor.execute('SELECT ' + check_field + ' FROM ' + table_name)
                existed_values = self.cursor.fetchall()            
            if not opt:
                if check_field and {check_field: row[check_field]} in existed_values:
                    row_select_key = check_field
                    row_select_value = row[check_field]
                    row_select = {row_select_key: row_select_value}
                    col_name_cond, col_value_cond = self.get_cols_for_select([row_select])
                    self.cursor.execute('SELECT id FROM ' + table_name + ' WHERE ' + col_name_cond, col_value_cond)
                    rid = self.cursor.fetchall()
                    if rid:
                        self.close()
                        return rid[0]['id']
                    else:
                        self.close()
                        return False
                self.cursor.execute('INSERT INTO ' + table_name + ' ('+col+') VALUES (' + col_values + ')', tuple(row.values()))
            
            elif opt == 'update':
                try: self.cursor.execute('INSERT INTO ' + table_name + ' ('+col+') VALUES (' + col_values + ')', tuple(row.values()))
                except:
                    row_select_key = check_field
                    row_select_value = row[check_field]
                    row_select = {row_select_key: row_select_value}
                    col_name_cond, col_value_cond = self.get_cols_for_select([row_select])
                    self.cursor.execute('SELECT id FROM ' + table_name + ' WHERE ' + col_name_cond, col_value_cond)
                    rid = self.cursor.fetchall()
                    if rid:
                        rid = rid[0]['id']
                        self.update_single(table_name, row, {'id': rid})
                    else:
                        if commit:
                            self.close()
                        return False
            
            elif opt == 'ignore': self.cursor.execute('INSERT INTO ' + table_name + ' ('+col+') VALUES (' + col_values + ') ON DUPLICATE KEY IGNORE ', tuple(row.values()))
            
            if not rid: rid = self.cursor.lastrowid
            if commit:
                self.commit()

        except Exception as e:
            if logger: logger.error(f"sql err: {e} \n{str(row)}")

        # if commit:
        #     self.close()

        return rid
    
    def update_single(self, table_name, item_update, conditions='', commit=True):
        '''
        update item_update in table_name in db, where:
            item_update: {'colunm': 'value'} item to update
            conditions: {'colunm': 'value', ...} conditions
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        col_name_update, col_value_update = self.get_set([item_update])
        col_value_cond = ()
        if conditions:
            col_name_cond, col_value_cond = self.get_cols_for_select([conditions])
            query = 'UPDATE ' + table_name + ' SET ' + col_name_update + ' WHERE ' + col_name_cond
        else: query = 'UPDATE ' + table_name + ' SET ' + col_name_update
        self.cursor.execute(query, (col_value_update + col_value_cond))
        if 'id' in conditions: rid = conditions['id']
        else: rid = None
        if commit:
            self.commit()
            # self.close()
        return rid

    def update_all(self, table_name, item_update, conditions='', commit=True):
        '''
        update item_update in table_name in db, where:
            item_update: {'colunm': 'value'} item to update
            conditions: {'colunm': 'value', ...}
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        col_name_update, col_value_update = self.get_set([item_update])
        col_value_cond = ()
        if conditions:
            col_name_cond, col_value_cond = self.get_cols_for_select([conditions])
            query = 'UPDATE ' + table_name + ' SET ' + col_name_update + ' WHERE ' + col_name_cond
        else:
            query = 'UPDATE ' + table_name + ' SET ' + col_name_update
        self.cursor.execute(query, (col_value_update + col_value_cond))
        if commit:
            self.commit()
            # self.close()

    def update_list(self, table_name, item_update, conditions='', commit=True):
        '''
        update item_update in table_name in db, where:
            item_update: {'colunm': 'value'} item to update
            conditions: {'colunm': list(1,2,3,'qwerty'), ...}
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        col_name_update, col_value_update = self.get_set([item_update])
        col_value_cond = ()
        if conditions:
            col_name_cond, col_value_cond = self.get_cols_for_update([conditions])
            query = 'UPDATE ' + table_name + ' SET ' + col_name_update + ' WHERE ' + col_name_cond
        else:
            query = 'UPDATE ' + table_name + ' SET ' + col_name_update
        self.cursor.execute(query, (col_value_update + col_value_cond))
        if commit:
            self.commit()
            # self.close()


    # def put_multi(self, table_name, columns, rows):
    #     '''
    #     insert list into table in db with single condition, where:
    #         rows - list of tuple [(1,'abc'), (2,'qwe') ...]
    #         in order of columns in db
    #     '''
    #     cols = self.get_cols_multi(columns)
    #     for r in rows:
    #         self.cursor.execute('INSERT INTO ' + table_name + ' (' + ', '.join(columns) + ') VALUES (' + cols + ')', tuple(r))  # json.dumps(rows)
    #     self.commit()
    #     # self.close()


    def remove_multi(self, table_name, row_conditions, commit=True):
        '''
        remove list from table, where:
            column - column where rows items are ex. 'id'
            rows - list of tuples [(1,), (2,), ...]
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        for r in row_conditions:
            col_name_cond, col_value_cond = self.get_cols_for_select([r])
            self.cursor.execute('DELETE FROM ' + table_name + ' WHERE ' + col_name_cond, col_value_cond)
        if commit:
            self.commit()
            # self.close()

    def remove_single(self, table_name, row_conditions, commit=True):
        '''
        remove item_remove in table_name in db, where:
            row_conditions: {'colunm': 'value', ...} conditions of the row to remove
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        col_name_cond, col_value_cond = self.get_cols_for_select([row_conditions])
        self.cursor.execute('DELETE FROM ' + table_name + ' WHERE ' + col_name_cond, col_value_cond)
        if commit:
            self.commit()
            # self.close()

    def get_all(self, table_name, name_column, conditions='', distinct=False, conditions_excl='', limit=None, order=None, asc=None):
        '''
        get from table some results, where
        name_column - what columns to return,
        conditions = {'col': ''}
        distinct - True to exclude duplicates
        limit - number to limit result
        order - provide column name to have ordered
        asc - ASC or DESC order
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        dist = 'DISTINCT ' if distinct else ''
        limit_str = ' LIMIT ' + str(limit) if limit else ''
        order_str = (' ORDER by ' + str(order)) if order else ''
        order_str = (order_str + ' ' + str(asc)) if asc else ''
        col_inv = ''
        col_values_inv = ()
        if conditions_excl:
            col_inv, col_values_inv = self.get_cols_for_update_inv([conditions_excl])
            if conditions: col_inv = col_inv + ' AND '
        if conditions:
            col, col_values = self.get_cols_for_select([conditions])
            self.cursor.execute('SELECT ' + dist + name_column + ' FROM ' + table_name + ' WHERE ' + col_inv + col + order_str + limit_str, col_values_inv + col_values)
        else:
            self.cursor.execute('SELECT ' + dist + name_column + ' FROM ' + table_name + order_str + limit_str)
        res = self.cursor.fetchall()
        self.close()
        return res

    def get_all_list(self, table_name, name_column, conditions='', invert=False, limit=None, order=None, asc=None):  # ORDER by Date ASC
        '''
        get from table some results, where
        name_column - what columns to return,
        conditions = {'col': list()}
        invert - True(get all except in list)
        limit - number to limit result
        order - provide column name to have ordered
        asc - ASC or DESC order
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        limit_str = ' LIMIT ' + str(limit) if limit else ''
        order_str = (' ORDER by ' + str(order)) if order else ''
        order_str = (order_str + ' ' + str(asc)) if asc else ''
        if conditions:
            if invert:  col, col_values = self.get_cols_for_update_inv([conditions])
            else: col, col_values = self.get_cols_for_update([conditions])
            self.cursor.execute('SELECT ' + name_column + ' FROM ' + table_name + ' WHERE ' + col + order_str + limit_str, col_values)
        else:
            self.cursor.execute('SELECT ' + name_column + ' FROM ' + table_name + order_str + limit_str)
        res = self.cursor.fetchall()
        self.close()
        return res

    def get_all_join(self, table_name, name_column, conn_table, conn_column, distinct=False, limit=None, order=None, asc=None):
        '''
        get from table some results, where
        name_column - what columns to return,
        conn_table - table name to JOIN
        conn_column - name of column to JOIN,
        distinct - to get unique values
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        dist = 'DISTINCT ' if distinct else ''
        limit_str = ' LIMIT ' + str(limit) if limit else ''
        order_str = (' ORDER by ' + str(order)) if order else ''
        order_str = (order_str + ' ' + str(asc)) if asc else ''
        self.cursor.execute(
            'SELECT ' + dist + name_column + ' FROM ' + table_name 
            + ' JOIN ' + conn_table + ' ON ' + table_name + '.' + conn_column + ' = ' + conn_table + '.id' + order_str + limit_str
            )
        res = self.cursor.fetchall()
        self.close()
        return res

    def get_all_join2(self, table_name, name_column, conn_table, conn_column, conn_table2, conn_column2, distinct=False, limit=None, order=None, asc=None):
        '''
        get from table some results, where
        name_column - what columns to return,
        conn_table - table name to JOIN
        conn_column - name of column to JOIN,
        distinct - to get unique values
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        dist = 'DISTINCT ' if distinct else ''
        limit_str = ' LIMIT ' + str(limit) if limit else ''
        order_str = (' ORDER by ' + str(order)) if order else ''
        order_str = (order_str + ' ' + str(asc)) if asc else ''
        self.cursor.execute(
            'SELECT ' + dist + name_column + ' FROM ' + table_name 
            + ' JOIN ' + conn_table + ' ON ' + table_name + '.' + conn_column + ' = ' + conn_table + '.id'
            + ' JOIN ' + conn_table2 + ' ON ' + table_name + '.' + conn_column2 + ' = ' + conn_table2 + '.id' + order_str + limit_str
            )
        res = self.cursor.fetchall()
        self.close()
        return res

    def get_all_join_condition(self, table_name, name_column, conn_table, conn_column, cond_column, cond_value, distinct=False, limit=None, order=None, asc=None):
        '''
        get from table some results, where
        name_column - what columns to return,
        conn_table - table name to JOIN
        conn_column - name of column to JOIN,
        cond_column - column where is condition, 
        cond_value - value of condition
        distinct - to get unique values
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        dist = 'DISTINCT ' if distinct else ''
        limit_str = ' LIMIT ' + str(limit) if limit else ''
        order_str = (' ORDER by ' + str(order)) if order else ''
        order_str = (order_str + ' ' + str(asc)) if asc else ''
        row = cond_value
        if type(row) == list or type(row) == dict:
            col, col_values = self.get_cols_for_select([row])
            self.cursor.execute(
                'SELECT ' + dist + name_column + ' FROM ' + table_name 
                + ' JOIN ' + conn_table + ' ON ' + table_name + '.' + conn_column + ' = ' + conn_table + '.id' 
                + ' WHERE ' + col + order_str + limit_str, col_values
                )
        else:
            self.cursor.execute(
                'SELECT ' + dist + name_column + ' FROM ' + table_name 
                + ' JOIN ' + conn_table + ' ON ' + table_name + '.' + conn_column + ' = ' + conn_table + '.id' 
                + ' WHERE ' + cond_column + '=%s' + order_str + limit_str, (cond_value,)
                )
        res = self.cursor.fetchall()
        self.close()
        return res

    def get_all_join2_condition(self, table_name, name_column, conn_table, conn_column, conn_table2, conn_column2, cond_column, cond_value, distinct=False, limit=None, order=None, asc=None):
        '''
        get from table some results, where
        name_column - what columns to return,
        conn_table - table name to JOIN
        conn_column - name of column to JOIN,
        cond_column - column where is condition (use only if cond_value is string),
        cond_value - value of condition {'col': ''}
        distinct - to get unique values
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        dist = 'DISTINCT ' if distinct else ''
        limit_str = ' LIMIT ' + str(limit) if limit else ''
        order_str = (' ORDER by ' + str(order)) if order else ''
        order_str = (order_str + ' ' + str(asc)) if asc else ''
        row = cond_value
        if type(row) == list or type(row) == dict:
            col, col_values = self.get_cols_for_select([row])
            self.cursor.execute(
                'SELECT ' + dist + name_column + ' FROM ' + table_name 
                + ' JOIN ' + conn_table + ' ON ' + table_name + '.' + conn_column + ' = ' + conn_table + '.id' 
                + ' JOIN ' + conn_table2 + ' ON ' + table_name + '.' + conn_column2 + ' = ' + conn_table2 + '.id' 
                + ' WHERE ' + col + order_str + limit_str, col_values
                )
        else:
            self.cursor.execute(
                'SELECT ' + dist + name_column + ' FROM ' + table_name 
                + ' JOIN ' + conn_table + ' ON ' + table_name + '.' + conn_column + ' = ' + conn_table + '.id' 
                + ' JOIN ' + conn_table2 + ' ON ' + table_name + '.' + conn_column2 + ' = ' + conn_table2 + '.id' 
                + ' WHERE ' + cond_column + '=%s' + order_str + limit_str, (cond_value,)
                )
        res = self.cursor.fetchall()
        self.close()
        return res

    def get_all_after_date(self, table_name, name_column, column_date, point_date, limit=None):
        '''
        get from table some results, where
        name_column - what columns to return,
        column_date - name of column with date
        point_date - date after which to request
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        limit_str = ' LIMIT ' + str(limit) if limit else ''
        self.cursor.execute('SELECT ' + name_column + ' FROM ' + table_name + ' WHERE ' + column_date + ' > %s ' + limit_str, (point_date,))
        res = self.cursor.fetchall()
        self.close()
        return res

    def get_colunm(self, table_name, name_column, name_value, row, limit=None):
        '''
        Get from table some results.
        get_colunm(table_name, name_column, name_value, row)
            table_name - table name, 
            name_column - what columns to return,
            name_value - colunms in structure to use with WHERE (only if there is some value)
            row - filter with it's values,
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        limit_str = ' LIMIT ' + str(limit) if limit else ''
        if type(row) == list or type(row) == dict:
            col, col_values = self.get_cols_for_select([row])
            self.cursor.execute('SELECT ' + name_column + ' FROM ' + table_name + ' WHERE ' + col + limit_str, col_values)
        else:
            trow = str(tuple([row,'']))
            self.cursor.execute('SELECT ' + name_column + ' FROM ' + table_name + ' WHERE ' + name_value + ' IN ' + trow + limit_str)
        rid = self.cursor.fetchall()
        self.close()
        return rid

    def get_id(self, table_name, name_value, row, limit=None):
        '''
        table_name - from table
        name_value - from column
        row - item's value
        return ids of found items
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        limit_str = ' LIMIT ' + str(limit) if limit else ''
        if type(row) == list or type(row) == dict:
            col, col_values = self.get_cols_for_select([row])
            self.cursor.execute('SELECT id FROM ' + table_name + ' WHERE ' + col + limit_str, col_values)
        else:
            row = str(tuple([row,'']))
            self.cursor.execute('SELECT id FROM ' + table_name + ' WHERE ' + name_value + ' IN ' + row + limit_str)
        rid = self.cursor.fetchall()
        if len(rid) == 0:
            self.close()
            return False
        self.close()
        return rid[0]['id']

    def get_cols_multi(self, rows):
        '''
        parse & return 
        column names as string: 'a=%s, b=%s, ...'
        for sql request
        '''
        # #col = '=%s, '.join(rows)
        # l = []
        # for r in rows:
        #     l.append('%s')
        # col = ', '.join(l)
        # return col
        return ', '.join('%s' for _ in rows)

    def get_set(self, rows):
        '''
        parse & return 
        column names as string: 'a=%s, b=%s, ...'
        values for names as list
        for sql request
        in SET part
        '''
        col = ''
        for a in rows[0].keys():
            new_value = '=%s, '
            if (type(rows[0][a]) == ipaddress.IPv4Address or type(rows[0][a]) == ipaddress.IPv6Address): new_value = '=inet6_aton(%s), '
            col += a + new_value
        return (col[:-2], tuple(rows[0].values()))

    def get_cols(self, rows):
        '''
        parse & return 
        column names & values for names as strings 
        for sql request
        in SET part
        '''
        col = ''
        col_values = ''
        for a in rows[0].keys():
            col += a + ', '
            new_value = '%s, '
            if (type(rows[0][a]) == ipaddress.IPv4Address or type(rows[0][a]) == ipaddress.IPv6Address): new_value = 'inet6_aton(%s), '
            col_values += new_value
        return (col[:-2], col_values[:-2])

    def get_cols_for_select(self, rows):
        '''
        parse & return 
        column names & values for names as strings 
        for sql request
        in WHERE part
        '''
        col = ''
        col_values = []
        i = 0
        for a in rows[0].keys():
            value = rows[0].get(a)
            if value or value == False:
                if type(value) == dict:
                    sign = value['sign']
                    value = value['value']
                    if type(value) == list: col += a + ' ' + sign + ' %s'
                    else: 
                        if value == None: col += a + ' ' + sign
                        else:
                            col += a + ' ' + sign + ' %s'
                            col_values.append(value)
                else:
                    if type(value) == list: col += a + ' IN %s'
                    elif type(value) == str: col += a + '=%s'
                    else: col += a + '=%s'

                    col_values.append(value)

            else: col += a + ' IS NULL'
            if len(rows[0].keys()) - 1 > i: col += ' AND '
            i += 1
        return (col, tuple(col_values))

    def get_cols_for_update(self, rows):
        '''
        parse & return 
        column names & values for names as strings 
        for sql request
        in WHERE part
        '''
        col = ''
        col_values = []
        for a in rows[0].keys():
            col += a + ' IN %s AND '
            cur_val = ()
            for b in rows[0].get(a):
                cur_val = cur_val + (b,)
            col_values.append(cur_val)
        return (col[:-5], tuple(col_values))

    def get_cols_for_update_inv(self, rows):
        '''
        parse & return 
        column names & values for names as strings 
        for sql request
        in WHERE part
        '''
        col = ''
        col_values = []
        for a in rows[0].keys():
            col += a + ' NOT IN %s AND '
            cur_val = ()
            for b in rows[0].get(a):
                cur_val = cur_val + (b,)
            col_values.append(cur_val)
        return (col[:-5], tuple(col_values))

    def delete_all(self, table_name, commit=True):
        '''
        delete all from table:
        table_name - table name current
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        self.cursor.execute('DELETE FROM ' + table_name)
        if commit:
            self.commit()
            # self.close()

    def delete_list(self, table_name, conditions={}, commit=True):
        '''
        delete all from table:
        table_name - table name current
        conditions - dict {'col_name': [a,b,c...]}
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        col, col_values = '', ''
        if conditions:
            col, col_values = self.get_cols_for_update([conditions])
        if not col or not col_values: return False
        self.cursor.execute('DELETE FROM TABLE ' + table_name + ' WHERE ' + col, col_values)
        if commit:
            self.commit()
            # self.close()

    def rename_table(self, table_name, table_name_new, commit=True):
        '''
        rename table:
        table_name - table name current
        table_name_new - table name new
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        self.cursor.execute('RENAME TABLE ' + table_name + ' TO ' + table_name_new)
        if commit:
            self.commit()
            # self.close()

    def drop_table(self, table_name, commit=True):
        '''
        rename table:
        table_name - table name to drop
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        self.cursor.execute('DROP TABLE ' + table_name)
        if commit:
            self.commit()
            # self.close()

    def rename_column(self, table_name, column_cur, column_new, commit=True):
        '''
        rename column:
        table_name - table name
        column_cur - current name
        column_new - new name
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        self.cursor.execute('ALTER TABLE ' + table_name + ' RENAME COLUMN ' + column_cur + ' TO ' + column_new)
        if commit:
            self.commit()
            # self.close()

    def drop_column(self, table_name, column_name, commit=True):
        '''
        drop column:
        table_name - table name
        column_cur - column name
        '''
        if not self.get_connection(): return False
        if not self.cursor: return False
        self.cursor.execute('ALTER TABLE ' + table_name + ' DROP COLUMN ' + column_name)
        if commit:
            self.commit()
            # self.close()


def get_file_rows_c(obj):
    cfile = [[0]]
    try:
        with open(obj, newline='') as csv_file:  # encoding='utf-8'
            csv_reader = csv.reader(csv_file, dialect='excel')
            cfile = list(csv_reader)
            return cfile
    except:
        return False
