# -*- encoding: utf-8 -*-

import MySQLdb

__author__ = 'Rubén Mulero'


class MySQLConnector(object):

    def __init__(self):
        pass

    def execute(self, p_query):
        res = None
        database = self.__conexion()
        if database is not None:
            # Creating a DictCursor (rather than basic cursor)
            cur = database.cursor(MySQLdb.cursors.DictCursor)
            try:
                # Tuple based query
                if type(p_query) == tuple:
                    res = cur.execute(*p_query)
                    if p_query[0].upper().startswith('SELECT'):
                        res = cur.fetchall()
                    else:
                        database.commit()
                else:
                    # Single query
                    res = cur.execute(p_query)
                    if p_query.upper().startswith('SELECT'):
                        res = cur.fetchall()
                    else:
                        database.commit()
            except MySQLdb.Error, e:
                try:
                    print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                except IndexError:
                    print "MySQL Error: %s" % str(e)
                finally:
                    if database:
                        database.rollback()
            finally:
                if database:
                    cur.close()
                    database.close()

        return res

    def __conexion(self):
        try:
            db = MySQLdb.connect(host="localhost",  # DB Host
                                 user="deusto",  # DB User
                                 passwd="deusto123",  # DB Password
                                 db="deustodata")  # DB Name
            return db
        except MySQLdb.Error, e:
            try:
                print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(e)
            finally:
                return None
