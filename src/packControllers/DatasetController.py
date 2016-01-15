# -*- encoding: utf-8 -*-

from src.packDB import MySQLConnector
from src.packAPI import BCAPI


__author__ = 'Rubén Mulero'


class Singleton(type):

    def __init__(cls, name, bases, dct):
        cls.__instance = None
        type.__init__(cls, name, bases, dct)

    def __call__(cls, *args, **kw):
        if cls.__instance is None:
            cls.__instance = type.__call__(cls, *args, **kw)
        return cls.__instance


class DatasetController(object):
    __metaclass__ = Singleton

    # Singleton class created. Ready for action!

    def create_table(self):
        """
        Check if exists any dataset table and create one if needed.

        :return: None
        """
        bd = MySQLConnector.MySQLConnector()
        select_query = "SHOW TABLES LIKE 'dataset';"
        bd_res_1 = bd.execute(select_query)
        if bd_res_1 == 0:
            # new table
            create_table = "CREATE TABLE dataset (id INT NOT NULL AUTO_INCREMENT,name varchar(225)," \
                           "license varchar(225),PRIMARY KEY (id));"
            bd.execute(create_table)

    def bc_query(self):
        """
        Makes a query to the British Columbia Catalogue API and return 'water' related datasets.

        Then, this method use MySQL Database and use Insert command to store the name and license of each dataset.

        :return: Boolean True or False based on the success of the operation.
        """
        res = False
        bc = BCAPI.BCAPI()
        datasets = bc.query()
        if datasets and len(datasets) > 0:
            bd = MySQLConnector.MySQLConnector()
            for data in datasets['results']:
                if 'title' in data and 'license_title' in data:
                    query_1 = "SELECT name,license FROM dataset WHERE name=%s AND license=%s", \
                              (data['title'], data['license_title'])  # check if exist
                    bd_res_1 = bd.execute(query_1)
                    if len(bd_res_1) == 0:
                        query_2 = "INSERT INTO dataset(name,license) VALUES (%s,%s);", \
                                  (data['title'], data['license_title'])
                        bd_res_2 = bd.execute(query_2)
                        if bd_res_2 == 1:  # Ok
                            res = True
                        else:
                            res = False
                            break

        return res
