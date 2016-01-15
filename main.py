# -*- encoding: utf-8 -*-

from src.packControllers import DatasetController

__author__ = 'Rubén Mulero'

if __name__ == "__main__":
    dataset = DatasetController.DatasetController()
    # Create Database table if not exists.
    dataset.create_table()
    # Want to some water?
    res = dataset.bc_query()
    if res:
        print res
        print ""
        print ""
        print "Result stored in database"
        print "############################"
        print "###      DATA OK         ###"
        print "############################"
    else:
        print "No new datasets."
