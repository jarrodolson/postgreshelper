import psycopg2 as ps

class dataframe:
    def __init__(self, finame):
        pass

    def liliToDF(self, inData):
        pass

    def csvToDF(self, finame, nrows=None):
        saved = []
        with open(finame, 'w', newline='') as fi:
            reader = csv.reader(fi, dialect='excel')
            for r in reader:
                saved.append(reader)
            
test = [['a','b'],
        [1,2],
        [3,4]]

mapTypesToPostgres(test)
