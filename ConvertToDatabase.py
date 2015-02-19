import psycopg2 as ps
import csv
import math

class DBO:
    ##Custom database object, retains information about dbname, user and pw
    def __init__(self,dbname,user,pw):
        self.user = user
        self.pw = pw
        self.dbname = dbname
        self.admin = ps.connect(database='postgres', user=self.user, password=self.pw)
        self.admin.autocommit=True
        self.checkDBName()
        self.conn = ps.connect(database=self.dbname, user=self.user, password=self.pw)
        self.conn.autocommit=True
        

    def checkDBName(self):
        try:
            conn = ps.connect(database=self.dbname, user=self.user, password=self.pw)
            conn.close()
        except ps.OperationalError:
            print("Failed to find that database")
            test = input("Would you like to create that database? (y/n)")
            if test == 'y':
                self.createNewDB()
            else:
                print("There is no database")
    
    def mapTypesToPostgres(self,dataset):
        ##Here we create data to help map python types to postgres types
        dic = {}
        tempD = {}
        for i,y in enumerate(dataset[0]):##Iterate through columns as headers
            if y!='':
                inconsistentWarning = False
                for k,x in enumerate(dataset):##Iterate through rows
                    if k==0:##Set list for header
                        header = x
                    if k>0:
                        try:
                            test = tempD[self.checkHeader(header[i])]
                            if test!=str(type(x[i])):
                                if inconsistentWarning==True:
                                    print("Error: inconsistent type in {0}".format(y))
                                    inconsistentWarning = True
                                tempD[self.checkHeader(header[i])] = "<class 'str'>"
                        except KeyError:
                            tempD[self.checkHeader(header[i])] = str(type(x[i]))
        return(tempD)

    def checkHeader(self,val):
        val = val.lower()
        exclude = [' ','"',"'",'.',',','/','?',';',':','[',']','{','}','\\','|',
                   '!','@','#','$','%','^','&','*','(',')','~','`','-','+','=']
        for e in exclude:
            val = val.replace(e,"_")
        return(val)
        
    def makeCreateTableStatement(self,dataset,mapping,tableName,cur):
        ##This creates a table based on the structure of the dataset
        typeDB = {"<class 'str'>":'text',
                  "<class 'int'>":'integer',
                  "<class 'float'>":'numeric'}
        createTable = 'CREATE TABLE {0} ('.format(tableName)
        for k,var in enumerate(mapping):
            #print(var, mapping[var], typeDB[mapping[var]])
            var = self.checkHeader(var)
            createTable = createTable + '"{0}" {1}'.format(var, typeDB[mapping[var]])
            if k<(len(mapping)-1):
                createTable = createTable+', '
        createTable = createTable+') WITH (OIDS=FALSE); ALTER TABLE {0} OWNER TO postgres;'.format(tableName)
        cur.execute(createTable)
        #print(createTable)

    def checkForBlanks(self,header):
        newH = []
        for h in header:
            if h!='':
                newH.append(h)
        return(newH)

    def writeData(self,dataset,mapping,tableName,cur):
        ##This iterates through the data and creates statements to post to db
        header = dataset[0]
        header = self.checkForBlanks(header)
        data = dataset[1:len(dataset)]
        cmd = 'INSERT INTO {0} ('.format(tableName)
        for k, h in enumerate(header):
            cmd = cmd+self.checkHeader(h)
            if k < (len(header)-1):
                cmd = cmd+", "
        cmd = cmd+") VALUES ("
        for i, row in enumerate(data):
            for k, r in enumerate(header):
                r = row[k]
                if mapping[self.checkHeader(header[k])]=="<class 'str'>":
                    cmd = cmd+"'{0}'".format(r)
                else:
                    cmd = cmd+"{0}".format(r)
                if k < (len(header)-1):
                    cmd = cmd+", "
            if i < (len(data)-1):
                cmd = cmd+"),("
        cmd = cmd+");"
        #print(cmd)
        #cur.execute(cmd)
        try:
            cur.execute(cmd)
        except ps.ProgrammingError:
            print('========================')
            print(cmd)
            print(len(header))
            for i in data:
                print(len(i))
                print(i)
            print('========================')
        cur.execute(cmd)

##    def writeData(self,dataset,mapping,tableName,cur):
##        ##This iterates through the data and creates statements to post to db
##        header = dataset[0]
##        header = self.checkForBlanks(header)
##        data = dataset[1:len(dataset)]
##        cmd = 'INSERT INTO {0} ('.format(tableName)
##        for k, h in enumerate(header):
##            cmd = cmd+self.checkHeader(h)
##            if k < (len(header)-1):
##                cmd = cmd+", "
##        cmd = cmd+") VALUES ("
##        for i, row in enumerate(data):
##            for k, r in enumerate(header):
##                r = row[k]
##                if mapping[self.checkHeader(header[k])]=="<class 'str'>":
##                    cmd = cmd+"'{0}'".format(r)
##                else:
##                    cmd = cmd+"{0}".format(r)
##                if k < (len(header)-1):
##                    cmd = cmd+", "
##            if i < (len(data)-1):
##                cmd = cmd+"),("
##        cmd = cmd+");"
##        #print(cmd)
##        #cur.execute(cmd)
##        try:
##            cur.execute(cmd)
##        except ps.ProgrammingError:
##            print('========================')
##            print(cmd)
##            print(len(header))
##            for i in data:
##                print(len(i))
##                print(i)
##            print('========================')
##        cur.execute(cmd)

##    def copyFromCSV(self,fiName,tblName,delim=",",overwrite=False):
##        chunker = 20.
##        tblName = self.checkHeader(tblName)
##        cur = self.conn.cursor()
##        if overwrite==True:
##            self.dropFromPostgres(tblName, objType='TABLE')
##        with open(fiName) as fi:
##            reader = csv.reader(fi,delimiter=delim,dialect='excel')
##            for k,r in enumerate(reader):
##                #print(k)
##                data = []
##                ##print(k,((float(k)/500.)*500)==math.ceil(float(k)/500.)*500)
##                if k==0:
##                    header = r
##                    fullData = [header]
##                else:
##                    for i in r:
##                        data.append(getType(i))
##                    #print(data)
##                    fullData.append(data)
##                if k>0 and ((float(k)/chunker)*chunker)==math.ceil(float(k)/chunker)*chunker:
##                    print("Time to write")
##                    print(k)
##                    #print(fullData)
##                    try:
##                        x = mapping
##                    except:
##                        mapping = self.mapTypesToPostgres(fullData)
##                        self.makeCreateTableStatement(fullData, mapping, tblName, cur)
##                    self.writeData(fullData, mapping, tblName, cur)
##                    fullData = [header]
##            if len(fullData)>1:
##                self.writeData(fullData,mapping,tblName,cur)##For final run through

    def copyFromCSV(self,fiName,tblName,delim=",",overwrite=False):
        print('Reading in data')
        data = readInData(fiName,delim)
        print('Finishesd reading in data')
        cur = self.conn.cursor()
        if overwrite==True:
            self.dropFromPostgres(tblName, objType='TABLE')
        mapping = self.mapTypesToPostgres(data)
        self.makeCreateTableStatement(data, mapping, tblName, cur)
        print("Writing data")
        self.writeData(data, mapping, tblName,cur)
        print("Finished writing data")

    def createNewDB(self):
        ##This should be done if no database exists
        cur = self.admin.cursor()
        cur.execute("CREATE DATABASE {0} WITH OWNER = {1} ENCODING = 'UTF8' TABLESPACE = pg_default LC_COLLATE = 'English_United States.1252' LC_CTYPE = 'English_United States.1252' CONNECTION LIMIT = -1;".format(self.dbname.lower(),self.user))
        print("Created new empty database - {0}".format(self.dbname))

    def dropFromPostgres(self, objName, objType='TABLE'):
        if objType=='TABLE':
            cur = self.conn.cursor()
        if objType=='DATABASE':
            if objName==self.dbname:
                self.conn.close()
            cur = self.admin.cursor()
        try:
            cur.execute("DROP {0} {1};".format(objType, objName))
            print("{0} ({1}) dropped".format(objType,objName))
        except:
            print("There was no {0} to drop".format(objName))

    def adHoc(self,query):
        cur = self.conn.cursor()
        cur.execute(query)
        out = cur.fetchall()
        print("Query executed:")
        print(query)
        return(out)

def getType(i):
    try:
        i = int(i)
        try:
            i = float(i)
        except ValueError:
            pass
    except ValueError:
        i = i.replace("'","")
    return(i)
    
        
def readInData(fiName, delim=','):
    data = []
    with open(fiName) as fi:
        reader = csv.reader(fi,delimiter=delim,dialect='excel')
        for r in reader:
            tempLi = []
            for i in r:
                try:
                    i = int(i)
                    try:
                        i = float(i)
                    except ValueError:
                        pass
                except ValueError:
                    i = i.replace("'","")
                tempLi.append(i)
            data.append(tempLi)

    return(data)
    
        
fiName = 'C:/Users/jarrodanderin/Documents/_RWork/_Datasets/COW_Alliance_v3.03.csv'
dbname = 'testing2'
tblName = 'test'
fiLi = open('fiNames.csv')
reader = csv.reader(fiLi)
saveDic = {}
for r in reader:
    try:
        saveDic[r[2]].append(r)
    except KeyError:
        saveDic[r[2]] = [r]
for ty in saveDic:
    print(ty)
    for i in saveDic[ty]:
        if i[1]=='polity_four':
            print("==>"+str(i[1]))
            db = DBO(ty,'postgres','pw')
            db.copyFromCSV(i[0],i[1],overwrite=True)
##db = DBO(dbname,'postgres','pw')
##db.copyFromCSV(fiName,tblName,overwrite=True)
