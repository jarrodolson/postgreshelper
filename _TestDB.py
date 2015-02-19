import psycopg2 as ps
import pandas as pd
import numpy as np
import cStringIO
import pandas.io.sql as psql
import csv

def postgresql_copy_from(df, name, con ):
    ##SOURCE: https://gist.github.com/catawbasam/3164289
    # append data into existing postgresql table using COPY
    
    # 1. convert df to csv no header
    output = cStringIO.StringIO()
    
    # deal with datetime64 to_csv() bug
    have_datetime64 = False
    dtypes = df.dtypes
    for i, k in enumerate(dtypes.index):
        dt = dtypes[k]
        print 'dtype', dt, dt.itemsize
        if str(dt.type)=="<type 'numpy.datetime64'>":
            have_datetime64 = True
 
    if have_datetime64:
        d2=df.copy()    
        for i, k in enumerate(dtypes.index):
            dt = dtypes[k]
            if str(dt.type)=="<type 'numpy.datetime64'>":
                d2[k] = [ v.to_pydatetime() for v in d2[k] ]                
        #convert datetime64 to datetime
        #ddt= [v.to_pydatetime() for v in dd] #convert datetime64 to datetime
        d2.to_csv(output, sep='\t', header=False, index=False)
    else:
        df.to_csv(output, sep='\t', header=False, index=False)                        
    output.seek(0)
    contents = output.getvalue()
    print 'contents\n', contents
       
    # 2. copy from
    cur = con.cursor()
    cur.copy_from(output, name)    
    con.commit()
    cur.close()
    return

def getColNames(tbl,cur):
    names = []
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='"+str(tbl)+"';")
    for el in cur.fetchall():
        names.append(el[0])
    return(names)

def getDataFromTable(tbl, cur, query="*",getHeader=False):
    if getHeader==True:
        colnames = getColNames(tbl, cur)
    cur.execute("SELECT"+query+"FROM test")
    saveLi = []
    if getHeader==True:
        saveLi = [colnames]
    for el in cur.fetchall():
        saveLi.append(el)
    return(saveLi)

def getDFFromTable(tbl, cur, query="*"):
    colnames = getColNames(tbl, cur)
    saveLi = getDataFromTable(tbl, cur, query)
    df2 = pd.DataFrame(saveLi,columns=colnames)
    return(df2)

def getDataFromCSV(fiName):
    dataOut = []
    with open(fiName, 'rb') as fIn:
        reader = csv.reader(fIn)
        for row in reader:
            dataOut.append(row)
    return(dataOut)
        

def makeCSVFromData(fiName,data):
    with open(fiName, 'wb') as fout:
        writer = csv.writer(fout)
        for row in data:
            writer.writerow(row)

def createValueChainFromLi(li):
    li = li[1:len(li)]
    start = "("
    l = 0
    for row in li:
        c = 0
        for el in row:
            start = start+str(el)
            if c < len(el):
                start = start+","
            if c == len(el) and l < len(li)-1:
                start = start+"),("
            c+=1
        l+=1
    start = start+");"
    return(start)

def createColChainFromDataLi(li):
    header = li.pop(0)
    keep = "("
    n = 0
    for c in header:
        keep = keep + c
        if n < len(header)-1:
            keep = keep+","
        n+=1
    keep = keep+")"
    return(keep)

def buildInsertFromDataLi(li, tbl, cur):
    head = createColChainFromDataLi(li)
    print(head)
    values = createValueChainFromLi(li)
    query = 'INSERT INTO '+str(tbl) + str(head) +"VALUES"+ str(values)
    print(query)
    cur.execute(query)
    return(query)

conn = ps.connect(user="postgres",password="pw",database="postgres")
cur = conn.cursor()
conn.autocommit=True
##

def getTableCreate(tableName,data,col):
    ty = 'text'
    ##print(type(data))
    if type(data)==list:
        rowC = 0
        for row in data:
            if rowC == 0:
                head = row[col]
            if type(row[col])==int:
                ty = 'integer'
            if type(row[col])==float:
                ty="numeric"
        obj = head+" "+ty+","
        print(obj)
                
####Approach without using pandas, approach with marked as "#P#"
data1 = [[1,2],[3,4],[2,2],[2,2],[2,2],[5,6]]
###P#df = pd.DataFrame(data1,columns=['name1','name2'])
getTableCreate("test",data1,2)
##try:
##    cur.execute("DROP TABLE test;")
##    ##conn.commit()
##except:
##    print("Table does not exist")
##
##cur.execute("CREATE TABLE test (name1 integer, name2 integer)")
##
###P#postgresql_copy_from(df,'test',conn)
###P#df2 = getDFFromTable('test',cur)
##
####Get data from db tables and write to CSV
##
##data = getDataFromTable('test',cur, getHeader=True)
##makeCSVFromData('test.csv',data)
##
####Get data from CSV and write to db table
##newData = getDataFromCSV('test.csv')
###P#head1 = newData.pop(0)
###P#newDF = pd.DataFrame(newData,columns=head1)
###P#postgresql_copy_from(newDF,'test',conn)
##
##df3 = getDFFromTable('test',cur)
##print(df3)
##
####conn.commit()
cur.close()
conn.close()
