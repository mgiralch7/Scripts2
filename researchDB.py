# MUST RUN WITH PYTHON3 !!
import mysql.connector
from getpass import getpass
from mysql.connector import Error
from mysql.connector import errorcode

def connect():
	usrname = input("Enter username: ")
	usrpass = getpass("Enter password: ")	
	return mysql.connector.connect(user=usrname, password=usrpass, host='localhost')

def disconnect(cnx):
	cnx.close()

def createDB(cursor, DBname):
	cursor.execute("CREATE DATABASE {}".format(DBname))

def initTables(cursor):
	cursor.execute("CREATE TABLE projects ( title VARCHAR(255) NOT NULL, PRIMARY KEY (title) ) ENGINE=InnoDB")
	# MySQL won't let me create a table called groups so I have to call it categories
	cursor.execute("CREATE TABLE categories ( grp VARCHAR(255) NOT NULL, PRIMARY KEY (grp) ) ENGINE=InnoDB")
	cursor.execute("CREATE TABLE sites ( site VARCHAR(255) NOT NULL, PRIMARY KEY (site) ) ENGINE=InnoDB")
	# MySQL won't let me call group to the column so I'm calling it grp
	cursor.execute("CREATE TABLE subjects ( sbjID VARCHAR(255) NOT NULL, project VARCHAR(255) NOT NULL, grp VARCHAR(255), site VARCHAR(255) NOT NULL, PRIMARY KEY (sbjID), FOREIGN KEY (project) REFERENCES projects(title) ON DELETE CASCADE, FOREIGN KEY (grp) REFERENCES categories(grp) ON DELETE CASCADE, FOREIGN KEY (site) REFERENCES sites(site) ON DELETE CASCADE ) ENGINE=InnoDB")
	# sess=sbjID_sessID
	cursor.execute("CREATE TABLE sessions ( sess VARCHAR(255) NOT NULL, sbjID VARCHAR(255) NOT NULL, sessID VARCHAR(255) NOT NULL DEFAULT '1', PRIMARY KEY (sess), FOREIGN KEY (sbjID) REFERENCES subjects(sbjID) ON DELETE CASCADE ) ENGINE=InnoDB")
	cursor.execute("CREATE TABLE excluded ( autokey int NOT NULL AUTO_INCREMENT, sess VARCHAR(255) NOT NULL, image VARCHAR(255) NOT NULL, criteria VARCHAR(255) NOT NULL, PRIMARY KEY (autokey), FOREIGN KEY (sess) REFERENCES sessions(sess) ON DELETE CASCADE, FOREIGN KEY (image) REFERENCES imageTypes (img) ON DELETE CASCADE ) ENGINE=InnoDB")
	cursor.execute("CREATE TABLE results ( autokey int NOT NULL AUTO_INCREMENT, sess VARCHAR(255) NOT NULL, result VARCHAR(255) NOT NULL, value VARCHAR(255) NOT NULL, PRIMARY KEY (autokey), FOREIGN KEY (sess) REFERENCES sessions(sess) ON DELETE CASCADE ) ENGINE=InnoDB")
	cursor.execute("CREATE TABLE demographicData ( autokey int NOT NULL AUTO_INCREMENT, sbjID VARCHAR(255) NOT NULL, session VARCHAR(255), measure VARCHAR(255) NOT NULL, value VARCHAR(255), PRIMARY KEY (autokey), FOREIGN KEY (sbjID) REFERENCES subjects(sbjID) ON DELETE CASCADE, FOREIGN KEY (session) REFERENCES sessions(sess) ON DELETE CASCADE ) ENGINE=InnoDB")
	cursor.execute("CREATE TABLE imageTypes ( img VARCHAR(255) NOT NULL, PRIMARY KEY (img) ) ENGINE=InnoDB")
	# ps should be the combination of pipeline and step
	cursor.execute("CREATE TABLE pipelines ( pipename VARCHAR(255) NOT NULL, project VARCHAR(255) NOT NULL, PRIMARY KEY (pipename), FOREIGN KEY (project) REFERENCES projects(title) ON DELETE CASCADE ) ENGINE=InnoDB")
	cursor.execute("CREATE TABLE pipesteps ( step VARCHAR(255) NOT NULL, script VARCHAR(255), description VARCHAR(255), next VARCHAR(255), pipeline VARCHAR(255) NOT NULL DEFAULT 'ECP', PRIMARY KEY (step), FOREIGN KEY (pipeline) REFERENCES pipelines(pipename) ON DELETE CASCADE ) ENGINE=InnoDB")
	cursor.execute("CREATE TABLE procs ( autokey int NOT NULL AUTO_INCREMENT, sess VARCHAR(255) NOT NULL, step VARCHAR(255) NOT NULL, status ENUM('ready','hold','running','done','checked','error') NOT NULL DEFAULT 'ready', jobID VARCHAR(255), jobIndex VARCHAR(255), exectimems DOUBLE, PRIMARY KEY (autokey), FOREIGN KEY (sess) REFERENCES sessions(sess) ON DELETE CASCADE, FOREIGN KEY (step) REFERENCES pipesteps(step) ) ENGINE=InnoDB")

def initDB(cnx):
	cursor = cnx.cursor()
	DBname = input("New database name: ")
	createDB(cursor, DBname)
	cnx.database = DBname
	initTables(cursor)
	print("Database sucessfully initiated")
	return cnx

def selectDB(cnx):
	cursor = cnx.cursor()
	cursor.execute("SHOW DATABASES")

	print("Existing databases:")
	dblist = []
	for (DBname,) in cursor:
		if (DBname != "information_schema") and (DBname != "mysql") and (DBname != "performance_schema") and (DBname != "sys"):
			db = str(DBname)
			print(db)
			dblist.append(db)

	selectedDB = input("To which existing database would you like to connect? ")
	if selectedDB in dblist:
		cnx.database = selectedDB
	else:
		print(selectedDB+" is not in the list")
	return cnx

def showTables(cnx):
	cursor = cnx.cursor()
	dbname = cnx.database
	cursor.execute("SHOW TABLES")
	print("Existing tables for "+dbname+':')
	for (tname,) in cursor:
		print(tname)
	cursor.close()

def findSbj(cnx,sbj,column):
	cursor = cnx.cursor()
	if column=='':
		cursor.execute("select count(*) from subjects where sbjID='"+sbj+"'")
	else:
		cursor.execute("select count(*) from subjects where sbjID='"+sbj+"' and "+column+" is not NULL")
	for (row,) in cursor:
		if(row==1):
			return True
		else:
			return False
	cursor.close()

def findMissingSubjects(cnx,column):
	sbjList = str(input("Enter path to subject list: "))
	flist = open(sbjList,'r')
	outFile = str(input("Enter path to output file: "))
	fout = open(outFile,'w')
	for line in flist:
		sbj = line.split('\t')[0].replace('\n','')
		if column=='' and findSbj(cnx,sbj,'')==0:
			fout.write(sbj+'\n')
		if column!='' and findSbj(cnx,sbj,column)==0:
			fout.write(sbj+'\n')
	fout.close()
	flist.close()

def addSbjGrp(cnx):
	cursor = cnx.cursor()
	fname = str(input("Enter file path: "))
	finfo = open(fname,'r')
	for line in finfo:
		line = line.replace('\n','')
		array = line.split('\t')
		sbjID = array[0]
		grp = array[1]
		cursor.execute("update subjects set grp = '"+grp+"' where sbjID = '"+sbjID+"'")
	finfo.close()
	cnx.commit()
	cursor.close()

def addScanGrp(cnx):
	cursor = cnx.cursor()
	fname = str(input("Enter file path: "))
	finfo = open(fname,'r')
	for line in finfo:
		line = line.replace('\n','')
		array = line.split('\t')
		sbjID = array[0]
		grp = array[1]
		cursor.execute("update subjects set scan_grp = '"+grp+"' where sbjID = '"+sbjID+"'")
	finfo.close()
	cnx.commit()
	cursor.close()

def getProject(cursor):
	cursor.execute("select title from projects")
	print("Current projects:")
	for (proj,) in cursor:
		print(proj)
	return(input("Select project: "))

def getPipeline(cursor,project):
	cursor.execute("select pipename from pipelines where project='"+project+"'")
	print("Pipelines from project "+project+":")
	for (pipe,) in cursor:
		print(pipe)
	return(input("Select pipeline: "))

def getStep(cursor,pipeline):
	cursor.execute("select step from pipesteps where pipeline='"+pipeline+"'")
	print("Steps from pipeline "+pipeline+":")
	for (step,) in cursor:
		print(step)
	return(input("Select step: "))

def getNextStep(cursor,step):
        cursor.execute("select next from pipesteps where step='"+step+"'")
        for (nstep,) in cursor:
            nstep=str(nstep);
        return(nstep)

def addSbjsToStep(cnx):
	cursor = cnx.cursor()
	project = getProject(cursor)
	pipeline = getPipeline(cursor,project)
	step = getStep(cursor,pipeline)
	status = str(input("Select status [ready / hold / running / done / checked / error']: "))
	if status=="running":
		jobID = str(input("Enter the jobID: "))
	location = str(input("Location: "))	
	sessList = str(input("File path session list: "))
	flist = open(sessList,'r')
	n1 = int(input("Index first subject: "))
	n2 = int(input("Index last subject: "))
	n=1
	for sess in flist:
		if n>=n1 and n<=n2:
			sess = sess.replace('\n','')
			if status!="running":
				cursor.execute("insert into procs(sess,step,status,location) values ('"+sess+"','"+step+"','"+status+"','"+location+"')")
			else:
				cursor.execute("insert into procs(sess,step,status,location,jobID,jobIndex) values ('"+sess+"','"+step+"','"+status+"','"+location+"','"+jobID+"','"+str(n)+"')")
		n+=1
	flist.close()
	cnx.commit()
	cursor.close()

def updateSbjsToStep(cnx):
    cursor = cnx.cursor()
    project = getProject(cursor)
    pipeline = getPipeline(cursor,project)
    step = getStep(cursor,pipeline)
    status = str(input("Select status [ready / hold / running / done / checked / error']: "))
    if status=="running":
                jobID = str(input("Enter the jobID: "))
    location = str(input("Location: "))
    sessList = str(input("File path session list: "))
    flist = open(sessList,'r')
    n1 = int(input("Index first subject: "))
    n2 = int(input("Index last subject: "))
    n=1
    for sess in flist:
        if n>=n1 and n<=n2:
            sess = sess.replace('\n','')
            if status!="running":
                cursor.execute("update procs set status='"+status+"', location='"+location+"' where sess='"+sess+"' and step='"+step+"'")
            else:
                 cursor.execute("update procs set status='"+status+"', location='"+location+"', jobID='"+jobID+"', jobIndex='"+str(n)+"' where sess='"+sess+"' and step='"+step+"'")
        n+=1
    flist.close()
    cnx.commit()
    cursor.close()

def inTable(cursor,table,sess,colname,colvalue):
        cursor.execute("select count(*) from "+table+" where sess='"+sess+"' and "+colname+"='"+colvalue+"'")
        res=False
        for (count,) in cursor:
            if count>0:
                res=True
        return(res)

def addDemographicData(cnx):
	cursor = cnx.cursor()
	fpath = str(input("File path with demographic data: "))
	fdemo = open(fpath,'r')
	header = fdemo.readline().replace('\n','').split('\t')
	for line in fdemo:
		line = line.replace('\n','').split('\t')
		sess = line[0]+"_1"
		i = 1
		while i<(len(line)-1):
			if inTable(cursor,'demographicData',sess,'measure',header[i])==False:
				cursor.execute("insert into demographicData(sess,measure,value) values('"+sess+"','"+header[i]+"','"+line[i]+"')")
			i+=1
	fdemo.close()
	cnx.commit()
	cursor.close()

def ttest(cnx,column,grp1,grp2):
	sbjList = open(input("Subject list: "),'r')
	outDir = input("Output dir: ")
	cursor = cnx.cursor()

	# 1. Generate design matrix (design.mat)
	mat = ["/Matrix"]
	for sbj in sbjList:
		cursor.execute("select "+column+" from subjects where sbjID='"+sbj.replace('\n','')+"' and ("+column+"='"+grp1+"' or "+column+"='"+grp2+"')")
		for (grp,) in cursor:
			grp = str(grp)
		[GRP1,GRP2] = ['1','0'] if grp==grp1 else ['0','1']
		mat.append(GRP1+' '+GRP2)
	design = open(outDir+"/design.mat",'w')
	design.write("/NumWaves 2\n")
	design.write("/NumPoints "+str(len(mat)-1)+'\n')
	for line in mat:
		design.write(line+'\n')
	design.close()

	# 2. Generate contrast file (design.con)
	contrast = open(outDir+"/design.con",'w')
	contrast.write("/NumWaves 2\n")
	contrast.write("/ContrastName1 "+grp1+'>'+grp2+"\n")
	contrast.write("/ContrastName1 "+grp2+'>'+grp1+"\n")
	contrast.write("/NumContrast 2\n")
	contrast.write("1 -1\n")
	contrast.write("-1 1\n")
	contrast.close()

	cursor.close()
	sbjList.close()
