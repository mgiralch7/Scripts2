# MUST RUN WITH PYTHON3 !!
import mysql.connector
from getpass import getpass
from mysql.connector import Error
from mysql.connector import errorcode
import statistics
import os

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

def findSbj(cnx,sbj,table,column):
	cursor = cnx.cursor()
	if column=='':
		cursor.execute("select count(*) from "+table+" where sbjID='"+sbj+"'")
	else:
		cursor.execute("select count(*) from "+table+" where sbjID='"+sbj+"' and "+column+" is not NULL")
	for (row,) in cursor:
		if(row==1):
			return True
		else:
			return False
	cursor.close()

def findMissingSubjects(cnx,table,column):
	sbjList = str(input("Enter path to tab-separated subject list: "))
	flist = open(sbjList,'r')
	outFile = str(input("Enter path to output file: "))
	fout = open(outFile,'w')
	for line in flist:
		sbj = line.split('\t')[0].replace('\n','')
		if column=='' and findSbj(cnx,sbj,table,'')==0:
			fout.write(sbj+'\n')
		if column!='' and findSbj(cnx,sbj,table,column)==0:
			fout.write(sbj+'\n')
	fout.close()
	flist.close()

def addMissingSubjects(cnx):
	cursor = cnx.cursor()
	fname = str(input("Enter space-separated file path: "))
	flist = open(fname,'r')
	excl = str(input("Exclusion criteria: "))
	for line in flist:
		array = line.replace('\n','').split(' ')
		sbjID = array[0]
		sess = sbjID+"_1"
		grp = array[1]
		site = array[2]
		# Add subject to subjects table
		cursor.execute("insert ignore into subjects(sbjID,grp,site) values('"+sbjID+"','"+grp+"','"+site+"')")
		# Add session to sessions table
		cursor.execute("insert ignore into sessions(sess,sbjID) values('"+sess+"','"+sbjID+"')")
		# Add session to excluded table
		cursor.execute("insert into excluded(sess,criteria) values('"+sess+"','"+excl+"')")
	cnx.commit()
	cursor.close()

def addSbjGrp(cnx):
	cursor = cnx.cursor()
	fname = str(input("Enter tab-separated file path: "))
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
	fname = str(input("Enter tab-separated file path: "))
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
            res = count>0
        return(res)

def findMissingDemographicData(cnx,array_measures):
	cursor = cnx.cursor()
	flist = open(input("Session list path: "),'r')
	fout = open(input("Path output file: "),'w')
	for sess in flist:
		i = 0
		cont = True
		while i<len(array_measures) and cont:
			cont = inTable(cursor,'demographicData',sess.replace('\n',''),'measure',array_measures[i])
			i+=1
		if cont == False:
			fout.write(sess.replace('\n','')+' '+array_measures[i-1]+'\n')
	fout.close()
	flist.close()

def addDemographicData(cnx):
	cursor = cnx.cursor()
	fdemo = open(input("File path with tab-separated demographic data: "),'r')
	header = fdemo.readline().replace('\n','').split('\t')
	for line in fdemo:
		line = line.replace('\n','').split('\t')
		sess = line[0]+"_1"
		i = 1
		while i<len(line):
			if len(line[i].replace(' ',''))>0 and inTable(cursor,'demographicData',sess,'measure',header[i])==False:
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
	design.write("/Ppheights 1 1\n")
	for line in mat:
		design.write(line+'\n')
	design.close()

	# 2. Generate contrast file (design.con)
	contrast = open(outDir+"/design.con",'w')
	contrast.write("/NumWaves 2\n")
	contrast.write("/ContrastName1 "+grp1+'>'+grp2+'\n')
	contrast.write("/ContrastName2 "+grp2+'>'+grp1+'\n')
	contrast.write("/NumContrasts 2\n")
	contrast.write("/PPheights 1 1\n")
	contrast.write("/Matrix\n")
	contrast.write("1 -1\n")
	contrast.write("-1 1\n")
	contrast.close()

	cursor.close()
	sbjList.close()

def ttestEV1(cnx,grp_col,grp1,grp2,ev_col,ev_grp1,ev_grp2):
	sbjList = open(input("Subject list: "),'r')
	outDir = input("Output dir: ")
	cursor = cnx.cursor()

	# 1. Get the information of each sbj from the db
	grp_dic = {}
	ev_dic = {}
	for sbj in sbjList:
		sbj = sbj.replace('\n','')
		cursor.execute("select "+grp_col+","+ev_col+" from subjects where sbjID='"+sbj+"'")
		for (grp,ev) in cursor:
			if (grp==grp1 or grp==grp2) and (ev==ev_grp1 or ev==ev_grp2):
				grp_dic[sbj] = '1 0' if grp==grp1 else '0 1'
				ev_dic[sbj] = 1 if ev==ev_grp1 else 0
	
	# 2. Generate design matrix (design.mat)
	# For the EV, mean center its values by subtracting the overal mean
	M = statistics.mean(ev_dic.values())
	design = open(outDir+"/design.mat",'w')
	design.write("/NumWaves 3\n")
	design.write("/NumPoints "+str(len(grp_dic.keys()))+'\n')
	design.write("/Matrix\n")
	for sbj,grp in grp_dic.items():
		ev = ev_dic[sbj]-M
		design.write(grp+' '+str(ev)+'\n')
	design.close()

	# 3. Generate contrast file (design.con)
	contrast = open(outDir+"/design.con",'w')
	contrast.write("/NumWaves 3\n")
	contrast.write("/ContrastName1 "+grp1+'>'+grp2+'\n')
	contrast.write("/ContrastName2 "+grp2+'>'+grp1+'\n')
	contrast.write("/NumContrasts 2\n")
	contrast.write("/Matrix\n")
	contrast.write("1 -1 0\n")
	contrast.write("-1 1 0\n")
	contrast.close()

	cursor.close()
	sbjList.close()

# grp_col: column (from subjects table) with the groups that are being compared. There must be only two groups.
# cat_covars: array of measures (from the behavioralData table) that are being used as categorical covariates
# cont_covars: array of measures (from the behavioralData table) that are being used as continuous covariates (must have numerical values)
# there cannot be any missing values for any subject
def ttestEVs(cnx,grp_col,cat_covars,cont_covars):
	cursor = cnx.cursor()
	outDir = input("Output dir: ")
	if os.path.isdir(outDir)==False:
		return

	### 1. Get the list of group values from the DB ###
	groups = []
	cursor.execute("select distinct "+grp_col+" from subjects where project='ECP'")
	for (grp,) in cursor:
		if grp != None:
			groups+=[grp]

	### 2. Get the list of subjects ###
	sbjs = []
	sbjpath = input("Path sbj list: ")
	sbjList = open(sbjpath,'r')
	if os.path.isfile(sbjpath)==False:
		return
	for sbj in sbjList:
		sbjs+=[sbj.replace('\n','')]
	sbjList.close()

	### 3. Get the values of each categorical measure ###
	cat_dic={}
	for measure in cat_covars:
		# Get the list of values for that measure from the DB
		cursor.execute("select distinct value from demographicData where measure='"+measure+"'")
		# save the values in the dictionary
		vals_dic = {}
		i=0
		for (value,) in cursor:
			vals_dic[value] = str(i)
			i+=1
		cat_dic[measure]=vals_dic

	### 4. Generate the design matrix ###
	# Generate the matirx
	grpcols = []
	othercols = {}
	for sbj in sbjs:
		# Get the subject group
		cursor.execute("select "+grp_col+" from subjects where sbjID='"+sbj+"'")
		for (sbjgrp,) in cursor:
			sbjgrp = str(sbjgrp)
		grpcols.append("1 0") if sbjgrp==groups[0] else grpcols.append("0 1")
		# Get the value of each categorical measure for this subject
		for measure in cat_covars:
			if measure in othercols:
				array_values = othercols[measure]
			else:
				array_values = []
			cursor.execute("select value from demographicData where measure='"+measure+"' and sess='"+sbj+"_1'")
			for (value,) in cursor:
				tmp_dic = cat_dic[measure]
				array_values+=[float(tmp_dic[value])]
			othercols[measure] = array_values
		# Get the value of each continuous measure for this subject
		for measure in cont_covars:
			if measure in othercols:
				array_values = othercols[measure]
			else:
				array_values = []
			cursor.execute("select value from demographicData where measure='"+measure+"' and sess='"+sbj+"_1'")
			for (value,) in cursor:
				array_values+=[float(value)]
			othercols[measure] = array_values

	# Mean center all the covariates
	for key,values in othercols.items():
		M = statistics.mean(values)
		mcentered = []
		for val in values:
			mcentered+=[val-M]
		othercols[key] = mcentered

	# write header
	design = open(outDir+"/design.mat",'w')
	design.write("/NumWaves "+str(2+len(cat_covars)+len(cont_covars))+'\n')
	design.write("/NumPoints "+str(len(sbjs))+'\n')
	design.write("/Matrix\n")

	# write the matrix
	i=0
	while i<len(sbjs):
		design.write(grpcols[i])
		for cov in cat_covars:
			values = othercols[cov]
			design.write(' '+str(values[i]))
		for cov in cont_covars:
			values = othercols[cov]
			design.write(' '+str(values[i]))
		design.write('\n')
		i+=1
	design.close()

	### 5. Generate the contrast file ###
	# write header
	contrast = open(outDir+"/design.con",'w')
	contrast.write("/NumWaves "+str(2+len(cat_covars)+len(cont_covars))+'\n')
	contrast.write("/ContrastName1 "+groups[0]+'>'+groups[1]+'\n')
	contrast.write("/ContrastName2 "+groups[1]+'>'+groups[0]+'\n')
	i=3
	for cov in cat_covars:
		contrast.write("/ContrastName"+str(i)+' '+cov+"+\n")
		contrast.write("/ContrastName"+str(i)+' '+cov+"-\n")
		i+=1
	for cov in cont_covars:
		contrast.write("/ContrastName"+str(i)+' '+cov+"+\n")
		contrast.write("/ContrastName"+str(i)+' '+cov+"-\n")
		i+=1
	contrast.write("/NumContrasts "+str(2+2*len(cat_covars)+2*len(cont_covars))+'\n')
	contrast.write("/Matrix\n")

	# write the matrix
	n = len(cat_covars)+len(cont_covars)
	contrast.write("1 -1"+n * " 0"+'\n')
	contrast.write("-1 1"+n * " 0"+'\n')
	pos = 2
	while pos<(n+2):
		line1="0 0"
		line2="0 0"
		i = 2
		while i<(n+2):
			if i==pos:
				line1+=" 1"
				line2+=" -1"
			else:
				line1+=" 0"
				line2+=" 0"
			i+=1
		pos+=1
		contrast.write(line1+'\n')
		contrast.write(line2+'\n')
	contrast.close()

	cursor.close()

def recoverPipestepsDesc(cnx):
	cursor = cnx.cursor()
	recfolder = input("Recovery folder path: ")
	db = cnx.database
	recfile = open(recfolder+'/'+db+".pipesteps.txt",'r')
	for line in recfile:
		line = line.replace('\n','')
		array = line.split('\t')
		step = array[0].replace("\"",'')
		desc = array[2].replace("\"",'')
		if desc=="\\N":
			cursor.execute("update pipesteps set description=NULL where step='"+step+"'")
		else:
			cursor.execute("update pipesteps set description='"+desc+"' where step='"+step+"'")
	recfile.close()
	cnx.commit()
	cursor.close()