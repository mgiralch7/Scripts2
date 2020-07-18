# MUST RUN WITH PYTHON3!
import researchDB
from mysql.connector import Error
from mysql.connector import errorcode

def menu1(cnx):
	print(30 * '-', "MENU", 30 * '-')
	print("0. Exit")
	print("1. Initialize database for the first time")
	print("2. Connect to an existing database")
	print(67 * '-')

	choice = input("Enter your choice [0-2]: ")
	if choice == "0":
		print("Good bye")
		return false
	elif choice == "1":
		print("Initializing database...")
		cnx = researchDB.initDB(cnx)
	elif choice == "2":
		cnx = researchDB.selectDB(cnx)
		db = cnx.database
		if db is None:
			print("Not connected to any database")
		else:
			print("Connected to "+db)
	else:
		print("Wrong choice")
	return True

def menu2(cnx):
	print(30 * '-', "MENU", 30 * '-')
	print("0. Exit")
	print("1. See list of tables")
	print("2. Find missing subjects from list")
	print("3. Add missing subjects from list")
	print("4. Find subjects missing info from list")
	print("5. Update subjects group from file")
	print("6. Add scan group")
	print("7. Add subjects to step")
	print("8. Update subjects to step")
	print("9. Add demographic data")
	print("10. Find missing demographic data")
	print("11. Generate two-sample unpaired ttest matrices (grp)")
	print("12. Generate two-sample unpaired ttest matrices (scan_grp)")
	print("13. Generate two-sample unpaired ttest matrices (grp, adjusted for scan_grp)")
	print("14. Generate two-sample unpaired ttest matrices (grp, adjusted for categorical & continous covariates)")
	print("15. Recover pipesteps description")
	print(67 * '-')

	choice = input("Enter your choice [0-15]: ")
	if choice == "0":
		print("Good bye")
		return False
	elif choice == "1":
		researchDB.showTables(cnx)
	elif choice == "2":
		researchDB.findMissingSubjects(cnx,'subjects','')
	elif choice == "3":
		researchDB.addMissingSubjects(cnx)
	elif choice == "4":
		table = input("Search what table? (default subjects): ")
		if len(table)==0:
			table = 'subjects'
		column = input("Find missing value from what column?: ")
		researchDB.findMissingSubjects(cnx,table,column)
	elif choice == "5":
		researchDB.addSbjGrp(cnx)
	elif choice == "6":
		researchDB.addScanGrp(cnx)
	elif choice == "7":
		researchDB.addSbjsToStep(cnx)
	elif choice == "8":
		researchDB.updateSbjsToStep(cnx)
	elif choice == "9":
		researchDB.addDemographicData(cnx)
	elif choice == "10":
		researchDB.findMissingDemographicData(cnx,['Sex','Education','Age','Race','Language1','MotherEduc','DomntHand','FSIQ'])
	elif choice == "11":
		researchDB.ttest(cnx,"grp","CON","EPI")
	elif choice == "12":
		researchDB.ttest(cnx,"scan_grp","POST","PRE")
	elif choice == "13":
		researchDB.ttestEV1(cnx,"grp","CON","EPI","scan_grp","POST","PRE")
	elif choice == "14":
		researchDB.ttestEVs(cnx,"grp",['Sex','Race'],['Education','Age'])
	elif choice == "15":
		researchDB.recoverPipestepsDesc(cnx)
	else:
		print("Wrong choice")
	return True

def start(cnx):
	loop = True
	while loop:
		if cnx.database is None:
			loop = menu1(cnx)
		else:
			loop = menu2(cnx)

def main():
	try:
		cnx = researchDB.connect()
		start(cnx)
		researchDB.disconnect(cnx)
	except Error as err:
		if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
			print("Wrong username or password")
		elif err.errno == errorcode.ER_DB_CREATE_EXISTS:
			print("Database already exists")
			start(cnx)
		else:
			print(err)

if __name__ == "__main__":
	main()
