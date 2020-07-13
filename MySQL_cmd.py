# MUST RUN WITH PYTHON3!
import researchDB
from mysql.connector import Error
from mysql.connector import errorcode

def printMenu():
	print(30 * '-', "MENU", 30 * '-')
	print("0. Exit")
	print("1. Initialize database for the first time")
	print("2. Connect to an existing database")
	print("3. See list of tables")
	print("4. Find missing subjects from list")
	print("5. Find subjects missing info from list")
	print("6. Update subjects group from file")
	print("7. Add scan group")
	print("8. Add subjects to step")
	print("9. Update subjects to step")
	print("10. Add demographic data")
	print("11. Generate two-sample unpaired ttest matrices (grp)")
	print("12. Generate two-sample unpaired ttest matrices (scan_grp)")
	print("13. Generate two-sample unpaired ttest matrices (grp, adjusted for scan_grp)")
	print(67 * '-')

def start(cnx):
	loop = True
	while loop:
		printMenu()
		choice = input("Enter your choice [0-13]: ")
			
		if choice == "1":
			print("Initializing database...")
			cnx = researchDB.initDB(cnx)
		elif choice == "2":
			cnx = researchDB.selectDB(cnx)
			db = cnx.database
			if db is None:
				print("Not connected to any database")
			else:
				print("Connected to "+db)
		elif choice == "3":
			if cnx.database is None:
				print("You're not connected to any database")
			else:
				researchDB.showTables(cnx)
		elif choice == "4":
			if cnx.database is None:
				print("You're not connected to any database")
			else:
				researchDB.findMissingSubjects(cnx,'')
		elif choice == "5":
			if cnx.database is None:
				print("You're not connected to any database")
			else:
				column = input("Find missing value from what column?: ")
				researchDB.findMissingSubjects(cnx,column)
		elif choice == "6":
			if cnx.database is None:
				print("You're not connected to any database")
			else:
				researchDB.addSbjGrp(cnx)
		elif choice == "0":
			if cnx.database is not None:
				cnx.close()
			print("Good bye")
			loop = False
		elif choice == "7":
			if cnx.database is None:
				print("You're not connected to any database")
			else:
				researchDB.addScanGrp(cnx)
		elif choice == "8":
			if cnx.database is None:
				print("You're not connected to any database")
			else:
				researchDB.addSbjsToStep(cnx)
		elif choice == "9":
			if cnx.database is None:
				print("You're not connected to any database")
			else:
				researchDB.updateSbjsToStep(cnx)
		elif choice == "10":
			if cnx.database is None:
				print("You're not connected to any database")
			else:
				researchDB.addDemographicData(cnx)
		elif choice == "11":
			if cnx.database is None:
				print("You're not connected to any database")
			else:
				researchDB.ttest(cnx,"grp","CON","EPI")
		elif choice == "12":
			if cnx.database is None:
				print("You're not connected to any database")
			else:
				researchDB.ttest(cnx,"scan_grp","POST","PRE")
		elif choice == "13":
			if cnx.database is None:
				print("You're not connected to any database")
			else:
				researchDB.ttestEV1(cnx,"grp","CON","EPI","scan_grp","POST","PRE")
		else:
			print("Wrong choice, good bye")

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
