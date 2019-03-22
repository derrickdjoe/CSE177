#include <iostream>
#include <cstring>

#include "Config.h"
#include "DBFile.h"
#include "Catalog.h"
#include "Schema.h"

using namespace std;

bool createDBFile(Schema _schema, string _heapPath, string _textPath){

	cout << "IN CREATE" << endl;

	char* heapPath = new char[_heapPath.length() + 1];
	char* textPath = new char[_textPath.length() + 1];
	strcpy(heapPath, _heapPath.c_str());
	strcpy(textPath, _textPath.c_str());

	cout << "CONVERTED TO CHAR*" << endl;

	DBFile dbFile;
	if(dbFile.Create(heapPath, Heap) == -1){

		cout << "CREATE FAILED IN DBCREATE" << endl;
		return false;

	}
	
	cout << "CREATE WORKED" << endl;

	dbFile.Load(_schema, textPath);
	cout << "LOAD WORKED" << endl;

	if(dbFile.Close() == -1){

		cout << "CLOSE FAILED" << endl;

	}

	if(dbFile.Open(heapPath) == 0){
	
		cout << "OPEN GOOD" << endl;

	}

	dbFile.MoveFirst();
	cout << "MOVED TO TOP" << endl;

	/*while(true){

		Record rec;
		if(dbFile.GetNext(rec) == 0){

			cout << "PRINTING A VALUE" << endl;
			rec.print(cout, _schema);
			cout << endl;

		}else{

			cout << "BREAKING" << endl;
			break;

		}

	}*/

	if(dbFile.Close() == -1){

		cout << "CLOSE FAILED IN DBCLOSE" << endl;
		return false;

	}

	cout << "BUILT CORRECTLY" << endl;
	return true;

}

bool testFile(Schema _schema, string _heapPath){

	char* heapPath = new char[_heapPath.length() + 1];
	strcpy(heapPath, _heapPath.c_str());

	DBFile dbFile;
	if(dbFile.Open(heapPath) == -1){

		cout << "CANT OPEN" << endl;
		return false;

	}

	dbFile.MoveFirst();
	cout << "MOVED TO TOP" << endl;

	while(true){

		Record rec;
		if(dbFile.GetNext(rec) == 0){

			
			rec.print(cout, _schema);
			cout << endl;

		}else{

			break;

		}

	}

	cout << "TRYING TO CLOSE" << endl;
	if(dbFile.Close() == -1){

		cout << "CANT CLOSE" << endl;
		return false;

	}

	cout << "CLOSED DONE" << endl;
	return true;

}

int main(int argc, char* argv[]){

	string dbFile = "catalog.sqlite";
	Catalog catalog(dbFile);

	string uInput;
	cout << "Create new DBFiles?" << endl;
	cin >> uInput;

	if(uInput == "1"){

		//string tableName;
		//string heapPath;
		//string textPath;
/*		
		string tableName = "customer";
		string heapPath = "/home/user/Documents/TPCH/customer.dat";
		string textPath = "/home/user/Documents/TPCH/customer.tbl";

		string tableName = "supplier";
		string heapPath = "/home/user/Documents/TPCH/supplier.dat";
		string textPath = "/home/user/Documents/TPCH/supplier.tbl";

		string tableName = "nation";
		string heapPath = "/home/user/Documents/TPCH/nation.dat";
		string textPath = "/home/user/Documents/TPCH/nation.tbl";
		
		string tableName = "region";
		string heapPath = "/home/user/Documents/TPCH/region.dat";
		string textPath = "/home/user/Documents/TPCH/region.tbl";
		
		string tableName = "orders";
		string heapPath = "/home/user/Documents/TPCH/orders.dat";
		string textPath = "/home/user/Documents/TPCH/orders.tbl";
*/		
		string tableName = "lineitem";
		string heapPath = "/home/user/Documents/TPCH/lineitem.dat";
		string textPath = "/home/user/Documents/TPCH/lineitem.tbl";
/*		
		string tableName = "part";
		string heapPath = "/home/user/Documents/TPCH/part.dat";
		string textPath = "/home/user/Documents/TPCH/part.tbl";
		
		string tableName = "partsupp";
		string heapPath = "/home/user/Documents/TPCH/partsupp.dat";
		string textPath = "/home/user/Documents/TPCH/partsupp.tbl";
		*/
		//cout << "Enter Table Name" << endl;
		//cin >> tableName;
		//cout << "Enter heap Path" << endl;
		//cin >> heapPath;
		//cout << "Enter text Path" << endl;
		//cin >> textPath;

		Schema sch;
		if(!catalog.GetSchema(tableName, sch)){

			cout << "NO TABLE" << endl;

		}

		cout << "TBL FOUND" << endl;

		if(createDBFile(sch, heapPath, textPath)){

			catalog.SetDataFile(tableName, heapPath);
			cout << "SET DATA FILE WORK" << endl;
			cout << catalog.Save() << endl;

			if(catalog.Save()){

				cout << "WORK" << endl;		
				return 0;

			}else{

				cout << "DIDNT SAVE CATALOG" << endl;
				return -1;

			}

		}else{

			cout << "CREATE ERROR" << endl;

		}

	}

	if(uInput == "2"){

		string tableName = "lineitem";
		string heapPath = "/home/user/Documents/TPCH/lineitem.dat";

		Schema sch;
		if(!catalog.GetSchema(tableName, sch)){

			cout << "NO TABLE" << endl;

		}


		if(testFile(sch, heapPath)){
			
			cout << "DONE" << endl;

		}else{

			cout << "TESTFILE ERROR" << endl;
		}

	}

	//catalog.Save();
	//return 0;

}
		

	

