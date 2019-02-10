#include <vector>
#include <string>
#include <iostream>

#include "Schema.h"
#include "Catalog.h"

using namespace std;
//https://www.tutorialspoint.com/sqlite/sqlite_c_cpp.htm
//https://www.geeksforgeeks.org/sql-using-c-c-and-sqlite/


using namespace std;

int main () {

	string dbFile = "catalog.sqlite";
	Catalog c(dbFile);

	int UserChoice;

	cout << "Welcome to the CSE 177 Main Menu" << endl;
	cout << "Press 1 to Create a table" << endl;
	cout << "Press 2 to Drop a table" << endl;
	cout << "Press 3 to Diplay the content of the Catalog" << endl;
	cout << "Press 4 to Save the content of the Catalog to the Database" << endl;
	cin >> UserChoice;

	while (UserChoice != 0){
	
		if(UserChoice == 1){

			string tableName;
			string attriName;
			string attriType;
			int numAtt;
			unsigned int dVal;
			vector<string> attriNV;
			vector<string> attriTV;
			vector<unsigned int> dVals;

			cout << "Enter the name of the Table" << endl;
			cin >> tableName;
			cout << "Enter the number of attributes you want to put in" << endl;
			cin >> numAtt;

			for(int i = 0; i < numAtt; i++){

				cout << "Enter the " << i << " attribute name" << endl;
				cin >> attriName;
				attriNV.push_back(attriName);
				
				cout << "Enter the " << i << " attribute type" << endl;
				cin >> attriType;
				attriTV.push_back(attriType);

				cout << "Enter the number of Distinct values for the " << i << " attribute" << endl;
				cout << "Enter 0 if you do not know" << endl;
				cin >> dVal;
				dVals.push_back(dVal);

			}

			c.CreateTable(tableName, attriNV, attriTV);

			for(int i = 0; i < attriNV.size(); i++){

				c.SetNoDistinct(tableName, attriNV[i], dVals[i]);

			}

			break;
			

		}

		else if(UserChoice == 2){
			
			int usec;
			cout << "Press 1 if you know the name of the table to drop" << endl;
			cout << "Press 2 to see a list of tables" << endl;
			cin >> usec;

			if(usec == 1){

				cout << "Enter the table name" << endl;
				string target;
				cin >> target;
				if(c.DropTable(target)){

					cout << "Deleted Table" << endl;

				}else{

					cout << "Error deleting" << endl;

				}

			}

			if(usec == 2){
			
				vector<string> tablesg;
				c.GetTables(tablesg);
				for (vector<string>::iterator it = tablesg.begin();
					
					it != tablesg.end(); it++) {
					cout << *it << endl;
				}

				cout << " " << endl;


				cout << "Enter the table name" << endl;
				string target;
				cin >> target;
				if(c.DropTable(target)){

					cout << "Deleted Table" << endl;

				}else{

					cout << "Error deleting" << endl;

				}
				

			}else{

				cout << "invalid choice" << endl;

			}

			break;

		}

		else if(UserChoice == 3){
	
			c.Print();
			break;

		}

		else if(UserChoice == 4){

			c.Save();
			break;
		
		}
	
		else{

		cout << "Please make a valid selection" << endl;

		}
	}


/*	string table = "region", attribute, type;
	vector<string> attributes, types;
	vector<unsigned int> distincts;

	attribute = "r_regionkey"; attributes.push_back(attribute);
	type = "INTEGER"; types.push_back(type);
	distincts.push_back(5);
	attribute = "r_name"; attributes.push_back(attribute);
	type = "STRING"; types.push_back(type);
	distincts.push_back(5);
	attribute = "r_comment"; attributes.push_back(attribute);
	type = "STRING"; types.push_back(type);
	distincts.push_back(5);

	Schema s(attributes, types, distincts);
	Schema s1(s), s2; s2 = s1;

	string a1 = "r_regionkey", b1 = "regionkey";
	string a2 = "r_name", b2 = "name";
	string a3 = "r_commen", b3 = "comment";

	s1.RenameAtt(a1, b1);
	s1.RenameAtt(a2, b2);
	s1.RenameAtt(a3, b3);

	s2.Append(s1);

	vector<int> keep;
	keep.push_back(5);
	keep.push_back(0);
	s2.Project(keep);

	cout << "PRINTING S TO S2" << endl;
	cout << s << endl;
	cout << s1 << endl;
	cout << s2 << endl;


	string dbFile = "catalog.sqlite";
	Catalog c(dbFile);

	cout << "LISTING TABLE NAME" << endl;
	cout << table << endl;

	cout << "LISTING ATTRIBUTE NAME" << endl;
	for(int i = 0; i < attributes.size(); i++){

		cout << attributes[i] << endl;

	}

	cout << "LISTING TYPE" << endl;
	for(int i = 0; i < types.size(); i++){

		cout << types[i] << endl;

	}


	c.CreateTable(table, attributes, types);

	cout << "PRINTING C" << endl;
	cout << c << endl;

	Catalog catalog(dbFile);
	int tNo = 5;
	int aNo = 10;

	for (int i = 0; i < 5; i++) {
		char tN[20]; sprintf(tN, "T_%d", i);
		string tName = tN;

		bool ret = catalog.DropTable(tName);
		if (true == ret) {
			cout << "DROP TABLE " << tName << " OK" << endl;
		}
		else {
			cout << "DROP TABLE " << tName << " FAIL" << endl;
		}
	}

	for (int i = 0; i < tNo; i++) {
		char tN[20]; sprintf(tN, "T_%d", i);
		string tName = tN;

		int taNo = i * aNo;
		vector<string> atts;
		vector<string> types;
		for (int j = 0; j < taNo; j++) {
			char aN[20]; sprintf(aN, "A_%d_%d", i, j);
			string aName = aN;
			atts.push_back(aN);

			string aType;
			int at = j % 3;
			if (0 == at) aType = "INTEGER";
			else if (1 == at) aType = "FLOAT";
			else if (2 == at) aType = "STRING";
			types.push_back(aType);
		}


		bool ret = catalog.CreateTable(tName, atts, types);
		if (true == ret) {
			cout << "CREATE TABLE " << tName << " OK" << endl;

			for (int j = 0; j < taNo; j++) {
				unsigned int dist = i * 10 + j;
				string aN = atts[j];
				catalog.SetNoDistinct(tName, atts[j], dist);
			}

			unsigned int tuples = i * 1000;
			catalog.SetNoTuples(tName, tuples);

			string path = tName + ".dat";
			catalog.SetDataFile(tName, path);
		}
		else {
			cout << "CREATE TABLE " << tName << " FAIL" << endl;
		}
	}

	catalog.Save();
	cout << catalog << endl; cout.flush();

	cout << "GOT HERE" << endl;

	////////////////////////////////
	vector<string> tables;
	catalog.GetTables(tables);
	for (vector<string>::iterator it = tables.begin();
		 it != tables.end(); it++) {
		cout << *it << endl;
	}
	cout << endl;

	cout << "GOT TO HERE" << endl;


	////////////////////////////////
	for (int i = 0; i < 1000; i++) {
		int r = rand() % tNo + 1;
		char tN[20]; sprintf(tN, "T_%d", r);
		string tName = tN;

		unsigned int tuples;
		catalog.GetNoTuples(tName, tuples);
		cout << tName << " tuples = " << tuples << endl;

		string path;
		catalog.GetDataFile(tName, path);
		cout << tName << " path = " << path << endl;

		cout << "STARTING ATTS" << endl;

		vector<string> atts;
		catalog.GetAttributes(tName, atts);
		for (vector<string>::iterator it = atts.begin();
			 it != atts.end(); it++) {
			cout << *it << " ";
		}
		cout << endl;

		cout << "ENDING ATTS" << endl;

		Schema schema;
		catalog.GetSchema(tName, schema);
		cout << schema << endl;

		////////////////////////////////
		for (int j = 0; j < 10; j++) {
			int s = rand() % (r * aNo) + 1;
			char aN[20]; sprintf(aN, "A_%d_%d", r, s);
			string aName = aN;

			unsigned int distinct;
			catalog.GetNoDistinct(tName, aName, distinct);
			cout << tName << "." << aName << " distinct = " << distinct << endl;
		}
	}

	////////////////////////////////
	for (int i = 0; i < 5; i++) {
		char tN[20]; sprintf(tN, "T_%d", i);
		string tName = tN;

		bool ret = catalog.DropTable(tName);
		if (true == ret) {
			cout << "DROP TABLE " << tName << " OK" << endl;
		}
		else {
			cout << "DROP TABLE " << tName << " FAIL" << endl;
		}
	}
*/
	return 0;
}
