#include <iostream>
#include "sqlite3.h"
#include <string.h>
#include <stdlib.h>
#include "Schema.h"
#include "Catalog.h"

using namespace std;

Catalog::Catalog(string& _fileName) {

	string sql;
	char *zErrMsg = 0;
	int rc;
	sqlite3_stmt *stmt;
	string lastTbl;

	rc = sqlite3_open(_fileName.c_str(), &db);

	if(rc){

		fprintf(stderr, "Can't open db : %s\n", sqlite3_errmsg(db));

	}else{

	//	fprintf(stderr, "Opened db %s\n", _fileName.c_str());
		sql = "SELECT t.name, t.noTuples, t.path, a.name , a.type, a.noDistinct, a.atO, t.tum FROM tableL t, attributeL a WHERE t.name == a.tblN ORDER BY t.tum, a.atO;";
		rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, 0);

		//cout << "Got to SELECT" << endl;

		while((rc = sqlite3_step(stmt)) == SQLITE_ROW){

			string tName = reinterpret_cast<const char*> (sqlite3_column_text(stmt, 0));
			
			//fprintf(stdout, "VALUE FOR tNAME IS  %s \n", tName.c_str());

			if(tName != lastTbl){

				lastTbl = tName;
				unsigned int tnt = sqlite3_column_int(stmt, 1);
				string tPath = reinterpret_cast<const char*> (sqlite3_column_text(stmt, 2));
				unsigned int tblIDN = sqlite3_column_int(stmt, 7);
	
				tbld temp;
				temp.name = tName;
				temp.tupN = tnt;
				temp.fileL = tPath;
				//cout << "STORING " << tblIDN << " AS " << tName << " ID " << endl;
				temp.tNum = tblIDN;

				string aName = reinterpret_cast<const char*> (sqlite3_column_text(stmt, 3));
				string aType = reinterpret_cast<const char*> (sqlite3_column_text(stmt, 4));
				unsigned int noD = sqlite3_column_int(stmt, 5);
				unsigned int attIDN = sqlite3_column_int(stmt, 6);

				//cout << "STORING " << aName << " AS " << attIDN << " ID " << endl;

				atdata temp1;
				temp1.attName = aName;
				temp1.attType = aType;
				temp1.disVal = noD;
				temp1.numOrder = attIDN;


				temp.attble.push_back(temp1);
				tableData.push_back(temp);

				//fprintf(stdout, "PUSHED \t%s \t%s \t%u \tINTO \t%s\n", aName.c_str(), aType.c_str(), noD, tName.c_str());

			}else{

				for(int i = 0; i < tableData.size(); i++){

					if(tableData[i].name == tName){

						string aName = reinterpret_cast<const char*> (sqlite3_column_text(stmt, 3));
						string aType = reinterpret_cast<const char*> (sqlite3_column_text(stmt, 4));
						unsigned int noD = sqlite3_column_int(stmt, 5);
						unsigned int attIDN = sqlite3_column_int(stmt, 6);

						//cout << "STORING " << aName << " AS " << attIDN << " ID " << endl;

						atdata temp1;
						temp1.attName = aName;
						temp1.attType = aType;
						temp1.disVal = noD;
						temp1.numOrder = attIDN;


						tableData[i].attble.push_back(temp1);
						//cout << "Put into " << tableData[i].name << endl;
						//fprintf(stdout, "PUSHED \t%s \t%s \t%u \tINTO \t%s\n", aName.c_str(), aType.c_str(), noD, tName.c_str());

					}
	
				}

			}

		}

		sqlite3_finalize(stmt);
		if(rc != SQLITE_DONE){

			fprintf(stderr, "SQL error : %s\n", zErrMsg);
			sqlite3_free(zErrMsg);

		}

		//cout << "COPIED TABLE DATA" << endl;

		//schema helper
		
		//vector<string> schHelper;
		//vector<string> schHelper1;
		//vector<unsigned int>schHelper2;

		for(int i = 0; i < tableData.size(); i++){

			vector<string> schHelper;
			vector<string> schHelper1;
			vector<unsigned int>schHelper2;

			for(int j = 0; j < tableData[i].attble.size(); j++){

				//cout << "putting atts into " << tableData[i].name << endl;

				schHelper.push_back(tableData[i].attble[j].attName);
				schHelper1.push_back(tableData[i].attble[j].attType);
				schHelper2.push_back(tableData[i].attble[j].disVal);

			}
			
			//cout << "Assigning schema to " << tableData[i].name << endl;
			tableData[i].sch = Schema(schHelper, schHelper1, schHelper2);

			//cout << "--------------" << endl;
			//cout << "Table " << i << endl;
			//cout << tableData[i].name << endl;
			//cout << tableData[i].sch << endl;

		}

		//cout << "BUILT SCHEMA HELPER" << endl;
		//Print();
	}
		
}

Catalog::~Catalog() {

	Save();

}

bool Catalog::Save() {

	string sql;
	int rc;
	char *zErrMsg = 0;
	sqlite3_stmt *stmt;
	
	sql = "DROP TABLE attributeL";

	rc = sqlite3_exec(db, sql.c_str(), 0, 0, &zErrMsg);

	if(rc != SQLITE_OK){

		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);

	}

	//cout << "DELETED ATTRIBUTE TABLE" << endl;

	sql = "DROP TABLE tableL";
	
	rc = sqlite3_exec(db, sql.c_str(), 0, 0, &zErrMsg);

	if(rc != SQLITE_OK){

		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);

	}

	//cout << "DELETED TABLE TABLE" << endl;

	sql = "CREATE TABLE attributeL (name STRING NOT NULL, type STRING NOT NULL, noDistinct INTEGER NOT NULL, tblN STRING NOT NULL, atO INTEGER NOT NULL)";

	rc = sqlite3_exec(db, sql.c_str(), 0, 0, &zErrMsg);

	if(rc != SQLITE_OK){

		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);

	}

	//cout << "REMADE ATTRIBUTE TABLE" << endl;

	sql = "CREATE TABLE tableL (name STRING NOT NULL, noTuples INTEGER NOT NULL, path STRING NOT NULL, tum INTEGER NOT NULL);";

	rc = sqlite3_exec(db, sql.c_str(), 0, 0, &zErrMsg);

	if(rc != SQLITE_OK){

		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);

	}

	//cout << "REMADE TABLE TABLE" << endl;

	sql = "INSERT INTO attributeL VALUES(?1,?2,?3,?4,?5);";

	rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, 0);

	//cout << "INSERTING INTO ATTRIBUTE TABLE" << endl;
	for(int i = 0; i < tableData.size(); i++){

		for(int j = 0; j < tableData[i].attble.size(); j++){

			sqlite3_reset(stmt);
			rc = sqlite3_bind_text(stmt, 1, tableData[i].attble[j].attName.c_str(), tableData[i].attble[j].attName.size(), SQLITE_STATIC);
			rc = sqlite3_bind_text(stmt, 2, tableData[i].attble[j].attType.c_str(), tableData[i].attble[j].attType.size(), SQLITE_STATIC);
			rc = sqlite3_bind_int(stmt, 3, tableData[i].attble[j].disVal);
			rc = sqlite3_bind_text(stmt, 4, tableData[i].name.c_str(), tableData[i].name.size(), SQLITE_STATIC);
			rc = sqlite3_bind_int(stmt, 5, tableData[i].attble[j].numOrder);
			rc = sqlite3_step(stmt);
			
			//cout << "PUTTING " << tableData[i].attble[j].numOrder << " ----- " << endl;
			//fprintf(stdout, "INSERTING THE FOLLOWING INTO %s\n", tableData[i].name.c_str());
			//fprintf(stdout, "%s \t%s \t%u \n", tableData[i].attble[j].attName.c_str(), tableData[i].attble[j].attType.c_str(), tableData[i].attble[j].disVal);

			if(rc != SQLITE_DONE){

				printf("ERROR %s\n", sqlite3_errmsg(db));

			}

		}

	}
	sqlite3_finalize(stmt);
	//cout << "DATAFIED ATTRIBUTE TABLE" << endl;


	sql = "INSERT INTO tableL VALUES(?1, ?2, ?3, ?4);";
	
	rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, 0);

	//cout << "INSERTING INTO TABLE TABLE" << endl;
	for(int i = 0; i < tableData.size(); i++){

		sqlite3_reset(stmt);
		rc = sqlite3_bind_text(stmt, 1, tableData[i].name.c_str(), tableData[i].name.size(), SQLITE_STATIC);
		rc = sqlite3_bind_int(stmt, 2, tableData[i].tupN);
		rc = sqlite3_bind_text(stmt, 3, tableData[i].fileL.c_str(), tableData[i].fileL.size(), SQLITE_STATIC);
		rc = sqlite3_bind_int(stmt, 4, tableData[i].tNum);
		rc = sqlite3_step(stmt);
		
		//cout << "PUTTING " << tableData[i].tNum << " AS  " << tableData[i].name << " ID " << endl;
		//fprintf(stdout, "%s \t%u \t%s\n", tableData[i].name.c_str(), tableData[i].tupN, tableData[i].fileL.c_str());

		if(rc != SQLITE_DONE){

			printf("ERROR %s\n", sqlite3_errmsg(db));

		}

	}

	sqlite3_finalize(stmt);
	return true;
	//cout << "DATAFIED TABLE TABLE" << endl;
	

}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	
	for(int i = 0; i < tableData.size(); i++){

		if(tableData[i].name == _table){

			_noTuples = tableData[i].tupN;
			return true;

		}

	}

	return false;

}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {

	for(int i = 0; i < tableData.size(); i++){

		if(tableData[i].name == _table){

			tableData[i].tupN = _noTuples;

		}

	}

}

bool Catalog::GetDataFile(string& _table, string& _path) {

	for(int i = 0; i < tableData.size(); i++){

		if(tableData[i].name == _table){

			_path = tableData[i].fileL;
			return true;

		}

	}

	return false;
}

void Catalog::SetDataFile(string& _table, string& _path) {

	for(int i = 0; i < tableData.size(); i++){

		if(tableData[i].name == _table){

			tableData[i].fileL = _path;
		}

	}

}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {

	for(int i = 0; i < tableData.size(); i++){

		if(tableData[i].name == _table){

			_noDistinct = tableData[i].sch.GetDistincts(_attribute);
			return true;

		}

	}

	return false;

}
void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {

	for(int i = 0; i < tableData.size(); i++){

		if(tableData[i].name == _table){

			//cout << "FOUND THE TABLE TO INSERT INTO" << endl;

			int temp = tableData[i].sch.Index(_attribute);
			tableData[i].sch.GetAtts()[temp].noDistinct = _noDistinct;

			for(int j = 0; j < tableData[i].attble.size(); j++){

				if(tableData[i].attble[j].attName == _attribute){

					//cout << "FOUND THE ATTRIBUTE TO INSERT INTO" << endl;
					//fprintf(stdout, "INSERTING  %u\n", _noDistinct);

					tableData[i].attble[j].disVal = _noDistinct;

				}

			}

		}

	}

}

void Catalog::GetTables(vector<string>& _tables) {

	for(int i = 0; i < tableData.size(); i++){

		_tables.push_back(tableData[i].name);

	}

}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {

	for(int i = 0; i < tableData.size(); i++){

		if(tableData[i].name == _table){

			//cout << "FOUND THE TABLE" << endl;

			vector<string> temp;
			for(int j = 0; j < tableData[i].attble.size(); j++){

				temp.push_back(tableData[i].attble[j].attName);
				//cout << "PARSING VECTOR" << endl;

			}

			_attributes = temp;
			return true;

		}

	}

	return false;

}

bool Catalog::GetSchema(string& _table, Schema& _schema) {

	for(int i = 0; i < tableData.size(); i++){

		if(tableData[i].name == _table){

			_schema = tableData[i].sch;	
			return true;

		}

	}

	return false;

}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {

	//cout << "TRYING TO BUILD TABLE" << endl;

	for(int i = 0; i < tableData.size(); i++){
		//cout << "TRYING TO SEARCH TABLE" << endl;
		if(tableData[i].name == _table){
			//cout << "LOOKING AT INDEX" << i << endl;
			printf("Rename the Table \n");
			return false;

		}

	}

	//cout << "CHECKING ATTRIBUTES" << endl;
	if(_attributes.size() == 0 || _attributeTypes.size() == 0 || _attributes.size() != _attributeTypes.size()){

		printf("Attribute Error\n");
		
		//cout << "ATTRIBUTE NAMES" << endl;
		for(int i = 0; i < _attributes.size(); i++){

			cout << _attributes[i] << endl;

		}

		//cout << "ATTRIBUTE TYPES" << endl;
		for(int i = 0; i < _attributeTypes.size(); i++){

			cout << _attributeTypes[i] << endl;

		}

		return false;

	}

	int tableiter = 0;
	for(int i = 0; i < tableData.size(); i++){

		tableiter++;

	}

	//cout << "BUILDING TEMP TABLE" << endl;
	tbld tempTableData;
	vector<atdata> tempatdata;
	atdata tempa;
	tempTableData.name = _table;
	//cout << "ASSIGNED NAME" << endl;
	vector<unsigned int> distVal;

	tempTableData.tNum = tableiter + 1;


	for(int i = 0; i < _attributes.size(); i++){
		//cout << "ATTEMPTING ITTERATION " << i << " " << endl;
		tempa.disVal = (0);
		//cout << "ASSIGNED DISTINCT VAL" << endl;
		tempa.attName = _attributes[i];
		//cout << "ASSIGNED ATT NAME " << endl;
		tempa.attType = _attributeTypes[i];
		tempa.numOrder = i;
		//cout << "ASSIGNED ATT TYPE " << endl;
		distVal.push_back(0);
		tempatdata.push_back(tempa);
		//cout << "PUSHING INTO VECTOR " << endl;

	}

	tempTableData.sch = Schema(_attributes, _attributeTypes, distVal);
	tempTableData.attble = tempatdata;
	//cout << "AT END" << endl;
	tableData.push_back(tempTableData);
	return true;
}

bool Catalog::DropTable(string& _table) {

	//cout << "GOT TO METHOD" << endl;
	for(int i = 0; i < tableData.size(); i++){

		//cout << "TRYING LOOP" << endl;
		if(tableData[i].name == _table){

			//cout << "FOUND TABLE TO DROP" << endl;

			tbld temp = tableData[tableData.size() - 1];
			//cout << "COPIED THE LAST ENTRY IN THE TABLE" << endl;

			tableData[i] = temp;
			tableData.erase(tableData.end());

			return true;

		}

	}

	return false;

}

void Catalog::Print(){

	cout << "TESTING PRINT" << endl;

	fprintf(stdout, "\tTable Name \tNum Tuples \tFile Location \n");
	for(int i = 0; i < tableData.size(); i++){

		fprintf(stdout, "\t%s \t %u \t\t%s\n", tableData[i].name.c_str(), tableData[i].tupN, tableData[i].fileL.c_str());
		fprintf(stdout, "\tAttribute Name \tType \t\tDistinct Values \n");

		for(int j = 0; j < tableData[i].attble.size(); j++){

			fprintf(stdout, "\t%s \t %s \t\t%u \n", tableData[i].attble[j].attName.c_str(), tableData[i].attble[j].attType.c_str(), tableData[i].attble[j].disVal);

		}

	}

}

ostream& operator<<(ostream& _os, Catalog& _c) {
	
	cout << "USING OVERLOAD" << endl;

	for(int i = 0; i < _c.getTableData().size(); i++){
		fprintf(stdout, "\tTable Name \tNum Tuples \tFile Location \n");
		fprintf(stdout, "\t%s \t\t %u \t\t%s\n", _c.getTableData()[i].name.c_str(), _c.getTableData()[i].tupN, _c.getTableData()[i].fileL.c_str());
		fprintf(stdout, "\n");

		fprintf(stdout, "\tAttribute Name \tType \t\tDistinct Values \n");
		for(int j = 0; j < _c.getTableData()[i].attble.size(); j++){

			fprintf(stdout, "\t %s \t %s \t\t%u \n", _c.getTableData()[i].attble[j].attName.c_str(), _c.getTableData()[i].attble[j].attType.c_str(), _c.getTableData()[i].attble[j].disVal);

		}

	}

	return _os;
}
