#include "helper.h"

using namespace std;

Helper::Helper(Catalog& _catalog) : catalog(&_catalog) {}

Helper::~Helper() {}

void Helper::createTable(char* _table, AttTypeList* _attTL){

	string table = _table;
	vector <string> attL;
	vector <string> attT;

	cout << "IN CREATE TABLE" << endl;

	while(_attTL != NULL){

		string name = string(_attTL->name);
		attL.push_back(name);

		string type = string(_attTL->type);
		if(type == "INTEGER" || type == "Integer"){

			attT.push_back("INTEGER");
			cout << "pushed back" << endl;

		}else if(type == "FLOAT" || type == "Float"){

			attT.push_back("FLOAT");
			cout << "pushed back" << endl;

		}else if(type == "STRING" || type == "String"){

			attT.push_back("STRING");
			cout << "pushed back" << endl;

		}else{

			cout << "Can't match type" << endl;
			return;

		}

		_attTL = _attTL->next;

	}

	if(!catalog->CreateTable(table, attL, attT)){
		
		cout << "FAILED CREATE TABLE" << endl;

	}

	cout << "DID CREATE TABLE" << endl;

}

void Helper::dropTable(char* _table){

	string table = string(_table);

	if(!catalog->DropTable(table)){

		cout << "FAILED TO DROP " << table << endl;

	}

	cout << "DROPPED TABLE " << table << endl;

}

void Helper::loadData(char* _table, char* _fileLoc){

	string table = string(_table);
	DBFile dbFile;
	string fileLoc;
	Schema sch;

	if(!catalog->GetDataFile(table, fileLoc) || !catalog->GetSchema(table, sch)){

		return;

	}

	char* tempLoc = new char[fileLoc.length() + 1];
	strcpy(tempLoc, fileLoc.c_str());

	if(dbFile.Open(tempLoc)){

		dbFile.Load(sch, _fileLoc);

		if(dbFile.Close() == -1){

			return;

		}

		cout << "LOADED DATA INTO " << table << endl;

	}else{

		return;

	}

}
