#include "helper.h"
#include "Config.h"
#include "DBFile.h"
#include "Catalog.h"
#include "Schema.h"
#include <algorithm>

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

		//cout << _attTL->name << endl;
		//cout << _attTL->type << endl;

		string type = string(_attTL->type);
		if(type == "INTEGER" || type == "Integer"){

			attT.push_back("INTEGER");
			//cout << "pushed back" << endl;

		}else if(type == "FLOAT" || type == "Float"){

			attT.push_back("FLOAT");
			//cout << "pushed back" << endl;

		}else if(type == "STRING" || type == "String"){

			attT.push_back("STRING");
			//cout << "pushed back" << endl;

		}else{

			cout << "Can't match type" << endl;
			return;

		}

		_attTL = _attTL->next;

	}

	reverse(attL.begin(), attL.end());
	reverse(attT.begin(), attT.end());

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

void Helper::loadData(char* _table, DirectoryList* _dList, char* _fileLoc){

	vector<string> nameHolder;

	while(_dList->next != NULL){

		//cout << _dList->name << endl;
		string temp = string(_dList->name);

		nameHolder.push_back(temp);

		_dList = _dList->next;

	}

	if(_dList->next == NULL){

		//cout << _dList->name << endl;
		string temp = string(_dList->name);
		nameHolder.push_back(temp);

	}

	string finalString = "/";
	reverse(nameHolder.begin(), nameHolder.end());
	
	for(int i = 0; i < nameHolder.size(); i++){

		finalString.append(nameHolder[i]);

		if(i < nameHolder.size() - 1){

			finalString.append("/");

		}

	}

	string forCreate;
	forCreate.append(finalString);
	forCreate.append(".dat");

	char* forC = new char[forCreate.length() + 1];
	strcpy(forC, forCreate.c_str());

	string ext = string(_fileLoc);
	finalString.append(".");
	finalString.append(ext);


	cout << "THIS IS THE STRING DONE " << finalString << endl;

	char* convert = new char[finalString.length() + 1];
	strcpy(convert, finalString.c_str());

	string table = string(_table);
	DBFile dbFile;
	string fileLoc;
	Schema sch;

	if(!catalog->GetDataFile(table, fileLoc) || !catalog->GetSchema(table, sch)){

		cout << "FAILED" << endl;
		return;

	}

	if(fileLoc == ""){

		cout << table << " HAS NO DBFILE, CREATING NEW ONE" << endl;

		cout << "CREATING " << forC << endl;

		if(dbFile.Create(forC, Heap) == -1){

			cout << "CREATE FAILED" << endl;
			return;

		}

		catalog->SetDataFile(table, forCreate);

		cout << "LOADING DATA FROM " << convert << endl;
		dbFile.Load(sch, convert);
		//cout << "LOADED DATA" << endl;

		if(dbFile.Close() == -1){

			cout << "CLOSED FAILED" << endl;

		}

		if(dbFile.Open(convert) == 0){

			//cout << "GOOD" << endl;

		}

		dbFile.MoveFirst();

		if(dbFile.Close() == -1){

			cout << "CLOSE FAILED" << endl;
			return;

		}

		cout << "LOADED DATA INTO " << table << endl;


	}else{

		//cout << "HERE" << endl;
		char* convertPath = new char[fileLoc.length() + 1];
		strcpy(convertPath, fileLoc.c_str());

		if(dbFile.Open(convertPath) == -1){

			cout << "OPEN FAILED" << endl;
			return;

		}

		dbFile.Load(sch, convert);
		//cout << "LOADED DATA" << endl;

		if(dbFile.Close() == -1){

			return;

		}

		if(dbFile.Open(convert) == 0){

			//cout << "GOOD" << endl;

		}

		dbFile.MoveFirst();

		if(dbFile.Close() == -1){

			cout << "CLOSE FAILED" << endl;
			return;

		}

		cout << "LOADED DATA INTO " << table << endl;

	}

}
