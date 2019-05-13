#include <string>
#include <iostream>
#include <sys/stat.h>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;


DBFile::DBFile () : fileName("") {

	isFirst = false;

}

DBFile::~DBFile () {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {
	
	fileName = f_path;
	return file.Open(0, f_path);

}

int DBFile::Open (char* f_path) {

	fileName = f_path;
	
	struct stat fileStat;
	if(stat(f_path, &fileStat) != 0){

		//cout << "CREATING FILE CANT FIND" << endl;
		return Create(f_path, Heap);

	}else{

		//cout << "FOUND FILE" << endl;
		//cout << fileStat.st_size << " <- SIZE FOUND " << endl;
		return file.Open(fileStat.st_size, f_path);

	}

}

void DBFile::Load (Schema& schema, char* textFile) {

	//cout << schema << endl;
	//cout << textFile << endl;

	MoveFirst();
	//cout << "MOVE FIRST WORKED" << endl;
	FILE* f = fopen(textFile, "r");
	//cout << "OPEN WORKED" << endl;
	/*Record rec;

	while(rec.ExtractNextRecord(schema, *f)){
		
		AppendRecord(rec);

	}

	file.AddPage(page, currPage++);
	//page.EmptyItOut();
	fclose(f);

	cout << file.GetLength() << endl;
	cout << "LENGTH" << endl;*/

	while(true){

		Record rec;
		if(rec.ExtractNextRecord(schema, *f)){

			AppendRecord(rec);

		}else{

			break;

		}

	}

	file.AddPage(page, currPage++);
	page.EmptyItOut();
	//cout << file.GetLength() << " FILE SIZE " << endl;
	//cout << currPage << " PAGES " << endl;
	fclose(f);

}

int DBFile::Close () {

	return file.Close();

}

void DBFile::MoveFirst () {

	//cout << "IN MOVE FIRST" << endl;
	currPage = 0;
	isFirst = true;
	page.EmptyItOut();
	//cout << "EMPTY IT OUT WORKED" << endl;
	file.GetPage(page, currPage);
	//cout << "GET PAGE TO FIRST STEP" << endl;

}

void DBFile::AppendRecord (Record& rec) {

	while(!page.Append(rec)){
		
		//cout << "PAGE FULL MAKING NEW ONE" << endl;
		file.AddPage(page, currPage++);
		page.EmptyItOut();
		//page.Append(rec);
		//cout << file.GetLength() << " FILE SIZE " << endl;
		//cout << currPage << " PAGES " << endl;

	}

}

int DBFile::GetNext (Record& rec) {

	//cout << "DBFILE GET NEXT" << endl;
	/*
	if(!isFirst){

		cout << "MOVING TO START" << endl;
		MoveFirst();

	}

	int ret = page.GetFirst(rec);
	if(ret){

		cout << "Stuff on page" << endl;
		cout << currPage << endl;
		cout << file.GetLength() << endl;
		cout << "---------------------------" << endl;
		return true;
		
	}else{

		cout << "NOTHING ON PAGE" << endl;
		if(currPage == file.GetLength()){
			
			cout << currPage << endl;
			cout << file.GetLength() << endl;
			cout << "AT THE END" << endl;
			return false;

		}else{

			currPage++;
			cout << "GETTING NEXT PAGE" << endl;
			file.GetPage(page, currPage);	
			ret = page.GetFirst(rec);
			return true;

		}

	}*/

	/*if(!isFirst){

		cout << "MOVING TO START" << endl;
		MoveFirst();

	}*/

	while(true){

		if(!page.GetFirst(rec)){

			if(currPage == file.GetLength()){

				//break;
				return -1;

			}

			cout << "GETTING " << currPage + 1 << " PAGE " << endl;
			file.GetPage(page, currPage++);
			page.GetFirst(rec);
			return 0;

		}else{

			//cout << "SOMETHING HERE PRINTING" << endl;
			return 0;

		}

		break;

	}

	return -1;

}

void DBFile::SetZero (){

	//cout << "BEEP BEEP SET ZERO" << endl;
	currPage = 0;
}
