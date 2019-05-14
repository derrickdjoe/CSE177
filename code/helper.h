#ifndef HELPER_H
#define HELPER_H

#include <iostream>
#include <cstring>

#include "Catalog.h"
#include "DBFile.h"
#include "ParseTree.h"
#include "Schema.h"
#include "Comparison.h"
#include "Function.h"
#include "Record.h"
#include "Config.h"

using namespace std;

class Helper {

	private:

		Catalog* catalog;

	public:

		Helper(Catalog& _catalog);
		~Helper();

		void createTable(char* _table, AttTypeList* _attTL);
		void dropTable(char* _table);
		void loadData(char* _table, DirectoryList* _dList, char* _fileLoc);

};

#endif
