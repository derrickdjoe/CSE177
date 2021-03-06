#include <iostream>
#include <string>
#include <cstring>

#include "Catalog.h"
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "RelOp.h"
#include "helper.h"

extern "C" {

	#include "QueryParser.h"

}

using namespace std;


// these data structures hold the result of the parsing
extern struct FuncOperator* finalFunction; // the aggregate function
extern struct TableList* tables; // the list of tables in the query
extern struct AndList* predicate; // the predicate in WHERE
extern struct NameList* groupingAtts; // grouping attributes
extern struct NameList* attsToSelect; // the attributes in SELECT
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query

extern char* toggle;
extern char* fileLoc;
extern char* table;
extern struct AttTypeList* attTypes;
extern struct DirectoryList* dList;

extern "C" int yyparse();
extern "C" int yylex_destroy();


int main () {
	// this is the catalog
	string dbFile = "catalog.sqlite";
	Catalog catalog(dbFile);

	// this is the query optimizer
	// it is not invoked directly but rather passed to the query compiler
	QueryOptimizer optimizer(catalog);

	// this is the query compiler
	// it includes the catalog and the query optimizer
	QueryCompiler compiler(catalog, optimizer);

	Helper helper(catalog);


	// the query parser is accessed directly through yyparse
	// this populates the extern data structures

	while (true){

		cout << "Parsing" << endl;

		int parse = -1;
		if (yyparse () == 0) {
			cout << "OK!" << endl;
			parse = 0;
		}
		else {
			cout << "Error: Query is not correct!" << endl;
			parse = -1;
		}
	
		yylex_destroy();

		//cout << "Done Parsing" << endl;

		if(parse == 0){

			if(table != NULL && attTypes != NULL){
			
				helper.createTable(table, attTypes);
				catalog.Save();

			}else if(table != NULL && dList != NULL && fileLoc != NULL){

				helper.loadData(table, dList, fileLoc);
				catalog.Save();

			}else if(table != NULL){

				helper.dropTable(table);
				catalog.Save();

			}else if(toggle != NULL){

				string toString = string(toggle);

				if(toString == "Exit"){

					cout << "Exiting" << endl;
					catalog.Save();
					return 0;

				}else if(toString == "Schema"){
	
					//cout << "TOGGLE IS SHCMEA" << endl;
					catalog.Print();

				}

			}else{

				
	
		//if (parse != 0) return -1;
	
				// at this point we have the parse tree in the ParseTree data structures
				// we are ready to invoke the query compiler with the given query
				// the result is the execution tree built from the parse tree and optimized
				QueryExecutionTree queryTree;
				compiler.Compile(tables, attsToSelect, finalFunction, predicate,
						groupingAtts, distinctAtts, queryTree);
	
				cout << queryTree << endl;
	
				//cout << "BEEP BEEP DOING EXECUTE QUERY" << endl;
				queryTree.ExecuteQuery();
				//cout << "BEEP BEEP DONE EXECUTE QUERY" << endl;
	
				cout << "DONE" << endl;

				//catalog.Print();

				catalog.Save();

			}

		}

		finalFunction = NULL;
		tables = NULL;
		predicate = NULL;
		groupingAtts = NULL;
		attsToSelect = NULL;
		distinctAtts = 0;
		toggle = NULL;
		fileLoc = NULL;
		table = NULL;
		attTypes = NULL;
		dList = NULL;
		
	}

	return 0;
}
