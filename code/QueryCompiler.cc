#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

#include <iostream>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <limits>

using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {

	unordered_map<string, RelationalOp*> pushDowns;

	TableList* tableHolder = _tables;

	// create a SCAN operator for each table in the query

	while(_tables != NULL){

		DBFile dbFile;
		string filePath;
		string tableName = string(_tables->tableName);

		//cout << "GOT HERE" << endl;
		//cout << tableName << endl;

		catalog->GetDataFile(tableName, filePath);
		Schema sch;

		char* filePathC = new char[filePath.length() + 1];
		strcpy(filePathC, filePath.c_str());

		if(dbFile.Open(filePathC) == -1){

			cout << "ERROR OPENING DB" << endl;

		}

		catalog->GetSchema(tableName, sch);

		//cout << sch << endl;
		//cout << "@@@@" << endl;

		//cout << "Got Here" << endl;

		Scan* scan = new Scan(sch, dbFile);
		scan->ScanHelper(tableName);
		pushDowns[tableName] = (RelationalOp*)scan;
		
		//cout << "Built Scan For " << tableName << " " << endl;

		// push-down selections: create a SELECT operator wherever necessary

		if(_predicate != NULL){

			CNF cnf;
			Record literal;

			//cout << "Got Here1" << endl;
			cnf.ExtractCNF(*_predicate, sch, literal);
			//cout << cnf.ExtractCNF(*_predicate, sch, literal) << endl;

		for(int i = 0; i < cnf.numAnds; i++){

		if(i > 0){

			cout << " AND ";

		}
		
		vector<Attribute> attL = sch.GetAtts();
		Comparison comp = cnf.andList[i];

		if(comp.operand1 != Literal) {

			cout << attL[comp.whichAtt1].name;

		}else{

			int pointer = ((int *)literal.GetBits())[comp.whichAtt1 + 1];

			if(comp.attType == Integer){

				int* pInt = (int *)&(literal.GetBits()[pointer]);
				//cout << endl;
				//cout << "THIS IS AN INT" << endl;
				cout << *pInt;

			}else if(comp.attType == Float){

				double* pDouble = (double *)&(literal.GetBits()[pointer]);
				//cout << endl;
				//cout << "THIS IS A FLAOT" << endl;
				cout << *pDouble;

			}else if(comp.attType == String){

				char* pCh = (char *)&(literal.GetBits()[pointer]);
				cout << pCh;

			}

		}

		if(comp.op == Equals){

			cout << " = ";

		}else if(comp.op == GreaterThan){

			cout << " > ";
		
		}else if(comp.op == LessThan){

			cout << " < ";

		}

		if(comp.operand2 != Literal) {
			
			//cout << "NOT LITERAL " << endl;
			cout << attL[comp.whichAtt2].name;

		}else{

			int pointer = ((int *)literal.GetBits())[comp.whichAtt2 + 1];

			if(comp.attType == Integer){

				int* pInt = (int *)&(literal.GetBits()[pointer]);
				//cout << endl;
				//cout << "THIS IS AN INT" << endl;
				cout << *pInt;

			}else if(comp.attType == Float){

				//cout << comp.attType << " TYPE " << endl;
				double* pDouble = (double *)&(literal.GetBits()[pointer]);
				//cout << endl;
				cout << *pDouble;
				cout << " THIS IS A FLAOT" << endl;

			}else if(comp.attType == String){

				//cout << comp.attType << " TYPE " << endl;
				char* pCh = (char *)&(literal.GetBits()[pointer]);
				cout << pCh;
				cout << " THIS IS A CHAR" << endl;

			}

		}
		cout << endl;
	}

			//literal.print(cout, sch);
			//cout << "ANYTHING IN HERE?" << endl;

			//cout << "GOT HERE" << endl;
			Schema selSch = sch;

			if(cnf.numAnds >= 1){

				Select* select = new Select(selSch, cnf, literal, (RelationalOp*)scan);
				pushDowns[tableName] = (RelationalOp*)select;
				cout << "Built Select For " << tableName << " " << endl;

			}

		}


		_tables = _tables->next;
		//cout << "Going to next Table" << endl;

	}

	// call the optimizer to compute the join order

	OptimizationTree* root = new OptimizationTree;
	optimizer->Optimize(tableHolder, _predicate, root);
	//cout << "Got to optimizer" << endl;
	//cout << "HERE123123" << endl;

	//if(root->leftChild == NULL && root->rightChild == NULL){

	//	cout << "GOT OUTPUT CORRECTLY" << endl;

	//}

	//cout << root->tables[0] << endl;
	//cout << "---" << endl;

	//cout << "---" << endl;
	//cout << oResult->tables[1] << endl;
	//cout << "---" << endl;
	//string helperString = "lineitem";
	
	//cout << tableHolder->tableName << endl;

	//RelationalOp* queryTree = pushDowns.find(helperString)->second;

	RelationalOp* queryTree = treeHelper(root, pushDowns, _predicate, 0);
	//cout << "Made first tree" << endl;
	RelationalOp* treeRoot = (RelationalOp*)queryTree;
	//cout << "Made second tree" << endl;

	Schema schIn = queryTree->GetSchema();

	//cout << schIn << endl;

	if(_groupingAtts == NULL){

		if(_finalFunction == NULL){

			cout << "IN FF" << endl;
			Schema schOut = schIn;
			int counter = schIn.GetNumAtts();
			int count = 0;

			cout << "--------------------" << endl;
			cout << schOut << endl;
			vector<int> attL;
			vector<Attribute> atts = schIn.GetAtts();

			for(int i = 0; i < atts.size(); i++){

				cout << atts[i].name << endl;

			}

			while(_attsToSelect != NULL){

				for(int i = 0; i < atts.size(); i++){

					if(atts[i].name == _attsToSelect->name){

						cout << "PUSHED ATT "  << atts[i].name << " " << i << " " << endl;
						attL.push_back(i);
						count++;
						break;

					}

				}

				_attsToSelect = _attsToSelect->next;

			}

			reverse(attL.begin(), attL.end());
			schOut.Project(attL);

			//cout << schOut.Project(attL) << endl;
			cout << schOut << endl;
			cout << "----" << endl;

			int* keepMe = new int[attL.size()];
			copy(attL.begin(), attL.end(), keepMe);

			Project* project = new Project(schIn, schOut, counter, count, keepMe, queryTree);
			cout << "Built Project" << endl;

			if(_distinctAtts != 0){

				Schema newschIn = schOut;
				DuplicateRemoval* distinct = new DuplicateRemoval(newschIn, project);
				treeRoot = (RelationalOp*)distinct;
				cout << "ASSIGNED ROOT DIST" << endl;

			}else{

				treeRoot = (RelationalOp*)project;
				cout << "ASSIGNED ROOT" << endl;

			}

		}else{

			cout << "BUILD FUNC" << endl;
			Function compute;
			vector<string> attL;
			vector<string> attT;
			vector<unsigned int> distinctValues;

			compute.GrowFromParseTree(_finalFunction, schIn);
			attL.push_back("sum");
			attT.push_back("function");
			distinctValues.push_back(1);

			Schema schOut(attL, attT, distinctValues);

			Sum* sum = new Sum(schIn, schOut, compute, queryTree);
			treeRoot = (RelationalOp*)sum;

		}

	}else{

		cout << "BUILD SUM" << endl;

		vector<string> attL;
		vector<string> attT;
		vector<unsigned int> distinctValues;
		vector<int> groupAtts;
		int counter = 0;

		while(_groupingAtts != NULL){

			cout << "WORKING ON GROUPING ATTS" << endl;

			string holder;
			int dVal;
			string nameHolder = string(_groupingAtts->name);
			dVal = schIn.GetDistincts(nameHolder);
			Type getCast = schIn.FindType(nameHolder);

			switch(getCast) {

				case Integer :

					holder = "INTEGER";
					break;

				case Float :

					holder = "FLOAT";
					break;

				case String : 

					holder = "STRING";
					break;

				default :

					holder = "???";
					break;

			}

			attL.push_back(nameHolder);
			cout << nameHolder << endl;
			cout << "###" << endl;
			attT.push_back(holder);
			distinctValues.push_back(dVal);
			
			groupAtts.push_back(schIn.Index(nameHolder));
			counter++;
			_groupingAtts = _groupingAtts->next;

		}

		Function compute;

		if(_finalFunction != NULL){

			compute.GrowFromParseTree(_finalFunction, schIn);

			attL.push_back("SUM");

			//cout << "SCHIN FOR FUNCTION" << endl;
			//cout << schIn << endl;

			attT.push_back("function");
			distinctValues.push_back(1);

			Schema schOut(attL, attT, distinctValues);

			cout << schOut << endl;

			Sum* sum = new Sum(schIn, schOut, compute, queryTree);
			treeRoot = (RelationalOp*)sum;


		}

		cout << "----" << endl;
		reverse(attL.begin(), attL.end());
		reverse(attT.begin(), attT.end());
		reverse(distinctValues.begin(), distinctValues.end());
		reverse(groupAtts.begin(), groupAtts.end());

		//cout << "GOT ABC" << endl;
		Schema schOut(attL, attT, distinctValues);
	
		//cout << " LAST STEP " << endl;
		int* holder = new int[groupAtts.size()];
		copy(groupAtts.begin(), groupAtts.end(), holder);
		OrderMaker groupingAtts(schIn, holder, counter);
		GroupBy* group = new GroupBy(schIn, schOut, groupingAtts, compute, queryTree);
		treeRoot = (RelationalOp*) group;

	}

	Schema schemaOut = treeRoot->GetSchema();
	//cout << "GOT SCHEMA" << endl;

	//cout << schemaOut << endl;

	string ofile = "tempfile.txt";
	//cout << ofile << endl;

	WriteOut* writeo = new WriteOut(schemaOut, ofile, treeRoot);

	//cout << schemaOut << endl;

	//cout << "BUILT WRITE OUT" << endl;
	treeRoot = (RelationalOp*)writeo;

	_queryTree.SetRoot(*treeRoot);
	//cout << "ASSIGNED QUERYTREE ROOT" << endl;

	

	// create join operators based on the optimal order computed by the optimizer

	// create the remaining operators based on the query
			
	// connect everything in the query execution tree and return

	// free the memory occupied by the parse tree since it is not necessary anymore

	_tables = NULL;
	_attsToSelect = NULL;
	_finalFunction = NULL;
	_predicate = NULL;
	_groupingAtts = NULL;

}

RelationalOp* QueryCompiler::treeHelper(OptimizationTree*& _tree, unordered_map<string, RelationalOp*>& _pushDowns, AndList* _predicate, int depth){

	cout << "IN QUERYTREE HELPER" << endl;

	OptimizationTree* treeHolder = _tree;

	for(int i = 0; i < treeHolder->tables.size(); i++){

		cout << treeHolder->tables[i] << endl;

	}

	cout << "ALL TABLLES LISTED" << endl;

	if(treeHolder->leftChild == NULL && treeHolder->rightChild == NULL){
	
		cout << "KIDS BOTH NULL" << endl;

		return _pushDowns.find(_tree->tables[0])->second;

	/*}else if(treeHolder->leftChild == NULL && treeHolder->rightChild != NULL){

		cout << "RIGHT KID NOT NULL" << endl;

	}else if(treeHolder->leftChild != NULL && treeHolder->rightChild == NULL){

		cout << "LEFT KID NOT NULL" << endl;*/

	}else{

		cout << "BUILDING JOINS" << endl;

		Schema schemaLeft;
		Schema schemaRight;
		Schema outSch;
		CNF cnf;

		RelationalOp* leftNode = treeHelper(_tree->leftChild, _pushDowns, _predicate, depth + 1);
		RelationalOp* rightNode = treeHelper(_tree->rightChild, _pushDowns, _predicate, depth + 1);

		schemaLeft = leftNode->GetSchema();
		schemaRight = rightNode->GetSchema();

		cnf.ExtractCNF(*_predicate, schemaLeft, schemaRight);
		outSch.Append(schemaLeft);
		outSch.Append(schemaRight);

		Join* join = new Join(schemaLeft, schemaRight, outSch, cnf, leftNode, rightNode);

		//join->depth = depth;
		//join->numTuples = _tree->noTuples;

		return (RelationalOp*)join;

	}
}
