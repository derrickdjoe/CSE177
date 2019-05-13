#include <string>
#include <vector>
#include <iostream>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

#include <unordered_map>

using namespace std;


QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
}

QueryOptimizer::~QueryOptimizer() {
}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* _root) {
	// compute the optimal join order

	int counter = 0;
	TableList* tables = _tables;
	TableList* tableIter = _tables;
	//use this for now lol
	vector<string> tableNList;

	//cout << "IN OPTIMIZER" << endl;

	while(tableIter != NULL){

		tableNList.push_back(string(tableIter->tableName));
		//cout << "PUT " << tableNList[counter] << " INTO VECTOR " << endl;
		tableIter = tableIter->next;
		counter++;

	}

	/*while(tables != NULL){

		OptimizationTree *node = new OptimizationTree;
		node->tables.push_back(tables->tableName);
		node->leftChild = NULL;
		node->rightChild = NULL;
		nodeList.push_back(node);
		i++;
		counter++;
		cout << "BUILT FIRST TABLE TREE" << endl;
		tables = tables->next;

	}*/

	if(tableNList.size() == 0){

		_root = NULL;
	
	}

	else if(tableNList.size() == 1){

		OptimizationTree* node = new OptimizationTree;
		node->tables.push_back(tableNList[0]);
		//cout << "SET " << node->tables[0] << " INTO VECTOR " << endl;
		node->leftChild = NULL;
		/*if(node->leftChild == NULL){

			cout << "ASSIGNED RIGHT" << endl;

		}*/
		node->rightChild = NULL;
		/*if(node->rightChild == NULL){

			cout << "ASSIGNED RIGHT" << endl;

		}
		cout << "SINGLE NODE TREE PRODUCED" << endl;*/
		*_root = *node;

		/*if(_root->leftChild == NULL && _root->rightChild == NULL){

			cout << "BUILT CORRECTLY" << endl;

		}*/

	}else if(tableNList.size() == 2){

		OptimizationTree* node = new OptimizationTree;
		OptimizationTree* nodeLeft = new OptimizationTree;
		OptimizationTree* nodeRight = new OptimizationTree;
		
		for(int i = 0; i < 2; i++){

			node->tables.push_back(tableNList[i]);

		}

		nodeLeft->tables.push_back(tableNList[0]);
		nodeRight->tables.push_back(tableNList[1]);

		node->leftChild = nodeLeft;
		nodeLeft->leftChild = NULL;
		nodeRight->leftChild = NULL;
		node->rightChild = nodeRight;
		nodeLeft->rightChild = NULL;
		nodeRight->rightChild = NULL;
		node->parent = NULL;
		nodeLeft->parent = node;
		nodeRight->parent = node;

		//cout << "NODE PAIR PRODUCED" << endl;
		*_root = *node;

		/*for(int i = 0; i < 2; i++){

			cout << "PARENT : " << node->tables[i] << endl;

		}

		cout << "LEFT CHILD : " << node->leftChild->tables[0] << endl;
		cout << "RIGHT CHILD : " << node->rightChild->tables[0] << endl;*/


	}else{

		//cout << "BUILDING MORE THAN 2 TABLES" << endl;

		OptimizationTree* node = new OptimizationTree;
		OptimizationTree* nodeLeft = new OptimizationTree;
		OptimizationTree* nodeRight = new OptimizationTree;
		
		for(int i = 0; i < 2; i++){

			node->tables.push_back(tableNList[i]);

		}

		nodeLeft->tables.push_back(tableNList[0]);
		nodeRight->tables.push_back(tableNList[1]);

		node->leftChild = nodeLeft;
		nodeLeft->leftChild = NULL;
		nodeRight->leftChild = NULL;
		node->rightChild = nodeRight;
		nodeLeft->rightChild = NULL;
		nodeRight->rightChild = NULL;
		node->parent = NULL;
		nodeLeft->parent = node;
		nodeRight->parent = node;

		OptimizationTree* abcNode = new OptimizationTree;
		
		int abc = 2;
		vector<OptimizationTree*> makethisEZ;

		while(abc < tableNList.size()){

			//cout << "DOING RECURISVE ADD" << endl;
			node = OpHelper(node, tableNList, abc);
			if(abc == tableNList.size() - 1){

				makethisEZ.push_back(node);

			}
			abc++;
			
		}

		

		/*vector<OptimizationTree *> nodeList;

		OptimizationTree* oldNode = new OptimizationTree;
		oldNode->parent = NULL;
		oldNode->leftChild = NULL;
		oldNode->rightChild = NULL;
		cout << "MADE 0 NODE" << endl;
		nodeList.push_back(oldNode);
		
		for(int i = 1; i < counter; i++){

			OptimizationTree* newNode = new OptimizationTree;
			newNode->parent = oldNode;
			newNode->leftChild = NULL;
			newNode->rightChild = NULL;
			cout << "MADE " << i << " NODE " << endl;
			nodeList.push_back(newNode);
			oldNode = newNode;

		}

		int counterTemp = counter;

		for(int i = 0; i < tableNList.size(); i++){

			if(i > 0){

				OptimizationTree* nodeHelper = new OptimizationTree;
				nodeHelper = nodeList[i - 1];
				nodeList[i]->parent = nodeHelper;
			}

			if(i != nodeList.size() - 1){
				
				OptimizationTree* nodeHelper = new OptimizationTree;
				nodeHelper = nodeList[i];
				nodeList[i - 1]->leftChild = nodeHelper;

			}

			for(int j = 0; j < counterTemp; j++){

				nodeList[i]->tables.push_back(tableNList[j]);
				

			}

			counter--;

		}*/

		*_root = *node;
		//cout << "DID THIS" << endl;

	}

	/*if(counter == 1){

		cout << "ONE TABLE" << endl;
		cout << nodeList[0]->tables[0] << endl;
		_root = nodeList[0];
		cout << "Returning" << endl;
	}

	vector<int> tableNames;
	vector<int> tableOrder;*/
	
}

OptimizationTree* QueryOptimizer::OpHelper(OptimizationTree* _root, vector<string> tableList, int iter){
	
	//just pairs the next one till were done lol

	OptimizationTree* node = new OptimizationTree;
	OptimizationTree* nodeLeft = new OptimizationTree;
	OptimizationTree* nodeRight = new OptimizationTree;

	int whereAmI = iter;
	OptimizationTree* rootHolder = _root;
	vector<string> tableListCopy = tableList;

	nodeLeft = rootHolder;
	nodeLeft->parent = node;

	nodeRight->tables.push_back(tableListCopy[iter]);
	nodeRight->leftChild = NULL;
	nodeRight->rightChild = NULL;
	nodeRight->parent = node;

	for(int i = 0; i < iter; i++){

		node->tables.push_back(tableListCopy[i]);

	}

	node->leftChild = nodeLeft;
	node->rightChild = nodeRight;
	node->parent = NULL;

	return node;

}
