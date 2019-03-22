#include <iostream>
#include "RelOp.h"
#include <cstring>
#include <fstream>

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}



Scan::Scan(Schema& _schema, DBFile& _file) : schema(_schema), file(_file) {

	cout << "Built Scan" << endl;

}

Scan::~Scan() {

}

void Scan::ScanHelper(string tableN){

	tableName = tableN;

}

bool Scan::GetNext(Record& _record){

	if(file.GetNext(_record) == 0){

		//cout << "SCAN TRUE" << endl;
		return true;

	}else{

		//cout << "SCAN FALSE" << endl;
		return false;

	}


}

ostream& Scan::print(ostream& _os) {

	file.SetZero();
	_os << "SCAN " << " ( " << "TABLE : [" << tableName << "] ) ";
	_os << endl;
	return _os;
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) : schema(_schema), predicate(_predicate), constants(_constants), producer(_producer) {

	//cout << "Built Select" << endl;
	//constants.print(cout, schema);
	//cout << "LITERALS ^^ " << endl;

}

Select::~Select() {

}

bool Select::GetNext(Record& _record){

	while(true){

		bool ret = producer->GetNext(_record);
		if(!ret){

			return false;

		}else{

			ret = predicate.Run(_record, constants);
			if(ret){

				return true;

			}

		}

	}

	/*
	while(producer->GetNext(_record)){

		cout << "AM HERE" << endl;

		//constants.print(cout, schema);
		//cout << endl;
		//cout << "RECORD" << endl;

		if(predicate.Run(_record, constants)) {

			cout << "RAN CONSTANTS" << endl;
			return true;

		}

		cout << "DIDNT RUN" << endl;

	}*/

	return false;

}

ostream& Select::print(ostream& _os) {

	_os << "SELECT " << "Schema : [";
	
	vector<Attribute> attL = schema.GetAtts();
	for(int i = 0; i < attL.size(); i++){

		if (i > 0){

			_os << ", ";

		}

		_os << attL[i].name;

	}

	_os << "]" << endl;
	_os << "	       " << "Predicate : [";

	for(int i = 0; i < predicate.numAnds; i++){

		if(i > 0){

			_os << " AND ";

		}
		
		vector<Attribute> attL = schema.GetAtts();
		Comparison comp = predicate.andList[i];

		if(comp.operand1 != Literal) {

			_os << attL[comp.whichAtt1].name;

		}else{

			int pointer = ((int *)constants.GetBits())[comp.whichAtt1 + 1];

			if(comp.attType == Integer){

				int* pInt = (int *)&(constants.GetBits()[pointer]);
				//_os << endl;
				//_os << "THIS IS AN INT" << endl;
				_os << *pInt;

			}else if(comp.attType == Float){

				double* pFloat = (double *)&(constants.GetBits()[pointer]);
				//_os << endl;
				//_os << "THIS IS A FLAOT" << endl;
				_os << *pFloat;

			}else if(comp.attType == String){

				char* pCh = (char *)&(constants.GetBits()[pointer]);
				_os << pCh;

			}

		}

		if(comp.op == Equals){

			_os << " = ";

		}else if(comp.op == GreaterThan){

			_os << " > ";
		
		}else if(comp.op == LessThan){

			_os << " < ";

		}

		if(comp.operand2 != Literal) {
			
			//_os << "NOT LITERAL " << endl;
			_os << attL[comp.whichAtt2].name;

		}else{

			int pointer = ((int *)constants.GetBits())[comp.whichAtt2 + 1];

			if(comp.attType == Integer){

				int* pInt = (int *)&(constants.GetBits()[pointer]);
				//_os << endl;
				//_os << "THIS IS AN INT" << endl;
				_os << *pInt;

			}else if(comp.attType == Float){

				double* pFloat = (double *)&(constants.GetBits()[pointer]);
				//_os << endl;
				//_os << "THIS IS A FLAOT" << endl;
				_os << *pFloat;

			}else if(comp.attType == String){

				char* pCh = (char *)&(constants.GetBits()[pointer]);
				_os << pCh;

			}

		}


	}

	_os << "]" << endl;
	_os << *producer;

	return _os;

}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) : schemaIn(_schemaIn), schemaOut(_schemaOut), numAttsInput(_numAttsInput), numAttsOutput(_numAttsOutput), keepMe(_keepMe), producer(_producer) {

}

Project::~Project() {

}

bool Project::GetNext(Record& _record){

	cout << "AM PROJECT" << endl;

	while(true){

		bool ret = producer->GetNext(_record);

		if(ret){

			_record.Project(keepMe, numAttsOutput, numAttsInput);
			return true;

		}else{

			return false;

		}

	}

	/*if(producer->GetNext(_record)){

		_record.Project(keepMe, numAttsOutput, numAttsInput);
		return true;

	}else{

		return false;

	}*/

}

ostream& Project::print(ostream& _os) {

	_os << "PROJECT" << " ";

	vector<Attribute> attL = schemaOut.GetAtts();

	for(auto it = attL.begin(); it != attL.end(); it++){

		if(it != attL.begin()){

			_os << ", ";

		}

		_os << it->name;

	}
	
	_os << endl;
	_os << *producer;
	return _os;

}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) : schemaLeft(_schemaLeft), schemaRight(_schemaRight), schemaOut(_schemaOut), predicate(_predicate), left(_left), right(_right){

	cout << "BUILT JOIN" << endl;
}

Join::~Join() {

}

ostream& Join::print(ostream& _os) {
	_os << "JOIN ";

	vector<Attribute> attL = schemaLeft.GetAtts();
	vector<Attribute> attL1 = schemaRight.GetAtts();
	vector<Attribute> attL2 = schemaOut.GetAtts();
	
	_os << "	Schema Left : ";
	for(int i = 0; i < attL.size(); i++){

		if(i > 0){

			_os << ", ";

		}

		_os << attL[i].name;

	}

	_os << endl;
	_os << "		Schema Right : ";
	for(int i = 0; i < attL1.size(); i++){

		if(i > 0){

			_os << ", ";

		}

		_os << attL1[i].name;

	}

	_os << endl;
	_os << "		Schema Out : ";
	for(int i = 0; i < attL2.size(); i++){

		if(i > 0){

			_os << ", ";
		
		}

		_os << attL2[i].name;

	}

	_os << endl;
	_os << "		Predicate : ";
	for(int i = 0; i < predicate.numAnds; i++){

		if(i > 0) {

			_os << " AND ";

		}

		Comparison comp = predicate.andList[i];

		if(comp.operand1 == Left){

			_os << schemaLeft.GetAtts()[comp.whichAtt1].name;

		}else if(comp.operand1 == Right){

			_os << schemaRight.GetAtts()[comp.whichAtt1].name;

		}

		if(comp.op == Equals){

			_os << " = ";

		}else if(comp.op == GreaterThan){

			_os << " > ";

		}else if(comp.op == LessThan){

			_os << " < ";

		}

		if(comp.operand2 == Left){

			_os << schemaLeft.GetAtts()[comp.whichAtt2].name;

		}else if(comp.operand2 == Right){

			_os << schemaRight.GetAtts()[comp.whichAtt2].name;

		}

	}

	_os << endl;
	_os << *right;
	_os << *left;
	_os << endl;
	return _os;

}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) :  schema(_schema), producer(_producer){

}

DuplicateRemoval::~DuplicateRemoval() {

}

ostream& DuplicateRemoval::print(ostream& _os) {

	_os << "DISTINCT ";

	vector<Attribute> attL = schema.GetAtts();

	for(auto it = attL.begin(); it != attL.end(); it++){

		if(it != attL.begin()){

			_os << ", ";

		}

		_os << it->name;

	}
	
	_os << *producer;

	return _os;
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) : schemaIn(_schemaIn), schemaOut(_schemaOut), compute(_compute), producer(_producer){

}

Sum::~Sum() {

}

ostream& Sum::print(ostream& _os) {

	_os << "SUM";
	vector<Attribute> attL = schemaOut.GetAtts();

	for(int i = 0; i < attL.size(); i++){

		_os << attL[i].name;

	}

	_os << *producer;
	
	return _os;
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) : schemaIn(_schemaIn), schemaOut(_schemaOut), groupingAtts(_groupingAtts), compute(_compute), producer(_producer) {

}

GroupBy::~GroupBy() {

}

ostream& GroupBy::print(ostream& _os) {
	
	_os << "GROUP BY ";
	vector<Attribute> attL = schemaOut.GetAtts();

	for(int i = 0; i < attL.size(); i++){

		if(i > 0){

			_os << ", ";

		}

		_os << attL[i].name;

	}

	_os << *producer;

	return _os;

}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) : schema(_schema), outFile(_outFile), producer(_producer) {
		
	outFileStream.open(outFile);

}

WriteOut::~WriteOut() {

	if (outFileStream.is_open()){

		outFileStream.close();

	}

}

bool WriteOut::GetNext(Record& _record){

	cout << "TRYING TO GET NEXT RECORD FROM WRITEOUT" << endl;
	bool ret = producer->GetNext(_record);
	if(!ret){

		cout << "NOTHING NEXT" << endl;
		return false;

	}else{

		cout << "TRYING TO PRINT STUFF" << endl;
		_record.print(outFileStream, schema);
		outFileStream << endl;
		return true;

	}

	/*cout << "PRINTING BOOL" << endl;
	bool ret = producer->GetNext(_record);
	cout << ret << endl;

	if(producer->GetNext(_record)){

		cout << "SEG FAULT" << endl;
		_record.print(outFileStream, schema);
		outFileStream << endl;
		return true;

	}else{

		cout << "NOTHING NEXt" << endl;
		outFileStream.close();
		return false;

	}*/

}

ostream& WriteOut::print(ostream& _os) {
	_os << "OUTPUT" << " (Schema Out : [";

	vector<Attribute> attL = schema.GetAtts();
	for(int i = 0; i < attL.size(); i++){

		if(i > 0){

			_os << ", ";

		}

		_os << attL[i].name;

	}

	_os << "])";

	_os << endl;
	_os << *producer;
	return _os;
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {

	_os << "QUERY EXECUTION TREE";
	_os << endl;
	_os << *_op.root << endl;
	return _os;

}
