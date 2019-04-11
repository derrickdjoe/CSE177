#include <iostream>
#include "RelOp.h"
#include <cstring>
#include <fstream>
#include <sstream>

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

bool DuplicateRemoval::GetNext(Record& _record) {

	while(producer->GetNext(_record)){

		stringstream ss;
		bool isFound = false;
		_record.print(ss, schema);

		/*unordered_set<string>::iterator it = hash_tbl.find(ss.str());
		if(it == hash_tbl.end()) {

			hash_tbl.insert(ss.str());
			return true;

		}*/

		for(int i = 0; i < dList.size(); i++){

			if(ss.str() == dList[i]){
		
				isFound = true;

			}

		}

		if(isFound == false){

			dList.push_back(ss.str());
			isFound = false;
			return true;

		}

	}

	return false;

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

bool Sum::GetNext(Record& _record){

	Type typeHolder;
	double runningSum;
	bool isDone = false;
	
	Record tempRec;
	while(producer->GetNext(tempRec)){

		int intHolder = 0;
		double dubsHolder = 0;
		typeHolder = compute.Apply(tempRec, intHolder, dubsHolder);
		runningSum += intHolder + dubsHolder;
	
		isDone = true;

	}

	if(isDone){

		int recSize;
		if(typeHolder == Integer){

			char* whatIWant = new char[sizeof(int)];

			sprintf(whatIWant, "%d", (int)runningSum);
			_record.Consume(whatIWant);
			return true;

		}else{

			char* whatIWant = new char[sizeof(double)];

			snprintf(whatIWant, recSize, "%f", runningSum);
			_record.Consume(whatIWant);
			return true;

		}


	}else{

		return false;

	}

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

	isFirst = true;
	//iter = 0;
}

GroupBy::~GroupBy() {

}

bool GroupBy::GetNext(Record& _record){

	cout << "GROUP BY GET NEXT" << endl;

	bool hasStuff = compute.HasStuff();

	//cout << "STUFF TO GROUP BY" << endl;

	if(isFirst){

		Record rec;

		//cout << "DOING AGGEGRATE" << endl;

		while(producer->GetNext(rec)) {

			double runningSum = 0;
			Schema tempSch = schemaOut;
			vector<int> attL;

			if(hasStuff) {

				int intHolder = 0;
				double dubsHolder = 0;

				compute.Apply(rec, intHolder, dubsHolder);
				runningSum = dubsHolder + intHolder;
			

				for(int i = 1; i < schemaOut.GetNumAtts(); i++){

					attL.push_back(i);

				}

				tempSch.Project(attL);

			}

			rec.Project(&groupingAtts.whichAtts[0], groupingAtts.numAtts, schemaIn.GetNumAtts());

			string key = rec.keyBuilder(tempSch);

			/*if(groupListVec.size() == 0){

				groupData tempHolder;

				tempHolder.rec = &rec;
				tempHolder.rec = rec;
				tempHolder.sum = runningSum;
				tempHolder.key = key;

				groupListVec.push_back(tempHolder);
				cout << " ADDING NEW KEY " << endl;

			}else{

			
				bool isFound = false;
				for(int i = 0; i < groupListVec.size(); i++){

					if(groupListVec[i].key == key){

						groupListVec[i].sum += runningSum;
						cout << " ADDING SUM " << endl;
						isFound = true;
						break;

					}


				}

				if(!isFound){

					groupData tempHolder;
					tempHolder.rec = &rec;
					tempHolder.rec = rec;
					tempHolder.sum = runningSum;
					tempHolder.key = key;

					groupListVec.push_back(tempHolder);
					cout << " ADDING NEW KEY " << endl;
					isFound = false;

				}

			}*/
			

			if(groupListMap.find(key) == groupListMap.end()){

				cout << key << " FIRST KEY " << endl;

				groupData tempHolder;
				tempHolder.rec = rec;
				tempHolder.sum = runningSum;
			
				groupListMap[key] = tempHolder;

				cout << key << " THIS IS KEY " << endl;

			}else{

				groupListMap[key].sum += runningSum;

			}

		}

		groupListIter = groupListMap.begin();
		isFirst = false;

	}

	if(groupListIter != groupListMap.end()){

		Record recToCreate;

		if(hasStuff) {

			Type holder = compute.GetAttType();
			Record runningSumRec;
			int whereAmI = sizeof(int) + sizeof(int);
	
			if(holder == Integer){

				char* whatIWant = new char[1];

				((int*)whatIWant)[0] = whereAmI + sizeof(int);
				((int*)whatIWant)[1] = whereAmI;
				*((int*)&whatIWant[whereAmI]) = (int)(*groupListIter).second.sum;

				//sprintf(whatIWant, "%d", (int)(*groupListIter).second.sum);
				//sprintf(whatIWant, "%d", (int)groupListVec[iter].sum);

				char* toCopy = new char[whereAmI + sizeof(int)];
				memcpy(toCopy, whatIWant, whereAmI + sizeof(int));

				runningSumRec.Consume(toCopy);
				_record.addToVec(1);

				//_record.addToVec(groupListVec[iter].sum);


			}else{

				char* whatIWant = new char[1];

				((int*)whatIWant)[0] = whereAmI + sizeof(double);
				((int*)whatIWant)[1] = whereAmI;
				*((double*)&whatIWant[whereAmI]) = (*groupListIter).second.sum;

				//snprintf(whatIWant, whereAmI + sizeof(double), "%f", (*groupListIter).second.sum);
				//snprintf(whatIWant, whereAmI + sizeof(double), "%f", groupListVec[iter].sum);

				char* toCopy = new char[whereAmI + sizeof(double)];
				memcpy(toCopy, whatIWant, whereAmI + sizeof(double));

				runningSumRec.Consume(toCopy);
				_record.addToVec(2);

				//_record.addToVec(groupListVec[iter].sum);


			}

			recToCreate.AppendRecords(runningSumRec, (*groupListIter).second.rec, 1, schemaOut.GetNumAtts() - 1);

			//recToCreate.AppendRecords(runningSumRec, *groupListVec[iter].rec, 1, schemaOut.GetNumAtts() - 1);


		}else{

		}

		_record = recToCreate;
		groupListIter++;
		//iter++;

		return true;

	}else{

		return false;

	}

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
