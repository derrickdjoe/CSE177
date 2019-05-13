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

	//cout << "Built Scan" << endl;

}

Scan::~Scan() {

}

void Scan::ScanHelper(string tableN){

	tableName = tableN;

}

bool Scan::GetNext(Record& _record){

	while(true){

		bool ret = file.GetNext(_record);
		//returns 0 if true

		if(!ret){

			return true;

		}else{

			return false;

		}

	}

}

ostream& Scan::print(ostream& _os) {

	file.SetZero();
	_os << "SCAN " << "(" << "TABLE : [" << tableName << "])";
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

				/*for(int i = 0; i < predicate.numAnds; i++){

					if(i > 0) {

						cout << " AND ";

					}

					Schema schemaLeft = schema;
					Schema schemaRight = schema;

					Comparison comp = predicate.andList[i];

					if(comp.operand1 == Left){

						cout << schemaLeft.GetAtts()[comp.whichAtt1].name;

					}else if(comp.operand1 == Right){

						cout << schemaRight.GetAtts()[comp.whichAtt1].name;

					}
		
					if(comp.op == Equals){
			
						cout << " = ";

					}else if(comp.op == GreaterThan){
		
						cout << " > ";

					}else if(comp.op == LessThan){

						cout << " < ";
					}

					if(comp.operand2 == Left){

						cout << schemaLeft.GetAtts()[comp.whichAtt2].name;

					}else if(comp.operand2 == Right){

						cout << schemaRight.GetAtts()[comp.whichAtt2].name;

					}

				cout << endl;

				}

				cout << "PRINTING SELECT RECORDS" << endl;
				_record.print(cout, schema);
				cout << endl;
				constants.print(cout, schema);
				cout << endl;
				cout << "-----------------------" << endl;*/

				return true;

			}

		}

	}

	return false;

}

ostream& Select::print(ostream& _os) {

	_os << "SELECT " << "(Schema : [";
	
	vector<Attribute> attL = schema.GetAtts();
	for(int i = 0; i < attL.size(); i++){

		_os << attL[i].name;

		if(i < attL.size() - 1){

			_os << ", ";

		}

	}

	_os << ")]" << endl;
	_os << "       " << "(Predicate : [";

	for(int i = 0; i < predicate.numAnds; i++){

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

		if(i < predicate.numAnds - 1){

			_os << " AND ";

		}


	}

	_os << "])" << endl;
	_os << *producer;

	return _os;

}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) : schemaIn(_schemaIn), schemaOut(_schemaOut), numAttsInput(_numAttsInput), numAttsOutput(_numAttsOutput), keepMe(_keepMe), producer(_producer) {

}

Project::~Project() {

}

bool Project::GetNext(Record& _record){

	//cout << "AM PROJECT" << endl;

	while(true){

		bool ret = producer->GetNext(_record);

		if(ret){

			_record.Project(keepMe, numAttsOutput, numAttsInput);
			return true;

		}else{

			return false;

		}

	}

}

ostream& Project::print(ostream& _os) {

	_os << "PROJECT (Schema Out : [";

	vector<Attribute> attL = schemaOut.GetAtts();

	for(int i = 0; i < attL.size(); i++){

		_os << attL[i].name;

		if(i < attL.size() - 1){

			_os << ", ";

		}

	}

	_os << "])";
	_os << endl;

	_os << "        (Schema In : [";
	vector<Attribute> attL1 = schemaIn.GetAtts();

	for(int i = 0; i < attL1.size(); i++){

		_os << attL1[i].name;

		if(i < attL1.size() - 1){

			_os << ", ";

		}

	}

	_os << "])" << endl;
	_os << *producer;
	return _os;

}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) : schemaLeft(_schemaLeft), schemaRight(_schemaRight), schemaOut(_schemaOut), predicate(_predicate), left(_left), right(_right){

	//cout << "BUILT JOIN" << endl;
	isFirst = true;
	hotPotato = true;
	leftDone = false;
	rightDone = false;
	leftWorking = false;
	rightWorking = false;
	isBig = false;

	//TODO figure out how big records are for isBig
	// need to manually set isBig

	if (predicate.GetSortOrders(orderLeft, orderRight) == 0){

		//cout << "Ordermaker Failed" << endl;

	}

	//cout << "Ordermaker worked" << endl;

}

Join::~Join() {

}

bool Join::GetNext(Record& _record){

	//cout << predicate.numAnds << " THIS IS NUM ANDS" << endl;

	Comparison comp = predicate.andList[0];

	if(comp.op == GreaterThan || comp.op == LessThan){

		//cout << "Using NLJ" << endl;

		if(!HJ(_record)){

			return false;

		}

		return true;

	}

	if(comp.op == Equals){

		//cout << "Using HJ" << endl;

		if(!HJ(_record)){

			return false;

		}

		return true;

	}

	if(comp.op == Equals && isBig){
	
		//cout << "Using SHJ" << endl;
		
		if(!SHJ(_record)){

			return false;

		}

		return true;

	}

	return false;

}


bool Join::NLJ(Record& _record){

	//cout << "GROUP BY JOIN" << endl;

	if(isFirst){

		//cout << "AM HERE" << endl;
		Record rec;
		while(right->GetNext(rec)){

			Record tempRec;
			tempRec = rec;
			recList.Insert(tempRec);

		}

		isFirst = false;

	}

	Record recs;
	Record newRec;
	int i = 0;
	while(left->GetNext(recs)){

		Record tempRec;
		recList.MoveToStart();
		while(true){

			if(recList.AtEnd()){
			
				break;

			}

			tempRec = recList.Current();
				
			if(predicate.Run(recs, tempRec)){

				//cout << "Yes" << endl;
				/*cout << "FOUND PAIR" << endl;
				recs.print(cout, schemaLeft);
				cout << endl;
				tempRec.print(cout, schemaRight);
				cout << endl;
				cout << "---------------------" << endl;*/

				newRec.AppendRecords(recs, tempRec, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
				_record = newRec;
				return true;
	
			}else{

				//cout << "No" << endl;

			}

			recList.Advance();
			//cout << "ADVANCED" << endl;

		}

		//cout << "DID 1 iter" << endl;
		//cout << "Joined " << i << " tuples " << endl;
		//i++;

	}

	//cout << "DONE JOIN" << endl;
	cout << endl;
	

	return false;

}

bool Join::HJ(Record& _record){

	if(isFirst){

		Record rec;
		while(right->GetNext(rec)){

			Record tempRec;
			tempRec = rec;
			//tempRec.print(cout, schemaRight);
			//cout << endl;
			orderRight.Run(tempRec, tempRec);
			//tempRec.print(cout, schemaRight);
			//cout << endl;

			KeyInt intData;
			intData = 0;

			recHash.Insert(tempRec, intData);
			//cout << "INSERTED INTO HASH" << endl;

		}

		isFirst = false;

	}

	Record recs;
	Record newRec;
	int count = 0;

	//cout << hashRecList.Length() << " LENGTH" << endl;

	while(true){
		
		if(hashRecList.AtEnd()){

			//cout << "Breaking" << endl;
			break;

		}

		//cout << "HERE" << endl;
		//cout << "IN REC LIST" << count << endl;
		
		Record recToSend;
		recToSend = hashRecList.Current();
		_record = recToSend;
		hashRecList.Remove(recToSend);
		//hashRecList.Advance();
		return true;

	}

	while(left->GetNext(recs)){

		Record tempRec;
		tempRec = recs;
		//tempRec.print(cout, schemaLeft);
		//cout << endl;
		orderLeft.Run(tempRec, tempRec);
		//tempRec.print(cout, schemaLeft);
		//cout << endl;
		//if(recHash.IsThere(tempRec)){

		//	cout << "FOUND" << endl;

		//}

		//cout << "AM HERE" << endl;
		recHash.MoveToStart();
		int counter = 0;

		while(true){

			//cout << "SEARCHING" << endl;
			if(recHash.AtEnd()){

				break;

			}

			Record tempHash;
			tempHash = recHash.CurrentKey();

			if(predicate.Run(tempRec, tempHash)){

				//cout << "FOUND MATCH" << endl;
				counter++;
				newRec.AppendRecords(tempRec, tempHash, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());

				//newRec.print(cout, schemaOut);
				//cout << endl;

				hashRecList.Insert(newRec);

			}

			recHash.Advance();

		}

		if(!hashRecList.AtEnd()){

			//cout << "Stuck here" << endl;
			hashRecList.MoveToStart();
			Record recToSend;
			recToSend = hashRecList.Current();
			_record = recToSend;
			hashRecList.Remove(recToSend);
			//cout << "SENT FIRST IN LIST" << endl;
			//cout << "COUNTER IS " << counter << endl;
			//cout << hashRecList.Length() << endl;
			return true;

		}
			
	}

	return false;

}

bool Join::SHJ(Record& _record){

	while(true){

		if(leftDone && rightDone){

			break;

		}

		if(hotPotato){

			hotPotato = false;
			rightWorking = false;

			Record rec;
			while(right->GetNext(rec)){

				Record tempRec;
				tempRec = rec;

				Record compRec;
				compRec = rec;

				orderRight.Run(tempRec, tempRec);
				KeyInt keyInt;
				keyInt = 1;
				recHashRight.Insert(tempRec, keyInt);

				//hotPotato = false;
				rightWorking = true;
				//cout << "Inserted Right" << endl;


				if(recHashLeft.Length() != 0){

					//cout << "Hash not empty" << endl;

					recHashLeft.MoveToStart();
					while(true){

						if(recHashLeft.AtEnd()){

							break;

						}

						Record compLeft;
;						compLeft = recHashLeft.CurrentKey();
						
						//cout << "Here?" << endl;
						if(predicate.Run(compRec, compLeft)){

							Record newRec;
							newRec.AppendRecords(compLeft, compRec, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
							//cout << "FOUND DATA" << endl;
							_record = newRec;
							return true;

						}

						recHashLeft.Advance();

					}

					//break;

				}
				
				break;

			}

			if(!rightWorking){

				//cout << "Right Done" << endl;
				rightDone = true;

			}

		}else{

			hotPotato = true;
			leftWorking = false;
			Record rec;
			while(left->GetNext(rec)){

				Record tempRec;
				tempRec = rec;

				Record compRec;
				compRec = rec;

				orderLeft.Run(tempRec, tempRec);
				KeyInt keyInt;
				keyInt = 1;
				recHashLeft.Insert(tempRec, keyInt);

				//hotPotato = true;
				leftWorking = true;
				//cout << "Inserted Left" << endl;

				if(recHashRight.Length() != 0){

					//cout << "Right hash not empty" << endl;
					recHashRight.MoveToStart();
					while(true){

						if(recHashRight.AtEnd()){

							break;

						}

						//cout << "Here?" << endl;
						Record compRight;
						compRight = recHashRight.CurrentKey();

						if(predicate.Run(compRec, compRight)){

							//cout << "FOUND DATA" << endl;
							Record newRec;
							newRec.AppendRecords(compRec, compRight, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
							_record = newRec;
							return true;

						}
					
						//cout << "Advancing" << endl;
						recHashRight.Advance();

					}

					//break;

				}
				//cout << "Done left" << endl;
				break;

			}

			//cout << "HERE???" << endl;
			if(!leftWorking){

				//cout << "Left Done" << endl;
				leftDone = true;

			}

		}

	}

	/*cout << recHashLeft.Length() << " LEFT LENGTH" << endl;
	cout << recHashRight.Length() << " RIGHT LENGTH" << endl;
	
	if(isFirst){

		recHashRight.MoveToStart();
		recHashLeft.MoveToStart();
		isFirst = false;
		cout << "INIT" << endl;

	}

	while(true){

		if(recHashLeft.AtEnd()){

			//cout << "Breaking Left Hash" << endl;
			break;

		}

		Record compLeft;
		compLeft = recHashLeft.CurrentKey();
		//compLeft.print(cout, schemaLeft);
		//cout << endl;
		//cout << "IN HERE" << endl;

		recHashRight.MoveToStart();
		while(true){

			if(recHashRight.AtEnd()){

				break;

			}

			Record compRight;
			compRight = recHashRight.CurrentKey();
			//compRight.print(cout, schemaRight);
			//cout << endl;
			//cout << "-----------------------" << endl;

			if(predicate.Run(compLeft, compRight)){

				cout << "FOUND STUFF" << endl;
				Record newRec;
				newRec.AppendRecords(compLeft, compRight, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());

				newRec.print(cout, schemaOut);
				cout << endl;

				_record = newRec;
				recHashRight.Advance();
				return true;

			}

			recHashRight.Advance();

		}

		recHashLeft.Advance();

	}*/

	return false;

}

ostream& Join::print(ostream& _os) {

	_os << "JOIN ";

	vector<Attribute> attL = schemaLeft.GetAtts();
	vector<Attribute> attL1 = schemaRight.GetAtts();
	vector<Attribute> attL2 = schemaOut.GetAtts();
	
	_os << "(Schema Left : [";
	for(int i = 0; i < attL.size(); i++){

		_os << attL[i].name;

		if(i < attL.size() - 1){

			_os << ", ";

		}

	}

	_os << "])";
	_os << endl;

	_os << "     (Schema Right : [";
	for(int i = 0; i < attL1.size(); i++){

		_os << attL1[i].name;

		if(i < attL1.size() - 1){

			_os << ", ";

		}

	}

	_os << "])";
	_os << endl;

	_os << "     (Schema Out : [";
	for(int i = 0; i < attL2.size(); i++){

		_os << attL2[i].name;

		if(i < attL.size() - 1){

			_os << ", ";

		}

	}

	_os << "])";
	_os << endl;

	_os << "     (Predicate : [";
	for(int i = 0; i < predicate.numAnds; i++){

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

		if(i < predicate.numAnds - 1){

			_os << " AND ";

		}

	}

	_os << "])";
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

	_os << "DISTINCT [";

	vector<Attribute> attL = schema.GetAtts();

	for(int i = 0; i < attL.size(); i++){

		_os << attL[i].name;

		if(i < attL.size() - 1){

			_os << ", ";

		}

	}
	
	_os << "]" << endl;
	_os << *producer;

	return _os;
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) : schemaIn(_schemaIn), schemaOut(_schemaOut), compute(_compute), producer(_producer){

	isDone = false;
	runningSum = 0;

}

Sum::~Sum() {

}

bool Sum::GetNext(Record& _record){

	Type typeHolder;

	if(!isDone){
	
		while(producer->GetNext(_record)){

			int intHolder = 0;
			double dubsHolder = 0;
			typeHolder = compute.Apply(_record, intHolder, dubsHolder);
			runningSum += intHolder;
			runningSum += dubsHolder;

		}

		/*int recSize;
		if(typeHolder == Integer){

			char* whatIWant = new char[sizeof(int)];

			sprintf(whatIWant, "%d", (int)runningSum);
			_record.Consume(whatIWant);
			isDone = true;
			return true;

		}else{

			char* whatIWant = new char[sizeof(double)];

			snprintf(whatIWant, recSize, "%f", runningSum);
			_record.Consume(whatIWant);
			isDone = true;
			return true;

		}*/

		int whereAmI = sizeof(int) + sizeof(int);

		if(typeHolder == Integer){

			char* whatIWant = new char[1];
		
			((int*)whatIWant)[0] = whereAmI + sizeof(int);
			((int*)whatIWant)[1] = whereAmI;
			*((int*)&whatIWant[whereAmI]) = (int)runningSum;

			char* toCopy = new char[whereAmI + sizeof(int)];
			memcpy(toCopy, whatIWant, whereAmI + sizeof(int));

			_record.Consume(toCopy);
			_record.addToVec(1);

			isDone = true;
			return true;

		}

		if(typeHolder == Float){

			char* whatIWant = new char[1];
		
			((int*)whatIWant)[0] = whereAmI + sizeof(double);
			((int*)whatIWant)[1] = whereAmI;
			*((double*)&whatIWant[whereAmI]) = runningSum;

			char* toCopy = new char[whereAmI + sizeof(double)];
			memcpy(toCopy, whatIWant, whereAmI + sizeof(double));

			_record.Consume(toCopy);
			_record.addToVec(2);

			isDone = true;
			return true;

		}


	}

	return false;

}

ostream& Sum::print(ostream& _os) {

	_os << "SUM (Schema Out : [";
	vector<Attribute> attL = schemaOut.GetAtts();

	for(int i = 0; i < attL.size(); i++){

		_os << attL[i].name;

	}

	_os << "])";
	_os << endl;

	_os << "    (Schema In : [";
	vector<Attribute> attL1 = schemaIn.GetAtts();

	for(int i = 0; i < attL.size(); i++){

		_os << attL1[i].name;

		if(i < attL1.size() - 1){

			_os << ", ";

		}

	}

	_os << "])" << endl;
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

	//cout << "GROUP BY GET NEXT" << endl;

	if(isFirst){

		//cout << "DOING AGGEGRATE" << endl;

		while(producer->GetNext(_record)) {

			double runningSum = 0;
			Schema sch = schemaOut;
			vector<int> attL;
			stringstream key;

			bool hasStuff = compute.HasStuff();

			if(hasStuff) {

				int intHolder = 0;
				double dubsHolder = 0;

				compute.Apply(_record, intHolder, dubsHolder);
				runningSum = intHolder + dubsHolder;
			

				for(int i = 1; i < schemaOut.GetNumAtts(); i++){

					attL.push_back(i);

				}

				sch.Project(attL);

			}

			_record.Project(&groupingAtts.whichAtts[0], groupingAtts.numAtts, schemaIn.GetNumAtts());

			//_record.print(cout, tempSch);
			//cout << endl;

			_record.print(key, sch);

			/*if(groupListVec.size() == 0){

				groupData tempHolder;

				tempHolder.rec = &rec;
				tempHolder.rec = rec;
				tempHolder.sum = runningSum;
				tempHolder.key = key.str();

				groupListVec.push_back(tempHolder);
				cout << " ADDING NEW KEY " << endl;

			}else{

			
				bool isFound = false;
				for(int i = 0; i < groupListVec.size(); i++){

					if(groupListVec[i].key == key.str()){

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
					tempHolder.key = key.str();

					groupListVec.push_back(tempHolder);
					cout << " ADDING NEW KEY " << endl;
					isFound = false;

				}

			}*/
			

			if(groupListMap.find(key.str()) == groupListMap.end()){

				groupData tempHolder;
				tempHolder.rec = _record;
				tempHolder.sum = runningSum;
			
				groupListMap[key.str()] = tempHolder;

				//cout << key.str() << " THIS IS KEY " << endl;

			}else{

				groupListMap[key.str()].sum += runningSum;

			}

		}

		groupListIter = groupListMap.begin();
		isFirst = false;

	}

	while(true){

		if(groupListIter == groupListMap.end()){

			break;

		}

		bool hasStuff = compute.HasStuff();

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


			}

			if(holder == Float){

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

			_record.AppendRecords(runningSumRec, (*groupListIter).second.rec, 1, schemaOut.GetNumAtts() - 1);

			//_record.AppendRecords(runningSumRec, *groupListVec[iter].rec, 1, schemaOut.GetNumAtts() - 1);


		}

		groupListIter++;
		//iter++;

		return true;

	}

	return false;

}

ostream& GroupBy::print(ostream& _os) {
	
	_os << "GROUP BY (Schema Out : [";
	vector<Attribute> attL = schemaOut.GetAtts();

	for(int i = 0; i < attL.size(); i++){

		_os << attL[i].name;

		if(i < attL.size() - 1){

			_os << ", ";

		}

	}

	_os << "])" << endl;
	
	_os << "         (Schema In : [";
	vector<Attribute> attL1 = schemaIn.GetAtts();

	for(int i = 0; i < attL1.size(); i++){

		_os << attL1[i].name;

		if(1 < attL1.size() - 1){

			_os << ", ";

		}

	}

	_os << "])";
	_os << *producer;

	return _os;

}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) : schema(_schema), outFile(_outFile), producer(_producer) {
		
	outFileStream.open(outFile);

}

WriteOut::~WriteOut() {

}

bool WriteOut::GetNext(Record& _record){

	//cout << "TRYING TO GET NEXT RECORD FROM WRITEOUT" << endl;
	bool ret = producer->GetNext(_record);
	if(!ret){

		//cout << "NOTHING NEXT" << endl;
		return false;

	}else{

		//cout << "TRYING TO PRINT STUFF" << endl;
		_record.print(outFileStream, schema);
		outFileStream << endl;
		return true;

	}

}

ostream& WriteOut::print(ostream& _os) {

	_os << "OUTPUT" << " (Schema Out : [";

	vector<Attribute> attL = schema.GetAtts();
	for(int i = 0; i < attL.size(); i++){

		_os << attL[i].name;

		if(i < attL.size() - 1){

			_os << ", ";

		}

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
