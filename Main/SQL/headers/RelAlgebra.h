#ifndef RELALGEBRA_H
#define RELALGEBRA_H

#include "MyDB_TableReaderWriter.h"
#include <memory>

#include "ExprTree.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "Aggregate.h"
#include "RegularSelection.h"

enum RelAlgebraType {
    UNKNOWN,
    TABLETYPE,
    SELECTIONPROJECTION,
    JOIN,
    AGGREGATE
};

class RelAlgebra {
public:
    RelAlgebra();
    RelAlgebraType getType() {
        return type;
    }

    //virtual int estimateCost() = 0;
    virtual MyDB_TableReaderWriterPtr execute() = 0;
protected:
    RelAlgebraType type;
};




class SelectionProjectionOp: public RelAlgebra {
    // MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
	//	string selectionPredicate, vector <string> projections
    SelectionProjectionOp(std::string inputTable, std::string selectionPredicate, vector <string> projections) {
        this->type = SELECTIONPROJECTION;
    }

    MyDB_TableReaderWriterPtr execute() override {
        return nullptr;
    }
};


/*
class JoinOp: public RelAlgebra {
public:


};
*/

/*
class AggregateOp: public RelAlgebra {
public:


};
*/
/*
class TableOp: public RelAlgebra {

};
*/

void ExecuteSFWQuery(
    	vector <ExprTreePtr>& valuesToSelect,
        vector <pair <string, string>>& tablesToProcess,
        vector <ExprTreePtr>& allDisjunctions,
        vector <ExprTreePtr>& groupingClauses,
		MyDB_CatalogPtr catelog, 
		MyDB_BufferManagerPtr bufferMgr, 
		map <string, MyDB_TableReaderWriterPtr>& allTableReaderWriters,
		map <string, MyDB_BPlusTreeReaderWriterPtr>& allBPlusReaderWriters); 

#endif
