#ifndef RELALGEBRA_H
#define RELALGEBRA_H

#include "MyDB_TableReaderWriter.h"
#include <memory>

enum RelAlgebraType {
    UNKNOWN,
    TABLE,
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

    MyDB_TableReaderWriterPtr execute() {

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


#endif
