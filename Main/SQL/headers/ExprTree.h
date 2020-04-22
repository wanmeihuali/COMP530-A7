
#ifndef SQL_EXPRESSIONS
#define SQL_EXPRESSIONS

#include "MyDB_AttType.h"
#include "MyDB_Table.h"
#include "MyDB_Catalog.h"
#include <string>
#include <vector>
#include <functional>

// create a smart pointer for database tables
using namespace std;
class ExprTree;
typedef shared_ptr <ExprTree> ExprTreePtr;

// this class encapsules a parsed SQL expression (such as "this.that > 34.5 AND 4 = 5")

// class ExprTree is a pure virtual class... the various classes that implement it are below
class ExprTree {

public:
	virtual string toString () = 0;
	virtual ~ExprTree () {}
	virtual MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) = 0;
};

class BoolLiteral : public ExprTree {

private:
	bool myVal;
public:
	
	BoolLiteral (bool fromMe) {
		myVal = fromMe;
	}

	string toString () {
		if (myVal) {
			return "bool[true]";
		} else {
			return "bool[false]";
		}
	}

	// return a ptr point to boolAttType
	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		return make_shared <MyDB_BoolAttType>();
	}

	~BoolLiteral () {}
};

class DoubleLiteral : public ExprTree {

private:
	double myVal;
public:

	DoubleLiteral (double fromMe) {
		myVal = fromMe;
	}

	string toString () {
		return "double[" + to_string (myVal) + "]";
	}	
	
	// return a ptr point to doubleAttType
	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		return make_shared <MyDB_DoubleAttType>();
	}

	~DoubleLiteral () {}
};

// this implement class ExprTree
class IntLiteral : public ExprTree {

private:
	int myVal;
public:

	IntLiteral (int fromMe) {
		myVal = fromMe;
	}

	string toString () {
		return "int[" + to_string (myVal) + "]";
	}
	
	// return a ptr point to IntAttType
	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		return make_shared <MyDB_IntAttType> ();
	}

	~IntLiteral () {}
};

class StringLiteral : public ExprTree {

private:
	string myVal;
public:

	StringLiteral (char *fromMe) {
		fromMe[strlen (fromMe) - 1] = 0;
		myVal = string (fromMe + 1);
	}

	string toString () {
		return "string[" + myVal + "]";
	}

	// return a ptr point to StringAttType
	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		return make_shared <MyDB_StringAttType> ();
	}

	~StringLiteral () {}
};

class Identifier : public ExprTree {

private:
	string tableName;
	string attName;
public:

	Identifier (char *tableNameIn, char *attNameIn) {
		tableName = string (tableNameIn);
		attName = string (attNameIn);
	}

	string toString () {
		// customer AS c, c_namtionkey->c_c_nationkey
		return "[" + tableName + "_" + attName + "]";
	}	

	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		/*
			inputs: catalog and a function that can get the full tableName;
			outputs: type of the current indentifier
		*/
		MyDB_Table table;
		// tableName is abbreviation, use tableNameGetter to get the full-name
		if (!table.fromCatalog(tableNameGetter(tableName), catalog)) {
			std::cout << "Error: referring to table that does not exist!" << std::endl;
			return nullptr;
		}
		auto attType = table.getSchema()->getAttByName(attName).second;
		if (attType == nullptr) {
			std::cout << "Error: referring to attribute that does not exist in table" << std::endl;
		}
		return attType;
	} 

	~Identifier () {}
};

class MinusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	MinusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "- (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		auto ltype = lhs->getType(catalog, tableNameGetter);
		auto rtype = rhs->getType(catalog, tableNameGetter);
		if ((!ltype) || (!rtype)) {
			return nullptr;
		}

		if (ltype->promotableToInt() && rtype->promotableToInt()) {
			return ltype;
		}
		if (ltype->promotableToDouble() && rtype->promotableToDouble()) {
			return make_shared <MyDB_DoubleAttType>();
		}
		std::cout << "Error: MinusOp type mismatch!" << std::endl;
		return nullptr;
	}

	~MinusOp () {}
};

class PlusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	PlusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "+ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		auto ltype = lhs->getType(catalog, tableNameGetter);
		auto rtype = rhs->getType(catalog, tableNameGetter);
		if ((!ltype) || (!rtype)) {
			return nullptr;
		}
		/*
		if (ltype->isBool() && rtype->isBool()) {
			return ltype;
		} else if (ltype->isBool() || rtype->isBool()) {
			return nullptr;
		}
		*/
		if (ltype->isBool() || rtype->isBool()) {
			std::cout << "Error: PlusOp type mismatch! Both ltype and rtype are bool!" << std::endl;
			return nullptr;
		}
		if (ltype->promotableToInt() && rtype->promotableToInt()) {
			return ltype;
		}
		if (ltype->promotableToDouble() && rtype->promotableToDouble()) {
			return make_shared <MyDB_DoubleAttType>();
		}
		if (ltype->promotableToDouble() || rtype->promotableToDouble()) {
			std::cout << "Error: PlusOp type mismatch! ltype or rtype is double, while the other is string!" << std::endl;
			return nullptr;
		}
		return make_shared <MyDB_StringAttType>();
	}

	~PlusOp () {}
};

class TimesOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	TimesOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "* (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		auto ltype = lhs->getType(catalog, tableNameGetter);
		auto rtype = rhs->getType(catalog, tableNameGetter);
		if ((!ltype) || (!rtype)) {
			return nullptr;
		}

		if (ltype->promotableToInt() && rtype->promotableToInt()) {
			return ltype;
		}
		if (ltype->promotableToDouble() && rtype->promotableToDouble()) {
			return make_shared <MyDB_DoubleAttType>();
		}
		std::cout << "Error: TimesOp type mismatch!" << std::endl;
		return nullptr;
	}

	~TimesOp () {}
};

class DivideOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	DivideOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "/ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		auto ltype = lhs->getType(catalog, tableNameGetter);
		auto rtype = rhs->getType(catalog, tableNameGetter);
		if ((!ltype) || (!rtype)) {
			return nullptr;
		}
		if (ltype->promotableToInt() && rtype->promotableToInt()) {
			return ltype;
		}
		if (ltype->promotableToDouble() && rtype->promotableToDouble()) {
			return make_shared <MyDB_DoubleAttType>();
		}
		std::cout << "Error: DivideOP type mismatch!" << std::endl;
		return nullptr;
	}

	~DivideOp () {}
};

class GtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	GtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "> (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		auto ltype = lhs->getType(catalog, tableNameGetter);
		auto rtype = rhs->getType(catalog, tableNameGetter);
		if ((!ltype) || (!rtype)) {
			return nullptr;
		}
		if (ltype->isBool() || rtype->isBool()) {
			std::cout << "Error: GtOp is comparing boolean value(s)!" << std::endl;
			return nullptr;
		}
		if (ltype->promotableToDouble() && rtype->promotableToDouble()) {
			return make_shared <MyDB_BoolAttType>();
		}
		if (ltype->promotableToDouble() || rtype->promotableToDouble()) {
			std::cout << "Error: GtOp is comparing numeric value and string!" << std::endl;
			return nullptr;
		}
		return make_shared <MyDB_BoolAttType>();
	}

	~GtOp () {}
};

class LtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	LtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "< (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		auto ltype = lhs->getType(catalog, tableNameGetter);
		auto rtype = rhs->getType(catalog, tableNameGetter);
		if ((!ltype) || (!rtype)) {
			return nullptr;
		}
		if (ltype->isBool() || rtype->isBool()) {
			std::cout << "Error: LtOp is comparing boolean value(s)!" << std::endl;
			return nullptr;
		}
		if (ltype->promotableToDouble() && rtype->promotableToDouble()) {
			return make_shared <MyDB_BoolAttType>();
		}
		if (ltype->promotableToDouble() || rtype->promotableToDouble()) {
			std::cout << "Error: LtOp is comparing numeric value and string!" << std::endl;
			return nullptr;
		}
		return make_shared <MyDB_BoolAttType>();
	}

	~LtOp () {}
};

class NeqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	NeqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "!= (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		auto ltype = lhs->getType(catalog, tableNameGetter);
		auto rtype = rhs->getType(catalog, tableNameGetter);
		if ((!ltype) || (!rtype)) {
			return nullptr;
		}
		if (ltype->isBool() || rtype->isBool()) {
			std::cout << "Error: NeqOp is comparing boolean value(s)!" << std::endl;
			return nullptr;
		}
		if (ltype->promotableToDouble() && rtype->promotableToDouble()) {
			return make_shared <MyDB_BoolAttType>();
		}
		if (ltype->promotableToDouble() || rtype->promotableToDouble()) {
			std::cout << "Error: NeqOp is comparing numeric value and string!" << std::endl;
			return nullptr;
		}
		return make_shared <MyDB_BoolAttType>();
	}

	~NeqOp () {}
};

class OrOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	OrOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "|| (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		auto ltype = lhs->getType(catalog, tableNameGetter);
		auto rtype = rhs->getType(catalog, tableNameGetter);
		if ((!ltype) || (!rtype)) {
			return nullptr;
		}
		if (ltype->isBool() && rtype->isBool()) {
			return ltype;
		}
		std::cout << "Error: OrOp type mismatch! " << std::endl;
		return nullptr;
	}	

	~OrOp () {}
};

class EqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	EqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "== (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		auto ltype = lhs->getType(catalog, tableNameGetter);
		auto rtype = rhs->getType(catalog, tableNameGetter);
		if ((!ltype) || (!rtype)) {
			return nullptr;
		}
		if (ltype->isBool() || rtype->isBool()) {
			std::cout << "Error: NeqOp is comparing boolean value(s)!" << std::endl;
			return nullptr;
		}
		if (ltype->promotableToDouble() && rtype->promotableToDouble()) {
			return make_shared <MyDB_BoolAttType>();
		}
		if (ltype->promotableToDouble() || rtype->promotableToDouble()) {
			std::cout << "Error: NeqOp is comparing numeric value and string!" << std::endl;
			return nullptr;
		}
		return make_shared <MyDB_BoolAttType>();
	}	

	~EqOp () {}
};

class NotOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	NotOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "!(" + child->toString () + ")";
	}	

	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		auto ctype = child->getType(catalog, tableNameGetter);
		if (!ctype) {
			return nullptr;
		}
		if (ctype->isBool()) {
			return ctype;
		}
		std::cout << "Error: NotOp type mismatch!" << std::endl;
		return nullptr;
	}	

	~NotOp () {}
};

class SumOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	SumOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "sum(" + child->toString () + ")";
	}	
	
	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		auto ctype = child->getType(catalog, tableNameGetter);
		if (!ctype) {
			return nullptr;
		}
		if (ctype->promotableToDouble()) {
			return ctype;
		}
		std::cout << "Error: SumOp type mismatch!" << std::endl;
		return nullptr;
	}	

	~SumOp () {}
};

class AvgOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	AvgOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "avg(" + child->toString () + ")";
	}	

	MyDB_AttTypePtr getType(MyDB_CatalogPtr catalog, std::function<string(string)> tableNameGetter) override {
		auto ctype = child->getType(catalog, tableNameGetter);
		if (!ctype) {
			return nullptr;
		}
		if (ctype->promotableToDouble()) {
			return ctype;
		}
		std::cout << "Error: AvgOp type mismatch!" << std::endl;
		return nullptr;
	}

	~AvgOp () {}
};

#endif
