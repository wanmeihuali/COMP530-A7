
#ifndef PARSER_TYPES_H
#define PARSER_TYPES_H

#include <iostream>
#include <stdlib.h>
#include "ExprTree.h"
#include "MyDB_Catalog.h"
#include "MyDB_Schema.h"
#include "MyDB_Table.h"
#include <string>
#include <utility>
#include "RelAlgebra.h"
#include <memory>
#include "MyDB_TableReaderWriter.h"

using namespace std;

/*************************************************/
/** HERE WE DEFINE ALL OF THE STRUCTS THAT ARE **/
/** PASSED AROUND BY THE PARSER                **/
/*************************************************/

// structure that encapsulates a parsed computation that returns a value
struct Value {

private:

        // this points to the expression tree that computes this value
        ExprTreePtr myVal;

public:
        ~Value () {}

        Value (ExprTreePtr useMe) {
                myVal = useMe;
        }

        Value () {
                myVal = nullptr;
        }
	
	friend struct CNF;
	friend struct ValueList;
	friend struct SFWQuery;
	#include "FriendDecls.h"
};

// structure that encapsulates a parsed CNF computation
struct CNF {

private:

        // this points to the expression tree that computes this value
        vector <ExprTreePtr> disjunctions;

public:
        ~CNF () {}

        CNF (struct Value *useMe) {
              	disjunctions.push_back (useMe->myVal); 
        }

        CNF () {}

	friend struct SFWQuery;
	#include "FriendDecls.h"
};

// structure that encapsulates a parsed list of value computations
struct ValueList {

private:

        // this points to the expression tree that computes this value
        vector <ExprTreePtr> valuesToCompute;

public:
        ~ValueList () {}

        ValueList (struct Value *useMe) {
              	valuesToCompute.push_back (useMe->myVal); 
        }

        ValueList () {}

	friend struct SFWQuery;
	#include "FriendDecls.h"
};


// structure to encapsulate a create table
struct CreateTable {

private:

	// the name of the table to create
	string tableName;

	// the list of atts to create... the string is the att name
	vector <pair <string, MyDB_AttTypePtr>> attsToCreate;

	// true if we create a B+-Tree
	bool isBPlusTree;

	// the attribute to organize the B+-Tree on
	string sortAtt;

public:
	string addToCatalog (string storageDir, MyDB_CatalogPtr addToMe) {

		// make the schema
		MyDB_SchemaPtr mySchema = make_shared <MyDB_Schema>();
		for (auto a : attsToCreate) {
			mySchema->appendAtt (a);
		}

		// now, make the table
		MyDB_TablePtr myTable;

		// just a regular file
		if (!isBPlusTree) {
			myTable =  make_shared <MyDB_Table> (tableName, 
				storageDir + "/" + tableName + ".bin", mySchema);	

		// creating a B+-Tree
		} else {
			
			// make sure that we have the attribute
			if (mySchema->getAttByName (sortAtt).first == -1) {
				cout << "B+-Tree not created.\n";
				return "nothing";
			}
			myTable =  make_shared <MyDB_Table> (tableName, 
				storageDir + "/" + tableName + ".bin", mySchema, "bplustree", sortAtt);	
		}

		// and add to the catalog
		myTable->putInCatalog (addToMe);

		return tableName;
	}

	CreateTable () {}

	CreateTable (string tableNameIn, vector <pair <string, MyDB_AttTypePtr>> atts) {
		tableName = tableNameIn;
		attsToCreate = atts;
		isBPlusTree = false;
	}

	CreateTable (string tableNameIn, vector <pair <string, MyDB_AttTypePtr>> atts, string sortAttIn) {
		tableName = tableNameIn;
		attsToCreate = atts;
		isBPlusTree = true;
		sortAtt = sortAttIn;
	}
	
	~CreateTable () {}

	#include "FriendDecls.h"

};

// structure that stores a list of attributes
struct AttList {

private:

	// the list of attributes
	vector <pair <string, MyDB_AttTypePtr>> atts;

public:
	AttList (string attName, MyDB_AttTypePtr whichType) {
		atts.push_back (make_pair (attName, whichType));
	}

	~AttList () {}

	friend struct SFWQuery;
	#include "FriendDecls.h"
};

struct FromList {

private:

	// the list of tables and aliases
	vector <pair <string, string>> aliases;

public:
	FromList (string tableName, string aliasName) {
		aliases.push_back (make_pair (tableName, aliasName));
	}

	FromList () {}

	~FromList () {}
	
	friend struct SFWQuery;
	#include "FriendDecls.h"
};


// structure that stores an entire SFW query
struct SFWQuery {

private:

	// the various parts of the SQL query
	vector <ExprTreePtr> valuesToSelect;
	vector <pair <string, string>> tablesToProcess;
	vector <ExprTreePtr> allDisjunctions;
	vector <ExprTreePtr> groupingClauses;

public:
	SFWQuery () {}

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause, 
		struct CNF *cnf, struct ValueList *grouping) {
		valuesToSelect = selectClause->valuesToCompute;
		tablesToProcess = fromClause->aliases;
		allDisjunctions = cnf->disjunctions;
		groupingClauses = grouping->valuesToCompute;
	}

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause, 
		struct CNF *cnf) {
		valuesToSelect = selectClause->valuesToCompute;
		tablesToProcess = fromClause->aliases;
		allDisjunctions = cnf->disjunctions;
	}

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause) {
		valuesToSelect = selectClause->valuesToCompute;
		tablesToProcess = fromClause->aliases;
		allDisjunctions.push_back (make_shared <BoolLiteral> (true));
	}
	
	~SFWQuery () {}

	void print () {
		cout << "Selecting the following:\n";
		for (auto a : valuesToSelect) {
			cout << "\t" << a->toString () << "\n";
		}
		cout << "From the following:\n";
		for (auto a : tablesToProcess) {
			cout << "\t" << a.first << " AS " << a.second << "\n";
		}
		cout << "Where the following are true:\n";
		for (auto a : allDisjunctions) {
			cout << "\t" << a->toString () << "\n";
		}
		cout << "Group using:\n";
		for (auto a : groupingClauses) {
			cout << "\t" << a->toString () << "\n";
		}
	}

	#include "FriendDecls.h"

	/*
	select 
			l.l_comment
	from 
			lineitem as l
	where
			(l.l_shipdate = "1994-05-12") and
			(l.l_commitdate = "1994-05-22") and
			(l.l_receiptdate = "1994-06-10");

	l->lineitem
	
	RegularSelection:
	MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
		string selectionPredicate, vector <string> projections

	selectionPredicate: (l.l_shipdate = "1994-05-12") and
			(l.l_commitdate = "1994-05-22") and
			(l.l_receiptdate = "1994-06-10");

	toString:
			== ([l_l_shipdate], string[1994-05-12])
			== ([l_l_commitdate], string[1994-05-22])
			== ([l_l_receiptdate], string[1994-06-10])

	TargetVersion:
			"|| ( == ([l_nationkey], int[3]), == ([l_nationkey], int[4]))"
			not right == ([l_shipdate], string[1994-05-12])
			lineitem: l_shipdate, l_commitdate...
			->lineitem: l_l_shipdate, l_l_commitdate
			== ([l_l_shipdate], string[1994-05-12])
	*/

	// find the full tableName from the shortName
	string getTableName(string shortName) {
		for (auto& namePair: tablesToProcess) {
			if (namePair.second == shortName) {
				return namePair.first;
			}
		}
		return ""; // table do not exist
	}

	std::shared_ptr<RelAlgebra> toRelAgebra(MyDB_CatalogPtr catelog, MyDB_BufferManagerPtr bufferMgr) {
    if (groupingClauses.size() == 0 && tablesToProcess.size() == 1) {
		// SFWQuery trans to RelAlgebra, only one selection from a single table
		auto all_tables = MyDB_Table :: getAllTables(catelog);
		string table_name = tablesToProcess[0].first;
		string table_alias = tablesToProcess[0].second;
		auto ori_input_table = std::make_shared<MyDB_TableReaderWriter>(all_tables[table_name], bufferMgr);
		auto input_table_copy = std::make_shared<MyDB_TableReaderWriter>(ori_input_table);
		
		input_table_copy->getTable()->getSchema()->addPrefix(table_alias);

		//Q: the result of selection might not have a name
		//Q: name of the output table
		

		

		return nullptr;
	}
	
}
};

// structure that sores an entire SQL statement
struct SQLStatement {

private:

	// in case we are a SFW query
	SFWQuery myQuery;
	bool isQuery;

	// in case we a re a create table
	CreateTable myTableToCreate;
	bool isCreate;

public:
	SQLStatement (struct SFWQuery* useMe) {
		myQuery = *useMe;
		isQuery = true;
		isCreate = false;
	}

	SQLStatement (struct CreateTable *useMe) {
		myTableToCreate = *useMe;
		isQuery = false;
		isCreate = true;
	}

	bool isCreateTable () {
		return isCreate;
	}

	bool isSFWQuery () {
		return isQuery;
	}

	string addToCatalog (string storageDir, MyDB_CatalogPtr addToMe) {
		return myTableToCreate.addToCatalog (storageDir, addToMe);
	}		
	
	void printSFWQuery () {
		myQuery.print ();
	}

	#include "FriendDecls.h"
};

#endif
