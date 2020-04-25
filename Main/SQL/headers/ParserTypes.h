
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
#include "MyDB_BPlusTreeReaderWriter.h"
#include "RegularSelection.h"
#include <string>
#include <sstream>
#include "cstdio"
#include "Aggregate.h"

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


	bool isValid(MyDB_CatalogPtr catalog) {
		auto tableNameGetter = [this](string shortName){
			return this->getTableName(shortName);
		};
		for (auto& expr: valuesToSelect) {
			if (expr->getType(catalog, this->tablesToProcess) == nullptr) {
				return false;
			}
		}
		for (auto& expr: allDisjunctions) {
			if (expr->getType(catalog, this->tablesToProcess) == nullptr) {
				return false;
			}
		}
		for (auto& expr: groupingClauses) {
			if (expr->getType(catalog, this->tablesToProcess) == nullptr) {
				return false;
			}
		}

		for (auto& selectExpr: valuesToSelect) {
			auto selectStr = selectExpr->toString();
			if (selectStr.size() < 3 || (selectStr.substr(0, 3) != "sum" && selectStr.substr(0, 3) != "avg")) {
				for (auto& groupExpr: groupingClauses) {
					auto groupStr = groupExpr->toString();
					if (groupStr == selectStr) {
						break;
					}
					std::cout << "Error: the only selected attributes must be functions of the grouping attributes! " << selectStr << "not found" << std::endl;
					return false;
				}
			}
		}
		
		// TO DO: check if all tables in tablesToProcess exist
		return true;
	}

	std::shared_ptr<RelAlgebra> toRelAgebra(
		MyDB_CatalogPtr catelog, 
		MyDB_BufferManagerPtr bufferMgr, 
		map <string, MyDB_TableReaderWriterPtr>& allTableReaderWriters,
		map <string, MyDB_BPlusTreeReaderWriterPtr>& allBPlusReaderWriters) {
		if (tablesToProcess.size() == 1) {
			// SFWQuery trans to RelAlgebra, only one selection from a single table
			string table_name = tablesToProcess[0].first;
			string table_alias = tablesToProcess[0].second;
			MyDB_TableReaderWriterPtr ori_input_table;
			if (allTableReaderWriters.find(table_name) != allTableReaderWriters.end()) {
				ori_input_table = allTableReaderWriters[table_name];
			} else {
				ori_input_table = allBPlusReaderWriters[table_name];
			}

			MyDB_TableReaderWriterPtr input_table_copy = std::make_shared<MyDB_TableReaderWriter>(ori_input_table);
			

			// modify the attributes in the copied input table
			input_table_copy->getTable()->getSchema()->addPrefix(table_alias);

			std::cout << input_table_copy->getTable()->getSchema() << std::endl;

/*
Selecting the following:
	sum(int[1])
	avg(/ (- ([o_o_totalprice], double[32592.140000]), double[32592.140000]))
From the following:
	orders AS o
Where the following are true:
	== ([o_o_orderstatus], string[F])
	|| (< ([o_o_orderpriority], string[2-HIGH]), == ([o_o_orderpriority], string[2-HIGH]))
Group using:



[<o_o_orderkey, int>, <o_o_custkey, int>, <o_o_orderstatus, string>, <o_o_totalprice, double>, <o_o_orderdate, string>, <o_o_orderpriority, string>, <o_o_clerk, string>, <o_o_shippriority, int>, <o_o_comment, string>]

for aggregation part
output schema = 
[all grouping attributes, <sum(int[1]) int>, <avg(/ (- ([o_o_totalprice], double[32592.140000]), double[32592.140000])), double>]

Aggregate (MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
		vector <pair <MyDB_AggType, string>> aggsToCompute,
		vector <string> groupings, string selectionPredicate);

for selection part
input schema = 
[all grouping attributes, <sum(int[1]) int>, <avg(/ (- ([o_o_totalprice], double[32592.140000]), double[32592.140000])), double>]
projections = original
selectionPredicate = ""
output schema = [<sum(int[1]) int>, <avg(/ (- ([o_o_totalprice], double[32592.140000]), double[32592.140000])), double>]

select
	"supplier name was " + s.s_name,
	sum (1) 
from
	supplier as s,
	lineitem as l1,
	lineitem as l2,
	orders as o,
	nation as n
where
	(s.s_suppkey = l1.l_suppkey)
	and (o.o_orderkey = l1.l_orderkey)
	and (o.o_orderstatus = "F")
	and (l1.l_receiptdate > l1.l_commitdate)
	and (l2.l_orderkey = l1.l_orderkey)
	and (not l2.l_suppkey = l1.l_suppkey)
group by
	s.s_name;

select
+ (string["supplier name was "], [s_s_name])
sum(int[1])

group by
s_s_name

[<s_s_name, string>, <Groupby1, int>]

*/

			
			// generate the projections and the schema for the output table
			
			vector<string> groupings;
			vector <pair <MyDB_AggType, string>> aggToCompute;
			MyDB_SchemaPtr output_schema = make_shared<MyDB_Schema>();
			MyDB_SchemaPtr inter_output_schema = make_shared<MyDB_Schema>();
			for (auto& grouping_clause : groupingClauses) {
				auto grouping_identifiers = grouping_clause->getIdentifiers();
				if (grouping_identifiers.size() == 1) {
					inter_output_schema->appendAtt(make_pair(grouping_clause->getIdentifiers()[0], grouping_clause->getType(catelog, this->tablesToProcess)));
				} else {
					inter_output_schema->appendAtt(make_pair(grouping_clause->toString(), grouping_clause->getType(catelog, this->tablesToProcess)));
				}

				groupings.push_back(grouping_clause->toString());
			}

			bool need_aggregation = false;
			int aggidx = 0;
			for (auto& value_to_select : valuesToSelect) {
				string value_str = value_to_select->toString();
				string agg_type = "";
				string expr_str = "";
				if (value_str.size() >= 5) {
					agg_type = value_str.substr(0, 3);
					// sum(int[1]) -> int[1]
					expr_str = value_str.substr(4, value_str.size() - 5);
				}
				if (agg_type == "sum") {
					// agg function using SUM
					need_aggregation = true;
					aggToCompute.push_back(std::make_pair(SUMAGG, expr_str));
					inter_output_schema->appendAtt(make_pair("MyDB_AggAtt" + std::to_string(aggidx), value_to_select->getType(catelog, this->tablesToProcess)));
					aggidx++;
				} else if (agg_type == "avg") {
					// agg fnction using AVG
					need_aggregation = true;
					aggToCompute.push_back(std::make_pair(AVGAGG, expr_str));
					inter_output_schema->appendAtt(make_pair("MyDB_AggAtt" + std::to_string(aggidx), value_to_select->getType(catelog, this->tablesToProcess)));
					aggidx++;
				} else {
					// no agg function
					
				}
				output_schema->appendAtt(make_pair(value_to_select->toString(), value_to_select->getType(catelog, this->tablesToProcess)));
			}
	
			//generate the output table based on the output_schema
			// generate a temp table with as our output table
			auto output_tablePtr = std::make_shared<MyDB_TableReaderWriter>(std::make_shared<MyDB_Table>("temp", "temp", output_schema), bufferMgr);

			// generate the selectionPredicate
			string selectionPredicate;

			if (allDisjunctions.empty()) {
				selectionPredicate = "bool[true]";
			} else {
				selectionPredicate = allDisjunctions[0]->toString();
				for (size_t idx = 1; idx < allDisjunctions.size(); ++idx) {
					selectionPredicate = "&& (" + allDisjunctions[idx]->toString() + ", " + selectionPredicate + ")";
				}
			}
			vector<string> projections;
			if (need_aggregation) {
				// generate the intermediate output schema for the aggregate function
				/*
				for (auto &a : output->getTable ()->getSchema ()->getAtts ()) {
					if (i < numGroups) 
						aggSchema->appendAtt (make_pair ("MyDB_GroupAtt" + to_string (i++), a.second));
					else
						aggSchema->appendAtt (make_pair ("MyDB_AggAtt" + to_string (i++ - numGroups), a.second));
				}
				aggSchema->appendAtt (make_pair ("MyDB_CntAtt", make_shared <MyDB_IntAttType> ()));
				*/
				int aggidx1 = 0;
				for (auto& value_to_select : valuesToSelect) {
					string value_str = value_to_select->toString();
					string agg_type = "";
					string expr_str = "";
					if (value_str.size() >= 5) {
						agg_type = value_str.substr(0, 3);
						// sum(int[1]) -> int[1]
						expr_str = value_str.substr(4, value_str.size() - 5);
					}
					if (agg_type == "sum" || agg_type == "avg") {		
						projections.push_back("[MyDB_AggAtt" + std::to_string(aggidx1) + "]");
						aggidx1++;
					} else {
						projections.push_back(value_str);
					}
					
				}

				auto inter_output_tablePtr = std::make_shared<MyDB_TableReaderWriter>(std::make_shared<MyDB_Table>("temp1", "temp1", inter_output_schema), bufferMgr);



				Aggregate agg(input_table_copy, inter_output_tablePtr, aggToCompute, groupings, selectionPredicate);
				agg.run();

				
				{
					stringstream ss;
					ss << inter_output_schema << "\n";

					printf("%s", ss.str().c_str());
				}

				{
					stringstream ss;
					ss << output_schema << "\n";

					printf("%s", ss.str().c_str());
				}


				RegularSelection regularSelect(inter_output_tablePtr, output_tablePtr, "bool[true]", projections);
				regularSelect.run();

			} else {
				for (auto& value_to_select : valuesToSelect) {
					string value_str = value_to_select->toString();
					projections.push_back(value_str);
				}

				// use the selection in Assignment 6 (RegularSelection)
				RegularSelection regularSelect(input_table_copy, output_tablePtr, selectionPredicate, projections);

				regularSelect.run();
			}


			{
				MyDB_RecordPtr tempRec = output_tablePtr->getEmptyRecord();
				MyDB_RecordIteratorAltPtr myIter = output_tablePtr->getIteratorAlt();

				while (myIter->advance()) {
					myIter->getCurrent(tempRec);
					cout << tempRec << "\n";
				}
			}

			return nullptr;
		}
	

	}
	#include "FriendDecls.h"
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

	bool isSFWValid(MyDB_CatalogPtr catalog) {
		return myQuery.isValid(catalog);
	}

	void executeSFWQuery(
		MyDB_CatalogPtr catelog, 
		MyDB_BufferManagerPtr bufferMgr, 
		map <string, MyDB_TableReaderWriterPtr>& allTableReaderWriters,
		map <string, MyDB_BPlusTreeReaderWriterPtr>& allBPlusReaderWriters) {
		myQuery.toRelAgebra(catelog, bufferMgr, allTableReaderWriters, allBPlusReaderWriters);
	}

	#include "FriendDecls.h"
};

#endif
