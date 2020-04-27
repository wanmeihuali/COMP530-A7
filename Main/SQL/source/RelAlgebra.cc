#include "RelAlgebra.h"
#include <sstream>
#include <cstdio>
#include <map>
#include <set>
#include <list>
#include <queue>
#include <limits>
#include "ScanJoin.h"

void ExecuteSingleTableQuery(
    MyDB_TableReaderWriterPtr input_table_copy,
    vector <ExprTreePtr>& valuesToSelect,
    vector <pair <string, string>>& tablesToProcess,
    vector <ExprTreePtr>& allDisjunctions,
    vector <ExprTreePtr>& groupingClauses,
    MyDB_CatalogPtr catelog, 
    MyDB_BufferManagerPtr bufferMgr) 
{ 

    // generate the projections and the schema for the output table
    vector<string> groupings;
    vector <pair <MyDB_AggType, string>> aggToCompute;
    MyDB_SchemaPtr output_schema = make_shared<MyDB_Schema>();
    MyDB_SchemaPtr inter_output_schema = make_shared<MyDB_Schema>();
    for (auto& grouping_clause : groupingClauses) {
        vector<std::pair<std::string, std::string>> grouping_identifiers = grouping_clause->getIdentifiers();
        if (grouping_identifiers.size() == 1) {
            string table_name = grouping_identifiers[0].first;
            string att_name = grouping_identifiers[0].second;
            inter_output_schema->appendAtt(make_pair(table_name + "_" + att_name, grouping_clause->getType(catelog, tablesToProcess)));
        } else {
            inter_output_schema->appendAtt(make_pair(grouping_clause->toString(), grouping_clause->getType(catelog, tablesToProcess)));
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
            inter_output_schema->appendAtt(make_pair("MyDB_AggAtt" + std::to_string(aggidx), value_to_select->getType(catelog, tablesToProcess)));
            aggidx++;
        } else if (agg_type == "avg") {
            // agg fnction using AVG
            need_aggregation = true;
            aggToCompute.push_back(std::make_pair(AVGAGG, expr_str));
            inter_output_schema->appendAtt(make_pair("MyDB_AggAtt" + std::to_string(aggidx), value_to_select->getType(catelog, tablesToProcess)));
            aggidx++;
        } else {
            // no agg function
            
        }
        output_schema->appendAtt(make_pair(value_to_select->toString(), value_to_select->getType(catelog, tablesToProcess)));
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

 /*       
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
*/

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

        int outputCounter = 0;
        while (myIter->advance()) {
            myIter->getCurrent(tempRec);
            
            outputCounter++;
            if (outputCounter <= 30) {
                cout << tempRec << "\n";
            } else if (outputCounter == 31) {
                cout << "...\n";
            }
        }
        cout << "There are " << outputCounter << " records in the output" << std::endl;
    }
}



MyDB_TableReaderWriterPtr findTableReaderWriter(
    map <string, MyDB_TableReaderWriterPtr>& allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr>& allBPlusReaderWriters,
    string table_name)
{
    MyDB_TableReaderWriterPtr ret;
    if (allTableReaderWriters.find(table_name) != allTableReaderWriters.end()) {
        ret = allTableReaderWriters[table_name];
    } else {
        ret = allBPlusReaderWriters[table_name];
    }
    return ret;
}



struct TableEdge {
    std::string lalias;
    std::string ralias;
    std::vector<std::pair<ExprTreePtr, ExprTreePtr>> equalityChecks;
};  // a set of equalityChecks between two table



size_t estimateJoinOutputSize(
    std::map<std::string, MyDB_TableReaderWriterPtr> tableReaderWriterLookUpMap,
    TableEdge& edge) 
{
    MyDB_TableReaderWriterPtr table0 = tableReaderWriterLookUpMap[edge.lalias];
    MyDB_TableReaderWriterPtr table1 = tableReaderWriterLookUpMap[edge.ralias];
    /*
    size_t table0Size = table0->getTable()->getTupleCount();
    size_t table1Size = table1->getTable()->getTupleCount();
    size_t estimateSize = table0Size * table1Size;
    // size0 * size1/ (d00 * d01 * d10 * d11 * ...)
    for (auto equalityCheck : edge.equalityChecks) {
        auto table0Atts = equalityCheck.first->getIdentifiers();
        auto table1Atts = equalityCheck.second->getIdentifiers();
        size_t table0distinctValues = table0->getTable()->getDistinctValues(table0Atts[0].second);
        size_t table1distinctValues = table1->getTable()->getDistinctValues(table1Atts[0].second);
        if (table0distinctValues < table1distinctValues) {
            estimateSize /= table1distinctValues;
        } else {
            estimateSize /= table0distinctValues;
        }
        
        
    }
    return estimateSize;
    */
   size_t table0PageNum = table0->getNumPages();
   size_t table1PageNum = table1->getNumPages();
   if (edge.equalityChecks.size() == 0) {
       return (std::numeric_limits<size_t>::max)();
   }
   return table0PageNum * table1PageNum;
}



void ExecuteSFWQuery(
    	vector <ExprTreePtr>& valuesToSelect,
        vector <pair <string, string>>& tablesToProcess,
        vector <ExprTreePtr>& allDisjunctions,
        vector <ExprTreePtr>& groupingClauses,
		MyDB_CatalogPtr catelog, 
		MyDB_BufferManagerPtr bufferMgr, 
		map <string, MyDB_TableReaderWriterPtr>& allTableReaderWriters,
		map <string, MyDB_BPlusTreeReaderWriterPtr>& allBPlusReaderWriters) 
{
    if (tablesToProcess.size() == 1) {
        // SFWQuery trans to RelAlgebra, only one selection from a single table
        string table_name = tablesToProcess[0].first;
        string table_alias = tablesToProcess[0].second;
        MyDB_TableReaderWriterPtr ori_input_table = findTableReaderWriter(allTableReaderWriters, allBPlusReaderWriters, table_name);
        // modify the attributes in the copied input table
        MyDB_TableReaderWriterPtr input_table_copy = std::make_shared<MyDB_TableReaderWriter>(ori_input_table);
        input_table_copy->getTable()->getSchema()->addPrefix(table_alias);
        // select from a single table, no join
        ExecuteSingleTableQuery(
            input_table_copy,
            valuesToSelect,
            tablesToProcess,
            allDisjunctions,
            groupingClauses,
            catelog,
            bufferMgr
        );
    } else { // query with join

        /*
        ScanJoin (MyDB_TableReaderWriterPtr leftInput, MyDB_TableReaderWriterPtr rightInput,
            MyDB_TableReaderWriterPtr output, string finalSelectionPredicate, 
            vector <string> projections,
            vector <pair <string, string>> equalityChecks, string leftSelectionPredicate,
            string rightSelectionPredicate);
        */
        std::map<std::string, MyDB_TableReaderWriterPtr> tableReaderWriterLookUpMap;
        std::map<std::string, MyDB_TableReaderWriterPtr> tableReaderWriterWithPrefixLookUpMap;
        std::map<std::string, std::set<std::string>> joinedTableOriginalAlias; // given an alias of a joined table get the original alias 
        std::map<std::string, std::string> tableNameToAlias;
        std::map<std::string, std::string> tableAliasToName;
        for (auto& table_pair: tablesToProcess) {
            string table_name = table_pair.first;
            string table_alias = table_pair.second;
            tableNameToAlias[table_name] = table_alias;
            tableAliasToName[table_alias] = table_name;
            tableReaderWriterLookUpMap[table_alias] = findTableReaderWriter(allTableReaderWriters, allBPlusReaderWriters, table_name);
            tableReaderWriterWithPrefixLookUpMap[table_alias] = std::make_shared<MyDB_TableReaderWriter>(tableReaderWriterLookUpMap[table_alias]);
            tableReaderWriterWithPrefixLookUpMap[table_alias]->getTable()->getSchema()->addPrefix(table_alias);
            joinedTableOriginalAlias[table_alias].insert(table_alias);
        }

        // find all attrs needed
        std::map<std::string, set<string>> tableProjectionRequirement;  // e.g. <"orders", <"o_o_orderdate">>

        // attrs in select
        for (ExprTreePtr value : valuesToSelect) {
            auto valueAtts = value->getIdentifiers();
            for (auto& aliasAttPair : valueAtts) {
                string tableAlias = aliasAttPair.first;
                string tableAtts = aliasAttPair.second;
                tableProjectionRequirement[tableAlias].insert(tableAlias + "_" + tableAtts);
            }
        }
        // attrs in where
        for (ExprTreePtr disjunction : allDisjunctions) {
            auto valueAtts = disjunction->getIdentifiers();
            for (auto& aliasAttPair : valueAtts) {
                string tableAlias = aliasAttPair.first;
                string tableAtts = aliasAttPair.second;
                tableProjectionRequirement[tableAlias].insert(tableAlias + "_" + tableAtts);
            }
        }
        // attrs in group by
        for (ExprTreePtr clause : groupingClauses) {
            auto valueAtts = clause->getIdentifiers();
            for (auto& aliasAttPair : valueAtts) {
                string tableAlias = aliasAttPair.first;
                string tableAtts = aliasAttPair.second;
                tableProjectionRequirement[tableAlias].insert(tableAlias + "_" + tableAtts);
            }
        }

        typedef std::set<std::string> DisjunctionPrerequests;  //< table to be include for the disjunction to be execute
        std::list<std::pair<ExprTreePtr, DisjunctionPrerequests>> nonEqualityCheckDisjunctions; //< all expression that are not for join equality check, example: || (> ([o_o_orderdate], String[1995-01-01]), (== ([o_o_orderdate], String[1995-01-01]))

        
        std::list<TableEdge> tableEdgeList;   // graph representation of tables and their join connctions
        //first do the selection on the smaller table (might can make it smaller and fit into the cache)
        // for each table, it needs to join 
        // start to join: equalityChecks        
        for (ExprTreePtr disjunction: allDisjunctions) {
            string disjunction_str = disjunction->toString();
            bool isJoinequalityCheck = false;
            if (disjunction_str[0] == '=') {
                //auto disjunctionAtts = disjunction->getIdentifiers();
                auto eqOp = dynamic_pointer_cast<EqOp>(disjunction);
                auto children = eqOp->getChildren();
                auto lhs = children.first;
                auto rhs = children.second;
                
                auto leftAtts = lhs->getIdentifiers();
                std::set<std::string> leftAlias;
                for (auto& iter : leftAtts) leftAlias.insert(iter.first);     
                auto rightAtts = rhs->getIdentifiers();
                for (auto& iter : rightAtts) {
                    if (leftAlias.find(iter.first) == leftAlias.end()) {// one of the table alias appear in left does not appear in right
                        isJoinequalityCheck = true;
                        break;
                    }
                }
                if (isJoinequalityCheck) {  // if the current disjunction is an equality check for join, create the equalityChecks
                    // For all possible case leftAtts.size() == 1 && rightAtts.size() == 1
                    string lAlias = leftAtts[0].first;
                    string rAlias = rightAtts[0].first;
 
                    bool edgeFound = false;
                    for (TableEdge& edge : tableEdgeList) {
                        if (edge.lalias == lAlias && edge.ralias == rAlias) { // find edge, add equalityChecks
                            edgeFound = true;
                            edge.equalityChecks.push_back(std::make_pair(lhs, rhs));
                            break;
                        } else if (edge.lalias == rAlias && edge.ralias == lAlias) {
                            edge.equalityChecks.push_back(std::make_pair(rhs, lhs));
                            edgeFound = true;
                            break;
                        }
                    }
                    if (!edgeFound) { // not in the current edges, add it to the vector
                        std::vector<std::pair<ExprTreePtr, ExprTreePtr>> equalityCheckVec;
                        equalityCheckVec.push_back(std::make_pair(lhs, rhs));
                        tableEdgeList.push_back({lAlias, rAlias, equalityCheckVec});
                    }
                } 
            } 
            // if (!isJoinequalityCheck) 
            { // the current disjunction is not an equality check for join
                auto disjunctionAtts = disjunction->getIdentifiers();
                DisjunctionPrerequests prerequests;
                for (auto& disjunctionAtt: disjunctionAtts) {
                    prerequests.insert(disjunctionAtt.first);
                } 
                nonEqualityCheckDisjunctions.push_back(std::make_pair(disjunction, prerequests));
            }
        }

        //  start from table 0
        //  in a while loop, find all reachable node of the initialNode table
        //  add empty edge from the initialNode table to the first unreachable node
        //  end loop if all tables are reachable
        string initialNode = tablesToProcess[0].second;
        int reachableCount = 1;
        while (reachableCount < tablesToProcess.size()) {
            std::queue<string> nodeQueue;
            std::set<string> reachNodeThisRound;
            reachNodeThisRound.insert(initialNode);
            nodeQueue.push(initialNode);
            while (!nodeQueue.empty()) {
                string nodeToVisit = nodeQueue.front();
                nodeQueue.pop();
                for (auto& edge : tableEdgeList) {
                    if (
                        edge.lalias == nodeToVisit && 
                        reachNodeThisRound.find(edge.ralias) == reachNodeThisRound.end()) 
                    {
                        reachNodeThisRound.insert(edge.ralias);
                        nodeQueue.push(edge.ralias);
                    } else if (
                        edge.ralias == nodeToVisit &&
                        reachNodeThisRound.find(edge.lalias) == reachNodeThisRound.end()) {
                        reachNodeThisRound.insert(edge.lalias);
                        nodeQueue.push(edge.lalias);
                    }
                }
            }
            reachableCount = reachNodeThisRound.size();
            if (reachableCount >= tablesToProcess.size()) break;
            for (auto tableInfo : tablesToProcess) {
                string tableAlias = tableInfo.second;
                if (reachNodeThisRound.find(tableAlias) == reachNodeThisRound.end()) {
                    tableEdgeList.push_back({initialNode, tableAlias, std::vector<std::pair<ExprTreePtr, ExprTreePtr>>()});
                    reachableCount++;
                    break;
                }
               
            }
        }


        int tableCount = tablesToProcess.size();
        int joinTableIdx = 0;
        // keep join until we only have one table
        while (tableCount > 1) {
            auto edge = tableEdgeList.begin();
            auto bestEdge = edge;
            size_t minEstimatedSize = estimateJoinOutputSize(tableReaderWriterWithPrefixLookUpMap, *edge);
            edge++;
            for (; edge != tableEdgeList.end(); ++edge) {
                size_t estimatedSize = estimateJoinOutputSize(tableReaderWriterWithPrefixLookUpMap, *edge);
                if (estimatedSize < minEstimatedSize) {
                    bestEdge = edge;
                    minEstimatedSize = estimatedSize;
                }
            }
            // then join bestEdge
            auto ltableReaderWriter = tableReaderWriterWithPrefixLookUpMap[bestEdge->lalias]; // leftInput of join
            auto rtableReaderWriter = tableReaderWriterWithPrefixLookUpMap[bestEdge->ralias]; // rightInput of join


            // create output schema for join & prepare projections
            vector <string> projections;
            MyDB_SchemaPtr join_output_schema = make_shared<MyDB_Schema>();
            
            set<string> outputProjectionRequirement;
            // ltable
            for (auto& attName: tableProjectionRequirement[bestEdge->lalias]) {
                MyDB_AttTypePtr attType = ltableReaderWriter->getTable()->getSchema()->getAttByName(attName).second;
                join_output_schema->appendAtt(std::make_pair(attName, attType));
                projections.push_back("[" + attName + "]");
                outputProjectionRequirement.insert(attName);
            }
            // rtable
            for (auto& attName: tableProjectionRequirement[bestEdge->ralias]) {
                MyDB_AttTypePtr attType = rtableReaderWriter->getTable()->getSchema()->getAttByName(attName).second;
                join_output_schema->appendAtt(std::make_pair(attName, attType));
                projections.push_back("[" + attName + "]");
                outputProjectionRequirement.insert(attName);
            }
            /*
            {
                std::stringstream ss;
                ss << join_output_schema << std::endl;
                string output_schema_string = ss.str();
                printf("join outputschema is %s\n", output_schema_string.c_str());
            }
            */
            
            // prepare leftSelectionPredicate
            string leftSelectionPredicate = "bool[true]";
            for (auto disjunction = nonEqualityCheckDisjunctions.begin(); disjunction != nonEqualityCheckDisjunctions.end(); ++disjunction) 
            {
                ExprTreePtr disjunctionExpr = disjunction->first;
                DisjunctionPrerequests disjunctionPrerequests = disjunction->second;
                bool allPrerequestFound = true;
                for (auto& prerequest: disjunctionPrerequests) {
                    auto& leftTableAlias = joinedTableOriginalAlias[bestEdge->lalias];
                    if (leftTableAlias.find(prerequest) == leftTableAlias.end()) {
                        allPrerequestFound = false;
                        break;
                    }
                }
                if (allPrerequestFound) {
                    leftSelectionPredicate = "&& (" + disjunctionExpr->toString() + ", " + leftSelectionPredicate + ")";
                    nonEqualityCheckDisjunctions.erase(disjunction++);
                }
            }

            // prepare rightSelectionPredicate
            string rightSelectionPredicate = "bool[true]";
            for (auto disjunction = nonEqualityCheckDisjunctions.begin(); disjunction != nonEqualityCheckDisjunctions.end(); ++disjunction) 
            {
                ExprTreePtr disjunctionExpr = disjunction->first;
                DisjunctionPrerequests disjunctionPrerequests = disjunction->second;
                bool allPrerequestFound = true;
                for (auto& prerequest: disjunctionPrerequests) {
                    auto& rightTableAlias = joinedTableOriginalAlias[bestEdge->ralias];
                    if (rightTableAlias.find(prerequest) == rightTableAlias.end()) {
                        allPrerequestFound = false;
                        break;
                    }
                }
                if (allPrerequestFound) {
                    rightSelectionPredicate = "&& (" + disjunctionExpr->toString() + ", " + rightSelectionPredicate + ")";
                    nonEqualityCheckDisjunctions.erase(disjunction++);
                }
            }

            // prepare finalSelectionPredicate
            string finalSelectionPredicate = "bool[true]";
            for (auto disjunction = nonEqualityCheckDisjunctions.begin(); disjunction != nonEqualityCheckDisjunctions.end(); ++disjunction) 
            {
                ExprTreePtr disjunctionExpr = disjunction->first;
                DisjunctionPrerequests disjunctionPrerequests = disjunction->second;
                bool allPrerequestFound = true;
                for (auto& prerequest: disjunctionPrerequests) {
                    auto& leftTableAlias = joinedTableOriginalAlias[bestEdge->lalias];
                    auto& rightTableAlias = joinedTableOriginalAlias[bestEdge->ralias];
                    if (
                        leftTableAlias.find(prerequest) == leftTableAlias.end() &&
                        rightTableAlias.find(prerequest) == rightTableAlias.end()
                    ) {
                        allPrerequestFound = false;
                        break;
                    }
                }
                if (allPrerequestFound) {
                    finalSelectionPredicate = "&& (" + disjunctionExpr->toString() + ", " + finalSelectionPredicate + ")";
                    nonEqualityCheckDisjunctions.erase(disjunction++);
                }
            }

            // equalityChecks: exprtreeptr->string()
            vector <pair <string, string>> equalityChecksForJoin;
            for(auto eqCheck : bestEdge->equalityChecks){
                equalityChecksForJoin.push_back(std::make_pair(eqCheck.first->toString(), eqCheck.second->toString()));
            }

            joinTableIdx++;
            string joinOutputTableAlias = "TempJoinOutput" + to_string(joinTableIdx);
            
            auto joinOutputTable = std::make_shared<MyDB_TableReaderWriter>(std::make_shared<MyDB_Table>(joinOutputTableAlias, joinOutputTableAlias, join_output_schema), bufferMgr);
            // add new table to tableReaderWriterWithPrefixLookUpMap
            tableReaderWriterWithPrefixLookUpMap[joinOutputTableAlias] = joinOutputTable;

            
            // run join
            ScanJoin scanJoin(
                ltableReaderWriter,
                rtableReaderWriter,
                joinOutputTable,
                finalSelectionPredicate,
                projections,
                equalityChecksForJoin,
                leftSelectionPredicate,
                rightSelectionPredicate
            );
            scanJoin.run();
/*
            {
                cout << joinOutputTableAlias << std::endl;
                MyDB_RecordPtr tempRec = joinOutputTable->getEmptyRecord();
                MyDB_RecordIteratorAltPtr myIter = joinOutputTable->getIteratorAlt();
                int iterIdx = 0;
                while (myIter->advance()) {
                    myIter->getCurrent(tempRec);
                    iterIdx++;
                    if (iterIdx < 10) {
                        cout << tempRec << "\n";
                    }  
                }
                cout << "There are " << iterIdx << " records in the table\n";
            }
*/            
            // update tableProjectionRequirement for new table
            tableProjectionRequirement[joinOutputTableAlias] = outputProjectionRequirement;

            // remove selected edge, iterate through edgelist, update all edges containing node in selected edge
            set<string> originalAliasInOutput = joinedTableOriginalAlias[bestEdge->lalias];
            for (auto& alias : joinedTableOriginalAlias[bestEdge->ralias]) {
                originalAliasInOutput.insert(alias);
            }
            joinedTableOriginalAlias[joinOutputTableAlias] = originalAliasInOutput;
            for (auto edgeIter = tableEdgeList.begin(); edgeIter != tableEdgeList.end(); ++edgeIter) {
                if (edgeIter != bestEdge) {
                    if (edgeIter->lalias == bestEdge->lalias || edgeIter->lalias == bestEdge->ralias) {
                        edgeIter->lalias = joinOutputTableAlias;
                    } else if (edgeIter->ralias == bestEdge->lalias || edgeIter->ralias == bestEdge->ralias) {
                        edgeIter->ralias = joinOutputTableAlias;
                    }
                }

            }

            tableEdgeList.erase(bestEdge);
            // after deleting edges in a cycle, two edges might coincide with each other, need to delete one of them
            
            for (auto edgeIter1 = tableEdgeList.begin(); edgeIter1 != tableEdgeList.end(); ++edgeIter1) {
                for (auto edgeIter2 = std::next(edgeIter1); edgeIter2!= tableEdgeList.end(); ++edgeIter2) {
                    if ((edgeIter1->lalias == edgeIter2->lalias && edgeIter1->ralias == edgeIter2->ralias)) {
                        edgeIter1->equalityChecks.insert(
                            edgeIter1->equalityChecks.end(),
                            edgeIter2->equalityChecks.begin(),
                            edgeIter2->equalityChecks.end()
                        );
                        tableEdgeList.erase(edgeIter2++);
                    } else if (edgeIter1->lalias == edgeIter2->ralias && edgeIter1->ralias == edgeIter2->lalias) {
                        // merge 2 coincided edges (equalityChecks)
                        for (auto& eqChecks : edgeIter2->equalityChecks) {
                            edgeIter1->equalityChecks.push_back(
                                std::make_pair(eqChecks.second, eqChecks.first));
                        }
                        tableEdgeList.erase(edgeIter2++);
                    }
                }
            }
            

            

            
            
            
            

            /*
            ScanJoin (MyDB_TableReaderWriterPtr leftInput, MyDB_TableReaderWriterPtr rightInput,
                MyDB_TableReaderWriterPtr output, string finalSelectionPredicate, 
                vector <string> projections,
                vector <pair <string, string>> equalityChecks, string leftSelectionPredicate,
                string rightSelectionPredicate);
            */
            

            tableCount--;

            // TO DO if use estimiatedOutputSize: set DistinctValues for output table after one join;
        }
        // TO DO: do selection
        string lastJoinOutputTableAlias = "TempJoinOutput" + to_string(joinTableIdx);
        auto lastJoinOutput = tableReaderWriterWithPrefixLookUpMap[lastJoinOutputTableAlias];



        ExecuteSingleTableQuery(
            lastJoinOutput,
            valuesToSelect,
            tablesToProcess,
            allDisjunctions,
            groupingClauses,
            catelog,
            bufferMgr
        );
        /*
        {
            MyDB_RecordPtr tempRec = lastJoinOutput->getEmptyRecord();
            MyDB_RecordIteratorAltPtr myIter = lastJoinOutput->getIteratorAlt();

            while (myIter->advance()) {
                myIter->getCurrent(tempRec);
                cout << tempRec << "\n";
            }
        }
        */

    }    
}