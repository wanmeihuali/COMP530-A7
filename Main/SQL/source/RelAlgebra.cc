#include "RelAlgebra.h"
#include <sstream>
#include <cstdio>

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

        
        // generate the projections and the schema for the output table
        
        vector<string> groupings;
        vector <pair <MyDB_AggType, string>> aggToCompute;
        MyDB_SchemaPtr output_schema = make_shared<MyDB_Schema>();
        MyDB_SchemaPtr inter_output_schema = make_shared<MyDB_Schema>();
        for (auto& grouping_clause : groupingClauses) {
            auto grouping_identifiers = grouping_clause->getIdentifiers();
            if (grouping_identifiers.size() == 1) {
                inter_output_schema->appendAtt(make_pair(grouping_clause->getIdentifiers()[0], grouping_clause->getType(catelog, tablesToProcess)));
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

    }
}