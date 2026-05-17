#ifndef _RECORD_MANAGER_H_
#define _RECORD_MANAGER_H_ 1
#define INF 1000000

#include <cstdio>
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include "basicType.h"
#include "index_manager.h"
#include "catalog_manager.h"
#include "buffer_manager.h"
#include "template_function.h"
#include "exception.h"
#include "const.h"
#include "template_function.h"

extern BufferManager buffer_manager;

class RecordManager {
public:
    void createTableFile(std::string table_name);
    void dropTableFile(std::string table_name);
    void insertRecord(std::string table_name, Tuple& tuple);
    int deleteRecord(std::string table_name);
    int deleteRecord(std::string table_name, std::string target_attr, Where where);
    Table selectRecord(std::string table_name, std::string result_table_name = "tmp_table");
    Table selectRecord(std::string table_name, std::string target_attr, Where where, std::string result_table_name = "tmp_table");
    void createIndex(IndexManager& index_manager, std::string table_name, std::string target_attr);
    std::string getDataFilePath(std::string table_name);

private:
    int getBlockNum(std::string table_name);
    void insertRecord1(char* p, int offset, int len, const std::vector<Data>& v);
    char* deleteRecord1(char* p);
    Tuple readTuple(const char* p, Attribute attr);
    int getTupleLength(char* p);
    bool isConflict(std::vector<Tuple>& tuples, std::vector<Data>& v, int index);
    void searchWithIndex(std::string table_name, std::string target_attr, Where where, std::vector<int>& block_ids);
    int conditionDeleteInBlock(std::string table_name, int block_id, Attribute attr, int index, Where where);
    void conditionSelectInBlock(std::string table_name, int block_id, Attribute attr, int index, Where where, std::vector<Tuple>& v);
};

#endif
