#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cstring>

#include "../rbf/rbfm.h"

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
public:
    RM_ScanIterator() = default;

    ~RM_ScanIterator() = default;

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data);

    RC close();

    FileHandle fileHandle;
    RBFM_ScanIterator scanIterator;
};

// Relation Manager
class RelationManager {
public:
    static RelationManager &instance();

    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

    RC deleteTable(const std::string &tableName);

    RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

    RC insertTuple(const std::string &tableName, const void *data, RID &rid);

    RC deleteTuple(const std::string &tableName, const RID &rid);

    RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

    RC readTuple(const std::string &tableName, const RID &rid, void *data);

    // Print a tuple that is passed to this utility method.
    // The format is the same as printRecord().
    RC printTuple(const std::vector<Attribute> &attrs, const void *data);

    RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    // Do not store entire results in the scan iterator.
    RC scan(const std::string &tableName,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparison type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RM_ScanIterator &rm_ScanIterator);

// Extra credit work (10 points)
    RC addAttribute(const std::string &tableName, const Attribute &attr);

    RC dropAttribute(const std::string &tableName, const std::string &attributeName);

    bool isSystemTable(const string &tableName);

    RC insertTablesRecord(const int table_id, const std::string &table_name, const std::string &file_name, const int systemTable);
    RC insertTableColumnsRecords(const int table_id, std::vector<Attribute> descriptor);
    RC insertColumnsRecord(FileHandle fileHandle, const int table_id, const std::string &column_name, const int column_type, const int column_length, const int column_position);

    void prepareTablesAttributeNames(vector<string> &attributeNames);
    void prepareColumnsAttributeNames(vector<string> &attributeNames);

    void prepareTablesDescriptor(std::vector<Attribute> &tablesDescriptor);
    void prepareColumnsDescriptor(std::vector<Attribute> &columnsDescriptor);
    void prepareTablesRecord(int tablesDescriptorSize, void *data, const int table_id, const std::string &table_name, const std::string &file_name, const int systemTable);
    void prepareColumnsRecord(int columnsDescriptorSize, void *data, const int table_id, const std::string &column_name, const int column_type, const int column_length, const int column_position);

    int generateNedtTableId();
    RC getTableId(const std::string &tableName, int &tableId);
protected:
    RelationManager();                                                  // Prevent construction
    ~RelationManager();                                                 // Prevent unwanted destruction
    RelationManager(const RelationManager &);                           // Prevent construction by copying
    RelationManager &operator=(const RelationManager &);                // Prevent assignment

private:
    static RelationManager *_relation_manager;
    RecordBasedFileManager *_rbf_manager;
    int numOfTables = 0;
};

#endif