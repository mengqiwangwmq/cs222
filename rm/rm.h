#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cstring>
#include <iostream>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

# define RM_EOF (-1)  // end of a scan operator
#define TABLES "Tables"
#define COLUMNS "Columns"
#define INDEX "Index"

using namespace std;

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

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
public:
    RM_IndexScanIterator();    // Constructor
    ~RM_IndexScanIterator() {};    // Destructor

    // "key" follows the same format as in IndexManager::insertEntry()
    RC getNextEntry(RID &rid, void *key);    // Get next matching entry
    RC close() { return -1; };                        // Terminate index scan
    const void *lowKey;
    const void *highKey;
    const bool lowKeyInclusive;
    const bool highKeyInclusive;
    IX_ScanIterator ixScanIterator;
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

    RC getAttributesName(vector<Attribute> &attributes, vector<string> &attributesName);

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

    void prepareTablesDescriptor(vector<Attribute> &tablesDescriptor);

    void prepareColumnsDescriptor(vector<Attribute> &columnsDescriptor);

    void prepareIndexDescriptor(vector<Attribute> &indexDescriptor);

    void prepareTablesRecord(int fieldCount, void *data, int table_id, const string &table_name,
                             const string &file_name);

    void prepareColumnsRecord(int fieldCount, void *data, int table_id, Attribute &attr, int attr_pos);

    void prepareIndexRecord(int &tableId, void *data, string &indexFielName);

    RC insertTablesRecord(const vector<Attribute> &tablesDescriptor, int table_id,
                          const string &table_name, const string &file_name);

    RC insertColumnsRecord(const vector<Attribute> &columnsDescriptor, int table_id,
                           const vector<Attribute> &targetDescriptor);

    void prepareTablesAttributeNames(vector<string> &attributeNames);

    void prepareColumnsAttributeNames(vector<string> &attributeNames);

    int getTableId(const string &tableName, RID &rid);

    int getTableTotal();

    // QE IX related
    RC createIndex(const std::string &tableName, const std::string &attributeName);

    RC destroyIndex(const std::string &tableName, const std::string &attributeName);

    RC updateIndexes(const string &tableName, const void *data, const RID &rid);

    RC getIndexAttributeNames(int tableId, vector<string> &indexAttributeNames);

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator);

    RC insertIndex(vector<Attribute> &attributes, const string &tableName, string &indexFileName, const void *data, const RID &rid);

    int getKey(vector<Attribute> &attrs, int &attrPos, const string &tableName, const string &indexFileName, void *keyData, const void *data);

protected:
    RelationManager();                                                  // Prevent construction
    ~RelationManager();                                                 // Prevent unwanted destruction
    RelationManager(const RelationManager &);                           // Prevent construction by copying
    RelationManager &operator=(const RelationManager &);                // Prevent assignment

private:
    static RelationManager *_relation_manager;
    RecordBasedFileManager *_rbf_manager;
    IndexManager *_ix_manager;
    int numOfTables = 0;
};

#endif