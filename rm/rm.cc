#include "rm.h"

RelationManager *RelationManager::_relation_manager = nullptr;

RelationManager &RelationManager::instance() {
    static RelationManager _relation_manager = RelationManager();
    return _relation_manager;
}

RelationManager::RelationManager() {
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    _rbf_manager = &rbfm;
}

RelationManager::~RelationManager() { delete _relation_manager; }

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

RC RelationManager::createCatalog() {
    const string tables = TABLES;
    const string columns = COLUMNS;

    // If creating tables file succeeds, open the file and write records to it
    RC rc = this->_rbf_manager->createFile(tables);
    if (rc != 0) {
        return rc;
    }
    rc = this->_rbf_manager->createFile(columns);
    if (rc != 0) {
        return rc;
    }

    vector<Attribute> tablesDescriptor;
    this->prepareTablesDescriptor(tablesDescriptor);
    vector<Attribute> columnsDescriptor;
    this->prepareColumnsDescriptor(columnsDescriptor);

    // Tables table
    this->insertTablesRecord(tablesDescriptor, 1, tables, tables, 1);
    this->insertTablesRecord(tablesDescriptor, 2, columns, columns, 1);
    this->insertColumnsRecord(columnsDescriptor, 1, tablesDescriptor);
    this->insertColumnsRecord(columnsDescriptor, 2, columnsDescriptor);
    return 0;
}

void RelationManager::prepareTablesDescriptor(std::vector<Attribute> &tablesDescriptor) {
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    tablesDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    tablesDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    tablesDescriptor.push_back(attr);

    attr.name = "system-flag";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    tablesDescriptor.push_back(attr);
}

void RelationManager::prepareColumnsDescriptor(std::vector<Attribute> &columnsDescriptor) {
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    columnsDescriptor.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    columnsDescriptor.push_back(attr);
}

void RelationManager::prepareTablesRecord(int fieldCount, void *data, int table_id,
                                          const string &table_name, const string &file_name,
                                          int systemFlag) {
    int nullFlagSize = this->_rbf_manager->getNullFlagSize(fieldCount);
    int dataPtr = 0;
    char *nullFlags = (char *) malloc(nullFlagSize);
    memset(nullFlags, 0, nullFlagSize);
    memcpy((char *) data + dataPtr, nullFlags, nullFlagSize);
    free(nullFlags);
    dataPtr += nullFlagSize;
    memcpy((char *) data + dataPtr, &table_id, sizeof(int));
    dataPtr += sizeof(int);
    int length = table_name.size();
    memcpy((char *) data + dataPtr, &length, sizeof(int));
    dataPtr += sizeof(int);
    memcpy((char *) data + dataPtr, table_name.c_str(), length);
    dataPtr += length;
    length = file_name.size();
    memcpy((char *) data + dataPtr, &length, sizeof(int));
    dataPtr += sizeof(int);
    memcpy((char *) data + dataPtr, file_name.c_str(), length);
    dataPtr += length;
    memcpy((char *) data + dataPtr, &systemFlag, sizeof(int));
}

void RelationManager::prepareColumnsRecord(int fieldCount, void *data, int table_id, Attribute &attr, int attr_pos) {
    int nullFlagSize = this->_rbf_manager->getNullFlagSize(fieldCount);
    int dataPtr = 0;
    char *nullFlags = (char *) malloc(nullFlagSize);
    memset(nullFlags, 0, nullFlagSize);
    memcpy((char *) data + dataPtr, nullFlags, nullFlagSize);
    free(nullFlags);
    dataPtr += nullFlagSize;
    memcpy((char *) data + dataPtr, &table_id, sizeof(int));
    dataPtr += sizeof(int);
    int attr_nameLength = attr.name.size();
    memcpy((char *) data + dataPtr, &attr_nameLength, sizeof(int));
    dataPtr += sizeof(int);
    memcpy((char *) data + dataPtr, attr.name.c_str(), attr_nameLength);
    dataPtr += attr_nameLength;
    memcpy((char *) data + dataPtr, &attr.type, sizeof(int));
    dataPtr += sizeof(int);
    memcpy((char *) data + dataPtr, &attr.length, sizeof(int));
    dataPtr += sizeof(int);
    memcpy((char *) data + dataPtr, &attr_pos, sizeof(int));
}

RC RelationManager::insertTablesRecord(const vector<Attribute> &tablesDescriptor, int table_id,
                                       const string &table_name, const string &file_name,
                                       int systemFlag) {
    RID rid;
    FileHandle fileHandle;
    RC rc = this->_rbf_manager->openFile(TABLES, fileHandle);
    if (rc != 0) {
        return rc;
    }
    char *cache = (char *) malloc(PAGE_SIZE);
    memset(cache, 0, PAGE_SIZE);
    this->prepareTablesRecord(tablesDescriptor.size(), cache, table_id,
                              table_name, file_name, systemFlag);
    rc = this->_rbf_manager->insertRecord(fileHandle, tablesDescriptor, cache, rid);
    free(cache);
    if (rc != 0) {
        return rc;
    }
    rc = this->_rbf_manager->closeFile(fileHandle);
    if (rc != 0) {
        return rc;
    }
    return 0;
}

RC RelationManager::insertColumnsRecord(const vector<Attribute> &columnsDescriptor, int table_id,
                                        const vector<Attribute> &targetDescriptor) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile(COLUMNS, fileHandle);
    if (rc != 0) {
        return rc;
    }
    char *cache = (char *) malloc(PAGE_SIZE);
    memset(cache, 0, PAGE_SIZE);
    RID rid;
    for (int i = 0; i < targetDescriptor.size(); i++) {
        Attribute attr = targetDescriptor[i];
        this->prepareColumnsRecord(columnsDescriptor.size(), cache, table_id, attr, i);
        rc = this->_rbf_manager->insertRecord(fileHandle, columnsDescriptor, cache, rid);
        if (rc != 0) {
            free(cache);
            return rc;
        }
    }
    free(cache);
    rc = _rbf_manager->closeFile(fileHandle);
    if (rc != 0) {
        return rc;
    }
    return 0;
}

void RelationManager::prepareTablesAttributeNames(vector<string> &attributeNames) {
    attributeNames.emplace_back("table-id");
    attributeNames.emplace_back("table-name");
    attributeNames.emplace_back("file-name");
    attributeNames.emplace_back("system-flag");
}

void RelationManager::prepareColumnsAttributeNames(vector<string> &attributeNames) {
    string name = "table-id";
    attributeNames.push_back(name);
    name = "column-name";
    attributeNames.push_back(name);
    name = "column-type";
    attributeNames.push_back(name);
    name = "column-length";
    attributeNames.push_back(name);
    name = "column-position";
    attributeNames.push_back(name);
}

RC RelationManager::deleteCatalog() {
    RC rc = _rbf_manager->destroyFile(TABLES);
    if (rc != 0) {
        return rc;
    }
    rc = _rbf_manager->destroyFile(COLUMNS);
    if (rc != 0) {
        return rc;
    }
    return 0;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    cout << tableName << endl;
    RC rc = this->_rbf_manager->createFile(tableName);
    if (rc != 0) {
        return rc;
    }

    vector<Attribute> tablesDescriptor;
    prepareTablesDescriptor(tablesDescriptor);
    vector<Attribute> columnsDescriptor;
    prepareColumnsDescriptor(columnsDescriptor);

    int newTableId = this->getTableTotal() + 1;
    this->insertTablesRecord(tablesDescriptor, newTableId, tableName, tableName, 0);
    this->insertColumnsRecord(columnsDescriptor, newTableId, attrs);
    return 0;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    if (this->isSystemTable(tableName)) {
        return -8;
    }

    RID rid;
    char *data = (char *) malloc(PAGE_SIZE);
    int delTableId = this->getTableId(tableName, rid);
    vector<Attribute> tablesDescriptor;
    this->prepareTablesDescriptor(tablesDescriptor);
    vector<string> attrNames;
    this->prepareTablesAttributeNames(attrNames);
    string condAttr = "table-id";
    CompOp compOp = EQ_OP;
    char *value = (char *) malloc(sizeof(int));
    memcpy(value, &delTableId, sizeof(int));
    RM_ScanIterator tablesIterator;
    this->scan(TABLES, condAttr, compOp, value, attrNames, tablesIterator);
    while (tablesIterator.getNextTuple(rid, data) != RBFM_EOF) {
        FileHandle tablesFileHandle;
        RC rc = this->_rbf_manager->openFile(TABLES, tablesFileHandle);
        if (rc != 0) {
            return -1;
        }
        this->_rbf_manager->deleteRecord(tablesFileHandle, tablesDescriptor, rid);
        this->_rbf_manager->closeFile(tablesFileHandle);
        this->_rbf_manager->destroyFile(tableName);
    }
    tablesIterator.close();

    attrNames.clear();
    vector<Attribute> columnsDescriptor;
    this->prepareColumnsDescriptor(columnsDescriptor);
    RM_ScanIterator columnsIterator;
    this->scan(COLUMNS, condAttr, compOp, value, attrNames, columnsIterator);
    vector<RID> targets;
    while (columnsIterator.getNextTuple(rid, data) != RBFM_EOF) {
        targets.emplace_back(rid);
    }
    free(data);
    free(value);
    columnsIterator.close();

    FileHandle columnsFileHandle;
    RC rc = this->_rbf_manager->openFile(COLUMNS, columnsFileHandle);
    if (rc != 0) {
        return rc;
    }
    for (int i = 0; i < targets.size(); i++) {
        this->_rbf_manager->deleteRecord(columnsFileHandle, tablesDescriptor, targets[i]);
    }
    this->_rbf_manager->closeFile(columnsFileHandle);
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, std::vector<Attribute> &attrs) {
    RID rid;
    int tableId = getTableId(tableName, rid);

    vector<Attribute> columnsDescriptor;
    vector<string> attrNames;
    attrNames.emplace_back("column-name");
    attrNames.emplace_back("column-type");
    attrNames.emplace_back("column-length");
    prepareColumnsDescriptor(columnsDescriptor);
    string condAttr = "table-id";
    CompOp compOp = EQ_OP;
    char *value = (char *) malloc(sizeof(int));
    memcpy(value, &tableId, sizeof(int));
    RM_ScanIterator rmScanIterator;
    this->scan(COLUMNS, condAttr, compOp, value, attrNames, rmScanIterator);

    char *data = (char *) malloc(PAGE_SIZE);
    int dataPtr;
    while (rmScanIterator.getNextTuple(rid, data) != RBFM_EOF) {
        dataPtr = this->_rbf_manager->getNullFlagSize(attrNames.size());
        Attribute returnedAttr;
        int length;
        memcpy(&length, (char *) data + dataPtr, sizeof(int));
        dataPtr += sizeof(int);
        char *name = (char *) malloc(length);
        memcpy(name, (char *) data + dataPtr, length);
        returnedAttr.name = string(name, length);
        dataPtr += length;
        memcpy(&returnedAttr.type, (char *) data + dataPtr, sizeof(int));
        dataPtr += sizeof(int);
        memcpy(&returnedAttr.length, (char *) data + dataPtr, sizeof(int));
        attrs.emplace_back(returnedAttr);
        free(name);
    }

    free(data);
    free(value);
    rmScanIterator.close();
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid) {
    FileHandle fileHandle;
    RC rc = this->_rbf_manager->openFile(tableName, fileHandle);

    // Check if openFile succeeds
    if (rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = this->_rbf_manager->insertRecord(fileHandle, recordDescriptor, data, rid);
    if (rc != 0) {
        return rc;
    }
    this->_rbf_manager->closeFile(fileHandle);
    return 0;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
    FileHandle fileHandle;
    RC rc = this->_rbf_manager->openFile(tableName, fileHandle);

    // Check if openFile succeeds
    if (rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = this->_rbf_manager->deleteRecord(fileHandle, recordDescriptor, rid);
    if (rc != 0) {
        return rc;
    }
    this->_rbf_manager->closeFile(fileHandle);
    return 0;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
    FileHandle fileHandle;
    RC rc = this->_rbf_manager->openFile(tableName, fileHandle);

    // Check if openFile succeeds
    if (rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = this->_rbf_manager->updateRecord(fileHandle, recordDescriptor, data, rid);
    if (rc != 0) {
        return rc;
    }
    this->_rbf_manager->closeFile(fileHandle);
    return 0;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
    FileHandle fileHandle;
    RC rc = this->_rbf_manager->openFile(tableName, fileHandle);

    // Check if openFile succeeds
    if (rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = this->_rbf_manager->readRecord(fileHandle, recordDescriptor, rid, data);
    if (rc != 0) {
        return rc;
    }
    this->_rbf_manager->closeFile(fileHandle);
    return 0;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    this->_rbf_manager->printRecord(attrs, data);
    return 0;
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
    FileHandle fileHandle;
    RC rc = this->_rbf_manager->openFile(tableName, fileHandle);

    // Check if openFile succeeds
    if (rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = this->_rbf_manager->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    if (rc != 0) {
        return rc;
    }
    this->_rbf_manager->closeFile(fileHandle);
    return 0;
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    vector<Attribute> recordDescriptor;
    if (tableName == TABLES) {
        this->prepareTablesDescriptor(recordDescriptor);
    } else if (tableName == COLUMNS) {
        this->prepareColumnsDescriptor(recordDescriptor);
    } else {
        this->getAttributes(tableName, recordDescriptor);
    }
    RC rc = this->_rbf_manager->openFile(tableName, rm_ScanIterator.fileHandle);
    if (rc != 0) {
        return -1;
    }

    return this->_rbf_manager->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute, compOp, value,
                                    attributeNames, rm_ScanIterator.scanIterator);
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    return -1;
}

bool RelationManager::isSystemTable(const string &tableName) {
    if (tableName == TABLES) {
        return true;
    } else if (tableName == COLUMNS) {
        return true;
    }
    return false;
}

int RelationManager::getTableId(const string &tableName, RID &rid) {
    vector<string> attrNames;
    attrNames.emplace_back("table-id");
    string condAttr = "table-name";
    int length = tableName.size();
    char *value = (char *) malloc(sizeof(int) + length);
    memcpy(value, &length, sizeof(int));
    memcpy(value + sizeof(int), tableName.c_str(), length);
    CompOp compOp = EQ_OP;
    RM_ScanIterator rmScanIterator;

    this->scan(TABLES, condAttr, compOp, value, attrNames, rmScanIterator);

    char *data = (char *) malloc(PAGE_SIZE);
    int tableId = -1;
    if (rmScanIterator.getNextTuple(rid, data) != RBFM_EOF) {
        memcpy(&tableId, (char *) data + sizeof(char), sizeof(int));
    }

    rmScanIterator.close();
    free(data);
    free(value);
    return tableId;
}

int RelationManager::getTableTotal() {
    vector<string> attributeNames;
    prepareTablesAttributeNames(attributeNames);
    RM_ScanIterator rmScanIterator;
    this->scan(TABLES, "", NO_OP, nullptr, attributeNames, rmScanIterator);
    RID rid;
    int tableNum = 0;
    char *data = (char *) malloc(PAGE_SIZE);
    while (rmScanIterator.getNextTuple(rid, data) != RBFM_EOF) {
        tableNum++;
    }
    free(data);
    rmScanIterator.close();
    return tableNum;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    RC rc = this->scanIterator.getNextRecord(rid, data);
    if (rc == RBFM_EOF) {
        return RM_EOF;
    }
    return 0;
}

RC RM_ScanIterator::close() {
    this->scanIterator.close();
    this->fileHandle.closeFile();
    return 0;
}


