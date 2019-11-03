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
    this->numOfTables = 0;
    // FileName & tableMame
    const string tables = "Tables";
    const string columns = "Columns";

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
    this->numOfTables++;
    this->insertTablesRecord(tablesDescriptor, this->numOfTables, tables, tables, 1);
    this->insertColumnsRecord(columnsDescriptor, this->numOfTables, tablesDescriptor);
    this->numOfTables++;
    this->insertTablesRecord(tablesDescriptor, this->numOfTables, columns, columns, 1);
    this->insertColumnsRecord(columnsDescriptor, this->numOfTables, columnsDescriptor);
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

// TODO: implement deletedColumn attribute later
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

    attr.name = "column-index";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    columnsDescriptor.push_back(attr);
}

void RelationManager::prepareTablesRecord(int fieldCount, void *data, int table_id,
                                          const string &table_name, const string &file_name,
                                          int systemFlag) {
    int nullFlagSize = this->_rbf_manager->getNullFlagSize(fieldCount);
    char *nullFlags = (char *) malloc(nullFlagSize);
    memset(nullFlags, '\0', nullFlagSize);
    memcpy((char *) data, nullFlags, nullFlagSize);
    int offset = nullFlagSize;
    memcpy((char *) data + offset, &table_id, sizeof(int));
    offset += sizeof(int);
    int length = table_name.size();
    memcpy((char *) data + offset, &length, sizeof(int));
    offset += sizeof(int);
    memcpy((char *) data + offset, table_name.c_str(), length);
    offset += length;
    length = file_name.size();
    memcpy((char *) data + offset, &length, sizeof(int));
    offset += sizeof(int);
    memcpy((char *) data + offset, file_name.c_str(), length);
    offset += length;
    memcpy((char *) data + offset, &systemFlag, sizeof(int));
    free(nullFlags);
}

void RelationManager::prepareColumnsRecord(int fieldCount, void *data, int table_id, Attribute &attr, int attr_pos) {
    int nullFlagSize = this->_rbf_manager->getNullFlagSize(fieldCount);
    char *nullFlags = (char *) malloc(nullFlagSize);
    memset(nullFlags, '\0', nullFlagSize);
    memcpy((char *) data, nullFlags, nullFlagSize);
    free(nullFlags);
    int offset = nullFlagSize;
    memcpy((char *) data + offset, &table_id, sizeof(int));
    offset += sizeof(int);
    int attr_nameLength = attr.name.size();
    memcpy((char *) data + offset, &attr_nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *) data + offset, attr.name.c_str(), attr_nameLength);
    offset += attr_nameLength;
    memcpy((char *) data + offset, &attr.type, sizeof(int));
    offset += sizeof(int);
    memcpy((char *) data + offset, &attr.length, sizeof(int));
    offset += sizeof(int);
    memcpy((char *) data + offset, &attr_pos, sizeof(int));
}

RC RelationManager::insertTablesRecord(const vector<Attribute> &tablesDescriptor, int table_id,
                                       const string &table_name, const string &file_name,
                                       int systemFlag) {
    RID rid;
    FileHandle fileHandle;
    RC rc = this->_rbf_manager->openFile("Tables", fileHandle);
    if (rc != 0) {
        return rc;
    }
    char *cache = (char *) malloc(PAGE_SIZE);
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
    RC rc = _rbf_manager->openFile("Columns", fileHandle);
    if (rc != 0) {
        return rc;
    }
    char *cache = (char *) malloc(PAGE_SIZE);
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


RC RelationManager::deleteCatalog() {
    RC rc = _rbf_manager->destroyFile("Tables");
    if (rc != 0) {
        return rc;
    }
    rc = _rbf_manager->destroyFile("Columns");
    if (rc != 0) {
        return rc;
    }
    return 0;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    RC rc = this->_rbf_manager->createFile(tableName);
    if (rc != 0) {
        return rc;
    }
    FileHandle fileHandle;
    rc = this->_rbf_manager->openFile("Tables", fileHandle);
    if (rc != 0) {
        return rc;
    }
    vector<Attribute> tablesDescriptor;
    prepareTablesDescriptor(tablesDescriptor);
    vector<Attribute> columnsDescriptor;
    prepareColumnsDescriptor(columnsDescriptor);
    vector<string> attributeNames;
    prepareTablesAttributeNames(attributeNames);
    RBFM_ScanIterator scanIterator;
    _rbf_manager->scan(fileHandle, tablesDescriptor, "", NO_OP,
                       nullptr, attributeNames, scanIterator);
    RID rid;
    int tableNum = 0;
    char *data = (char *) malloc(PAGE_SIZE);
    while (scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        tableNum++;
    }
    tableNum++;
    free(data);
    this->insertTablesRecord(tablesDescriptor, tableNum, tableName, tableName, 0);
    this->insertColumnsRecord(columnsDescriptor, tableNum, tablesDescriptor);
    return 0;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    // Delete tuple in tables
    if (isSystemTable(tableName)) {
        // TODO: Add -8, delete system table, as error code
        return -8;
    }
    FileHandle tablesFileHandle;
    RC rc = _rbf_manager->openFile("Tables", tablesFileHandle);
    if (rc != 0) {
        return -1;
    }
    vector<Attribute> attrs;
    prepareTablesDescriptor(attrs);
    vector<string> attributeNames;
    prepareTablesAttributeNames(attributeNames);

    RBFM_ScanIterator scanIterator;
    string conditionalAttribute = "table-name";
    int length = tableName.size();
    char *value = (char *) malloc(length + sizeof(int));
    memcpy(value, &length, sizeof(int));
    memcpy(value + sizeof(int), tableName.c_str(), length);
    CompOp compOp = EQ_OP;
    _rbf_manager->scan(tablesFileHandle, attrs, conditionalAttribute, compOp, value, attributeNames, scanIterator);

    string deletedTableName;
    int deletedTableId;
    RID rid;
    auto *data = (char *) malloc(PAGE_SIZE);
    int cout = 0;
    if (scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        _rbf_manager->printRecord(attrs, data);
        cout++;
        memcpy(&deletedTableId, (char *) data + sizeof(char), sizeof(int));
        int length;
        memcpy(&length, (char *) data + 5, sizeof(int));
        char *tableName = (char *) malloc(length);
        memcpy(tableName, (char *) data + 9, length);
        deletedTableName = string(tableName, length);
    }
    if (cout == 0) {
        // Error code: table to be deleted doesn't exist
        return -9;
    }
    _rbf_manager->deleteRecord(tablesFileHandle, attrs, rid);
    _rbf_manager->destroyFile(deletedTableName);

    scanIterator.close();
    _rbf_manager->closeFile(tablesFileHandle);
    attrs.clear();
    attributeNames.clear();

    prepareColumnsDescriptor(attrs);
    prepareColumnsAttributeNames(attributeNames);
    conditionalAttribute = "table-id";
    free(value);
    value = (char *) malloc(sizeof(int));
    memcpy(value, &deletedTableId, sizeof(int));
    FileHandle columnsFileHandle;
    _rbf_manager->openFile("Columns", columnsFileHandle);
    _rbf_manager->scan(columnsFileHandle, attrs, conditionalAttribute, compOp, value, attributeNames, scanIterator);
    while (scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        _rbf_manager->printRecord(attrs, data);
        _rbf_manager->deleteRecord(columnsFileHandle, attrs, rid);
    }

    // columnsFileHandle.closeFile();

    scanIterator.close();
    _rbf_manager->closeFile(columnsFileHandle);
    attrs.clear();
    attributeNames.clear();

    free(data);
    free(value);
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, std::vector<Attribute> &attrs) {
    int tableId;
    getTableId(tableName, tableId);

    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile("Columns", fileHandle);
    if (rc != 0) {
        return -1;
    }

    vector<Attribute> columnsDescriptor;
    vector<string> attrNames;
    prepareColumnsDescriptor(columnsDescriptor);
    attrNames.push_back("column-name");
    attrNames.push_back("column-type");
    attrNames.push_back("column-length");
    string conditionalAttribute = "table-id";
    CompOp compOp = EQ_OP;
    auto *value = (char *) malloc(sizeof(int));
    memcpy(value, &tableId, sizeof(int));
    RBFM_ScanIterator scanIterator;
    _rbf_manager->scan(fileHandle, columnsDescriptor, conditionalAttribute, compOp, value, attrNames, scanIterator);

    auto *data = (char *) malloc(PAGE_SIZE);
    RID rid;
    int offset;
    while (scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        vector<Attribute> projectedColumns;
        Attribute attr;
        attr.name = "column-name";
        attr.type = TypeVarChar;
        attr.length = 50;
        projectedColumns.push_back(attr);
        attr.name = "column-type";
        attr.type = TypeInt;
        attr.length = 4;
        projectedColumns.push_back(attr);
        attr.name = "column-length";
        attr.type = TypeInt;
        attr.length = 4;
        projectedColumns.push_back(attr);

//        _rbf_manager->printRecord(projectedColumns, data);

        offset = ceil((double) projectedColumns.size() / CHAR_BIT);
        Attribute returnedAttr;
        int length;
        memcpy(&length, (char *) data + offset, sizeof(int));
//        cout<<length<<endl;
        offset += sizeof(int);
        char *name = (char *) malloc(length);
        memcpy(name, (char *) data + offset, length);
        returnedAttr.name = string(name, length);
//        cout<<returnedAttr.name<<endl;
        offset += length;
        memcpy(&returnedAttr.type, (char *) data + offset, sizeof(int));
        offset += sizeof(int);
//        cout<<returnedAttr.type<<endl;
        memcpy(&returnedAttr.length, (char *) data + offset, sizeof(int));
//        cout<<returnedAttr.length<<endl;
        attrs.push_back(returnedAttr);
        free(name);
    }

    free(data);
    free(value);
    scanIterator.close();

    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile(tableName, fileHandle);

    // Check if openFile succeeds
    if (rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = _rbf_manager->insertRecord(fileHandle, recordDescriptor, data, rid);
    if (rc != 0) {
        return rc;
    }
    _rbf_manager->closeFile(fileHandle);
    return 0;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile(tableName, fileHandle);

    // Check if openFile succeeds
    if (rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = _rbf_manager->deleteRecord(fileHandle, recordDescriptor, rid);
    if (rc != 0) {
        return rc;
    }
    _rbf_manager->closeFile(fileHandle);
    return 0;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile(tableName, fileHandle);

    // Check if openFile succeeds
    if (rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = _rbf_manager->updateRecord(fileHandle, recordDescriptor, data, rid);
    if (rc != 0) {
        return rc;
    }
    _rbf_manager->closeFile(fileHandle);
    return 0;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile(tableName, fileHandle);

    // Check if openFile succeeds
    if (rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = _rbf_manager->readRecord(fileHandle, recordDescriptor, rid, data);
    if (rc != 0) {
        return rc;
    }
    _rbf_manager->closeFile(fileHandle);
    return 0;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    _rbf_manager->printRecord(attrs, data);
    return 0;
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile(tableName, fileHandle);

    // Check if openFile succeeds
    if (rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = _rbf_manager->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    if (rc != 0) {
        return rc;
    }
    _rbf_manager->closeFile(fileHandle);
    return 0;
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    RC rc = _rbf_manager->openFile(tableName, rm_ScanIterator.fileHandle);
    if (rc != 0) {
        cout << rc << endl;
        return -1;
    }

    return _rbf_manager->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute, compOp, value,
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
    if (tableName == "Tables") {
        return true;
    } else if (tableName == "Columns") {
        return true;
    }
    return false;
}

RC RelationManager::getTableId(const string &tableName, int &tableId) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile("Tables", fileHandle);
    if (rc != 0) {
        return rc;
    }
    vector<Attribute> tablesDescriptor;
    vector<string> attrNames;
    prepareTablesDescriptor(tablesDescriptor);
    attrNames.emplace_back("table-id");
    string condAttr = "table-name";
    int length = tableName.size();
    char *value = (char *) malloc(sizeof(int) + length);
    memcpy(value, &length, sizeof(int));
    memcpy(value + sizeof(int), tableName.c_str(), length);
    CompOp compOp = EQ_OP;
    RBFM_ScanIterator scanIterator;

    _rbf_manager->scan(fileHandle, tablesDescriptor, condAttr, compOp, value, attrNames, scanIterator);

    char *data = (char *) malloc(PAGE_SIZE);
    RID rid;
    if (scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        memcpy(&tableId, (char *) data + sizeof(char), sizeof(int));
    }

    scanIterator.close();
    _rbf_manager->closeFile(fileHandle);
    free(data);
    free(value);
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

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    RC rc = scanIterator.getNextRecord(rid, data);
    if (rc == RBFM_EOF) {
        return RM_EOF;
    }
    return 0;
}

RC RM_ScanIterator::close() {
    scanIterator.close();
    fileHandle.closeFile();
    return 0;
}


