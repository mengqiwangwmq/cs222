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
    numOfTables = 0;
    // FileName & tableMame
    const std::string tables = "Tables";
    const std::string columns = "Columns";

    // If creating tables file succeeds, open the file and write records to it
    RC rc = _rbf_manager->createFile(tables);
    if(rc != 0) {
        return rc;
    }
    rc = _rbf_manager->createFile(columns);
    if(rc != 0) {
        return rc;
    }

    // 1 is for systemTables, 0 is for user tables
    insertTablesRecord(++numOfTables, tables, tables, 1);
    insertTablesRecord(++numOfTables, columns, columns, 1);

    // Populate "Columns" table
    std::vector<Attribute> tablesDescriptor;
    std::vector<Attribute> columnsDescriptor;
    prepareTablesDescriptor(tablesDescriptor);
    prepareColumnsDescriptor(columnsDescriptor);
    insertTableColumnsRecords(1, tablesDescriptor);
    insertTableColumnsRecords(2, columnsDescriptor);
    return 0;
}

RC RelationManager::deleteCatalog() {
    RC rc = _rbf_manager->destroyFile("Tables");
    if(rc != 0) {
        return rc;
    }
    rc = _rbf_manager->destroyFile("Columns");
    if(rc != 0) {
        return rc;
    }
    return 0;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    _rbf_manager->createFile(tableName);
    FileHandle fileHandle;
    _rbf_manager->openFile("Tables", fileHandle);
    int tableId = generateNedtTableId();
    insertTablesRecord(tableId, tableName, tableName, 0);
    insertTableColumnsRecords(tableId, attrs);
    return 0;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    // Delete tuple in tables
    if(isSystemTable(tableName)) {
        // TODO: Add -8, delete system table, as error code
        return -8;
    }
    FileHandle tablesFileHandle;
    RC rc = _rbf_manager->openFile("Tables", tablesFileHandle);
    if(rc != 0) {
        return -1;
    }
    vector<Attribute> attrs;
    prepareTablesDescriptor(attrs);
    vector<string> attributeNames;
    prepareTablesAttributeNames(attributeNames);

    RBFM_ScanIterator scanIterator = new RBFM_ScanIterator();
    string conditionalAttribute = "table-name";
    int length = tableName.size();
    char *value = (char *)malloc(length + sizeof(int));
    memcpy(value, &length, sizeof(int));
    memcpy(value + sizeof(int), tableName.c_str(), length);
    CompOp compOp = EQ_OP;
    _rbf_manager->scan(tablesFileHandle, attrs, conditionalAttribute, compOp, value, attributeNames, scanIterator);

    string deletedTableName;
    int deletedTableId;
    RID rid;
    auto * data = (char *)malloc(PAGE_SIZE);
    int cout = 0;
    if(scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        _rbf_manager->printRecord(attrs, data);
        cout ++;
        memcpy(&deletedTableId, (char *)data + sizeof(char), sizeof(int));
        int length;
        memcpy(&length, (char *)data + 5, sizeof(int));
        char *tableName = (char *)malloc(length);
        memcpy(tableName, (char *)data + 9, length);
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
    value = (char *)malloc(sizeof(int));
    memcpy(value, &deletedTableId, sizeof(int));
    FileHandle columnsFileHandle;
    _rbf_manager->openFile("Columns", columnsFileHandle);
    _rbf_manager->scan(columnsFileHandle, attrs, conditionalAttribute, compOp, value, attributeNames, scanIterator);
    while(scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
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

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
    int tableId;
    getTableId(tableName, tableId);
//    cout<<"tableId "<<tableId<<endl;
//    cout<<"tableName"<<tableName<<endl;

    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile("Columns", fileHandle);
    if(rc != 0) {
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
    auto *value = (char *)malloc(sizeof(int));
    memcpy(value, &tableId, sizeof(int));
    RBFM_ScanIterator scanIterator;
    _rbf_manager->scan(fileHandle, columnsDescriptor, conditionalAttribute, compOp, value, attrNames, scanIterator);

    auto *data = (char *)malloc(PAGE_SIZE);
    RID rid;
    int offset;
    while(scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
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

        offset = ceil((double)projectedColumns.size()/CHAR_BIT);
        Attribute returnedAttr;
        int length;
        memcpy(&length, (char *)data + offset, sizeof(int));
//        cout<<length<<endl;
        offset += sizeof(int);
        char *name = (char *)malloc(length);
        memcpy(name, (char *)data + offset, length);
        returnedAttr.name = string(name, length);
//        cout<<returnedAttr.name<<endl;
        offset += length;
        memcpy(&returnedAttr.type, (char *)data + offset, sizeof(int));
        offset += sizeof(int);
//        cout<<returnedAttr.type<<endl;
        memcpy(&returnedAttr.length, (char *)data + offset, sizeof(int));
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
    if(rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = _rbf_manager->insertRecord(fileHandle, recordDescriptor, data, rid);
    if(rc != 0) {
        return rc;
    }
    _rbf_manager->closeFile(fileHandle);
    return 0;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile(tableName, fileHandle);

    // Check if openFile succeeds
    if(rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = _rbf_manager->deleteRecord(fileHandle, recordDescriptor, rid);
    if(rc != 0) {
        return rc;
    }
    _rbf_manager->closeFile(fileHandle);
    return 0;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile(tableName, fileHandle);

    // Check if openFile succeeds
    if(rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = _rbf_manager->updateRecord(fileHandle, recordDescriptor, data, rid);
    if(rc != 0) {
        return rc;
    }
    _rbf_manager->closeFile(fileHandle);
    return 0;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile(tableName, fileHandle);

    // Check if openFile succeeds
    if(rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = _rbf_manager->readRecord(fileHandle, recordDescriptor, rid, data);
    if(rc != 0) {
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
    if(rc != 0) {
        return rc;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    rc = _rbf_manager->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    if(rc != 0) {
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
    if(rc != 0) {
        cout<<rc<<endl;
        return -1;
    }

    return _rbf_manager->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.scanIterator);
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    return -1;
}

int RelationManager::generateNedtTableId() {
    int tableNum = 0;
    FileHandle fileHandle;
    _rbf_manager->openFile("Tables", fileHandle);
    vector<Attribute> tablesDescriptor;
    vector<string> attributeNames;
    prepareTablesDescriptor(tablesDescriptor);
    prepareTablesAttributeNames(attributeNames);
    RBFM_ScanIterator scanIterator;
    _rbf_manager->scan(fileHandle, tablesDescriptor, "", NO_OP, NULL, attributeNames, scanIterator);
    RID rid;
    auto *data = (char *)malloc(PAGE_SIZE);
    while(scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        tableNum ++;
    }

    free(data);
    return ++tableNum;
}

bool RelationManager::isSystemTable(const string &tableName) {
    if(tableName == "Tables") {
        return true;
    } else if(tableName == "Columns") {
        return true;
    }
    return false;
}

RC RelationManager::getTableId(const std::string &tableName, int &tableId) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile("Tables", fileHandle);
    if(rc != 0) {
        return rc;
    }

    vector<Attribute> attrs;
    vector<string> attrNames;
    prepareTablesDescriptor(attrs);
    attrNames.push_back("table-id");
    string conditionalAttribute = "table-name";
    int length = tableName.size();
    char *value = (char *)malloc(length + sizeof(int));
    memcpy(value, &length, sizeof(int));
    memcpy(value + sizeof(int), tableName.c_str(), length);
    CompOp compOp = EQ_OP;

    RBFM_ScanIterator scanIterator = RBFM_ScanIterator();
    _rbf_manager->scan(fileHandle, attrs, conditionalAttribute, compOp, value, attrNames, scanIterator);

    auto *data = (char *)malloc(PAGE_SIZE);
    RID rid;
    if(scanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        memcpy(&tableId, (char *)data + sizeof(char), sizeof(int));
//        cout<<tableId<<endl;
    }

    scanIterator.close();
    attrs.clear();
    attrNames.clear();
    _rbf_manager->closeFile(fileHandle);
    free(data);
    free(value);
    return 0;
}

void RelationManager::prepareTablesAttributeNames(vector<string> &attributeNames) {
    string name = "table-id";
    attributeNames.push_back(name);
    name = "table-name";
    attributeNames.push_back(name);
    name = "file-name";
    attributeNames.push_back(name);
    name = "systemTable";
    attributeNames.push_back(name);
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

RC RelationManager::insertTablesRecord(const int table_id, const std::string &table_name, const std::string &file_name, const int systemTable) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile("Tables", fileHandle);

    // Check if openFile succeeds
    if(rc != 0) {
        return rc;
    }

    RID rid;
    std::vector<Attribute> tablesDescriptor;
    prepareTablesDescriptor(tablesDescriptor);
    void *data = (char *)malloc(PAGE_SIZE);
    prepareTablesRecord(tablesDescriptor.size(), data, table_id, table_name, file_name, systemTable);
    rc = _rbf_manager->insertRecord(fileHandle, tablesDescriptor, data, rid);
    if(rc != 0) {
        return rc;
    }

    /*
    auto *testData = (char *)malloc(PAGE_SIZE);
    _rbf_manager->readRecord(fileHandle, tablesDescriptor, rid, testData);
    _rbf_manager->printRecord(tablesDescriptor, testData);
     */
    rc = _rbf_manager->closeFile(fileHandle);
    // Check if closeFile succeeds
    if(rc != 0){
        return rc;
    }
    free(data);
    return 0;
}

RC RelationManager::insertTableColumnsRecords(const int table_id, std::vector<Attribute> descriptor) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile("Columns", fileHandle);
    if(rc != 0) {
        return rc;
    }
    Attribute attr;
    int pos = 1;
    for(int i = 0; i < descriptor.size(); i ++) {
        attr = descriptor[i];
        insertColumnsRecord(fileHandle, table_id, attr.name, attr.type, attr.length, pos++);
    }

    rc = _rbf_manager->closeFile(fileHandle);
    if(rc != 0) {
        return rc;
    }
    return 0;
}

RC RelationManager::insertColumnsRecord(FileHandle fileHandle, const int table_id, const std::string &column_name, const int column_type, const int column_length, const int column_position) {
    RID rid;
    std::vector<Attribute> columnsDescriptor;
    prepareColumnsDescriptor(columnsDescriptor);
    void *data = (char *)malloc(PAGE_SIZE);
    prepareColumnsRecord(columnsDescriptor.size(), data, table_id, column_name, column_type, column_length, column_position);
    RC rc = _rbf_manager->insertRecord(fileHandle, columnsDescriptor, data, rid);
    if(rc != 0) {
        free(data);
        return rc;
    }

    auto *testData = (char *)malloc(PAGE_SIZE);
    _rbf_manager->readRecord(fileHandle, columnsDescriptor, rid, testData);
    _rbf_manager->printRecord(columnsDescriptor, testData);
    free(testData);

    free(data);
    return 0;
}

void RelationManager::prepareTablesDescriptor(std::vector<Attribute> &tablesDescriptor) {
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    tablesDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tablesDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tablesDescriptor.push_back(attr);

    attr.name = "systemTable";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    tablesDescriptor.push_back(attr);
}

// TODO: implement deletedColumn attribute later
void RelationManager::prepareColumnsDescriptor(std::vector<Attribute> &columnsDescriptor) {
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    columnsDescriptor.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);
}

void RelationManager::prepareTablesRecord(int tablesDescriptorSize, void *data, const int table_id, const std::string &table_name, const std::string &file_name, const int systemTable) {
    int fieldNum = tablesDescriptorSize;
    int nullFieldsIndicatorSize = ceil((double)fieldNum / CHAR_BIT);
    auto *nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorSize);
    memset(nullFieldsIndicator, 0, nullFieldsIndicatorSize);
    memcpy((char *)data, nullFieldsIndicator, nullFieldsIndicatorSize);
    int offset = nullFieldsIndicatorSize;
    memcpy((char *)data + offset, &table_id, sizeof(int));
    offset += sizeof(int);
    int length = table_name.size();
    memcpy((char *)data + offset, &length, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)data + offset, table_name.c_str(), length);
    offset += length;
    length = file_name.size();
    memcpy((char *)data + offset, &length, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)data + offset, file_name.c_str(), length);
    offset += length;
    memcpy((char *)data + offset, &systemTable, sizeof(int));
    offset += sizeof(int);
    free(nullFieldsIndicator);
}

void RelationManager::prepareColumnsRecord(int columnsDescriptorSize, void *data, const int table_id, const std::string &column_name, const int column_type, const int column_length, const int column_position) {
    int fieldNum = columnsDescriptorSize;
    int nullFieldsIndicatorSize = ceil((double)fieldNum / CHAR_BIT);
    auto *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorSize);
    memset(nullFieldsIndicator, 0, nullFieldsIndicatorSize);
    memcpy((char *)data, nullFieldsIndicator, nullFieldsIndicatorSize);
    int offset = nullFieldsIndicatorSize;
    memcpy((char *)data + offset, &table_id, sizeof(int));
    offset += sizeof(int);
    int length = column_name.size();
    memcpy((char *)data + offset, &length, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)data + offset, column_name.c_str(), length);
    offset += length;
    memcpy((char *)data + offset, &column_type, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)data + offset, &column_length, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)data + offset, &column_position, sizeof(int));
    offset += sizeof(int);
    free(nullFieldsIndicator);
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    RC rc = scanIterator.getNextRecord(rid, data);
    if(rc == RBFM_EOF) {
        return RM_EOF;
    }
    return 0;
}

RC RM_ScanIterator::close() {
    scanIterator.close();
    fileHandle.closeFile();
    return 0;
}


