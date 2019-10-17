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
    const std::string tables = "tables";
    const std::string columns = "columns";

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

    // Populate "columns" table
    std::vector<Attribute> tablesDescriptor;
    std::vector<Attribute> columnsDescriptor;
    prepareTablesDescriptor(tablesDescriptor);
    prepareColumnsDescriptor(columnsDescriptor);
    insertTableColumnsRecords(1, tablesDescriptor);
    insertTableColumnsRecords(2, columnsDescriptor);
    return 0;
}

RC RelationManager::deleteCatalog() {
    RC rc = _rbf_manager->destroyFile("tables");
    if(rc != 0) {
        return rc;
    }
    rc = _rbf_manager->destroyFile("columns");
    if(rc != 0) {
        return rc;
    }
    return 0;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    _rbf_manager->createFile(tableName);
    FileHandle fileHandle;
    _rbf_manager->openFile("tables", fileHandle);
    insertTablesRecord(++numOfTables, tableName, tableName, 0);
    insertTableColumnsRecords(numOfTables, attrs);
    return 0;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    return -1;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
    return -1;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
    return -1;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
    return -1;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
    return -1;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    return -1;
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
    return -1;
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    //return -1;
    // FileHandle fileHandle;
    // _rbf_manager->openFile(tableName, fileHandle);

    // _rbf_manager->scan(&fileHandle, )
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    return -1;
}

RC RelationManager::getTableId(std::string &tableName, int &tableId) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile("tables", fileHandle);
    if(rc != 0) {
        return rc;
    }

    int length = tableName.size();
    char *value = (char *)malloc(length + sizeof(int));
    memcpy(value, &length, sizeof(int));
    memcpy(value + sizeof(int), tableName.c_str(), length);

}

RC RelationManager::insertTablesRecord(const int table_id, const std::string &table_name, const std::string &file_name, const int systemTable) {
    FileHandle fileHandle;
    RC rc = _rbf_manager->openFile("tables", fileHandle);

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

    auto *testData = (char *)malloc(PAGE_SIZE);
    _rbf_manager->readRecord(fileHandle, tablesDescriptor, rid, testData);
    _rbf_manager->printRecord(tablesDescriptor, testData);
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
    RC rc = _rbf_manager->openFile("columns", fileHandle);
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
        return rc;
    }

    auto *testData = (char *)malloc(PAGE_SIZE);
    _rbf_manager->readRecord(fileHandle, columnsDescriptor, rid, testData);
    _rbf_manager->printRecord(columnsDescriptor, testData);
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

    attr.name = "column_position";
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
}



