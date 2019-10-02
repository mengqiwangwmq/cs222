#include "rbfm.h"
#include <cstring>

using namespace std;

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = nullptr;

RecordBasedFileManager &RecordBasedFileManager::instance() {
    static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() = default;

RecordBasedFileManager::~RecordBasedFileManager() { delete _rbf_manager; }

RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

RC RecordBasedFileManager::createFile(const std::string &fileName) {
    return _pf_manager->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
    // return -1;

}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    return -1;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    // Get nullFieldIndicator
    int nullFieldByteLength = ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char * nullFieledsIndicator = (unsigned char *)malloc(nullFieldByteLength);
    memcpy(nullFieledsIndicator, (char *)data, nullFieldByteLength);
    int offset = nullFieldByteLength;
    std::cout<<"print records"<<std::endl;
    for(int i = 0; i < recordDescriptor.size(); i ++) {
        // Check if field is null
        Attribute attr = recordDescriptor[i];
        bool nullBit = nullFieledsIndicator[i/ sizeof(char)] & (1 << (7 - i % sizeof(char)));
        if(!nullBit) {
            if(attr.type == TypeInt) {
                int value;
                memcpy(&value, (char *)data + offset, attr.length);
                offset += attr.length;
                std::cout<<attr.name.c_str()<<": "<<value<<std::endl;
            }
            if(attr.type == TypeReal) {
                float value;
                memcpy(&value, (char *)data + offset, attr.length);
                offset += attr.length;
                std::cout<<attr.name.c_str()<<": "<<value<<std::endl;
            }
            if(attr.type == TypeVarChar) {
                int length;
                char * value;
                memcpy(&length, (char *)data + offset, sizeof(int));
                offset += sizeof(int);
                memcpy(&value, (char *)data + offset, length);
                std::cout<<attr.name.c_str()<<": "<<value<<std::endl;
            } else {
                std::cout<<attr.name<<": "<<"Null"<<std::endl;
            }
        }
    }
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
    return -1;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {
    return -1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    return -1;
}



