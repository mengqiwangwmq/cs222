#include "rbfm.h"

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

// Referenced from test_util getActualByteForNullsIndicator function
int RecordBasedFileManager::getNullFlagSize(int fieldCount) {
    return ceil((double) fieldCount / CHAR_BIT);
}

unsigned RecordBasedFileManager::readPageSpace(void *data) {
    unsigned pageSpace;
    std::memcpy(&pageSpace, (char *) data + 0, sizeof(unsigned));
    return pageSpace;
}

unsigned RecordBasedFileManager::readPageRecTotal(void *data) {
    unsigned pageRecLoad;
    std::memcpy(&pageRecLoad, (char *) data + 1 * sizeof(unsigned), sizeof(unsigned));
    return pageRecLoad;
}

void RecordBasedFileManager::setPageSpace(void *data, unsigned space) {
    unsigned temp = space;
    std::memcpy((char *) data + 0, &temp, sizeof(unsigned));
}

void RecordBasedFileManager::setPageRecTotal(void *data, unsigned recTotal) {
    unsigned temp = recTotal;
    std::memcpy((char *) data + 0, &temp, sizeof(unsigned));
}

// Referenced from test_util prepareRecord function
int RecordBasedFileManager::getRecordSize(const std::vector<Attribute> &recordDescriptor, const void *data) {
    int fieldCount = recordDescriptor.size();
    int nullFlagSize = this->getNullFlagSize(fieldCount);
    int offset = 0;
    auto *nullFlags = (unsigned char *) std::malloc(nullFlagSize);
    std::memset(nullFlags, 0, nullFlagSize);
    std::memcpy((char *) data + offset, nullFlags, nullFlagSize);
    offset += nullFlagSize;
    for (int i = 0; i < fieldCount; i++) {
        // Add handler for null flags larger than 1 byte
        int bytePos = i / 8;
        int bitPos = i % 8;
        bool nullBit = nullFlags[bytePos] & (unsigned) 1 << (unsigned)(7 - bitPos);
        if (!nullBit) {
            if (recordDescriptor[i].type == TypeVarChar) {
                int nameLength;
                std::memcpy(&nameLength, (char *) data + offset, sizeof(int));
                offset += sizeof(int);
                offset += nameLength;
            } else {
                offset += sizeof(int);
            }
        }
    }
    return offset;
}

unsigned RecordBasedFileManager::getInsertOffset(void *data) {
    unsigned recTotal = this->readPageRecTotal(data);
    if (recTotal == 0) {
        return 2 * sizeof(unsigned);
    }
    int endOffset;
    std::memcpy(&endOffset, (char *) data + PAGE_SIZE - 1 - recTotal * sizeof(unsigned), sizeof(unsigned));
    int offset;
    std::memcpy(&offset, (char *) data + endOffset, sizeof(unsigned));
    return offset;
}

void RecordBasedFileManager::setInsertOffset(void *data, unsigned offset, unsigned recordSize) {
    unsigned pos = offset + recordSize;
    std::memcpy((char *) data + offset, &pos, sizeof(unsigned));
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
    int fieldCount = recordDescriptor.size();
    void *page = std::malloc(PAGE_SIZE);
    unsigned numberOfPages = fileHandle.getNumberOfPages();
    unsigned currentPID = numberOfPages - 1;
    int recordSize = this->getRecordSize(recordDescriptor, data);
    fileHandle.readPage(currentPID, page);
    int i = -1;
    if (this->readPageSpace(page) < recordSize + sizeof(unsigned)) {
        for (i = 0; i < numberOfPages - 1; i++) {
            fileHandle.readPage(i, page);
            if (this->readPageSpace(page) >= recordSize + sizeof(unsigned)) {
                break;
            }
        }
    }
    if (i == numberOfPages - 1) {
        std::free(page);
        page = std::malloc(PAGE_SIZE);
        this->setPageSpace(page, PAGE_SIZE - 2 * sizeof(unsigned));
        this->setPageRecTotal(page, 0);
        fileHandle.appendPage(page);
        numberOfPages = fileHandle.getNumberOfPages();
        currentPID = numberOfPages - 1;
        fileHandle.readPage(currentPID, page);
    }
    rid.pageNum = currentPID;
    unsigned recTotal = this->readPageRecTotal(page);
    rid.slotNum = recTotal;
    unsigned offset = this->getInsertOffset(page);
    std::memcpy((char *) page + offset, data, recordSize);
    this->setInsertOffset(page, offset, recordSize);
    return 0;
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
        bool nullBit = nullFieledsIndicator[i/ CHAR_BIT] & (1 << (7 - i % CHAR_BIT));
        if(!nullBit) {
            if(attr.type == TypeInt) {
                int value;
                memcpy(&value, (char *)data + offset, attr.length);
                offset += attr.length;
                std::cout<<attr.name.c_str()<<": "<<value<<std::endl;
            }
            else if(attr.type == TypeReal) {
                float value;
                memcpy(&value, (char *)data + offset, attr.length);
                offset += attr.length;
                std::cout<<attr.name.c_str()<<": "<<value<<std::endl;
            }
            else if(attr.type == TypeVarChar) {
                int length;
                memcpy(&length, (char *)data + offset, sizeof(int));
                offset += sizeof(int);
                char * value = (char *)malloc(length);
                std::cout<<"the length of name: "<<length<<std::endl;
                memcpy(value, (char *)data + offset, length);
                offset += length;
                std::cout<<attr.name.c_str()<<": "<<std::string(value, length)<<std::endl;
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



