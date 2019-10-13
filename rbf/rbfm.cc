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

short RecordBasedFileManager::getPageRecTotal(void *data) {
    short pageRecLoad;
    std::memcpy(&pageRecLoad, (char *) data + PAGE_SIZE - sizeof(short), sizeof(short));
    return pageRecLoad;
}

void RecordBasedFileManager::setPageRecTotal(void *data, short recTotal) {
    short temp = recTotal;
    std::memcpy((char *) data + PAGE_SIZE - sizeof(short), &temp, sizeof(short));
}

short RecordBasedFileManager::getPageSpace(void *data) {
    short pageSpace;
    std::memcpy(&pageSpace, (char *) data + PAGE_SIZE - 2 * sizeof(short), sizeof(short));
    return pageSpace;
}

void RecordBasedFileManager::setPageSpace(void *data, short space) {
    short temp = space;
    std::memcpy((char *) data + PAGE_SIZE - 2 * sizeof(short), &temp, sizeof(short));
}

short RecordBasedFileManager::getRecordOffset(void *data, unsigned slotNum) {
    if (slotNum == -1) {
        return 0;
    }
    short offset;
    int ptr = PAGE_SIZE - 2 * sizeof(short);
    ptr = ptr - (slotNum + 1) * sizeof(short);
    std::memcpy(&offset, (char *) data + ptr, sizeof(short));
    return offset;
}

void RecordBasedFileManager::setRecordOffset(void *data, short offset, short recordSize, unsigned slotNum) {
    short pos = offset + recordSize;
    int ptr = PAGE_SIZE - 2 * sizeof(short);
    ptr = ptr - (slotNum + 1) * sizeof(short);
    std::memcpy((char *) data + ptr, &pos, sizeof(short));
}

short RecordBasedFileManager::getRecordSize(void *data, unsigned slotNum) {
    short offset = this->getRecordOffset(data, slotNum);
    short prevOffset = this->getRecordOffset(data, slotNum - 1);
    return offset - prevOffset;
}

// Referenced from test_util prepareRecord function
short RecordBasedFileManager::parseRecord(const std::vector<Attribute> &recordDescriptor, const void *data, const void *offsetTable) {
    int fieldCount = recordDescriptor.size();
    int nullFlagSize = this->getNullFlagSize(fieldCount);
    int dataPtr = 0;
    short attrOffset = 0;
    char *nullFlags = (char *) std::malloc(nullFlagSize);
    std::memcpy(nullFlags, (char *) data + dataPtr, nullFlagSize);
    dataPtr += nullFlagSize;
    attrOffset += sizeof(bool) + nullFlagSize + fieldCount * sizeof(short);
    for (int i = 0; i < fieldCount; i++) {
        // Add handler for null flags larger than 1 byte
        int bytePos = i / 8;
        int bitPos = i % 8;
        bool nullBit = nullFlags[bytePos] & (unsigned) 1 << (unsigned) (7 - bitPos);
        Attribute attr = recordDescriptor[i];
        if (!nullBit) {
            if (attr.type == TypeVarChar) {
                int nameLength;
                std::memcpy(&nameLength, (char *) data + dataPtr, sizeof(int));
                dataPtr += sizeof(int) + nameLength;
                attrOffset += sizeof(int) + nameLength;
            } else if (attr.type == TypeInt) {
                dataPtr += sizeof(int);
                attrOffset += sizeof(int);
            } else if (attr.type == TypeReal) {
                dataPtr += sizeof(float);
                attrOffset += sizeof(float);
            }
        }
        std::memcpy((char *) offsetTable + i * sizeof(short), &attrOffset, sizeof(short));
    }
    std::free(nullFlags);
    return dataPtr + sizeof(short) * fieldCount + sizeof(bool);
}

RC RecordBasedFileManager::copyRecord(const void *page, short prevOffset, const std::vector<Attribute> &recordDescriptor, 
        const void *data, const void *offsetTable) {
    int fieldCount = recordDescriptor.size();
    int nullFlagSize = this->getNullFlagSize(fieldCount);
    short dataPtr = 0;
    short pagePtr = prevOffset;
    bool ptrFlag = false;
    std::memcpy((char *) page + pagePtr, &ptrFlag, sizeof(bool));
    pagePtr += sizeof(bool);
    std::memcpy((char *) page + pagePtr, (char *) data + dataPtr, nullFlagSize);
    dataPtr += nullFlagSize;
    pagePtr += nullFlagSize;
    std::memcpy((char *) page + pagePtr, (char *) offsetTable, fieldCount * sizeof(short));
    pagePtr += fieldCount * sizeof(short);
    short recordSize;
    std::memcpy(&recordSize, (char *) offsetTable + (fieldCount - 1) * sizeof(short), sizeof(short));
    recordSize -= (pagePtr - prevOffset);
    std::memcpy((char *) page + pagePtr, (char *) data + dataPtr, recordSize);
    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
    char *offsetTable = (char *) std::malloc(recordDescriptor.size() * sizeof(short));
    short recordSize = this->parseRecord(recordDescriptor, data, offsetTable);
    void *page = std::malloc(PAGE_SIZE);
    unsigned numberOfPages = fileHandle.getNumberOfPages();
    bool needNewPage = true;
    unsigned currentPID = 0;
    if (numberOfPages != 0) {
        currentPID = numberOfPages - 1;
        fileHandle.readPage(currentPID, page);
        needNewPage = false;
        if (this->getPageSpace(page) < recordSize + sizeof(short)) {
            for (int i = 0; i < numberOfPages - 1; i++) {
                fileHandle.readPage(i, page);
                if (this->getPageSpace(page) >= recordSize + sizeof(short)) {
                    break;
                }
            }
            needNewPage = true;
        }
    }
    if (needNewPage) {
        this->setPageSpace(page, PAGE_SIZE - 2 * sizeof(short));
        this->setPageRecTotal(page, 0);
        fileHandle.appendPage(page);
        numberOfPages = fileHandle.getNumberOfPages();
        currentPID = numberOfPages - 1;
        fileHandle.readPage(currentPID, page);
    }
    short recTotal = this->getPageRecTotal(page);
    short prevOffset = this->getRecordOffset(page, recTotal - 1);
    short space = this->getPageSpace(page);
    this->copyRecord(page, prevOffset, recordDescriptor, data, offsetTable);
    recTotal += 1;
    this->setPageRecTotal(page, recTotal);
    this->setPageSpace(page, space - recordSize - sizeof(short));
    this->setRecordOffset(page, prevOffset, recordSize, recTotal - 1);
    fileHandle.writePage(currentPID, page);
    std::free(page);
    std::free(offsetTable);
    rid.pageNum = currentPID;
    rid.slotNum = recTotal - 1;
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    unsigned numberOfPages = fileHandle.getNumberOfPages();
    if (rid.pageNum >= numberOfPages) {
        return -1;
    }
    void *page = std::malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    short recTotal = this->getPageRecTotal(page);
    if (rid.slotNum >= recTotal) {
        return -1;
    }
    short prevOffset = this->getRecordOffset(page, rid.slotNum - 1);
    short recordSize = this->getRecordSize(page, rid.slotNum);
    int fieldCount = recordDescriptor.size();
    int nullFlagSize = this->getNullFlagSize(fieldCount);
    std::memcpy((char *) data, (char *) page + prevOffset + sizeof(bool), nullFlagSize);
    short headerSize = nullFlagSize + fieldCount * sizeof(short) + sizeof(bool);
    std::memcpy((char *) data + nullFlagSize, (char *) page + prevOffset + headerSize, recordSize - headerSize);
    std::free(page);
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    return -1;
}

// Referenced from test_util prepareRecord function
RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    int fieldCount = recordDescriptor.size();
    int nullFlagSize = this->getNullFlagSize(fieldCount);
    int dataPtr = 0;
    bool nullBit;
    auto *nullFlags = (unsigned char *) std::malloc(nullFlagSize);
    std::memcpy(nullFlags, (char *) data + dataPtr, nullFlagSize);
    dataPtr += nullFlagSize;
    for (int i = 0; i < fieldCount; i++) {
        // Add handler for null flags larger than 1 byte
        int bytePos = i / 8;
        int bitPos = i % 8;
        nullBit = nullFlags[bytePos] & (unsigned) 1 << (unsigned) (7 - bitPos);
        Attribute attr = recordDescriptor[i];
        std::cout << attr.name << ": ";
        if (!nullBit) {
            if (attr.type == TypeVarChar) {
                int nameLength;
                std::memcpy(&nameLength, (char *) data + dataPtr, sizeof(int));
                dataPtr += sizeof(int);
                char *value = (char *) malloc(nameLength);
                memcpy(value, (char *) data + dataPtr, nameLength);
                dataPtr += nameLength;
                std::cout << std::string(value, nameLength) << std::endl;
                std::cout << "Var Char Length: " << nameLength;
            } else if (attr.type == TypeInt) {
                int value;
                memcpy(&value, (char *) data + dataPtr, attr.length);
                dataPtr += attr.length;
                std::cout << value;
            } else if (attr.type == TypeReal) {
                float value;
                memcpy(&value, (char *) data + dataPtr, attr.length);
                dataPtr += attr.length;
                std::cout << value;
            }
        } else {
            std::cout << ": Null";
        }
        std::cout << std::endl;
    }
    std::free(nullFlags);
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



