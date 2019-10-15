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

RC RecordBasedFileManager::createFile(const string &fileName) {
    return _pf_manager->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

// Referenced from test_util getActualByteForNullsIndicator function
short RecordBasedFileManager::getNullFlagSize(int fieldCount) {
    return ceil((double) fieldCount / CHAR_BIT);
}

short RecordBasedFileManager::getPageSlotTotal(const void *page) {
    short pageSlotTotal;
    memcpy(&pageSlotTotal, (char *) page + PAGE_SIZE - sizeof(short), sizeof(short));
    return pageSlotTotal;
}

void RecordBasedFileManager::setPageSlotTotal(const void *page, short slotTotal) {
    memcpy((char *) page + PAGE_SIZE - sizeof(short), &slotTotal, sizeof(short));
}

short RecordBasedFileManager::getPageFreeSpace(const void *page) {
    short pageSpace;
    memcpy(&pageSpace, (char *) page + PAGE_SIZE - 2 * sizeof(short), sizeof(short));
    return pageSpace;
}

void RecordBasedFileManager::setPageFreeSpace(const void *page, short space) {
    memcpy((char *) page + PAGE_SIZE - 2 * sizeof(short), &space, sizeof(short));
}

short RecordBasedFileManager::getRecordOffset(const void *page, short slotNum) {
    short offset;
    int ptr = PAGE_SIZE - 2 * sizeof(short);
    ptr -= 2 * (slotNum + 1) * sizeof(short);
    memcpy(&offset, (char *) page + ptr, sizeof(short));
    return offset;
}

void RecordBasedFileManager::setRecordOffset(const void *page, short offset, short slotNum) {
    int ptr = PAGE_SIZE - 2 * sizeof(short);
    ptr -= 2 * (slotNum + 1) * sizeof(short);
    memcpy((char *) page + ptr, &offset, sizeof(short));
}

short RecordBasedFileManager::getInsertOffset(const void *page) {
    return PAGE_SIZE - this->getPageFreeSpace(page) -
           this->getPageSlotTotal(page) * 2 * sizeof(short) -
           2 * sizeof(short);
}

short RecordBasedFileManager::countRemainSpace(const void *page, short recordSize) {
    short freeSpace = this->getPageFreeSpace(page);
    recordSize = recordSize >= sizeof(unsigned) + sizeof(short) ?
                 recordSize : sizeof(unsigned) + sizeof(short);
    if (this->findFreeSlot(page) == this->getPageSlotTotal(page)) {
        return freeSpace - recordSize - 2 * sizeof(short);
    }
    return freeSpace - recordSize;
}

short RecordBasedFileManager::getRecordSize(const void *page, unsigned slotNum) {
    short recordSize;
    short ptr = PAGE_SIZE - 2 * sizeof(short);
    ptr -= (2 * slotNum + 1) * sizeof(short);
    memcpy(&recordSize, (char *) page + ptr, sizeof(short));
    return recordSize;
}

void RecordBasedFileManager::setRecordSize(const void *page, short recordSize, unsigned slotNum) {
    int ptr = PAGE_SIZE - 2 * sizeof(short);
    ptr -= (2 * slotNum + 1) * sizeof(short);
    memcpy((char *) page + ptr, &recordSize, sizeof(short));
}

short RecordBasedFileManager::findFreeSlot(const void *page) {
    short slotTotal = this->getPageSlotTotal(page);
    short slot = 0;
    short offset;
    while (slot < slotTotal) {
        offset = this->getRecordOffset(page, slot);
        if (offset == -1) {
            break;
        }
        slot++;
    }
    return slot;
}

// Referenced from test_util prepareRecord function
short RecordBasedFileManager::parseRecord(const vector<Attribute> &recordDescriptor,
                                          const void *data, const void *offsetTable) {
    int fieldCount = recordDescriptor.size();
    short nullFlagSize = this->getNullFlagSize(fieldCount);
    short dataPtr = 0;
    short attrOffset = 0;
    char *nullFlags = (char *) malloc(nullFlagSize);
    memcpy(nullFlags, (char *) data + dataPtr, nullFlagSize);
    dataPtr += nullFlagSize;
    attrOffset += nullFlagSize + fieldCount * sizeof(short);
    for (int i = 0; i < fieldCount; i++) {
        // Add handler for null flags larger than 1 byte
        int bytePos = i / 8;
        int bitPos = i % 8;
        bool nullBit = nullFlags[bytePos] & (unsigned) 1 << (unsigned) (7 - bitPos);
        Attribute attr = recordDescriptor[i];
        if (!nullBit) {
            if (attr.type == TypeVarChar) {
                int nameLength;
                memcpy(&nameLength, (char *) data + dataPtr, sizeof(int));
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
        memcpy((char *) offsetTable + i * sizeof(short), &attrOffset, sizeof(short));
    }
    free(nullFlags);
    return attrOffset;
}

RC RecordBasedFileManager::copyRecord(const void *page, int fieldCount,
                                      const void *data, const void *offsetTable, short recordSize) {
    short nullFlagSize = this->getNullFlagSize(fieldCount);
    short dataPtr = 0;
    short insertOffset = this->getInsertOffset(page);
    short pagePtr = insertOffset;
    memcpy((char *) page + pagePtr, (char *) data + dataPtr, nullFlagSize);
    dataPtr += nullFlagSize;
    pagePtr += nullFlagSize;
    memcpy((char *) page + pagePtr, (char *) offsetTable, fieldCount * sizeof(short));
    pagePtr += fieldCount * sizeof(short);
    short sz = recordSize - (pagePtr - insertOffset);
    memcpy((char *) page + pagePtr, (char *) data + dataPtr, sz);
    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
    char *offsetTable = (char *) malloc(recordDescriptor.size() * sizeof(short));
    short recordSize = this->parseRecord(recordDescriptor, data, offsetTable);
    void *page = malloc(PAGE_SIZE);
    unsigned numberOfPages = fileHandle.getNumberOfPages();
    bool needNewPage = true;
    unsigned currentPID = 0;
    if (numberOfPages != 0) {
        currentPID = numberOfPages - 1;
        fileHandle.readPage(currentPID, page);
        needNewPage = false;
        if (this->countRemainSpace(page, recordSize) < 0) {
            for (int i = 0; i < numberOfPages - 1; i++) {
                fileHandle.readPage(i, page);
                if (this->countRemainSpace(page, recordSize) >= 0) {
                    break;
                }
            }
            needNewPage = true;
        }
    }
    if (needNewPage) {
        this->setPageFreeSpace(page, PAGE_SIZE - 2 * sizeof(short));
        this->setPageSlotTotal(page, 0);
        fileHandle.appendPage(page);
        numberOfPages = fileHandle.getNumberOfPages();
        currentPID = numberOfPages - 1;
        fileHandle.readPage(currentPID, page);
    }
    rid.pageNum = currentPID;
    rid.slotNum = this->findFreeSlot(page);
    short slotTotal = this->getPageSlotTotal(page);
    short insertOffset = this->getInsertOffset(page);
    this->copyRecord(page, recordDescriptor.size(), data, offsetTable, recordSize);
    if (rid.slotNum == slotTotal) {
        this->setPageSlotTotal(page, slotTotal + 1);
    }
    this->setPageFreeSpace(page, this->countRemainSpace(page, recordSize));
    this->setRecordOffset(page, insertOffset + recordSize, rid.slotNum);
    this->setRecordSize(page, recordSize, rid.slotNum);
    fileHandle.writePage(currentPID, page);
    free(page);
    free(offsetTable);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    unsigned numberOfPages = fileHandle.getNumberOfPages();
    if (rid.pageNum >= numberOfPages) {
        return -1;
    }
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    if (rid.slotNum >= this->getPageSlotTotal(page)) {
        return -1;
    }
    short recordOffset, recordSize;
    RID *id = this->locateRecord(fileHandle, page, &recordOffset, &recordSize, rid);
    if (id == nullptr) {
        return -5;
    }
    free(id);
    int fieldCount = recordDescriptor.size();
    int nullFlagSize = this->getNullFlagSize(fieldCount);
    short pagePtr = recordOffset - recordSize;
    memcpy((char *) data, (char *) page + pagePtr, nullFlagSize);
    short headerSize = nullFlagSize + fieldCount * sizeof(short);
    memcpy((char *) data + nullFlagSize, (char *) page + pagePtr + headerSize,
           recordSize - headerSize);
    free(page);
    return 0;
}

RID *RecordBasedFileManager::locateRecord(FileHandle &fileHandle, void *page,
                                          short *recordOffset, short *recordSize, const RID &rid) {
    *recordOffset = this->getRecordOffset(page, rid.slotNum);
    if (*recordOffset == -1) {
        return nullptr;
    }
    *recordSize = this->getRecordSize(page, rid.slotNum);
    short pagePtrSize = sizeof(unsigned) + sizeof(short);
    RID *id = (RID *) malloc(sizeof(RID));
    memcpy(id, &rid, sizeof(RID));
    unsigned pageNum;
    short slotNum;
    while (*recordSize == -1) {
        *recordOffset -= pagePtrSize;
        memcpy(&pageNum, (char *) page + *recordOffset, sizeof(unsigned));
        memcpy(&slotNum, (char *) page + *recordOffset + sizeof(unsigned), sizeof(short));
        id->pageNum = pageNum;
        id->slotNum = slotNum;
        fileHandle.readPage(id->pageNum, page);
        *recordOffset = this->getRecordOffset(page, id->slotNum);
        if (*recordOffset == -1) {
            return nullptr;
        }
        *recordSize = this->getRecordSize(page, id->slotNum);
    }
    return id;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    unsigned numberOfPages = fileHandle.getNumberOfPages();
    if (rid.pageNum >= numberOfPages) {
        return -1;
    }
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    short slotTotal = this->getPageSlotTotal(page);
    if (rid.slotNum >= slotTotal) {
        return -1;
    }
    short recordOffset, recordSize;
    RID *id = this->locateRecord(fileHandle, page, &recordOffset, &recordSize, rid);
    if (id == nullptr) {
        return -5;
    }
    short offset;
    bool moveFlag = false;
    for (short i = 0; i < slotTotal; i++) {
        offset = this->getRecordOffset(page, i);
        if (offset > recordOffset) {
            moveFlag = true;
            this->setRecordOffset(page, offset - recordSize, i);
        }
    }
    if (moveFlag) {
        short len = this->getInsertOffset(page) - recordOffset;
        char *cache = (char *) malloc(len);
        memcpy(cache,(char *) page + recordOffset, len);
        memcpy((char *) page + recordOffset - recordSize, cache, len);
        free(cache);
    }
    this->setRecordOffset(page, -1, id->slotNum);
    fileHandle.writePage(id->pageNum, page);
    free(id);
    free(page);
    return 0;
}

// Referenced from test_util prepareRecord function
RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    int fieldCount = recordDescriptor.size();
    int nullFlagSize = this->getNullFlagSize(fieldCount);
    int dataPtr = 0;
    bool nullBit;
    auto *nullFlags = (unsigned char *) malloc(nullFlagSize);
    memcpy(nullFlags, (char *) data + dataPtr, nullFlagSize);
    dataPtr += nullFlagSize;
    for (int i = 0; i < fieldCount; i++) {
        // Add handler for null flags larger than 1 byte
        int bytePos = i / 8;
        int bitPos = i % 8;
        nullBit = nullFlags[bytePos] & (unsigned) 1 << (unsigned) (7 - bitPos);
        Attribute attr = recordDescriptor[i];
        cout << attr.name << " ";
        if (!nullBit) {
            if (attr.type == TypeVarChar) {
                int nameLength;
                memcpy(&nameLength, (char *) data + dataPtr, sizeof(int));
                dataPtr += sizeof(int);
                char *value = (char *) malloc(nameLength);
                memcpy(value, (char *) data + dataPtr, nameLength);
                dataPtr += nameLength;
                cout << string(value, nameLength) << " ";
            } else if (attr.type == TypeInt) {
                int value;
                memcpy(&value, (char *) data + dataPtr, attr.length);
                dataPtr += attr.length;
                cout << value << " ";
            } else if (attr.type == TypeReal) {
                float value;
                memcpy(&value, (char *) data + dataPtr, attr.length);
                dataPtr += attr.length;
                cout << value << " ";
            }
        } else {
            cout << "Null ";
        }
    }
    cout << endl;
    free(nullFlags);
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
    return -1;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                         const RID &rid, const string &attributeName, void *data) {
    return -1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute, const CompOp compOp, const void *value,
                                const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    return -1;
}



