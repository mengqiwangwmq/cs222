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

short RecordBasedFileManager::getRecordSize(const void *page, short slotNum) {
    short recordSize;
    short ptr = PAGE_SIZE - 2 * sizeof(short);
    ptr -= (2 * slotNum + 1) * sizeof(short);
    memcpy(&recordSize, (char *) page + ptr, sizeof(short));
    return recordSize;
}

void RecordBasedFileManager::setRecordSize(const void *page, short recordSize, short slotNum) {
    int ptr = PAGE_SIZE - 2 * sizeof(short);
    ptr -= (2 * slotNum + 1) * sizeof(short);
    memcpy((char *) page + ptr, &recordSize, sizeof(short));
}

void RecordBasedFileManager::getAttributeOffset(const void *page, short pagePtr, int fieldCount, short nullFlagSize,
                                                short attrIdx, short &offset, short &prevOffset) {
    memcpy(&offset, (char *) page + pagePtr + nullFlagSize + attrIdx * sizeof(short), sizeof(short));
    prevOffset = nullFlagSize + fieldCount * sizeof(short);
    if (attrIdx != 0) {
        memcpy(&prevOffset, (char *) page + pagePtr + nullFlagSize + (attrIdx - 1) * sizeof(short), sizeof(short));
    }
}

short RecordBasedFileManager::getInsertPtr(const void *page) {
    return PAGE_SIZE - this->getPageFreeSpace(page) -
           this->getPageSlotTotal(page) * 2 * sizeof(short) -
           2 * sizeof(short);
}

short RecordBasedFileManager::countRemainSpace(const void *page, short freeSpace, short recordSize, bool newFlag) {
    recordSize = recordSize >= sizeof(unsigned) + sizeof(short) ?
                 recordSize : sizeof(unsigned) + sizeof(short);
    if (newFlag && this->findFreeSlot(page) == this->getPageSlotTotal(page)) {
        return freeSpace - recordSize - 2 * sizeof(short);
    }
    return freeSpace - recordSize;
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

RC RecordBasedFileManager::copyRecord(const void *page, short insertPtr, int fieldCount,
                                      const void *data, const void *offsetTable, short recordSize) {
    short nullFlagSize = this->getNullFlagSize(fieldCount);
    short dataPtr = 0;
    short pagePtr = insertPtr;
    memcpy((char *) page + pagePtr, (char *) data + dataPtr, nullFlagSize);
    dataPtr += nullFlagSize;
    pagePtr += nullFlagSize;
    memcpy((char *) page + pagePtr, (char *) offsetTable, fieldCount * sizeof(short));
    pagePtr += fieldCount * sizeof(short);
    short sz = recordSize - (pagePtr - insertPtr);
    memcpy((char *) page + pagePtr, (char *) data + dataPtr, sz);
    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
    char *offsetTable = (char *) malloc(recordDescriptor.size() * sizeof(short));
    short recordSize = this->parseRecord(recordDescriptor, data, offsetTable);
    void *page = malloc(PAGE_SIZE);
    memset(page, '\0', PAGE_SIZE);
    unsigned numberOfPages = fileHandle.getNumberOfPages();
    bool needNewPage = true;
    short remainSpace = 0;
    unsigned currentPID = 0;
    if (numberOfPages != 0) {
        currentPID = numberOfPages - 1;
        fileHandle.readPage(currentPID, page);
        needNewPage = false;
        remainSpace = this->countRemainSpace(page, this->getPageFreeSpace(page),
                                             recordSize, true);
        if (remainSpace < 0) {
            needNewPage = true;
            if (numberOfPages > 1) {
                for (int i = numberOfPages - 2; i >= 0; i--) {
                    fileHandle.readPage(i, page);
                    remainSpace = this->countRemainSpace(page, this->getPageFreeSpace(page),
                                                         recordSize, true);
                    if (remainSpace >= 0) {
                        needNewPage = false;
                        currentPID = i;
                        break;
                    }
                }
            }
        }
    }
    if (needNewPage) {
        this->setPageFreeSpace(page, PAGE_SIZE - 2 * sizeof(short));
        this->setPageSlotTotal(page, 0);
        remainSpace = this->countRemainSpace(page, PAGE_SIZE - 2 * sizeof(short),
                                             recordSize, true);
        numberOfPages++;
        currentPID = numberOfPages - 1;
    }
    rid.pageNum = currentPID;
    rid.slotNum = this->findFreeSlot(page);
    short slotTotal = this->getPageSlotTotal(page);
    short insertPtr = this->getInsertPtr(page);
    this->copyRecord(page, insertPtr, recordDescriptor.size(),
                     data, offsetTable, recordSize);
    this->setPageFreeSpace(page, remainSpace);
    if (rid.slotNum == slotTotal) {
        this->setPageSlotTotal(page, slotTotal + 1);
    }
    this->setRecordOffset(page, insertPtr + recordSize, rid.slotNum);
    this->setRecordSize(page, recordSize, rid.slotNum);
    if (needNewPage) {
        fileHandle.appendPage(page);
    } else {
        fileHandle.writePage(currentPID, page);
    }
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
        free(page);
        return -1;
    }
    RID *id = (RID *) malloc(sizeof(RID));
    memcpy(id, &rid, sizeof(RID));
    short recordOffset, recordSize;
    this->locateRecord(fileHandle, page, recordOffset, recordSize, id);
    if (id == nullptr) {
        free(page);
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

void RecordBasedFileManager::locateRecord(FileHandle &fileHandle, void *page,
                                          short &recordOffset, short &recordSize, RID *&id) {
    recordOffset = this->getRecordOffset(page, id->slotNum);
    if (recordOffset == -1) {
        free(id);
        id = nullptr;
        return;
    }
    recordSize = this->getRecordSize(page, id->slotNum);
    short pagePtrSize = sizeof(unsigned) + sizeof(short);
    unsigned pageNum;
    short slotNum;
    while (recordSize == -1) {
        recordOffset -= pagePtrSize;
        memcpy(&pageNum, (char *) page + recordOffset, sizeof(unsigned));
        memcpy(&slotNum, (char *) page + recordOffset + sizeof(unsigned), sizeof(short));
        id->pageNum = pageNum;
        id->slotNum = slotNum;
        fileHandle.readPage(id->pageNum, page);
        recordOffset = this->getRecordOffset(page, id->slotNum);
        if (recordOffset == -1) {
            free(id);
            id = nullptr;
            return;
        }
        recordSize = this->getRecordSize(page, id->slotNum);
    }
}

void RecordBasedFileManager::shiftRecord(const void *page, short recordOffset, short distance) {
    short slotTotal = this->getPageSlotTotal(page);
    short offset;
    bool moveFlag = false;
    for (short i = 0; i < slotTotal; i++) {
        offset = this->getRecordOffset(page, i);
        if (offset > recordOffset) {
            moveFlag = true;
            this->setRecordOffset(page, offset + distance, i);
        }
    }
    short len = this->getInsertPtr(page) - recordOffset;
    if (moveFlag && len != 0) {
        char *cache = (char *) malloc(len);
        memcpy(cache, (char *) page + recordOffset, len);
        memcpy((char *) page + recordOffset + distance, cache, len);
        free(cache);
    }
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
        free(page);
        return -1;
    }
    short recordOffset, recordSize;
    RID *id = (RID *) malloc(sizeof(RID));
    memcpy(id, &rid, sizeof(RID));
    this->locateRecord(fileHandle, page, recordOffset, recordSize, id);
    if (id == nullptr) {
        free(page);
        return -5;
    }
    this->shiftRecord(page, recordOffset, -recordSize);
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
                free(value);
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
    unsigned numberOfPages = fileHandle.getNumberOfPages();
    if (rid.pageNum >= numberOfPages) {
        return -1;
    }
    void *page = malloc(PAGE_SIZE);
    memset(page, '\0', PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    short slotTotal = this->getPageSlotTotal(page);
    if (rid.slotNum >= slotTotal) {
        return -1;
    }
    short recordOffset, recordSize;
    RID *id = (RID *) malloc(sizeof(RID));
    memcpy(id, &rid, sizeof(RID));
    this->locateRecord(fileHandle, page, recordOffset, recordSize, id);
    if (id == nullptr) {
        free(page);
        return -5;
    }
    int fieldCount = recordDescriptor.size();
    char *offsetTable = (char *) malloc(fieldCount * sizeof(short));
    short newSize = this->parseRecord(recordDescriptor, data, offsetTable);
    short remainSpace = this->countRemainSpace(page, this->getPageFreeSpace(page) + recordSize, newSize, false);
    if (remainSpace >= 0) {
        short distance = newSize - recordSize;
        if (distance <= 0) {
            this->copyRecord(page, recordOffset - recordSize, fieldCount,
                             data, offsetTable, newSize);
            this->shiftRecord(page, recordOffset, distance);
        } else {
            this->shiftRecord(page, recordOffset, distance);
            this->copyRecord(page, recordOffset - recordSize, fieldCount,
                             data, offsetTable, newSize);
        }
        this->setPageFreeSpace(page, remainSpace);
        this->setRecordOffset(page, recordOffset + distance, id->slotNum);
        this->setRecordSize(page, newSize, id->slotNum);
    } else {
        char *cache = (char *) malloc(PAGE_SIZE);
        memset(cache, '\0', PAGE_SIZE);
        unsigned pageNum = numberOfPages;
        bool needNewPage = true;
        if (numberOfPages > 1) {
            for (int i = numberOfPages - 1; i >= 0; i--) {
                if (i == id->pageNum) continue;
                pageNum = i;
                fileHandle.readPage(pageNum, cache);
                remainSpace = this->countRemainSpace(cache, this->getPageFreeSpace(cache), newSize, true);
                if (remainSpace >= 0) {
                    needNewPage = false;
                    break;
                }
            }
        }
        if (needNewPage) {
            this->setPageFreeSpace(cache, PAGE_SIZE - 2 * sizeof(short));
            this->setPageSlotTotal(cache, 0);
            remainSpace = this->countRemainSpace(cache, PAGE_SIZE - 2 * sizeof(short), newSize, true);
            numberOfPages++;
            pageNum = numberOfPages - 1;
        }
        short slotNum = this->findFreeSlot(cache);
        short slotTotal = this->getPageSlotTotal(cache);
        short insertPtr = this->getInsertPtr(cache);
        this->copyRecord(cache, insertPtr, fieldCount,
                         data, offsetTable, newSize);
        this->setPageFreeSpace(cache, remainSpace);
        if (slotNum == slotTotal) {
            this->setPageSlotTotal(cache, slotTotal + 1);
        }
        this->setRecordOffset(cache, insertPtr + newSize, slotNum);
        this->setRecordSize(cache, newSize, slotNum);
        if (needNewPage) {
            fileHandle.appendPage(cache);
        } else {
            fileHandle.writePage(pageNum, cache);
        }
        free(cache);
        insertPtr = recordOffset - recordSize;
        memcpy((char *) page + insertPtr, &pageNum, sizeof(unsigned));
        memcpy((char *) page + insertPtr + sizeof(unsigned), &slotNum, sizeof(short));
        short distance = sizeof(unsigned) + sizeof(short) - recordSize;
        this->shiftRecord(page, recordOffset, distance);
        this->setRecordOffset(page, recordOffset + distance, id->slotNum);
        this->setRecordSize(page, -1, id->slotNum);
    }
    fileHandle.writePage(id->pageNum, page);
    free(id);
    free(offsetTable);
    free(page);
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                         const RID &rid, const string &attributeName, void *data) {
    unsigned numberOfPages = fileHandle.getNumberOfPages();
    if (rid.pageNum >= numberOfPages) {
        return -1;
    }
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    if (rid.slotNum >= this->getPageSlotTotal(page)) {
        return -1;
    }
    int fieldCount = recordDescriptor.size();
    int i;
    for (i = 0; i < fieldCount; i++) {
        Attribute attr = recordDescriptor[i];
        if (attr.name == attributeName) {
            break;
        }
    }
    if (i == fieldCount) {
        return -6;
    }
    RID *id = (RID *) malloc(sizeof(RID));
    memcpy(id, &rid, sizeof(RID));
    short recordOffset, recordSize;
    this->locateRecord(fileHandle, page, recordOffset, recordSize, id);
    if (id == nullptr) {
        free(page);
        return -5;
    }
    free(id);
    int nullFlagSize = this->getNullFlagSize(fieldCount);
    short pagePtr = recordOffset - recordSize;
    short offset, prevOffset;
    this->getAttributeOffset(page, pagePtr, fieldCount, nullFlagSize, i, offset, prevOffset);
    short sz = offset - prevOffset;
    bool nullBit;
    if (sz == 0) {
        nullBit = false;
        memcpy(data, &nullBit, sizeof(bool));
    } else {
        nullBit = true;
        memcpy(data, &nullBit, sizeof(bool));
        memcpy((char *) data + sizeof(bool), (char *) page + pagePtr + prevOffset, sz);
    }
    free(page);
    return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute, const CompOp compOp, const void *value,
                                const vector<string> &attrNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    rbfm_ScanIterator.init(fileHandle, recordDescriptor, compOp, value, attrNames);

    int fieldCount = recordDescriptor.size();
    int i, j;
    // Get positions of attributes that are projected
    for (i = 0; i < attrNames.size(); i++) {
        for (j = 0; j < fieldCount; j++) {
            if (recordDescriptor[j].name == attrNames[i]) {
                rbfm_ScanIterator.attrIdx.emplace_back(j);
                break;
            }
        }
        if (j == fieldCount) {
            return -7; // AttributeNotFoundException
        }
    }

    for (i = 0; i < fieldCount; i++) {
        if (recordDescriptor[i].name == conditionAttribute) {
            rbfm_ScanIterator.condAttrIdx = i;
            break;
        }
    }
    if (i == fieldCount) {
        if (compOp == NO_OP) {
            rbfm_ScanIterator.condAttrIdx = -1;
        } else {
            return -7; // AttributeNotFoundException
        }
    }
    return 0;
}

void RBFM_ScanIterator::init(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                             const CompOp compOp, const void *value, const vector<string> &attrNames) {
    this->fileHandle = &fileHandle;
    this->recordDescriptor = recordDescriptor;
    this->compOp = compOp;
    this->value = value;
    this->attrNames = attrNames;

    this->pageNum = 0;
    this->slotNum = -1;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    this->fileHandle->readPage(this->pageNum, page);
    short slotTotal = rbfm.getPageSlotTotal(page);
    RID *id = (RID *) malloc(sizeof(RID));
    bool satisfied = false;
    while (true) {
        this->slotNum++;
        if (this->slotNum > slotTotal - 1) {
            this->pageNum++;
            if (this->pageNum > this->fileHandle->getNumberOfPages()-1) {
                free(id);
                free(page);
                return RBFM_EOF;
            } else {
                this->fileHandle->readPage(this->pageNum, page);
                slotTotal = rbfm.getPageSlotTotal(page);
                this->slotNum = 0;
            }
        }
        rid.pageNum = this->pageNum;
        rid.slotNum = this->slotNum;

        memcpy(id, &rid, sizeof(RID));
        short recordOffset, recordSize;
        this->fileHandle->readPage(this->pageNum, page);
        rbfm.locateRecord(*this->fileHandle, page, recordOffset, recordSize, id);
        if (id == nullptr) {
            id = (RID *) malloc(sizeof(RID));
            continue; // Deleted slot, skip.
        }

        short pagePtr = recordOffset - recordSize;
        int fieldCount = this->recordDescriptor.size();
        int nullFlagSize = rbfm.getNullFlagSize(fieldCount);

        if (this->condAttrIdx == -1) {
            satisfied = true;
        } else {
            short offset, prevOffset;
            rbfm.getAttributeOffset(page, pagePtr, fieldCount, nullFlagSize,
                                    this->condAttrIdx, offset, prevOffset);
            short sz = offset - prevOffset;
            if (sz > 0) {
                void *checkValue = malloc(sz);
                memcpy((char *) checkValue, (char *) page + pagePtr + prevOffset, sz);
                satisfied = this->checkSatisfied(checkValue);
                free(checkValue);
            } else {
                continue;
            }
        }

        if (satisfied) {
            int reNullFlagsSize = rbfm.getNullFlagSize(this->attrNames.size());
            unsigned char *reNullFlags = (unsigned char *) malloc(reNullFlagsSize);
            memset(reNullFlags, 0, reNullFlagsSize);
            short offset, prevOffset, sz;
            short dataPtr = reNullFlagsSize;
            for (int i = 0; i < this->attrNames.size(); i++) {
                rbfm.getAttributeOffset(page, pagePtr, fieldCount, nullFlagSize,
                                        this->attrIdx[i], offset, prevOffset);
                sz = offset - prevOffset;

                if (sz > 0) {
                    memcpy((char *) data + dataPtr, (char *) page + pagePtr + prevOffset, sz);
                    dataPtr += sz;
                } else {
                    int nullByte = i / 8;
                    int nullBit = i % 8;
                    reNullFlags[nullByte] |= (1 << (7 - nullBit));
                }
            }
            memcpy((char *) data, reNullFlags, reNullFlagsSize);
            free(reNullFlags);
            free(id);
            free(page);
            return 0;
        }
    }
}

bool RBFM_ScanIterator::checkSatisfied(void *checkValue) {
    int v1 = 0;
    int s1 = 0;
    float v2 = 0;
    float s2 = 0;
    string v3 = "";
    string s3 = "";
    Attribute attr = this->recordDescriptor[this->condAttrIdx];
    if (attr.type == TypeInt) {
        memcpy(&v1, (char *) checkValue, sizeof(int));
        memcpy(&s1, (char *) this->value, sizeof(int));
    } else if (attr.type == TypeReal) {
        memcpy(&v2, (char *) checkValue, sizeof(float));
        memcpy(&s2, (char *) this->value, sizeof(float));
    } else if (attr.type == TypeVarChar) {
        int checkLen;
        memcpy(&checkLen, (char *) checkValue, sizeof(int));
        int searchLen;
        memcpy(&searchLen, (char *) this->value, sizeof(int));
        char *vChar = (char *) malloc(checkLen);
        char *sChar = (char *) malloc(searchLen);
        memcpy(vChar, (char *) checkValue + sizeof(int), checkLen);
        memcpy(sChar, (char *) this->value + sizeof(int), searchLen);
        v3 = string(vChar, checkLen);
        s3 = string(sChar, searchLen);
        free(vChar);
        free(sChar);
    }
    bool satisfied = false;
    switch (compOp) {
        case EQ_OP:
            if (attr.type == TypeInt) {
                satisfied = v1 == s1;
            } else if (attr.type == TypeReal) {
                satisfied = v2 == s2;
            } else if (attr.type == TypeVarChar) {
                satisfied = v3 == s3;
            }
            break;
        case LT_OP:
            if (attr.type == TypeInt) {
                satisfied = v1 < s1;
            } else if (attr.type == TypeReal) {
                satisfied = v2 < s2;
            } else if (attr.type == TypeVarChar) {
                satisfied = v3 < s3;
            }
            break;
        case LE_OP:
            if (attr.type == TypeInt) {
                satisfied = v1 <= s1;
            } else if (attr.type == TypeReal) {
                satisfied = v2 <= s2;
            } else if (attr.type == TypeVarChar) {
                satisfied = v3 <= s3;
            }
            break;
        case GT_OP:
            if (attr.type == TypeInt) {
                satisfied = v1 > s1;
            } else if (attr.type == TypeReal) {
                satisfied = v2 > s2;
            } else if (attr.type == TypeVarChar) {
                satisfied = v3 > s3;
            }
            break;
        case GE_OP:
            if (attr.type == TypeInt) {
                satisfied = v1 >= s1;
            } else if (attr.type == TypeReal) {
                satisfied = v2 >= s2;
            } else if (attr.type == TypeVarChar) {
                satisfied = v3 >= s3;
            }
            break;
        case NE_OP:
            if (attr.type == TypeInt) {
                satisfied = v1 != s1;
            } else if (attr.type == TypeReal) {
                satisfied = v2 != s2;
            } else if (attr.type == TypeVarChar) {
                satisfied = v3 != s3;
            }
            break;
        case NO_OP:
            satisfied = true;
            break;
    }
    return satisfied;
}

RBFM_ScanIterator::RBFM_ScanIterator() {
    this->pageNum = 0;
    this->slotNum = -1;
}

RC RBFM_ScanIterator::close() {
    this->pageNum = 0;
    this->slotNum = -1;
    this->attrIdx.clear();
    return 0;
}



