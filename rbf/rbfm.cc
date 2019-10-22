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
    unsigned numberOfPages = fileHandle.getNumberOfPages();
    bool needNewPage = true;
    unsigned currentPID = 0;
    if (numberOfPages != 0) {
        currentPID = numberOfPages - 1;
        fileHandle.readPage(currentPID, page);
        needNewPage = false;
        if (this->countRemainSpace(page, this->getPageFreeSpace(page),
                                   recordSize, true) < 0) {
            for (int i = 0; i < numberOfPages - 1; i++) {
                fileHandle.readPage(i, page);
                if (this->countRemainSpace(page, this->getPageFreeSpace(page),
                                           recordSize, true) >= 0) {
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
    short insertPtr = this->getInsertPtr(page);
    this->copyRecord(page, insertPtr, recordDescriptor.size(),
                     data, offsetTable, recordSize);
    if (rid.slotNum == slotTotal) {
        this->setPageSlotTotal(page, slotTotal + 1);
    }
    this->setPageFreeSpace(page, this->countRemainSpace(page, this->getPageFreeSpace(page),
                                                        recordSize, true));
    this->setRecordOffset(page, insertPtr + recordSize, rid.slotNum);
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
    RID *id = (RID *) malloc(sizeof(RID));
    memcpy(id, &rid, sizeof(RID));
    short recordOffset, recordSize;
    this->locateRecord(fileHandle, page, &recordOffset, &recordSize, id);
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

void RecordBasedFileManager::locateRecord(FileHandle &fileHandle, void *page,
                                          short *recordOffset, short *recordSize, RID* &id) {
    *recordOffset = this->getRecordOffset(page, id->slotNum);
    if (*recordOffset == -1) {
        id = nullptr;
        return;
    }
    *recordSize = this->getRecordSize(page, id->slotNum);
    short pagePtrSize = sizeof(unsigned) + sizeof(short);
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
            id = nullptr;
            return;
        }
        *recordSize = this->getRecordSize(page, id->slotNum);
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
        return -1;
    }
    short recordOffset, recordSize;
    RID *id = (RID *) malloc(sizeof(RID));
    memcpy(id, &rid, sizeof(RID));
    this->locateRecord(fileHandle, page, &recordOffset, &recordSize, id);
    if (id == nullptr) {
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
    fileHandle.readPage(rid.pageNum, page);
    short slotTotal = this->getPageSlotTotal(page);
    if (rid.slotNum >= slotTotal) {
        return -1;
    }
    short recordOffset, recordSize;
    RID *id = (RID *) malloc(sizeof(RID));
    memcpy(id, &rid, sizeof(RID));
    this->locateRecord(fileHandle, page, &recordOffset, &recordSize, id);
    if (id == nullptr) {
        return -5;
    }
    int fieldCount = recordDescriptor.size();
    char *offsetTable = (char *) malloc(fieldCount * sizeof(short));
    short newSize = this->parseRecord(recordDescriptor, data, offsetTable);
    if (this->countRemainSpace(page, this->getPageFreeSpace(page) + recordSize, newSize, false) >= 0) {
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
        this->setRecordOffset(page, recordOffset + distance, id->slotNum);
        this->setRecordSize(page, newSize, id->slotNum);
    } else {
        char *cache = (char *) malloc(PAGE_SIZE);
        unsigned pageNum = 0;
        for (pageNum = 0; pageNum < numberOfPages; pageNum++) {
            if (pageNum == id->pageNum) continue;
            fileHandle.readPage(pageNum, cache);
            if (this->countRemainSpace(cache, this->getPageFreeSpace(cache), newSize, true) >= 0) {
                break;
            }
        }
        if (pageNum == numberOfPages) {
            this->setPageFreeSpace(cache, PAGE_SIZE - 2 * sizeof(short));
            this->setPageSlotTotal(cache, 0);
            fileHandle.appendPage(cache);
            numberOfPages = fileHandle.getNumberOfPages();
            pageNum = numberOfPages - 1;
            fileHandle.readPage(pageNum, cache);
        }
        short slotNum = this->findFreeSlot(cache);
        short slotTotal = this->getPageSlotTotal(cache);
        short insertPtr = this->getInsertPtr(cache);
        this->copyRecord(cache, insertPtr, fieldCount,
                         data, offsetTable, newSize);
        if (slotNum == slotTotal) {
            this->setPageSlotTotal(cache, slotTotal + 1);
        }
        this->setPageFreeSpace(cache, this->countRemainSpace(cache, this->getPageFreeSpace(cache),
                                                             newSize, true));
        this->setRecordOffset(cache, insertPtr + newSize, slotNum);
        this->setRecordSize(cache, newSize, slotNum);
        fileHandle.writePage(pageNum, cache);
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
    this->locateRecord(fileHandle, page, &recordOffset, &recordSize, id);
    if (id == nullptr) {
        return -5;
    }
    free(id);
    int nullFlagSize = this->getNullFlagSize(fieldCount);
    short pagePtr = recordOffset - recordSize;
    short offset;
    memcpy(&offset, (char *) page + pagePtr + nullFlagSize + i * sizeof(short), sizeof(short));
    short prevOffset = 0;
    if (i != 0) {
        memcpy(&prevOffset, (char *) page + pagePtr + nullFlagSize + (i - 1) * sizeof(short), sizeof(short));
    }
    memcpy(data, (char *) page + pagePtr + prevOffset, offset - prevOffset);
    return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute, const CompOp compOp, const void *value,
                                const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    rbfm_ScanIterator.fileHandle = fileHandle;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = value;
    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.attributePositions = new int[attributeNames.size()];
    rbfm_ScanIterator.attributeNames = attributeNames;

    // Get positions of attributes that are projected
    for(int i = 0; i < attributeNames.size(); i ++) {
        int j;
        for(j = 0; j < recordDescriptor.size(); j ++) {
            if(recordDescriptor[j].name == attributeNames[i]) {
                rbfm_ScanIterator.attributePositions[i] = j;
                break;
            }
        }
        if (j == recordDescriptor.size())
        {
            printf("[rbfm::scan] Cannot find attributes Id\n");
            // Cannot find attributes error
            return -7;
        }
    }

    // Get conditionalAttribute position
    cout<<(conditionAttribute.empty() == true)<<endl;
    if(conditionAttribute.empty()) {
        rbfm_ScanIterator.conditionAttributePosition = -1;
        return 0;
    }
    int i;
    for(i = 0 ; i < recordDescriptor.size(); i ++) {
        if(recordDescriptor[i].name == conditionAttribute) {
            rbfm_ScanIterator.conditionAttributePosition = i;
            break;
        }
    }
    if(i == recordDescriptor.size()) {
        printf("[rbfm::scan] Cannot find attributes Id\n");
        return -7;
    }
    return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(cPage, page);
    short slotNum = rbfm.getPageSlotTotal(page);
    while(true) {
        cSlot ++;
        if(cSlot > slotNum -1) {
            cPage ++;
            cSlot = 0;
            if(cPage > fileHandle.getNumberOfPages() - 1) {
                free(page);
                return RBFM_EOF;
            } else {
                // realloc(page, PAGE_SIZE);
                free(page);
                page = (char *)malloc(PAGE_SIZE);
                fileHandle.readPage(cPage, page);
                slotNum = rbfm.getPageSlotTotal(page);
            }
        }

        short offset;
        short startOffset;
        short recordLength;
        bool valid = true;
        bool satisfied = true;
        offset = rbfm.getRecordOffset(page, cSlot);

        if(offset == -1) {
            valid = false;
        }

        rid.pageNum = cPage;
        rid.slotNum = cSlot;
        cout<<"cPage : "<<cPage<<" "<<"cSlot "<<cSlot<<endl;

        if(valid) {
            recordLength = rbfm.getRecordSize(page, cSlot);
            while(recordLength == -1) {
                short tPage;
                short tSlot;
                startOffset = offset - sizeof(int) - sizeof(short);
                memcpy(&tPage, (char *)page + startOffset, sizeof(int));
                memcpy(&tSlot, (char *)page + startOffset + sizeof(int), sizeof(short));
                fileHandle.readPage(tPage, page);
                offset = rbfm.getRecordOffset(page, tSlot);
                recordLength = rbfm.getRecordSize(page, tSlot);
            }
            startOffset = offset - recordLength;

            // Get attribute offset
            int nullFieldsIndicatorSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
            auto *nullFlags = (unsigned char *) std::malloc(nullFieldsIndicatorSize);
            std::memcpy(nullFlags, (char *) page + startOffset, nullFieldsIndicatorSize);
            int nullFieldsCounter = 0;
            int offsetTableIndex = 0;
            bool conditionalNullBit;
            conditionalNullBit = nullFlags[conditionAttributePosition/8] & (unsigned) 1 << (unsigned) (7 - conditionAttributePosition%8);
            bool nullBit;
            if(conditionAttributePosition == 0) {
                offsetTableIndex = -1;

            } else if(conditionAttributePosition > 0) {
                for(int i = conditionAttributePosition - 1; i >= 0; i --) {
                    nullBit = nullFlags[i/8] & (unsigned) 1 << (unsigned) (7 - i%8);
                    if(!nullBit) {
                        offsetTableIndex = i;
                        break;
                    }
                }
            }
            cout<<"conditionAttributePosition is "<<conditionAttributePosition<<endl;
            cout<<"nullFieldsCounter"<<nullFieldsCounter<<endl;
            cout<<"startoffset "<<startOffset<<endl;


            // int testData;
            // memcpy(&testData, (char *)page + startOffset + 11, 4);
            // cout<<testData<<endl;

            short attributeOffset;

            // cout<<"offsetTableIndex "<<offsetTableIndex<<endl;
            // cout<<(offsetTableIndex == -1)<<endl;
            if(offsetTableIndex == -1) {
                attributeOffset = nullFieldsIndicatorSize + recordDescriptor.size() * sizeof(short);
            } else {
                memcpy(&attributeOffset, (char *)page + startOffset + nullFieldsIndicatorSize + offsetTableIndex* sizeof(short), sizeof(short));
            }
            cout<<attributeOffset<<endl;

            if(compOp == NO_OP) {
                satisfied = true;
            } else if(!conditionalNullBit) {
                int fieldOffset = startOffset + attributeOffset;
                // cout<<fieldOffset<<endl;
                if(recordDescriptor[conditionAttributePosition].type == TypeInt) {
                    int valueToCheck;
                    memcpy(&valueToCheck, (char *)page + fieldOffset, sizeof(int));
                    cout<<"table-id "<<valueToCheck<<endl;
                    checkSatisfied(satisfied, compOp, &valueToCheck, value, -1, 1);
                } else if(recordDescriptor[conditionAttributePosition].type == TypeReal) {
                    float valueToCheck;
                    memcpy(&valueToCheck, (char *)page + fieldOffset, sizeof(int));
                    checkSatisfied(satisfied, compOp, &valueToCheck, value, -1, 2);
                } else if(recordDescriptor[conditionAttributePosition].type = TypeVarChar) {
                    int length;
                    memcpy(&length, (char *)page + fieldOffset, sizeof(int));
                    // cout<<length<<endl;
                    char *valueToCheck = (char *)malloc(length);
                    memcpy(valueToCheck, (char *)page + fieldOffset + sizeof(int), length);
                    int valueToSearchLength;
                    memcpy(&valueToSearchLength, (char *)value, sizeof(int));
                    // cout<<valueToSearchLength<<endl;
                    char *valueToSearch = (char *)malloc(valueToSearchLength);
                    memcpy(valueToSearch, (char *)value + sizeof(int), valueToSearchLength);

                    if(length != valueToSearchLength) {
                        satisfied = false;
                        continue;
                    }

                    checkSatisfied(satisfied, compOp, valueToCheck, valueToSearch, length, 3);

                    free(valueToCheck);
                    free(valueToSearch);
                }


            }

            cout<<"satisfied "<<satisfied<<endl;
            if(satisfied) {
                int nullFlagsSize = ceil((double)attributeNames.size() / CHAR_BIT);
                auto *returnedNullFlags = (char *)malloc(nullFlagsSize);
                memset(returnedNullFlags, 0, nullFlagsSize);
                int pageOffset;
                int dataOffset = nullFlagsSize;
                for(int i = 0; i < attributeNames.size(); i ++) {
                    int attributePosition = attributePositions[i];
                    short off;
                    int ind;
                    if(attributePosition == 0) {
                        ind = -1;
                    } else if(attributePosition > 0) {
                        for(int i = attributePosition-1; i >=0; i --) {
                            bool null = nullFlags[i/8] & (unsigned) 1 << (unsigned) (7 - i%8);
                            cout<<null<<endl;
                            if(null == 0) {
                                ind = i;
                                cout<<ind<<endl;
                                break;
                            }
                        }
                    }
                    if(ind == -1) {
                        off = nullFieldsIndicatorSize + recordDescriptor.size() * sizeof(short);
                    } else {
                        memcpy(&off, (char *)page + startOffset + nullFieldsIndicatorSize + ind * sizeof(short), sizeof(short));
                    }
                    cout<<"attribute offset "<<off<<endl;
                    pageOffset = startOffset + off;

                    bool nullBit = nullFlags[attributePosition / CHAR_BIT] & (1 << (7 - attributePosition % CHAR_BIT));
                    if(nullBit) {
                        returnedNullFlags[i/CHAR_BIT] |= (1 << (7 - i % CHAR_BIT));
                    } else {
                        if(recordDescriptor[attributePositions[i]].type == TypeInt) {
                            memcpy((char *)data + dataOffset, (char *)page + pageOffset, sizeof(int));
                            pageOffset += sizeof(int);
                            dataOffset += sizeof(int);
                        } else if(recordDescriptor[attributePositions[i]].type == TypeReal) {
                            memcpy((char *)data + dataOffset, (char *)page + pageOffset, sizeof(int));
                            pageOffset += sizeof(int);
                            dataOffset += sizeof(int);
                        } else if(recordDescriptor[attributePositions[i]].type == TypeVarChar) {
                            int length;
                            memcpy(&length, (char *)page + pageOffset, sizeof(int));
                            memcpy((char *)data + dataOffset, (char *)page + pageOffset, sizeof(int));
                            pageOffset += sizeof(int);
                            dataOffset += sizeof(int);
                            memcpy((char *)data + dataOffset, (char *)page + pageOffset, length);
                            pageOffset += length;
                            dataOffset += length;
                        }
                    }
                }
                memcpy((char *)data, (char *)returnedNullFlags, nullFlagsSize);
                // rbfm.printRecord(recordDescriptor, data);
                free(returnedNullFlags);
                return 0;
            }
            free(nullFlags);
        }
    }
}

bool RBFM_ScanIterator::checkSatisfied(bool &satisfied, CompOp &comOp, void *valueToCheck, const void *searchValue, int length, int type) {
    int v1;
    int s1;
    float v2;
    float s2;
    string v3;
    string s3;
    if(type == 1) {
        memcpy(&v1, valueToCheck, sizeof(int));
        memcpy(&s1, searchValue, sizeof(int));
    } else if(type == 2) {
        memcpy(&v2, valueToCheck, sizeof(int));
        memcpy(&s2, valueToCheck, sizeof(int));
    }
    switch (compOp)
    {
        case EQ_OP:
            if(length == -1) {
                if(type == 1) {
                    satisfied = v1 == s1;
                } else if(type == 2) {
                    satisfied = v2 == s2;
                }

            } else {
                satisfied = (memcmp(valueToCheck, searchValue, length) == 0);
                // cout<<satisfied<<endl;
            }
            break;
        case LT_OP:
            satisfied = valueToCheck < searchValue;
            break;
        case LE_OP:
            satisfied = valueToCheck <= searchValue;
            break;
        case GT_OP:
            satisfied = valueToCheck > searchValue;
            break;
        case GE_OP:
            satisfied = valueToCheck >= searchValue;
            break;
        case NE_OP:
            satisfied = valueToCheck != searchValue;
            break;
        case NO_OP:
            satisfied = true;
            break;
    }
    return satisfied;
}

RBFM_ScanIterator::RBFM_ScanIterator()
{
    cPage = 0;
    cSlot = -1;
}

RC RBFM_ScanIterator::close()
{
    cPage = 0;
    cSlot = -1;
    return 0;
}



