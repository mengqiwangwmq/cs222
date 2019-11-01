#include <iostream>
#include "pfm.h"

PagedFileManager *PagedFileManager::_pf_manager = nullptr;

PagedFileManager &PagedFileManager::instance() {
    static PagedFileManager _pf_manager = PagedFileManager();
    return _pf_manager;
}

PagedFileManager::PagedFileManager() = default;

PagedFileManager::~PagedFileManager() { delete _pf_manager; }

PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

bool fileExist(const string &fileName) {
    FILE *file = fopen(fileName.c_str(), "r");
    if (file == nullptr) {
        return false;
    } else {
        if (fclose(file) == 0) {
            file = nullptr;
        } else {
            cerr << "File Close Fail" << endl;
        }
        return true;
    }
}

RC PagedFileManager::createFile(const string &fileName) {
    if (fileExist(fileName)) {
        return -2; //FileDuplicateException
    }
    FILE *file = fopen(fileName.c_str(), "w");
    if (file == nullptr) {
        return -3; //FileOpException
    }
    char *hiddenPage = (char *) malloc(PAGE_SIZE);
    memset(hiddenPage, 0, PAGE_SIZE);
    unsigned cache = 0;
    // Allocate space for readPageCounter
    memcpy(hiddenPage, &cache, sizeof(unsigned));
    // Allocate space for writePageCounter
    memcpy(hiddenPage + 1 * sizeof(unsigned), &cache, sizeof(unsigned));
    // Allocate space for appendPageCounter
    memcpy(hiddenPage + 2 * sizeof(unsigned), &cache, sizeof(unsigned));

    fwrite(hiddenPage, sizeof(char), PAGE_SIZE, file);
    free(hiddenPage);
    if (fclose(file) == 0) {
        file = nullptr;
    } else {
        cerr << "File Close Fail" << endl;
    }
    return 0;
}

RC PagedFileManager::destroyFile(const string &fileName) {
    if (!fileExist(fileName)) {
        return -1; //FileNotFoundException
    }
    remove(fileName.c_str());
    return 0;
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    if (!fileExist(fileName)) {
        return -1; //FileNotFoundException
    }
    return fileHandle.setFile(fileName);
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    return fileHandle.closeFile();
}

FileHandle::FileHandle() {
    this->readPageCounter = 0;
    this->writePageCounter = 0;
    this->appendPageCounter = 0;
    this->fpt = nullptr;
}

FileHandle::~FileHandle() {
    if (this->fpt == nullptr) {
    } else {
        this->updateCounterValues();
        if (fclose(this->fpt) == 0) {
            this->fpt = nullptr;
        } else {
            cerr << "File Close Failed" << endl;
        }
    }
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
    // Check if page requested exists or not
    if (pageNum >= this->getNumberOfPages()) {
        return -1;
    }

    // Skip hiddenPage
    fseek(this->fpt, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
    fread((char *) data, sizeof(char), PAGE_SIZE, this->fpt);

    // Update counters
    this->readPageCounter++;
    this->updateCounterValues();
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    // Check if page to be written exists
    if (pageNum >= this->getNumberOfPages()) {
        return -1;
    }
    fseek(this->fpt, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
    fwrite(data, sizeof(char), PAGE_SIZE, this->fpt);
    this->writePageCounter++;
    this->updateCounterValues();
    return 0;
}

RC FileHandle::appendPage(const void *data) {
    fseek(this->fpt, 0, SEEK_END);
    fwrite(data, sizeof(char), PAGE_SIZE, this->fpt);
    this->appendPageCounter++;
    this->updateCounterValues();
    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    fseek(this->fpt, 0L, SEEK_END);
    return ftell(this->fpt) / PAGE_SIZE - 1;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return 0;
}

RC FileHandle::updateCounterValues() {
    fseek(this->fpt, 0, SEEK_SET);
    fwrite(&this->readPageCounter, sizeof(unsigned), 1, this->fpt);
    fwrite(&this->writePageCounter, sizeof(unsigned), 1, this->fpt);
    fwrite(&this->appendPageCounter, sizeof(unsigned), 1, this->fpt);
    return 0;
}

RC FileHandle::setFile(const string &fileName) {
    if (!fileHandleEmpty()) {
        return -4; //FileHandleOccupiedException
    }
    this->fpt = fopen(fileName.c_str(), "r+");
    if (this->fpt == nullptr) {
        return -3;
    }
    this->readHiddenPage();
    return 0;
}

RC FileHandle::readHiddenPage() {
    fseek(this->fpt, 0, SEEK_SET);
    fread(&this->readPageCounter, sizeof(char), sizeof(unsigned), this->fpt);
    fread(&this->writePageCounter, sizeof(char), sizeof(unsigned), this->fpt);
    fread(&this->appendPageCounter, sizeof(char), sizeof(unsigned), this->fpt);
    return 0;
}

RC FileHandle::closeFile() {
    if (this->fpt == nullptr) {
        return -3;  // FileOpException
    }
    this->updateCounterValues();
    if (fclose(this->fpt) == 0) {
        this->fpt = nullptr;
    } else {
        cerr << "File Close Error" << endl;
    }
    return 0;
}

bool FileHandle::fileHandleEmpty() {
    return this->fpt == nullptr;
}
