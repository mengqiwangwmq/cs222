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
    if (file) {
        fclose(file);
        return true;
    } else {
        return false;
    }
}

RC PagedFileManager::createFile(const string &fileName) {
    if (fileExist(fileName)) {
        return -2; //FileDuplicateException
    }
    FILE *file = fopen(fileName.c_str(), "w");
    if (file) {
        char *hiddenPage = new char[PAGE_SIZE];

        unsigned cache = 0;
        // Allocate space for readPageCounter
        memcpy(hiddenPage + 0 * sizeof(unsigned), &cache, sizeof(unsigned));
        // Allocate space for writePageCounter
        memcpy(hiddenPage + 1 * sizeof(unsigned), &cache, sizeof(unsigned));
        // Allocate space for appendPageCounter
        memcpy(hiddenPage + 2 * sizeof(unsigned), &cache, sizeof(unsigned));

        fwrite(hiddenPage, sizeof(char), PAGE_SIZE, file);
        fclose(file);
        return 0;
    }
    return -3; //FileOpException
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
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    fpt = nullptr;
}

FileHandle::~FileHandle() = default;

RC FileHandle::readPage(PageNum pageNum, void *data) {
    // Check if page requested exists or not
    if (pageNum >= this->getNumberOfPages()) {
        return -1;
    }

    // Skip hiddenPage
    fseek(fpt, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
    fread((char *) data, sizeof(char), PAGE_SIZE, fpt);

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
    fseek(fpt, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
    fwrite(data, sizeof(char), PAGE_SIZE, fpt);
    this->writePageCounter++;
    this->updateCounterValues();
    return 0;
}

RC FileHandle::appendPage(const void *data) {
    fseek(fpt, 0, SEEK_END);
    fwrite(data, sizeof(char), PAGE_SIZE, fpt);
    this->appendPageCounter++;
    this->updateCounterValues();
    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    fseek(fpt, 0L, SEEK_END);
    return ftell(fpt) / PAGE_SIZE - 1;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return 0;
}

RC FileHandle::updateCounterValues() {
    fseek(fpt, 0, SEEK_SET);
    fwrite(&this->readPageCounter, sizeof(unsigned), 1, fpt);
    fwrite(&this->writePageCounter, sizeof(unsigned), 1, fpt);
    fwrite(&this->appendPageCounter, sizeof(unsigned), 1, fpt);
    return 0;
}

RC FileHandle::setFile(const string &fileName) {
    if (fileHandleOccupied()) {
        return -4; //FileHandleOccupiedException
    }
    fpt = fopen(fileName.c_str(), "r+");
    if (fpt) {
        this->readHiddenPage();
        return 0;
    }
    return -3;
}

RC FileHandle::readHiddenPage() {
    fseek(fpt, 0, SEEK_SET);
    fread(&this->readPageCounter, sizeof(char), sizeof(unsigned), fpt);
    fread(&this->writePageCounter, sizeof(char), sizeof(unsigned), fpt);
    fread(&this->appendPageCounter, sizeof(char), sizeof(unsigned), fpt);
    return 0;
}

RC FileHandle::closeFile() {
    if (fpt) {
        this->updateCounterValues();
        fclose(fpt);
        return 0;
    }
    return -3;  // FileOpException
}

bool FileHandle::fileHandleOccupied() {
    return fpt != nullptr;
}
