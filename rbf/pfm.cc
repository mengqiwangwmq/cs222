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

bool fileExist(const std::string &fileName) {
    FILE *file = fopen(fileName.c_str(), "r");
    if (file) {
        fclose(file);
        return true;
    } else {
        return false;
    }
}

RC PagedFileManager::createFile(const std::string &fileName) {
    if (fileExist(fileName)) {
        return -2; //FileDuplicateException
    }
    FILE *file = fopen(fileName.c_str(), "w");
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

RC PagedFileManager::destroyFile(const std::string &fileName) {
    if (!fileExist(fileName)) {
        return -1; //FileNotFoundException
    }
    remove(fileName.c_str());
    return 0;
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    if (!fileExist(fileName)) {
        return -1; //FileNotFoundException
    }
    return fileHandle.setFile(fileName);
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
}

FileHandle::FileHandle() {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}

FileHandle::~FileHandle() {
    fclose(fpt);
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
    // Check if page requested exists or not
    if (pageNum >= this->getNumberOfPages()) {
        return -1;
    }

    // Skip hiddenPage
    fseek(fpt, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
    char *ptr = static_cast<char *>(data);
    fread(ptr, sizeof(char), PAGE_SIZE, fpt);

    // Update counters
    this->readPageCounter++;
    this->updateCounterValues();
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    return -1;
}

RC FileHandle::appendPage(const void *data) {
    return -1;
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
    return -1;
}

RC FileHandle::setFile(const std::string &fileName) {
    if (fileHandleOccupied()) {
        return -3; //FilePointerOccupiedException
    }
    fpt = fopen(fileName.c_str(), "r+");
    void *cache = malloc(PAGE_SIZE);
    this->readPage(-1, cache);
    memcpy(&this->readPageCounter, (char *)cache, sizeof(unsigned));
    memcpy(&this->writePageCounter, (char *)cache + sizeof(unsigned), sizeof(unsigned));
    memcpy(&this->appendPageCounter, (char *)cache + 2 * sizeof(unsigned), sizeof(unsigned));
    return 0;
}

bool FileHandle::fileHandleOccupied() {
    return fpt != nullptr;
}