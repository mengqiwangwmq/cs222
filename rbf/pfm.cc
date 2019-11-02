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
    struct stat buf;
    return stat(fileName.c_str(), &buf) != -1;
}

RC PagedFileManager::createFile(const string &fileName) {
    if (fileExist(fileName)) {
        return -2; //FileDuplicateException
    }
    fstream fs;
    fs.open(fileName, fstream::out);
    if (fs.fail()) {
        return -3; //FileOpException
    }
    char *hiddenPage = (char *) malloc(PAGE_SIZE);
    memset(hiddenPage, '\0', PAGE_SIZE);
    unsigned cache = 0;
    // Allocate space for readPageCounter
    memcpy(hiddenPage, &cache, sizeof(unsigned));
    // Allocate space for writePageCounter
    memcpy(hiddenPage + 1 * sizeof(unsigned), &cache, sizeof(unsigned));
    // Allocate space for appendPageCounter
    memcpy(hiddenPage + 2 * sizeof(unsigned), &cache, sizeof(unsigned));

    fs.write(hiddenPage, PAGE_SIZE);
    free(hiddenPage);
    fs.close();
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
    return fileHandle.openFile(fileName);
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    return fileHandle.closeFile();
}

FileHandle::FileHandle() {
    this->readPageCounter = 0;
    this->writePageCounter = 0;
    this->appendPageCounter = 0;
}

FileHandle::~FileHandle() {
    this->closeFile();
}

RC FileHandle::openFile(const string &fileName) {
    if (!fileExist(fileName)) {
        return -1; //FileNotFoundException
    }
    if (this->fs.is_open()) {
        return -4; //FileHandleOccupiedException
    }
    try {
        this->fs.open(fileName);
        this->readHiddenPage();
    } catch (fstream::failure &e) {
        return -3; //FileOpException
    }
    return 0;
}

RC FileHandle::closeFile() {
    if (!this->fs.is_open()) {
        return -1;  // FileNotFoundException
    }
    try {
        this->updateCounterValues();
        this->fs.close();
    } catch (fstream::failure &e) {
        return -3; // FileOpException
    }
    return 0;
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
    if (pageNum >= this->getNumberOfPages()) {
        return -1; // PageNotFoundException
    }
    this->fs.seekg((pageNum + 1) * PAGE_SIZE);
    this->fs.read((char *) data, PAGE_SIZE);
    this->readPageCounter++;
    this->updateCounterValues();
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    if (pageNum >= this->getNumberOfPages()) {
        return -1; //PageNotFoundException
    }
    this->fs.seekp((pageNum + 1) * PAGE_SIZE);
    this->fs.write((char *) data, PAGE_SIZE);
    this->writePageCounter++;
    this->updateCounterValues();
    return 0;
}

RC FileHandle::appendPage(const void *data) {
    this->fs.seekp(0, this->fs.end);
    this->fs.write((char *) data, PAGE_SIZE);
    this->appendPageCounter++;
    this->updateCounterValues();
    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    this->fs.seekg(0, this->fs.end);
    return fs.tellg() / PAGE_SIZE - 1;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return 0;
}

RC FileHandle::updateCounterValues() {
    this->fs.seekp(0);
    int length = 3 * sizeof(unsigned);
    char *cache = (char *) malloc(length);
    memset(cache, '\0', length);
    memcpy(cache, &this->readPageCounter, sizeof(unsigned));
    memcpy(cache + 1 * sizeof(unsigned), &this->writePageCounter, sizeof(unsigned));
    memcpy(cache + 2 * sizeof(unsigned), &this->appendPageCounter, sizeof(unsigned));
    this->fs.write(cache, length);
    free(cache);
    return 0;
}

RC FileHandle::readHiddenPage() {
    this->fs.seekg(0);
    int length = 3 * sizeof(unsigned);
    char *cache = (char *) malloc(length);
    memset(cache, '\0', length);
    this->fs.read(cache, length);
    memcpy(&this->readPageCounter, cache, sizeof(unsigned));
    memcpy(&this->writePageCounter, cache + 1 * sizeof(unsigned), sizeof(unsigned));
    memcpy(&this->appendPageCounter, cache + 2 * sizeof(unsigned), sizeof(unsigned));
    free(cache);
    return 0;
}
