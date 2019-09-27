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
        return -1;
    }
    std::ofstream fs(fileName);
    char *hiddenPage = new char[PAGE_SIZE];

    unsigned cache = 0;
    // Allocate space for readPageCounter
    memcpy(hiddenPage, &cache, sizeof(unsigned));
    // Allocate space for writePageCounter
    memcpy(hiddenPage + sizeof(unsigned), &cache, sizeof(unsigned));
    // Allocate space for appendPageCounter
    memcpy(hiddenPage + 2 * sizeof(unsigned), &cache, sizeof(unsigned));

    fs.write(hiddenPage, PAGE_SIZE);
    fs.close();
    return 0;
}

RC PagedFileManager::destroyFile(const std::string &fileName) {
    if (!fileExist(fileName)) {
        return -1;
    }
    remove(fileName.c_str());
    return 0;
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    return -1;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    return -1;
}

FileHandle::FileHandle() {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}

FileHandle::~FileHandle() = default;

RC FileHandle::readPage(PageNum pageNum, void *data) {
    return -1;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    return -1;
}

RC FileHandle::appendPage(const void *data) {
    return -1;
}

unsigned FileHandle::getNumberOfPages() {
    return -1;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    return -1;
}