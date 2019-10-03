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
    FILE *file = std::fopen(fileName.c_str(), "r");
    if (file) {
        std::fclose(file);
        return true;
    } else {
        return false;
    }
}

RC PagedFileManager::createFile(const std::string &fileName) {
    if (fileExist(fileName)) {
        return -2; //FileDuplicateException
    }
    FILE *file = std::fopen(fileName.c_str(), "w");
    if (file) {
        char *hiddenPage = new char[PAGE_SIZE];

        unsigned cache = 0;
        // Allocate space for readPageCounter
        std::memcpy(hiddenPage + 0 * sizeof(unsigned), &cache, sizeof(unsigned));
        // Allocate space for writePageCounter
        std::memcpy(hiddenPage + 1 * sizeof(unsigned), &cache, sizeof(unsigned));
        // Allocate space for appendPageCounter
        std::memcpy(hiddenPage + 2 * sizeof(unsigned), &cache, sizeof(unsigned));

        std::fwrite(hiddenPage, sizeof(char), PAGE_SIZE, file);
        std::fclose(file);
        return 0;
    }
    return -3; //FileOpException
}

RC PagedFileManager::destroyFile(const std::string &fileName) {
    if (!fileExist(fileName)) {
        return -1; //FileNotFoundException
    }
    std::remove(fileName.c_str());
    return 0;
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
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

FileHandle::~FileHandle() {}

RC FileHandle::readPage(PageNum pageNum, void *data) {
    // Check if page requested exists or not
    if (pageNum >= this->getNumberOfPages()) {
        return -1;
    }

    // Skip hiddenPage
    std::fseek(fpt, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
    std::fread((char *) data, sizeof(char), PAGE_SIZE, fpt);

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
    std::fseek(fpt, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
    std::fwrite(data, sizeof(char), PAGE_SIZE, fpt);
    this->writePageCounter++;
    this->updateCounterValues();
    return 0;
}

RC FileHandle::appendPage(const void *data) {
    std::fseek(fpt, 0, SEEK_END);
    std::fwrite(data, sizeof(char), PAGE_SIZE, fpt);
    this->appendPageCounter++;
    this->updateCounterValues();
    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    std::fseek(fpt, 0L, SEEK_END);
    return std::ftell(fpt) / PAGE_SIZE - 1;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return 0;
}

RC FileHandle::updateCounterValues() {
    std::fseek(fpt, 0, SEEK_SET);
    std::fwrite(&this->readPageCounter, sizeof(unsigned), 1, fpt);
    std::fwrite(&this->writePageCounter, sizeof(unsigned), 1, fpt);
    std::fwrite(&this->appendPageCounter, sizeof(unsigned), 1, fpt);
    return 0;
}

RC FileHandle::setFile(const std::string &fileName) {
    if (fileHandleOccupied()) {
        return -4; //FileHandleOccupiedException
    }
    fpt = std::fopen(fileName.c_str(), "r+");
    if (fpt) {
        this->readHiddenPage();
        return 0;
    }
    return -3;
}

RC FileHandle::readHiddenPage() {
    std::fseek(fpt, 0, SEEK_SET);
    std::fread(&this->readPageCounter, sizeof(char), sizeof(unsigned), fpt);
    std::fread(&this->writePageCounter, sizeof(char), sizeof(unsigned), fpt);
    std::fread(&this->appendPageCounter, sizeof(char), sizeof(unsigned), fpt);
    return 0;
}

RC FileHandle::closeFile() {
    if (fpt) {
        this->updateCounterValues();
        std::fclose(fpt);
        return 0;
    }
    return -3;  // FileOpException
}

bool FileHandle::fileHandleOccupied() {
    return fpt != nullptr;
}
