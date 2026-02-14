#pragma once

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstdbool>
#include <vector>
#include <list>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <semaphore>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "pfs_common/pfs_config.hpp"
#include "pfs_common/pfs_common.hpp"

struct pfs_filerecipe {
    uint64_t block_size;
    int32_t stripe_width;
    int32_t stripe_blocks;

    // additional;
};

struct pfs_metadata {
    char filename[256];
    uint64_t file_size;
    time_t ctime;
    time_t mtime;
    struct pfs_filerecipe recipe;
        // additional;

};

struct pfs_execstat {
    long num_read_hits = 0;
    long num_write_hits = 0;
    long num_evictions = 0;
    long num_writebacks = 0;
    long num_invalidations = 0;
    long num_close_writebacks = 0;
    long num_close_evictions = 0;
};
enum TokenStatus {NOT_COMPLETED, RUNNING, COMPLETED};



// struct TokenInfo {
//     long start_offset;
//     size_t length;
//     bool is_write;
//     TokenStatus status;
//     std::condition_variable cv; // Added for signaling
//     std::mutex mtx;             // Mutex for synchronization
// };
struct TokenInfo {
    bool is_write;
    uint64_t start_offset;
    uint64_t length;
    int status;
    std::mutex mtx;
    std::condition_variable cv;

    // Custom copy constructor
    TokenInfo(const TokenInfo& other)
        : is_write(other.is_write),
          start_offset(other.start_offset),
          length(other.length),
          status(other.status) {
        // Do not copy mtx or cv
    }

    // Custom copy assignment operator
    TokenInfo& operator=(const TokenInfo& other) {
        if (this != &other) {
            is_write = other.is_write;
            start_offset = other.start_offset;
            length = other.length;
            status = other.status;
            // Do not copy mtx or cv
        }
        return *this;
    }

    // Explicit default constructor
    TokenInfo(bool is_write, uint64_t start_offset, uint64_t length, int status)
        : is_write(is_write), start_offset(start_offset), length(length), status(status) {}
};

int pfs_initialize();
int pfs_finish(int client_id);
int pfs_create(const char *filename, int stripe_width);
int pfs_open(const char *filename, int mode);
int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset);
int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset);
int pfs_close(int fd); // TODO
int pfs_delete(const char *filename); // TODO
int pfs_fstat(int fd, struct pfs_metadata *meta_data);
int pfs_execstat(struct pfs_execstat *execstat_data); // TODO
