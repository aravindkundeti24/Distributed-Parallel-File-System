#ifndef PFS_CACHE_HPP
#define PFS_CACHE_HPP
#include "pfs_api.hpp"

#include <unordered_map>
#include <list>
#include <vector>
#include <mutex>
#include <string>
#include "pfs_metaserver/pfs_metaserver_api.hpp"
#include "pfs_fileserver/pfs_fileserver_api.hpp"
#include "pfs_common/pfs_config.hpp"    // Include for constants like NUM_FILE_SERVERS
#include "pfs_proto/pfs_metaserver.grpc.pb.h"  // Include generated gRPC code for metaserver
#include "pfs_proto/pfs_fileserver.grpc.pb.h"  // Include generated gRPC code for fileserver
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <grpcpp/grpcpp.h>
using pfsmeta::MetadataService;
using pfsfile::FileServerService;
using pfsfile::FileServerStatus;
using pfsfile::WriteBlockRequest;
using pfsfile::ReadBlockRequest;
using pfsfile::ReadBlockResponse;

using pfsmeta::MetaStatus;
using pfsmeta::RegistrationRequest;
using pfsmeta::TokenRequest;
using pfsmeta::TokenResponse;
// using pfsmeta::RegistrationResponse;
using pfsmeta::Notification;

struct CacheBlock {
    std::vector<char> data;
    bool is_dirty = false;
    uint64_t last_accessed = 0; // For LRU tracking
};

struct MetaData{
    int client_id;
     int stripe_width;
};
// struct pfs_execstat;

class ClientCache {
public:
    ClientCache(std::vector<std::unique_ptr<pfsfile::FileServerService::Stub>>& file_server_stubs, struct pfs_execstat& global_execstat);

    // Check if a specific offset of a file is in the cache
    bool IsInCache(const std::string& filename, uint64_t offset);

    // Retrieve data from the cache if it exists
    bool GetFromCache(const std::string& filename, uint64_t offset, std::vector<char>& buffer);

    // Cache data and update LRU
    void CacheData(const std::string& filename, uint64_t offset, const std::vector<char>& data, bool is_dirty, int client_id, int stripe_width, int block_size);

    // Invalidate a specific range in the cache
    void InvalidateCacheRange(const std::string& filename, uint64_t start_offset, uint64_t length);

    // Write back all dirty blocks and clear the cache (used for pfs_close)
    void WriteBackAllAndClear();

    void InvalidateBlock(const std::string& filename, uint64_t block_number);
    void WriteBackBlock(const std::string& key, const std::vector<char>& data);
    void WriteBackBlockButCache(const std::string& filename, uint64_t block_number);
    void WriteBackAllFileName(const std::string& filename);

private:
    // Evict the least recently used block
    void EvictCacheEntry();


    std::unordered_map<std::string, CacheBlock> cache_map; // Cache storage
    std::list<std::string> lru_list;                       // LRU tracking list
    uint64_t access_counter;                               // Counter to track access order for LRU
    std::mutex cache_mutex;                                // Mutex for thread safety
    std::vector<std::unique_ptr<pfsfile::FileServerService::Stub>>& file_server_stubs;
    std::unordered_map<std::string, MetaData> metadata_map; 
    struct pfs_execstat& global_execstat;
};

#endif // PFS_CACHE_HPP
