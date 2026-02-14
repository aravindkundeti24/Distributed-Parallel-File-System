#include "pfs_cache.hpp"
#include <iostream>
#include <thread>
#include <algorithm>
#include <cstring>
#include "pfs_common/pfs_common.hpp"

constexpr size_t CACHE_SIZE = CLIENT_CACHE_BLOCKS;

ClientCache::ClientCache(std::vector<std::unique_ptr<pfsfile::FileServerService::Stub>> &stubs, struct pfs_execstat& global_execstat)
    : file_server_stubs(stubs), access_counter(0), global_execstat(global_execstat) {}
// Check if data is in cache
bool ClientCache::IsInCache(const std::string &filename, uint64_t offset)
{
    std::lock_guard<std::mutex> lock(cache_mutex);

    // Calculate block number based on offset and filename
    int block_number = offset / PFS_BLOCK_SIZE;
    std::string key = filename + "_" + std::to_string(block_number);

    // Check if the block exists in the cache
    return cache_map.find(key) != cache_map.end();
}

// Retrieve data from the cache if it exists
bool ClientCache::GetFromCache(const std::string &filename, uint64_t offset, std::vector<char> &buffer)
{
    std::lock_guard<std::mutex> lock(cache_mutex);

    // Calculate block number based on offset and filename
    int block_number = offset / PFS_BLOCK_SIZE;
    std::string key = filename + "_" + std::to_string(block_number);

    // Attempt to find the block in the cache
    auto it = cache_map.find(key);
     // std::cout << "Keys :: ";
    for (const auto& key : lru_list) {
        // std::cout << key << " ";
    }
    // std::cout << std::endl;
    if (it == cache_map.end())
    {
        // std::cout << "NOT IN CACHE" << key <<  std::endl;
        return false; // Cache miss
    }

    // Cache hit: Retrieve the data and update LRU status
    CacheBlock &block = it->second;
    buffer = block.data; // Copy data to buffer

    // Update LRU by moving the accessed item to the front
    block.last_accessed = ++access_counter;
    lru_list.remove(key);
    lru_list.push_front(key);

    return true; // Cache hit
}

// Cache data and update LRU
void ClientCache::CacheData(const std::string &filename, uint64_t offset, const std::vector<char> &data, bool is_dirty, int client_id, int stripe_width, int block_size)
{
    std::lock_guard<std::mutex> lock(cache_mutex);
    MetaData m = {client_id, stripe_width};
    metadata_map[filename] = m;
    // Calculate block number based on offset and filename
    int block_number = offset / PFS_BLOCK_SIZE;
    std::string key = filename + "_" + std::to_string(block_number);

    // Evict if cache size exceeds limit
    if (cache_map.size() >= CACHE_SIZE)
    {
        // std::cout << "cache size exceeded evicting" << std::endl;
        global_execstat.num_evictions++;
        EvictCacheEntry();
    }

    // Insert or update the cache
    CacheBlock block{data, is_dirty, ++access_counter};
    cache_map[key] = block;

    // Update LRU
    lru_list.remove(key); // Remove if already exists in the list
    lru_list.push_front(key);
}

// Evict the least recently used block
void ClientCache::EvictCacheEntry()
{
    if (lru_list.empty())
    {
        // // std::cout << "[DEBUG] LRU list is empty. No cache entries to evict." << std::endl;
        return;
    }

    std::string key_to_evict = lru_list.back();
    lru_list.pop_back();

    // // std::cout << "[DEBUG] Evicting cache entry with key: " << key_to_evict << std::endl;

    // Write back if the block is dirty
    CacheBlock &block = cache_map[key_to_evict];
    if (block.is_dirty)
    {
        // // std::cout << "[DEBUG] Cache block is dirty. Writing back block with key: " << key_to_evict << std::endl;
        WriteBackBlock(key_to_evict, block.data); // Hypothetical write-back function
        // // std::cout << "[DEBUG] Successfully wrote back block with key: " << key_to_evict << std::endl;
    }
    else
    {
        // // std::cout << "[DEBUG] Cache block is clean. No write-back needed for key: " << key_to_evict << std::endl;
    }
            global_execstat.num_invalidations++;

    cache_map.erase(key_to_evict);
    // // std::cout << "[DEBUG] Cache entry with key: " << key_to_evict << " has been evicted from cache." << std::endl;
}

// Invalidate a specific range in the cache
void ClientCache::InvalidateCacheRange(const std::string &filename, uint64_t start_offset, uint64_t length)
{
    std::lock_guard<std::mutex> lock(cache_mutex);

    // Calculate the range of blocks to invalidate
    for (uint64_t offset = start_offset; offset < start_offset + length; offset += PFS_BLOCK_SIZE)
    {
        int block_number = offset / PFS_BLOCK_SIZE;
        std::string key = filename + "_" + std::to_string(block_number);
        auto it = cache_map.find(key);
        if (it != cache_map.end())
        {
            lru_list.remove(key); // Remove from LRU tracking
            cache_map.erase(it);  // Invalidate the block
        }
    }
}

// Write back all dirty blocks and clear the cache (used for pfs_close)
void ClientCache::WriteBackAllAndClear()
{
    std::lock_guard<std::mutex> lock(cache_mutex);
    for (const auto &[key, block] : cache_map)
    {
        if (block.is_dirty)
        {
            WriteBackBlock(key, block.data); // Hypothetical write-back function
        }
    }
    cache_map.clear();
    lru_list.clear();
}

void ClientCache::WriteBackAllFileName(const std::string &filename)
{
    std::lock_guard<std::mutex> lock(cache_mutex);

    std::vector<std::thread> threads;

    // Iterate over the cache map to process the blocks
    for (auto &[key, block] : cache_map)
    {
        if (key.find(filename) == 0)
        { // Check if the key starts with the provided filename
            // If the block is dirty, we want to write it back
            if (block.is_dirty)
            {
                // Calculate the server index based on block number (you need to implement your logic for this)
                // Create a new thread to handle this write-back operation
                global_execstat.num_close_writebacks++;
                threads.push_back(std::thread([this, key, block_data = block.data]()
                                              {
                                                  WriteBackBlock(key, block_data); // Call the member function on the current instance
                                              }));
                global_execstat.num_writebacks--;
                // // std::cout << "[DEBUG] Successfully scheduled write-back for block " << key << std::endl;
            }
            else
            {
                // // std::cout << "[DEBUG] Block " << key << " for file: " << filename << " is clean. No write-back needed." << std::endl;
            }
        }
    }

    // Join all threads to ensure they all finish before proceeding
    for (auto &t : threads)
    {
        t.join();
    }

    // // std::cout << "[DEBUG] All dirty blocks for file " << filename << " have been written back." << std::endl;

    // Collect keys to remove after the first loop (to avoid modification during iteration)
    std::vector<std::string> keys_to_remove;

    // Now, iterate over the cache map again to collect the keys to remove
    for (const auto &[key, block] : cache_map)
    {
        if (key.find(filename) == 0)
        {
            keys_to_remove.push_back(key); // Store the key to remove later
        }
    }

    // Remove blocks from the cache and LRU list after the iteration
    for (const auto &key : keys_to_remove)
    {
         global_execstat.num_close_evictions++;
        lru_list.remove(key); // Remove from LRU list
        cache_map.erase(key); // Remove from cache map
        // // std::cout << "[DEBUG] Removed block " << key << " for file: " << filename << " from cache." << std::endl;
    }

    // // std::cout << "[DEBUG] Write-back and cache removal completed for file: " << filename << std::endl;
}

void ClientCache::WriteBackBlock(const std::string &key, const std::vector<char> &data)
{
    // Extract filename and block number from the key
    global_execstat.num_writebacks++;
    size_t delimiter_pos = key.find_last_of('_');
    if (delimiter_pos == std::string::npos)
    {
        std::cerr << "Invalid cache key format: " << key << std::endl;
        return;
    }
    std::string filename = key.substr(0, delimiter_pos);
    int client_id = metadata_map[filename].client_id;
    int stripe_width = metadata_map[filename].stripe_width;
    int block_size = PFS_BLOCK_SIZE;
    int stripe_blocks = STRIPE_BLOCKS;
    uint64_t block_number = std::stoull(key.substr(delimiter_pos + 1));

    // Determine the file server to send the request to based on stripe_width and block_size
    int server_index = (block_number / stripe_blocks) % stripe_width;

    // Prepare the write request to the file server
    WriteBlockRequest write_request;
    write_request.mutable_block()->set_filename(filename);
    write_request.mutable_block()->set_block_number(block_number);
    write_request.set_data(std::string(data.begin(), data.end()));
    write_request.set_offset_in_block(0);
    write_request.set_length(data.size());
    write_request.set_client_id(client_id);

    // Send the write request to the correct file server
    FileServerStatus file_response;
    grpc::ClientContext file_context;
    grpc::Status status = file_server_stubs[server_index]->WriteBlock(&file_context, write_request, &file_response);

    if (!status.ok() || !file_response.success())
    {
        std::cerr << "Failed to write back block to file server " << server_index
                  << " for block " << block_number << ": " << file_response.message() << std::endl;
    }
    else
    {
        // std::cout << "Successfully wrote back block " << block_number << " to file server " << server_index << std::endl;
    }
}

// In pfs_cache.cpp

void ClientCache::InvalidateBlock(const std::string &filename, uint64_t block_number)
{
    std::lock_guard<std::mutex> lock(cache_mutex);

    // Create the cache key for the block
    std::string key = filename + "_" + std::to_string(block_number);

    auto it = cache_map.find(key);
    if (it != cache_map.end())
    {
        // If the block is dirty, write it back to the file server
        CacheBlock &block = it->second;
        if (block.is_dirty)
        {
            WriteBackBlock(key, block.data);
        }
        global_execstat.num_invalidations++;
        // Remove the block from the cache map and LRU list
        lru_list.remove(key);
        cache_map.erase(it);
    }
}

void ClientCache::WriteBackBlockButCache(const std::string &filename, uint64_t block_number)
{
    std::lock_guard<std::mutex> lock(cache_mutex);

    // Create the cache key for the block
    std::string key = filename + "_" + std::to_string(block_number);
    auto it = cache_map.find(key);
    if (it != cache_map.end())
    {
        // If the block is dirty, write it back to the file server
        CacheBlock &block = it->second;
        if (block.is_dirty)
        {
            
            WriteBackBlock(key, block.data);
            block.is_dirty=false;
        }
    }
}
