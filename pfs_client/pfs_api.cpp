
#include "pfs_api.hpp"

#include "pfs_cache.hpp"
#include "pfs_metaserver/pfs_metaserver_api.hpp"
#include "pfs_fileserver/pfs_fileserver_api.hpp"
#include "pfs_common/pfs_config.hpp"          // Include for constants like NUM_FILE_SERVERS
#include "pfs_proto/pfs_metaserver.grpc.pb.h" // Include generated gRPC code for metaserver
#include "pfs_proto/pfs_fileserver.grpc.pb.h" // Include generated gRPC code for fileserver
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <grpcpp/grpcpp.h>
using pfsfile::FileServerService;
using pfsfile::FileServerStatus;
using pfsfile::ReadBlockRequest;
using pfsfile::ReadBlockResponse;
using pfsfile::WriteBlockRequest;
using pfsmeta::MetadataService;

using pfsmeta::InvalidateBlockRequest;
using pfsmeta::InvalidateBlockResponse;
using pfsmeta::MetaStatus;
using pfsmeta::RegistrationRequest;
using pfsmeta::TokenRequest;
using pfsmeta::TokenResponse;
// using pfsmeta::RegistrationResponse;
using pfsmeta::Notification;
// Assume that these global variables or objects will be defined elsewhere
std::unique_ptr<MetadataService::Stub> metadata_stub;
std::vector<std::unique_ptr<FileServerService::Stub>> file_server_stubs;
std::thread listener_thread;
std::shared_ptr<grpc::ClientContext> client_context;

std::shared_ptr<grpc::ClientReaderWriter<pfsmeta::RegistrationRequest, pfsmeta::Notification>> stream;
std::unordered_map<int, std::vector<TokenInfo>> token_map;
std::mutex token_map_mutex;
std::unordered_map<int, std::string> fd_map;
std::mutex fd_map_mutex;
std::unordered_map<std::string, int> name_fd_map;
// Cache to store file metadata
std::unordered_map<std::string, pfs_metadata> metadata_cache;
std::mutex metadata_cache_mutex;
int client_id;
std::unique_ptr<ClientCache> client_cache; // Global cache instance
struct pfs_execstat global_execstat;
std::mutex global_execstat_mutex;

struct BlockInfo
{
    int block_number;      // Block number
    int server_index;      // File server index
    uint64_t start_offset; // Start offset within the block
    uint64_t length;       // Length of the block
};
std::vector<BlockInfo> SplitIntoBlocks(const std::string &filename, uint64_t start_offset, uint64_t length)
{
    std::lock_guard<std::mutex> lock(metadata_cache_mutex);
    std::vector<BlockInfo> result;

    // Validate input parameters
    if (PFS_BLOCK_SIZE == 0 || STRIPE_BLOCKS == 0)
    {
        std::cerr << "Error: Invalid block size configuration" << std::endl;
        return result;
    }

    // Retrieve metadata for the file
    if (metadata_cache.find(filename) == metadata_cache.end())
    {
        std::cerr << "Error: File metadata not found for " << filename << std::endl;
        return result;
    }
    const pfs_filerecipe &recipe = metadata_cache[filename].recipe;

    // Validate recipe
    if (recipe.stripe_width == 0)
    {
        std::cerr << "Error: Invalid stripe width" << std::endl;
        return result;
    }

    uint64_t current_offset = start_offset;
    uint64_t end_offset = start_offset + length - 1;

    while (current_offset <= end_offset)
    {
        int block_number = static_cast<int>(current_offset / PFS_BLOCK_SIZE);
        int logical_block_index = block_number / STRIPE_BLOCKS;
        int server_index = logical_block_index % recipe.stripe_width;
        uint64_t block_offset = current_offset % PFS_BLOCK_SIZE;
        uint64_t bytes_to_process = std::min(end_offset - current_offset + 1, 
                                           recipe.block_size - block_offset);

        BlockInfo block_info = {
            block_number,
            server_index,
            current_offset,
            bytes_to_process
        };
        result.push_back(block_info);

        current_offset += bytes_to_process;
    }

    return result;
}

bool RemoveOverlappingTokens(std::vector<TokenInfo> &tokens, const TokenInfo &new_token)
{
        std::lock_guard<std::mutex> lock(token_map_mutex);

    // Logic for removing overlapping tokens
    for (auto it = tokens.begin(); it != tokens.end(); ++it)
    {
        if (it->start_offset == new_token.start_offset && it->length == new_token.length && it->is_write == new_token.is_write)
        {
            tokens.erase(it); // Remove the token if it overlaps
            return true;
        }
    }
    return false;
}

void HandleNotification(const pfsmeta::Notification &notification)
{
    if (notification.action() == "revoke")
    {
        // std::cout << "sleeping in revoke" << std::endl;
        // sleep(60);
        std::string filename = notification.filename();
        uint64_t revoke_start = notification.start_offset();
        uint64_t revoke_length = notification.length();
        uint64_t revoke_end = revoke_start + revoke_length - 1;

        // std::cout << "[DEBUG] HandleNotification: Received revoke notification for "<< "filename: " << filename<< " for range [" << revoke_start << ", " << revoke_end << ")" << std::endl;

        std::unique_lock<std::mutex> lock(token_map_mutex);

        // Get the file descriptor and corresponding tokens
        int file_fd = name_fd_map[filename];
        auto &tokens = token_map[file_fd];

        for (size_t it = 0; it < tokens.size(); ++it)
        {
            TokenInfo &token = tokens[it];
            uint64_t token_start = token.start_offset;
            uint64_t token_end = token_start + token.length - 1;

            // std::cout << "[DEBUG] Checking token: Start = " << token_start<< ", End = " << token_end << ", Status = " << (token.status == RUNNING ? "RUNNING" : "COMPLETED")<< std::endl;

            // Check for overlap with the revocation range
            bool overlaps = !(revoke_end < token_start || token_end < revoke_start);

            if (overlaps)
            {
                // std::cout << "[DEBUG] Token overlaps with revoke range ["<< revoke_start << ", " << revoke_end << ")" << std::endl;

                if (token.status == RUNNING)
                {
                    // std::cout << "[DEBUG] Token is RUNNING. Waiting for completion...\n";

                    // Lock the specific token's mutex and wait
                    lock.unlock();
                    {
                        std::unique_lock<std::mutex> token_lock(token.mtx);
                    // std::cout << "[DEBUG] waiting on token " << "Start = " << token.start_offset << ", Length = " << token.length << ", Is Write = " << token.is_write << ", Status = " << token.status << std::endl;

                        token.cv.wait(token_lock, [&]()
                                      { return token.status == COMPLETED; });
                    }
                    lock.lock();

                    // std::cout << "[DEBUG] Token completed. Proceeding with revocation/adjustment.\n";
                }
                if (token.status == COMPLETED)
                {
                    // std::cout << "[DEBUG] Splitting COMPLETED token due to overlap" << std::endl;

                    // Handle split cases
                    std::vector<TokenInfo> new_tokens;

                    if (revoke_start > token_start)
                    {
                        // Add token for range before the revoked part
                        TokenInfo pre_revoke_token(token.is_write, token_start, revoke_start - token_start, COMPLETED);
                        new_tokens.push_back(pre_revoke_token);

                        // std::cout << "[DEBUG] Created pre-revocation token: " << "Start = " << pre_revoke_token.start_offset << ", Length = " << pre_revoke_token.length << ", Is Write = " << pre_revoke_token.is_write << ", Status = " << pre_revoke_token.status << std::endl;
                    }

                    if (revoke_end < token_end)
                    {
                        // Add token for range after the revoked part
                        TokenInfo post_revoke_token(token.is_write, revoke_end + 1, token_end - revoke_end, COMPLETED);
                        new_tokens.push_back(post_revoke_token);

                        // std::cout << "[DEBUG] Created post-revocation token: " << "Start = " << post_revoke_token.start_offset << ", Length = " << post_revoke_token.length << ", Is Write = " << post_revoke_token.is_write << ", Status = " << post_revoke_token.status << std::endl;
                    }

                    // Invalidate the cache for the revoked range
                    // std::cout << "[DEBUG] Invalidating cache for revoked range [" << revoke_start << ", " << revoke_end << ")" << std::endl;
                    // InvalidateCacheRange(filename, revoke_start, revoke_length); // Hypothetical function

                    // Replace current token with new tokens
                    tokens.erase(tokens.begin() + it); // Remove the old token
                    tokens.insert(tokens.end(), new_tokens.begin(), new_tokens.end());
                    // std::cout << "[DEBUG] Replaced token with new non-overlapping tokens." << std::endl;

                    --it; // Adjust iterator to avoid skipping
                }
            }
        }

        // If no tokens remain for this file descriptor, remove it from token_map
        if (tokens.empty())
        {
            token_map.erase(file_fd);
            // std::cout << "[DEBUG] All tokens for file descriptor removed from token_map." << std::endl;
        }
    }
    else if (notification.action() == "invalidate")
    {
        std::string filename = notification.filename();
        int block_number = notification.block_number();

        // // std::cout << "[DEBUG] HandleNotification: Received invalidate notification for block "<< block_number << " of file " << filename << std::endl;

        // Invalidate the cache block
        
        client_cache->InvalidateBlock(filename, block_number);
        // // std::cout << "[DEBUG] Block " << block_number << " invalidated for file " << filename << std::endl;
    }
    else if (notification.action() == "writeback")
    {
        std::string filename = notification.filename();
        int block_number = notification.block_number();
        // // std::cout << "[DEBUG] HandleNotification: Received invalidate notification for block " << block_number << " of file " << filename << std::endl;
        
        // write back the cache but we can cache it
        client_cache->WriteBackBlockButCache(filename, block_number);
    }
    // Handle other actions as needed
}

// Declare the context as a shared pointer accessible from both the listener thread and pfs_finish

void StartClientRegistration(int client_id, const std::string &client_hostname)
{
    // Start a new thread to handle the registration and incoming notifications
    listener_thread = std::thread([client_id, client_hostname]()
                                  {
        // Use the shared client_context
        client_context = std::make_shared<grpc::ClientContext>();

        // Create the stream
        stream = metadata_stub->ClientRegister(client_context.get());

        // Send initial registration request
        pfsmeta::RegistrationRequest request;
        request.set_client_id(client_id);
        request.set_client_hostname(client_hostname);
                request.set_is_shutdown(false);  // Not a shutdown message

        if (!stream->Write(request)) {
            std::cerr << "Failed to register client." << std::endl;
            return;
        }

        // Read acknowledgment
        pfsmeta::Notification notification;
        if (!stream->Read(&notification)) {
            std::cerr << "Failed to receive acknowledgment." << std::endl;
            return;
        }
        // std::cout << "Server acknowledgment: " << notification.message() << std::endl;

        // Main loop to receive notifications
        while (stream->Read(&notification)) {
            // std::cout << "Received notification: Action = " << notification.action()
            //           << ", Filename = " << notification.filename()
            //           << ", Message = " << notification.message() << std::endl;

            // Handle the notification (e.g., invalidate cache)
            HandleNotification(notification);
            std::string requested_by = notification.requested_by();

            pfsmeta::RegistrationRequest request;
            request.set_client_id(client_id);
            request.set_client_hostname(client_hostname);
                    request.set_is_shutdown(false);  // Not a shutdown message
            request.set_requested_id(requested_by);
            request.set_message("ack");
            // std::cout << "Sending ack to metaserver that we handled the notifcaiton" << std::endl;
            if (!stream->Write(request)) {
                std::cerr << "Failed to register client." << std::endl;
                return;
            }

        }

        // After stream->Read() returns false, check the status
        grpc::Status status = stream->Finish();
        if (!status.ok()) {
            std::cerr << "Client registration stream closed with error: " << status.error_message() << std::endl;
        } else {
            // std::cout << "Server closed the stream normally." << std::endl;
        } });
}

int pfs_initialize()
{
    // Step 1: Read pfs_list.txt
    client_cache = std::make_unique<ClientCache>(file_server_stubs, global_execstat);

    std::ifstream pfs_list("../pfs_list.txt");
    if (!pfs_list.is_open())
    {
        std::cerr << "pfs_initialize: Unable to open pfs_list.txt file." << std::endl;
        return -1;
    }

    // Step 2: Parse server addresses
    std::string line;
    std::vector<std::string> server_addresses;
    while (std::getline(pfs_list, line))
    {
        server_addresses.push_back(line);
    }
    pfs_list.close();

    if (server_addresses.size() != NUM_FILE_SERVERS + 1)
    {
        std::cerr << "pfs_initialize: Expected " << NUM_FILE_SERVERS + 1
                  << " servers but found " << server_addresses.size() << "." << std::endl;
        return -1;
    }

    // Step 3: Connect to the metadata server
    std::string metadata_address = server_addresses[0];
    metadata_stub = MetadataService::NewStub(grpc::CreateChannel(
        metadata_address, grpc::InsecureChannelCredentials()));

    // Check if metadata server is reachable via Alive call
    google::protobuf::Empty request;
    MetaStatus status_response;
    grpc::ClientContext context;
    grpc::Status status = metadata_stub->Alive(&context, request, &status_response);
    if (!status.ok() || !status_response.success())
    {
        std::cerr << "pfs_initialize: Metadata server is unreachable or failed health check." << std::endl;
        std::cerr << "Server message: " << status_response.message() << std::endl;
        return -1;
    }
    else
    {
        // std::cout << "Metadata server is alive. Server message: " << status_response.message() << std::endl;
    }

    // Step 4: Connect to each file server
    for (int i = 1; i <= NUM_FILE_SERVERS; ++i)
    {
        std::string file_server_address = server_addresses[i];
        file_server_stubs.push_back(FileServerService::NewStub(
            grpc::CreateChannel(file_server_address, grpc::InsecureChannelCredentials())));

        // Check if file server is reachable via Alive call
        grpc::ClientContext file_context;
        FileServerStatus file_status_response;
        status = file_server_stubs.back()->Alive(&file_context, request, &file_status_response);
        if (!status.ok() || !file_status_response.success())
        {
            std::cerr << "pfs_initialize: File server at " << file_server_address
                      << " is unreachable or failed health check." << std::endl;
            std::cerr << "Server message: " << file_status_response.message() << std::endl;
            return -1;
        }
        else
        {
            // std::cout << "File server at " << file_server_address << " is alive. Server message: "
                    //   << file_status_response.message() << std::endl;
        }
    }

    // Step 5: Obtain client ID from metadata server
    pfsmeta::ClientInitRequest init_request;
    pfsmeta::ClientInitResponse init_response;
    grpc::ClientContext init_context;
    status = metadata_stub->InitializeClient(&init_context, init_request, &init_response);

    if (!status.ok() || !init_response.success())
    {
        std::cerr << "pfs_initialize: Failed to initialize client with metadata server." << std::endl;
        std::cerr << "Server message: " << init_response.message() << std::endl;
        return -1;
    }
    else
    {
        // std::cout << "pfs_initialize: Successfully initialized with client ID " << init_response.client_id()
                //   << ". Server message: " << init_response.message() << std::endl;
    }
    StartClientRegistration(init_response.client_id(), getMyHostname());
    client_id = init_response.client_id();

    return init_response.client_id();
}

int pfs_finish(int client_id)
{
    // std::cout << "pfs_finish: Shutting down client with ID " << client_id << std::endl;
    // client_cache->WriteBackAllAndClear();
    // Send shutdown message to the server
    if (stream != nullptr)
    {
        pfsmeta::RegistrationRequest shutdown_request;
        shutdown_request.set_client_id(client_id);
        shutdown_request.set_is_shutdown(true); // Indicate shutdown
        if (!stream->Write(shutdown_request))
        {
            std::cerr << "Failed to send shutdown message to the server." << std::endl;
        }

        // Close the write side of the stream
        stream->WritesDone();
    }

    // Wait for the listener thread to finish reading any remaining messages
    if (listener_thread.joinable())
    {
        listener_thread.join();
    }

    // Do NOT call stream->Finish() here; listener thread handles it

    // std::cout << "pfs_finish: Client shutdown complete." << std::endl;
    return 0;
}

int pfs_create(const char *filename, int stripe_width)
{
    // Prepare the request
    pfsmeta::CreateFileRequest request;
    request.set_filename(filename);
    request.set_stripe_width(stripe_width);

    // Prepare the response and context
    pfsmeta::MetaStatus response;
    grpc::ClientContext context;

    // Call the MetadataService CreateFile RPC

    grpc::Status status = metadata_stub->CreateFile(&context, request, &response);

    if (!status.ok())
    {
        std::cerr << "pfs_create: RPC failed with error code " << status.error_code()
                  << ": " << status.error_message() << std::endl;
        return -1;
    }

    if (!response.success())
    {
        std::cerr << "pfs_create: File creation failed. Server message: "
                  << response.message() << std::endl;
        return -1;
    }

    // std::cout << "pfs_create: File created successfully. Server message: "
            //   << response.message() << std::endl;
    return 0;
}

int pfs_open(const char *filename, int mode)
{
    // Check if fd already exists in fd_map
    {
        std::lock_guard<std::mutex> lock(fd_map_mutex);
        if (name_fd_map.find(filename) != name_fd_map.end())
        {
            std::cerr << "pfs_open: File descriptor " << filename << " already in use." << std::endl;
            return -1;
        }
    }
    // Prepare the request
    pfsmeta::OpenFileRequest request;
    request.set_filename(filename);
    request.set_mode(mode);

    // Prepare the response and context
    pfsmeta::OpenFileResponse response;
    grpc::ClientContext context;

    // Call the MetadataService OpenFile RPC
    grpc::Status status = metadata_stub->OpenFile(&context, request, &response);

    if (!status.ok())
    {
        std::cerr << "pfs_open: RPC failed with error code " << status.error_code()
                  << ": " << status.error_message() << std::endl;
        return -1;
    }

    if (!response.success())
    {
        std::cerr << "pfs_open: File open failed. Server message: "
                  << response.message() << std::endl;
        return -1;
    }

    int fd = response.fd();
    std::string str(filename);
    // std::cout << "pfs_open: File opened successfully with FD " << fd
            //   << ". Server message: " << response.message() << std::endl;
    {
        std::lock_guard<std::mutex> lock(fd_map_mutex);
        fd_map[fd] = str;
        name_fd_map[str] = fd;
    }
    pfs_metadata fmd;
    if (pfs_fstat(fd, &fmd) != 0)
    {
        std::cerr << "Failed to fetch file metadata for FD " << fd << std::endl;
        return -1;
    }

    
    return fd;
}

bool token_exists(int fd, uint64_t start_offset, uint64_t length, bool is_write)
{
    std::lock_guard<std::mutex> lock(token_map_mutex); // Ensure thread-safe access to token_map
    auto it = token_map.find(fd);
    if (it == token_map.end())
    {
        return false; // No tokens exist for this file descriptor
    }

    auto &tokens = it->second; // Get the tokens for the file descriptor
    for (const auto &token : tokens)
    {
        if (token.start_offset <= start_offset &&
            token.start_offset + token.length >= start_offset + length &&
            token.is_write == is_write)
        {
            return true; // Found a matching token
        }
    }

    return false; // No matching token found
}

int pfs_read(int fd, void *buf, size_t length, off_t start_offset)
{
    std::string filename = fd_map[fd];
    uint64_t current_offset = start_offset;
    uint64_t remaining_length = length;

    // Extract file metadata for block size and stripe width
    pfs_metadata fmd;
    {
        std::lock_guard<std::mutex> lock(metadata_cache_mutex);
        fmd = metadata_cache[filename];
    }
    
    
    int stripe_width = fmd.recipe.stripe_width;
    int block_size = fmd.recipe.block_size;

    std::vector<int> cachable_blocks;
    bool write_back = false;
    // Step 1: Check for existing tokens
    if (!token_exists(fd, start_offset, length, false))
    {
        // Request a read token from the metadata server
        TokenRequest request;
        request.set_fd(fd);
        request.set_start_offset(start_offset);
        request.set_length(length);
        request.set_is_write(false);
        request.set_client_id(client_id);

        TokenResponse response;
        grpc::ClientContext context;
        grpc::Status status = metadata_stub->RequestToken(&context, request, &response);
        write_back = true;
        if (!status.ok() || !response.granted())
        {
            std::cerr << "Failed to obtain read token: " << response.message() << std::endl;
            return -1;
        }
        length = response.length();
        remaining_length = length;
        {
            std::lock_guard<std::mutex> lock(metadata_cache_mutex);
            metadata_cache[filename].file_size = response.filesize();
        }
        // Extract cacheable blocks from the response
        for (const auto &block : response.cacheable_blocks())
        {
            cachable_blocks.push_back(block);
        }
        TokenInfo new_token = {false, start_offset, length, RUNNING};
        RemoveOverlappingTokens(token_map[fd], new_token);

        // Add the new token to the token map
        {
            std::lock_guard<std::mutex> lock(token_map_mutex);

            token_map[fd].emplace_back(new_token);
        }
    }
    else
    {
        // Update the existing token to RUNNING
        std::lock_guard<std::mutex> lock(token_map_mutex);
        for (auto &token : token_map[fd])
        {
            if (token.start_offset <= start_offset &&
                token.start_offset + token.length >= start_offset + length &&
                token.is_write == false)
            {
                token.status = RUNNING;
                // std::cout << "Token updated to RUNNING for FD " << fd
                        //   << ", Start Offset: " << start_offset
                        //   << ", Length: " << length << std::endl;
                break;
            }
        }
    }
    // // std::cout << "[DEBUG] Tokens for FD " << fd << ":" << std::endl;

    for (const auto &token : token_map[fd])
    {
        // std::cout << "  Token - Start Offset: " << token.start_offset
        //           << ", Length: " << token.length
        //           << ", Is Write: " << (token.is_write ? "true" : "false")
        //           << ", Status: " << (token.status == RUNNING ? "RUNNING" : "COMPLETED")
        //           << std::endl;
    }

    // Step 2: Cache only the blocks that are marked as cacheable
    std::vector<BlockInfo> blocks = SplitIntoBlocks(filename, start_offset, length);

    for (const auto &block : blocks)
    {
        std::vector<char> cached_block;
        uint64_t block_offset = current_offset % block_size;
        uint64_t bytes_to_read = std::min(remaining_length, block_size - block_offset);

        // Check if the block is cached
        if (client_cache->GetFromCache(filename, current_offset, cached_block))
        {
            {
                std::lock_guard<std::mutex> lock(global_execstat_mutex);
                global_execstat.num_read_hits++;
            }
            
            // Case 1: Cache hit
            std::copy(cached_block.begin() + block_offset,
                      cached_block.begin() + block_offset + bytes_to_read,
                      static_cast<char *>(buf) + (current_offset - start_offset));
            // std::cout << "Cache hit :" << block.block_number << std::endl;
        }
        else
        {
            bool can_cache = false;
            if (!write_back)
            {
                // Case 2: Cache miss
                InvalidateBlockRequest invalidate_request;
                invalidate_request.set_filename(filename);
                invalidate_request.set_block_number(block.block_number);
                invalidate_request.set_client_id(client_id);
                invalidate_request.set_is_write(false);

                InvalidateBlockResponse invalidate_response;
                grpc::ClientContext invalidate_context;

                grpc::Status invalidate_status = metadata_stub->InvalidateBlock(&invalidate_context, invalidate_request, &invalidate_response);
                if (!invalidate_status.ok() || !invalidate_response.success())
                {
                    std::cerr << "Failed to invalidate block " << block.block_number << ": " << invalidate_response.message() << std::endl;
                    return -1;
                }
                can_cache = invalidate_response.can_cache();
                // // std::cout << "[DEBUG] Successfully writebacked block " << block.block_number << " for write-back." << "Can cache:" << can_cache << std::endl;
                // If no write tokens exist, fetch the block and cache it if cacheable
            }
            ReadBlockRequest read_request;
            read_request.mutable_block()->set_filename(filename);
            read_request.mutable_block()->set_block_number(block.block_number);
            read_request.set_offset(0);
            read_request.set_num_bytes(block_size);

            ReadBlockResponse read_response;
            grpc::ClientContext file_context;
            int server_index = block.server_index;

            grpc::Status read_status = file_server_stubs[server_index]->ReadBlock(&file_context, read_request, &read_response);
            if (!read_status.ok() || !read_response.success())
            {
                std::cerr << "Failed to fetch block " << block.block_number << ": " << read_response.message() << std::endl;
                return -1;
            }

            // Copy data to user buffer
            std::copy(read_response.data().begin() + block_offset,
                      read_response.data().begin() + block_offset + bytes_to_read,
                      static_cast<char *>(buf) + (current_offset - start_offset));

            // Cache the fetched block if cacheable

            if ((write_back && std::find(cachable_blocks.begin(), cachable_blocks.end(), block.block_number) != cachable_blocks.end()) || can_cache)
            {
                // std::cout << "In READ Caching the block at client side of number:" << block.block_number << std::endl;
                std::vector<char> data_vector(read_response.data().begin(), read_response.data().end());
                client_cache->CacheData(filename, current_offset, data_vector, false, client_id, stripe_width, block_size);
            }
            else
            {
                // std::cout << "Not cache the block as it may have write tokens and cached at another client :" << block.block_number << std::endl;
            }
        }

        // Update offsets and lengths
        current_offset += bytes_to_read;
        remaining_length -= bytes_to_read;
    }

    // Step 3: Update token status to COMPLETED after the read operation
     
    {
        std::lock_guard<std::mutex> lock(token_map_mutex);
        for (auto &token : token_map[fd])
        {
            if (token.start_offset <= start_offset &&
                token.start_offset + token.length >= start_offset + length &&
                token.is_write == false)
            {
                std::lock_guard<std::mutex> lock(token.mtx);

            token.status = COMPLETED;
            token.cv.notify_all(); // Notify waiting clients

                // std::cout << "changed to completed" << std::endl; 
                // std::cout << "[DEBUG] Token marked as COMPLETED: , Start Offset = " << token.start_offset<< ", Length = " << token.length << std::endl;

                // break;
            }
        }
    }
    // // std::cout << "[DEBUG] Tokens for FD " << fd << ":" << std::endl;

    for (const auto &token : token_map[fd])
    {
        // std::cout << "  Token - Start Offset: " << token.start_offset
        //           << ", Length: " << token.length
        //           << ", Is Write: " << (token.is_write ? "true" : "false")
        //           << ", Status: " << (token.status == RUNNING ? "RUNNING" : "COMPLETED")
        //           << std::endl;
    }

    // std::cout << "Read operation completed successfully for FD " << fd << std::endl;
    return length; // Return the number of bytes read
}

int pfs_write(int fd, const void *buf, size_t length, off_t start_offset)
{
    std::string filename = fd_map[fd];
    uint64_t current_offset = start_offset;
    uint64_t remaining_length = length;

    // Extract file metadata for block size and stripe width
    pfs_metadata fmd;
    {
        std::lock_guard<std::mutex> lock(metadata_cache_mutex);
        fmd = metadata_cache[filename];
    }
    
    int stripe_width = fmd.recipe.stripe_width;
    int block_size = fmd.recipe.block_size;

    std::vector<int> cachable_blocks;
    bool invalidated = false;
    // Step 1: Check for existing tokens
    if (!token_exists(fd, start_offset, length, true))
    {
        // Request a write token from the metadata server
        TokenRequest request;
        request.set_fd(fd);
        request.set_start_offset(start_offset);
        request.set_length(length);
        request.set_is_write(true);
        request.set_client_id(client_id);

        TokenResponse response;
        grpc::ClientContext context;
        grpc::Status status = metadata_stub->RequestToken(&context, request, &response);

        if (!status.ok() || !response.granted())
        {
            std::cerr << "Failed to obtain write token: " << response.message() << std::endl;
            return -1;
        }
        {
            std::lock_guard<std::mutex> lock(metadata_cache_mutex);
            metadata_cache[filename].file_size = response.filesize();
        }

        // Extract cacheable blocks from the response
        // std::cout << "Cachable blocks : [";
        for (const auto &block : response.cacheable_blocks())
        {
            cachable_blocks.push_back(block);
            // std::cout << block << std::endl;
        }
        invalidated = true;
        // Add the new token to the token map
        TokenInfo new_token = {true, start_offset, length, RUNNING};
        RemoveOverlappingTokens(token_map[fd], new_token);

        {
            std::lock_guard<std::mutex> lock(token_map_mutex);

            token_map[fd].emplace_back(new_token);
        }
    }
    else
    {
        // Update the existing token to RUNNING
        std::lock_guard<std::mutex> lock(token_map_mutex);
        for (auto &token : token_map[fd])
        {
            if (token.start_offset <= start_offset &&
                token.start_offset + token.length >= start_offset + length &&
                token.is_write == true)
            {
                token.status = RUNNING;
                // std::cout << "Token updated to RUNNING for FD " << fd
                        //   << ", Start Offset: " << start_offset
                        //   << ", Length: " << length << std::endl;
                break;
            }
        }
    }
    // // std::cout << "[DEBUG] Tokens for FD " << fd << ":" << std::endl;

    for (const auto &token : token_map[fd])
    {
        // std::cout << "  Token - Start Offset: " << token.start_offset
        //           << ", Length: " << token.length
        //           << ", Is Write: " << (token.is_write ? "true" : "false")
        //           << ", Status: " << (token.status == RUNNING ? "RUNNING" : "COMPLETED")
        //           << std::endl;
    }

    // Step 2: Cache only the blocks that are marked as cacheable
    std::vector<BlockInfo> blocks = SplitIntoBlocks(filename, start_offset, length);

    for (const auto &block : blocks)
    {
        // For write, we cache every block
        
        std::vector<char> cached_block;
        uint64_t block_offset = current_offset % block_size;
        uint64_t bytes_to_write = std::min(remaining_length, block_size - block_offset);

        // Check if the block is already cached
        if (client_cache->GetFromCache(filename, current_offset, cached_block))
        {
            {
                std::lock_guard<std::mutex> lock(global_execstat_mutex);
                global_execstat.num_write_hits++;
            }
            // Update the cached block with new data
            std::copy(static_cast<const char *>(buf) + (current_offset - start_offset),
                      static_cast<const char *>(buf) + (current_offset - start_offset) + bytes_to_write,
                      cached_block.begin() + block_offset);
            // std::cout << "Cache hit: " << block.block_number << std::endl;
        }
        else
        {
            // Invalidate this block across all clients
            if (!invalidated)
            { // already invalidated in request token as it is write by metaserver
                InvalidateBlockRequest invalidate_request;
                invalidate_request.set_filename(filename);
                invalidate_request.set_block_number(block.block_number);
                invalidate_request.set_client_id(client_id);
                invalidate_request.set_is_write(true);
                InvalidateBlockResponse invalidate_response;
                grpc::ClientContext invalidate_context;

                grpc::Status invalidate_status = metadata_stub->InvalidateBlock(&invalidate_context, invalidate_request, &invalidate_response);
                if (!invalidate_status.ok() || !invalidate_response.success())
                {
                    std::cerr << "Failed to invalidate block " << block.block_number << ": " << invalidate_response.message() << std::endl;
                    return -1;
                }

                // // std::cout << "[DEBUG] Successfully invalidated block " << block.block_number << " across all clients." << std::endl;
            }
            // Fetch the block from the file server and cache it
            ReadBlockRequest read_request;
            read_request.mutable_block()->set_filename(filename);
            read_request.mutable_block()->set_block_number(block.block_number);
            read_request.set_offset(0);
            read_request.set_num_bytes(block_size);

            ReadBlockResponse read_response;
            grpc::ClientContext file_context;
            int server_index = block.server_index;

            grpc::Status read_status = file_server_stubs[server_index]->ReadBlock(&file_context, read_request, &read_response);
            if (!read_status.ok() || !read_response.success())
            {
                // std::cout << "[DEBUG] Block " << block.block_number << " does not exist or failed to fetch. Initializing a new block." << std::endl;

                // Initialize a new block filled with zeroes
                cached_block.resize(block_size, 0);

                // Update the block with new data
                std::copy(static_cast<const char *>(buf) + (current_offset - start_offset),
                          static_cast<const char *>(buf) + (current_offset - start_offset) + bytes_to_write,
                          cached_block.begin() + block_offset);

                // Cache the newly initialized block
                client_cache->CacheData(filename, current_offset, cached_block, true, client_id, stripe_width, block_size);
                // // std::cout << "[DEBUG] Initialized and cached a new block " << block.block_number << std::endl;
            }
            else
            {
                // Cache the fetched block
                cached_block.assign(read_response.data().begin(), read_response.data().end());
                std::copy(static_cast<const char *>(buf) + (current_offset - start_offset),
                          static_cast<const char *>(buf) + (current_offset - start_offset) + bytes_to_write,
                          cached_block.begin() + block_offset);

                // Cache the updated block
                client_cache->CacheData(filename, current_offset, cached_block, true, client_id, stripe_width, block_size);
                // // std::cout << "[DEBUG] Fetched and updated block " << block.block_number << " in cache." << std::endl;
            }
        }

        // Update offsets and lengths
        current_offset += bytes_to_write;
        remaining_length -= bytes_to_write;
    }

    // Step 3: Update token status to COMPLETED after the write operation
    {
        std::lock_guard<std::mutex> lock(token_map_mutex);
        for (auto &token : token_map[fd])
        {
            if (token.start_offset <= start_offset &&
                token.start_offset + token.length >= start_offset + length &&
                token.is_write == true)
            {
                std::lock_guard<std::mutex> token_lock(token.mtx);
                token.status = COMPLETED;
                token.cv.notify_all(); // Notify waiting clients
                // break;
            }
        }
    }
    // // std::cout << "[DEBUG] Tokens for FD " << fd << ":" << std::endl;

    for (const auto &token : token_map[fd])
    {
        // std::cout << "  Token - Start Offset: " << token.start_offset
        //           << ", Length: " << token.length
        //           << ", Is Write: " << (token.is_write ? "true" : "false")
        //           << ", Status: " << (token.status == RUNNING ? "RUNNING" : "COMPLETED")
        //           << std::endl;
    }

    // std::cout << "Write operation completed successfully for FD " << fd << std::endl;
    return length; // Return the number of bytes written
}

int pfs_close(int fd)
{
    // Step 1: Call CloseFile on the metaserver to close the file (you already have this code)
    pfsmeta::CloseFileRequest request;
    request.set_fd(fd);
    request.set_client_id(client_id);

    MetaStatus response;
    grpc::ClientContext context;

    grpc::Status status = metadata_stub->CloseFile(&context, request, &response);
    if (!status.ok())
    {
        std::cerr << "[ERROR] pfs_close failed: " << status.error_message() << std::endl;
        return -1;
    }

    if (!response.success())
    {
        std::cerr << "[ERROR] pfs_close failed: " << response.message() << std::endl;
        return -1;
    }

    // // std::cout << "[DEBUG] pfs_close succeeded: " << response.message() << std::endl;

    // Step 2: Write back all dirty blocks for the filename stored in fd_map
    // Get the filename associated with the file descriptor
    std::string fn;
    {
        std::lock_guard<std::mutex> lock(fd_map_mutex);
        auto it = fd_map.find(fd);
        
        if (it != fd_map.end())
        {
            std::string filename = it->second;
            fn=filename;
            // // std::cout << "[DEBUG] Writing back all dirty blocks for file: " << filename << std::endl;

            // Call WriteBackAllFileName to write all dirty blocks to file server
            client_cache->WriteBackAllFileName(filename);
            
                fd_map.erase(fd);
                name_fd_map.erase(filename);
            
            
            // You can also choose to invalidate or clear the cache for that file
            // client_cache.InvalidateCacheRange(filename, 0, 0); // Invalidate all blocks of the file
        }

        else
        {
            std::cerr << "[ERROR] Invalid file descriptor. Cannot find associated filename." << std::endl;
            return -1;
        }
    }
    // Step 3: Clean up any other resources if needed (this part depends on your logic)
     {
        std::lock_guard<std::mutex> lock(metadata_cache_mutex);
        // metadata_db[filename].last_close_time = time(nullptr);
        metadata_cache.erase(fn);
     }
    return 0; // Success
}

int pfs_delete(const char *filename)
{
    // Step 1: Call DeleteFile on the MetaService stub to check if deletion is allowed
    pfsmeta::FileIdentifier file_identifier_meta;
    file_identifier_meta.set_filename(filename);

    pfsmeta::MetaStatus meta_status;
    grpc::ClientContext meta_context;

    grpc::Status meta_call_status = metadata_stub->DeleteFile(&meta_context, file_identifier_meta, &meta_status);
    if (!meta_call_status.ok())
    {
        std::cerr << "[ERROR] pfs_Delete: Metaserver RPC failed with error: " << meta_call_status.error_message() << std::endl;
        return -1;
    }

    if (!meta_status.success())
    {
        std::cerr << "[ERROR] pfs_Delete: Metaserver rejected delete request: " << meta_status.message() << std::endl;
        return -1;
    }

    // // std::cout << "[DEBUG] pfs_Delete: Metaserver confirmed deletion for file: " << filename << std::endl;

    // Step 2: Distribute the deletion of blocks across file servers
    std::map<int, std::vector<int>> server_block_map;                   // server_index -> list of block numbers
    int block_number = 0;                                               // Initialize block number
    int file_size = metadata_cache[filename].file_size;                 // Retrieve file size from the response
    int num_blocks = (file_size + PFS_BLOCK_SIZE - 1) / PFS_BLOCK_SIZE; // Calculate the total number of blocks
    int stripe_width = metadata_cache[filename].recipe.stripe_width;
    int block_size = PFS_BLOCK_SIZE;
    int stripe_blocks = STRIPE_BLOCKS;
    for (int i = 0; i < num_blocks; ++i)
    {
        int server_index = (block_number / stripe_blocks) % stripe_width;
        server_block_map[server_index].push_back(block_number);
        ++block_number;
    }

    // Step 3: Send delete requests to the respective file servers
    for (const auto &entry : server_block_map)
    {
        int server_index = entry.first;
        const std::vector<int> &blocks_to_delete = entry.second;

        // Prepare the request for the file server
        pfsfile::FileBlocks file_request;
        file_request.set_filename(filename);
        for (int block_num : blocks_to_delete)
        {
            file_request.add_block_numbers(block_num);
        }

        pfsfile::FileServerStatus file_status;
        grpc::ClientContext file_context;

        grpc::Status file_call_status = file_server_stubs[server_index]->DeleteFileBlocks(&file_context, file_request, &file_status);
        if (!file_call_status.ok())
        {
            std::cerr << "[ERROR] pfs_Delete: Fileserver RPC failed for server " << server_index
                      << " with error: " << file_call_status.error_message() << std::endl;
            return -1;
        }

        if (!file_status.success())
        {
            std::cerr << "[ERROR] pfs_Delete: Fileserver " << server_index << " failed to delete blocks: "
                      << file_status.message() << std::endl;
            return -1;
        }

        // // std::cout << "[DEBUG] pfs_Delete: Successfully deleted blocks from server " << server_index << std::endl;
    }
    

    // std::cout << "[INFO] pfs_Delete: File " << filename << " deleted successfully from all file servers." << std::endl;
    return 0; // Success
}

int pfs_fstat(int fd, struct pfs_metadata *meta_data)
{
    if (meta_data == nullptr)
    {
        std::cerr << "pfs_fstat: meta_data pointer is null." << std::endl;
        return -1;
    }

    std::string filename = fd_map[fd];

    // Check if metadata is already in the cache
    // {
    //     std::lock_guard<std::mutex> lock(metadata_cache_mutex);
    //     auto it = metadata_cache.find(filename);
    //     if (it != metadata_cache.end())
    //     {
    //         // Metadata is found in the cache, copy it to the provided structure
    //         *meta_data = it->second;
    //         // std::cout << "pfs_fstat: Retrieved metadata for file " << filename << " from cache." << std::endl;
    //         return 0;
    //     }
    // }

    // Metadata is not in the cache; retrieve it using gRPC
    pfsmeta::FileIdentifier request;
    request.set_filename(filename);

    // Prepare the response and context
    pfsmeta::FileMetadata response;
    grpc::ClientContext context;

    // Call the GetFileMetadata RPC
    grpc::Status status = metadata_stub->GetFileMetadata(&context, request, &response);

    if (!status.ok())
    {
        std::cerr << "pfs_fstat: RPC failed with error code " << status.error_code()
                  << ": " << status.error_message() << std::endl;
        return -1;
    }

    // Populate the metadata structure with the response
    std::strncpy(meta_data->filename, response.filename().c_str(), sizeof(meta_data->filename) - 1);
    meta_data->filename[sizeof(meta_data->filename) - 1] = '\0'; // Ensure null termination
    meta_data->file_size = response.size();
    meta_data->ctime = static_cast<time_t>(response.creation_time());
    meta_data->mtime = static_cast<time_t>(response.last_close_time());

    // Populate the recipe details from the response
    meta_data->recipe.block_size = response.recipe().block_size();
    meta_data->recipe.stripe_width = response.recipe().stripe_width();
    meta_data->recipe.stripe_blocks = response.recipe().stripe_blocks();

    // Store the metadata in the cache for future requests
    {
        std::lock_guard<std::mutex> lock(metadata_cache_mutex);
        metadata_cache[filename] = *meta_data;
        // std::cout << "pfs_fstat: Metadata for file " << filename << " added to cache." << std::endl;
    }

    // std::cout << "pfs_fstat: Successfully retrieved metadata for file " << meta_data->filename << " using gRPC." << std::endl;
    return 0;
}
int pfs_execstat(struct pfs_execstat *execstat_data)
{
    if (!execstat_data)
    {
        // Return -1 if the pointer is null
        std::cerr << "[ERROR] pfs_execstat: Null pointer provided for execstat_data." << std::endl;
        return -1;
    }

    // Copy the global execution stats to the provided struct
    execstat_data->num_read_hits = global_execstat.num_read_hits;
    execstat_data->num_write_hits = global_execstat.num_write_hits;
    execstat_data->num_evictions = global_execstat.num_evictions;
    execstat_data->num_writebacks = global_execstat.num_writebacks;
    execstat_data->num_invalidations = global_execstat.num_invalidations;
    execstat_data->num_close_writebacks = global_execstat.num_close_writebacks;
    execstat_data->num_close_evictions = global_execstat.num_close_evictions;

    // // std::cout << "[DEBUG] pfs_execstat: Execution stats successfully copied to provided struct." << std::endl;

    return 0;
}