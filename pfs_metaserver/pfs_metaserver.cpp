#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <grpcpp/grpcpp.h>
#include "pfs_proto/pfs_fileserver.grpc.pb.h"
#include "pfs_metaserver.hpp" // Include for getMyHostname, getMyIP, etc.
#include <queue>
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using pfsmeta::ClientInitRequest;
#include <random> // For random number generation

using pfsmeta::ClientInitResponse;
using pfsmeta::CloseFileRequest;
using pfsmeta::CreateFileRequest;
using pfsmeta::FileIdentifier;
using pfsmeta::FileMetadata;
using pfsmeta::MetadataService;
using pfsmeta::MetaStatus;
using pfsmeta::OpenFileRequest;
using pfsmeta::OpenFileResponse;
using pfsmeta::RegistrationRequest;
using pfsmeta::TokenRequest;
using pfsmeta::TokenResponse;
// using pfsmeta::RegistrationResponse;
using grpc::ServerReaderWriter;
using pfsmeta::InvalidateBlockRequest;
using pfsmeta::InvalidateBlockResponse;
using pfsmeta::Notification;

#include "pfs_common/pfs_config.hpp"

// In-memory metadata storage structure
struct FileRecipe
{
    int block_size = PFS_BLOCK_SIZE;
    int stripe_width;
    int stripe_blocks = STRIPE_BLOCKS;
};

struct FileData
{
    std::string filename;
    uint64_t size = 0;
    FileRecipe recipe;
    uint64_t creation_time = 0;
    uint64_t last_close_time = 0;
};

// Token data structure to manage access control
struct Token
{
    bool is_write;
    uint64_t start_offset;
    uint64_t length;
    int32_t client_id;
};
enum class TokenStatus
{
    RUNNING,
    COMPLETED
};
struct TokenCv
{
    bool is_write;
    uint64_t start_offset;
    uint64_t length;
    int32_t client_id;
    TokenStatus status;
    std::mutex mtx;
    std::condition_variable cv;

    TokenCv(bool is_write, uint64_t start_offset, uint64_t length, int32_t client_id, TokenStatus status)
        : is_write(is_write), start_offset(start_offset), length(length), client_id(client_id), status(status)
    {
    }

    // Delete copy and move constructors to avoid issues
    TokenCv(const TokenCv &) = delete;
    TokenCv &operator=(const TokenCv &) = delete;
    TokenCv(TokenCv &&) = delete;
    TokenCv &operator=(TokenCv &&) = delete;
};

struct Block
{
    int read_tokens = 0;
    int write_tokens = 0;
    std::unordered_set<int> inCache;
};
struct FdInfo
{
    std::string filename;
    int mode; // 1 = read, 2 = read/write
};
struct ClientSession
{
    ServerReaderWriter<Notification, RegistrationRequest> *stream;
    std::mutex stream_mutex; // Mutex to synchronize access to the stream
};
struct PendingAck
{
    int count;
    std::shared_ptr<std::condition_variable> ack_cv;
    std::unordered_map<int32_t, bool> ack_received;
};
struct BlockInfo
{
    int block_number;      // Block number
    int server_index;      // File server index
    uint64_t start_offset; // Start offset within the block
    uint64_t length;       // Length of the block
};

// private:

class MetadataServiceImpl final : public MetadataService::Service
{
private:
    std::mutex client_id_mutex;
    std::mutex total_mutex;
    std::unordered_map<std::string, FileData> metadata_db;
    std::mutex metadata_db_mutex;
    std::unordered_map<std::string, std::unordered_map<int, Block>> blocksMap; // filename -> block_num -> blocks
    std::unordered_map<int32_t, FdInfo> fd_map;                                // Map FD to FdInfo (filename and mode)
    std::mutex fd_map_mutex;
    int32_t next_fd = 1000;                                       // Starting file descriptor number
    std::unordered_map<std::string, std::vector<Token>> token_db; // Map from filename to a list of tokens
    std::mutex fd_mutex;
    std::unordered_map<std::string, std::set<int>> opened_fd; // filename -> set(fd)
    // Mutex to protect access to client sessions
    std::mutex client_mutex_;
    std::unordered_map<std::string, PendingAck> pending_ack_map; // Map to track revocation acknowledgments
    std::mutex pending_ack_map_mutex;                            // Mutex to protect the map
    std::mutex token_map_mutex;
    std::mutex blocks_map_mutex;
    // Map to store client sessions: client_id -> ClientSession
    std::unordered_map<int32_t, std::shared_ptr<ClientSession>> client_sessions_;
    std::unordered_map<std::string, std::unordered_map<std::string, std::unique_ptr<TokenCv>>> waiting_map; // Maps unique token ID to its associated Token
    std::mutex waiting_map_mutex;                                                                           // Protects the revocation_map

public:
    // Function to deep copy the inner map for a specific filename
    std::unordered_map<int, Block> deepCopyBlocksForFilename(
    const std::unordered_map<std::string, std::unordered_map<int, Block>> &original,
    const std::string &filename)
{
    std::unordered_map<int, Block> copied_blocks;

    // Check if the filename exists in the original map
    auto it = original.find(filename);
    if (it != original.end())
    {
        // Iterate over the inner map (blocks) and copy
        for (const auto &block_pair : it->second)
        {
            copied_blocks[block_pair.first] = block_pair.second; // copy the Block
        }
    }
    else
    {
        // // std::cout << "[DEBUG] Filename not found in the blocks map: " << filename << std::endl;
    }

    return copied_blocks; // Return the deep copied inner map
}

    bool RemoveOverlappingTokens(std::vector<Token> &tokens, const Token &new_token)
    {
        bool removed = false;

        // Iterate through the tokens to find any that should be removed
        for (auto it = tokens.begin(); it != tokens.end();)
        {
            uint64_t new_token_end = new_token.start_offset + new_token.length;
            uint64_t existing_token_end = it->start_offset + it->length;

            // Check if the new token is completely inside the existing token
            bool is_inside = (new_token.start_offset <= it->start_offset) && (new_token_end >= existing_token_end);

            // If the new token is completely inside the existing token, remove the existing token
            if (is_inside && it->client_id == new_token.client_id)
            {
                // // std::cout << "[DEBUG] Removing token that is completely inside new token: Start = " << it->start_offset << ", Length = " << it->length << std::endl;

                it = tokens.erase(it); // Remove the token and advance the iterator
                removed = true;
            }
            else
            {
                ++it; // Only advance if no token was removed
            }
        }

        return removed;
    }
    void UpdateBlocksMapFromTokens(const std::string &filename)
    {
        {
            std::lock_guard<std::mutex> lock(blocks_map_mutex);
        // Clear existing block information for the file in blocksMap
        auto file_blocks = blocksMap.find(filename);

        if (file_blocks != blocksMap.end())
        {
            for (auto &[block_num, block] : file_blocks->second)
            {
                block.read_tokens = 0;
                block.write_tokens = 0;
            }
            // std::cout << "Cleared read_tokens and write_tokens for all blocks in file: " << filename << std::endl;
        }
        else
        {
            std::cerr << "Filename " << filename << " not found in blocksMap." << std::endl;
        }
}
        // Retrieve all tokens for the file
        // const FileRecipe &recipe;
        // {
        //     std::lock_guard<std::mutex> lock(metadata_db_mutex);

        //     // Get the file's metadata
        //     if (metadata_db.find(filename) == metadata_db.end())
        //     {
        //         std::cerr << "Error: Metadata not found for file " << filename << std::endl;
        //         return;
        //     }
        //     recipe = metadata_db[filename].recipe;
        // }
        
        // {
        //     std::lock_guard<std::mutex> lock(token_map_mutex);
        // auto &tokens = token_db[filename];
        // std::lock_guard<std::mutex> meta_lock(metadata_db_mutex);
        // std::lock_guard<std::mutex> blocks_lock(blocks_map_mutex);
        // if (metadata_db.find(filename) == metadata_db.end())
        // {
        //     std::cerr << "Error: Metadata not found for file " << filename << std::endl;
        //     return;
        // }
        // const FileRecipe &recipe = metadata_db[filename].recipe;

        // // Iterate through the tokens to update blocksMap
        // for (const auto &token : tokens)
        // {
        //     uint64_t current_offset = token.start_offset;
        //     uint64_t end_offset = token.start_offset + token.length - 1;

        //     while (current_offset <= end_offset)
        //     {
        //         // Calculate block number
        //         int block_number = current_offset / recipe.block_size;

        //         // Calculate offset within the block
        //         uint64_t block_offset = current_offset % recipe.block_size;

        //         // Calculate bytes to process in this block
        //         uint64_t bytes_to_process = std::min(end_offset - current_offset + 1, recipe.block_size - block_offset);

        //         // Ensure the block exists in blocksMap
        //         Block &block_info_entry = blocksMap[filename][block_number];

        //         // Update token counts for the block
        //         if (token.is_write)
        //         {
        //             block_info_entry.write_tokens += bytes_to_process;
        //         }
        //         else
        //         {
        //             block_info_entry.read_tokens += bytes_to_process;
        //         }

        //         // std::cout << "Updated Block " << block_number
        //                   << ": " << (token.is_write ? "write_tokens" : "read_tokens")
        //                   << " += " << bytes_to_process << " for client " << token.client_id << std::endl;

        //         // Move to the next range
        //         current_offset += bytes_to_process;
        //     }
        // }
        // }
        // Check metadata first
FileRecipe recipe;
{
    std::lock_guard<std::mutex> meta_lock(metadata_db_mutex);

    if (metadata_db.find(filename) == metadata_db.end())
    {
        std::cerr << "Error: Metadata not found for file " << filename << std::endl;
        return;
    }
    recipe = metadata_db[filename].recipe; // Copy recipe outside the lock
}

std::vector<Token> tokens;
{
    std::lock_guard<std::mutex> lock(token_map_mutex);
    tokens = token_db[filename]; // Copy tokens outside the lock
}

{
    std::lock_guard<std::mutex> blocks_lock(blocks_map_mutex);

    // Iterate through the tokens to update blocksMap
    for (const auto &token : tokens)
    {
        uint64_t current_offset = token.start_offset;
        uint64_t end_offset = token.start_offset + token.length - 1;

        while (current_offset <= end_offset)
        {
            // Calculate block number
            int block_number = current_offset / recipe.block_size;

            // Calculate offset within the block
            uint64_t block_offset = current_offset % recipe.block_size;

            // Calculate bytes to process in this block
            uint64_t bytes_to_process = std::min(end_offset - current_offset + 1, recipe.block_size - block_offset);

            // Ensure the block exists in blocksMap
            Block &block_info_entry = blocksMap[filename][block_number];

            // Update token counts for the block
            if (token.is_write)
            {
                block_info_entry.write_tokens += bytes_to_process;
            }
            else
            {
                block_info_entry.read_tokens += bytes_to_process;
            }

            // std::cout << "Updated Block " << block_number<< ": " << (token.is_write ? "write_tokens" : "read_tokens")<< " += " << bytes_to_process << " for client " << token.client_id << std::endl;

            // Move to the next range
            current_offset += bytes_to_process;
        }
    }
}

        // std::cout << "BlocksMap updated successfully from tokens for file: " << filename << std::endl;
    }

    // std::vector<BlockInfo> SplitIntoBlocks(const std::string &filename, uint64_t start_offset, uint64_t length)
    // {
    //     std::vector<BlockInfo> result;

    //     // Retrieve metadata for the file
    //     if (metadata_db.find(filename) == metadata_db.end())
    //     {
    //         std::cerr << "Error: File metadata not found for " << filename << std::endl;
    //         return result;
    //     }
    //     const FileRecipe &recipe = metadata_db[filename].recipe;

    //     uint64_t current_offset = start_offset;
    //     uint64_t end_offset = start_offset + length - 1;

    //     while (current_offset <= end_offset)
    //     {
    //         // Calculate block number
    //         int block_number = current_offset / PFS_BLOCK_SIZE;

    //         // Calculate server index
    //         int logical_block_index = block_number / STRIPE_BLOCKS;
    //         int server_index = logical_block_index % recipe.stripe_width;

    //         // Calculate offset within the block
    //         uint64_t block_offset = current_offset % recipe.block_size;

    //         // Calculate bytes to process in this block
    //         uint64_t bytes_to_process = std::min(end_offset - current_offset + 1, recipe.block_size - block_offset);

    //         // Add block info to the result
    //         BlockInfo block_info = {
    //             block_number,
    //             server_index,
    //             current_offset,
    //             bytes_to_process};
    //         result.push_back(block_info);

    //         // Move to the next block
    //         current_offset += bytes_to_process;
    //     }

    //     return result;
    // }
    std::vector<BlockInfo> SplitIntoBlocks(const std::string &filename, uint64_t start_offset, uint64_t length)
{
    std::vector<BlockInfo> result;

    // Lock metadata_db to ensure thread safety (if shared among threads)
    {
        std::lock_guard<std::mutex> lock(metadata_db_mutex);

        // Retrieve metadata for the file
        auto it = metadata_db.find(filename);
        if (it == metadata_db.end())
        {
            std::cerr << "Error: File metadata not found for " << filename << std::endl;
            return result;
        }
        const FileRecipe &recipe = it->second.recipe;

        // Validate block size and stripe width
        if (recipe.block_size == 0 || recipe.stripe_width == 0)
        {
            std::cerr << "Error: Invalid recipe configuration for file " << filename << std::endl;
            return result;
        }

        uint64_t current_offset = start_offset;
        uint64_t end_offset = start_offset + length - 1;

        while (current_offset <= end_offset)
        {
            // Calculate block number
            int block_number = current_offset / recipe.block_size;

            // Calculate server index
            int logical_block_index = block_number / STRIPE_BLOCKS;
            int server_index = logical_block_index % recipe.stripe_width;

            // Calculate offset within the block
            uint64_t block_offset = current_offset % recipe.block_size;

            // Calculate bytes to process in this block
            uint64_t bytes_to_process = std::min(end_offset - current_offset + 1, recipe.block_size - block_offset);

            // Add block info to the result
            BlockInfo block_info = {
                block_number,
                server_index,
                current_offset,
                bytes_to_process};
            result.push_back(block_info);

            // Move to the next block
            current_offset += bytes_to_process;
        }
    }

    return result;
}

    void ManageTokenRange(const std::string &filename, uint64_t start_offset, uint64_t length, bool is_write, int client_id)
    {
            std::lock_guard<std::mutex> meta_lock(metadata_db_mutex);

        uint64_t current_offset = start_offset;
        uint64_t end_offset = start_offset + length - 1;

        // Retrieve file recipe details
        int stripe_width = metadata_db[filename].recipe.stripe_width;
        int block_size = PFS_BLOCK_SIZE;
        int stripe_blocks = STRIPE_BLOCKS;

        while (current_offset <= end_offset)
        {
            // Calculate block number and determine server index using correct formula
            int block_number = current_offset / block_size;
            int server_index = (block_number / stripe_blocks) % stripe_width; // Fixed formula

            // Calculate the offset within the block and bytes to process
            uint64_t block_offset = current_offset % block_size;
            uint64_t bytes_to_process = std::min(end_offset - current_offset + 1, block_size - block_offset);

            // Ensure that this block exists in blocksMap for the given file
            {
                std::lock_guard<std::mutex> lock(blocks_map_mutex);

                Block &block_info = blocksMap[filename][block_number];

                // Update token counters
                if (is_write)
                {
                    block_info.write_tokens += bytes_to_process;
                    // std::cout << "Client " << client_id << " requested write token on block "<< block_number << " adding " << bytes_to_process << " bytes, handled by server "<< server_index << ".\n";
                }
                else
                {
                    block_info.read_tokens += bytes_to_process;
                    // std::cout << "Client " << client_id << " requested read token on block "<< block_number << " adding " << bytes_to_process << " bytes, handled by server "<< server_index << ".\n";
                }

                // Move to the next range
                current_offset += bytes_to_process;
            }
        }
    }

    // Bidirectional streaming method for client registration
    Status ClientRegister(ServerContext *context,
                          ServerReaderWriter<Notification, RegistrationRequest> *stream) override
    {
        RegistrationRequest request;

        // Read the initial registration request
        if (!stream->Read(&request))
        {
            return Status::OK;
        }

        int32_t client_id = request.client_id();
        std::string client_hostname = request.client_hostname();

        // Create a new client session
        auto client_session = std::make_shared<ClientSession>();
        client_session->stream = stream;

        {
            std::lock_guard<std::mutex> lock(client_mutex_);
            client_sessions_[client_id] = client_session;
        }

        // std::cout << "Client registered: ID = " << client_id<< ", Hostname = " << client_hostname << std::endl;

        // Send acknowledgment response
        Notification notification;
        notification.set_action("ack");
        notification.set_message("Registration successful");
        {
            std::lock_guard<std::mutex> lock(client_session->stream_mutex);
            stream->Write(notification);
        }

        // Main loop to handle client messages
        while (true)
        {
            RegistrationRequest incoming_request;
            if (stream->Read(&incoming_request))
            {
                // std::cout << "Received RegistrationRequest:" << std::endl;
                // std::cout << "  Client ID: " << incoming_request.client_id() << std::endl;
                // std::cout << "  Client Hostname: " << incoming_request.client_hostname() << std::endl;
                // std::cout << "  Is Shutdown: " << (incoming_request.is_shutdown() ? "true" : "false") << std::endl;
                // std::cout << "  Requested ID: " << incoming_request.requested_id() << std::endl;
                // std::cout << "  Message: " << incoming_request.message() << std::endl;
                if (incoming_request.is_shutdown())
                {
                    // std::cout << "Received shutdown signal from client ID: " << client_id << std::endl;
                    break; // Exit the loop and clean up
                }
                else if (incoming_request.message() == "ack")
                {
                    int ack_client_id = incoming_request.client_id();
                    std::string request_id = incoming_request.requested_id(); // Add request_id in client messages

                    // Mark acknowledgment as received
                    {
                        std::lock_guard<std::mutex> lock(pending_ack_map_mutex);
                        auto it = pending_ack_map.find(request_id);
                        if (it != pending_ack_map.end())
                        {
                            // it->second.ack_received[ack_client_id] = true;
                            --it->second.count;
                            // std::cout << it->second.count << std::endl;
                            // Notify the condition variable when all acks are received
                            if (it->second.count == 0)
                            {
                                // std::cout << "Sending signal that we received all acks" << std::endl;
                                it->second.ack_cv->notify_all();
                            }
                        }
                    }

                    // std::cout << "Acknowledgment received from client ID: " << ack_client_id<< " for request ID: " << request_id << std::endl;
                }
            }
            else
            {
                // Handle other incoming messages from the client if necessary
                // For now, we can ignore them or log them
                // std::cout << "Client stream closed for client ID: " << client_id << std::endl;
                break;
            }
        }
        // Currently, no condition variables are used; any additional message handling can be done here

        // Remove client session on disconnection
        {
            std::lock_guard<std::mutex> lock(client_mutex_);
            client_sessions_.erase(client_id);
        }

        // std::cout << "Client disconnected: ID = " << client_id << std::endl;
        return Status::OK;
    }

    // Method to send notifications to a client
    void
    SendNotification(int32_t client_id, const Notification &notification)
    {
        std::shared_ptr<ClientSession> client_session;
        {
            std::lock_guard<std::mutex> lock(client_mutex_);
            auto it = client_sessions_.find(client_id);
            if (it == client_sessions_.end())
            {
                std::cerr << "Client " << client_id << " is not registered." << std::endl;
                return;
            }
            client_session = it->second;
        }

        {
            std::lock_guard<std::mutex> lock(client_session->stream_mutex);
            // std::cout << "recieveid notifcation sending to client via stream" << std::endl;
            if (!client_session->stream->Write(notification))
            {
                std::cerr << "Failed to send notification to client ID: " << client_id << std::endl;
                // Optionally handle the failure (e.g., remove the client session)
            }
        }
    }

    // ... Existing methods like InitializeClient, CreateFile, etc. ...
    std::vector<Token> remove_overlap(const Token &token1, const Token &token2)
    {
        uint64_t start1 = token1.start_offset;
        uint64_t end1 = token1.start_offset + token1.length - 1;

        uint64_t start2 = token2.start_offset;
        uint64_t end2 = token2.start_offset + token2.length - 1;

        std::vector<Token> result;

        // Handle the part before the overlap
        if (start1 < start2)
        {
            result.push_back({token1.is_write,
                              start1,
                              std::min(start2 - start1, token1.length),
                              token1.client_id});
        }

        // Handle the part after the overlap
        if (end1 > end2)
        {
            uint64_t after_overlap_start = std::max(start1, end2 + 1);
            result.push_back({token1.is_write,
                              after_overlap_start,
                              end1 - after_overlap_start + 1,
                              token1.client_id});
        }

        return result;
    }
    // bool WaitForOverlappingTokens(const std::string &filename, uint64_t start_offset, uint64_t length)
    // {
    //     std::lock_guard<std::mutex> lock(waiting_map_mutex);
    //     uint64_t end_offset = start_offset + length - 1;
    //     bool all_completed = false;

    //     while (!all_completed)
    //     {
    //         all_completed = true;

    //         for (auto &[token_id, token] : waiting_map[filename])
    //         {
    //             uint64_t token_end = token->start_offset + token->length - 1;
    //             bool overlap = !(end_offset < token->start_offset || start_offset > token_end);

    //             if (overlap)
    //             {
    //                 std::unique_lock<std::mutex> lock(token->mtx);

    //                 // If the token is RUNNING, wait until it becomes COMPLETED
    //                 if (token->status == TokenStatus::RUNNING)
    //                 {
    //                     // // std::cout << "[DEBUG] Token is RUNNING. Waiting for completion...\n"
    //                               << "Overlapping Token: ID = " << token_id
    //                               << ", Start = " << token->start_offset
    //                               << ", Length = " << token->length
    //                               << ", Status = RUNNING" << std::endl;

    //                     token->cv.wait(lock, [&]()
    //                                    { return token->status == TokenStatus::COMPLETED; });

    //                     // // std::cout << "[DEBUG] Overlapping token completed: ID = " << token_id
    //                               << ", Start = " << token->start_offset
    //                               << ", Length = " << token->length << std::endl;

    //                     // Once awakened, recheck all tokens
    //                     all_completed = false;
    //                     break;
    //                 }
    //             }
    //         }
    //     }

    //     return true; // All overlapping tokens are now completed
    // }
    bool WaitForOverlappingTokens(const std::string &filename, uint64_t start_offset, uint64_t length)
{
    uint64_t end_offset = start_offset + length - 1;

    while (true) {
        std::unique_lock<std::mutex> map_lock(waiting_map_mutex);
        bool all_completed = true;
        TokenCv* running_token = nullptr;
        std::string running_token_id;


        // Iterate over all tokens for the given filename
        for (auto &[token_id, token_ptr] : waiting_map[filename]) {
            uint64_t token_end = token_ptr->start_offset + token_ptr->length - 1;
            bool overlap = !(end_offset < token_ptr->start_offset || start_offset > token_end);

            if (overlap && token_ptr->status == TokenStatus::RUNNING) {
                all_completed = false;
                running_token = token_ptr.get();
                running_token_id = token_id;
                break; // Wait for the first overlapping running token
            }
        }

        if (all_completed) {
            // No overlapping running tokens found
            return true;
        }

        // Release waiting_map_mutex before waiting on the token's condition variable
        map_lock.unlock();

        // Lock the specific token's mutex and wait for its completion
                // std::cout << "LOCKINGGGGGGGg" << filename <<"," << start_offset <<"," << length << std::endl;

        std::unique_lock<std::mutex> token_lock(running_token->mtx);
        // std::cout << "[DEBUG] Waiting for completion of Token ID: " << running_token_id << ", Start Offset: " << running_token->start_offset << ", Length: " << running_token->length << std::endl;

        // Wait until the token's status becomes COMPLETED
        running_token->cv.wait(token_lock, [&]() {
            return running_token->status == TokenStatus::COMPLETED;
        });

        // std::cout << "WAKED UPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP" << std::endl;

        // After waiting, loop again to check if any other overlapping tokens are still running
    }

    // This point is never reached
    return true;
}

    void MarkTokenCompleted(const std::string &filename, const std::string &token_id)
    {
        std::lock_guard<std::mutex> lock(waiting_map_mutex);

        auto &token = waiting_map[filename][token_id];
        if (token)
        {
            std::unique_lock<std::mutex> token_lock(token->mtx);
            token->status = TokenStatus::COMPLETED;
            std::cout << "[DEBUG] Token marked as COMPLETED: File = " << filename << ", Token ID = " << token_id << ", Start Offset = " << token->start_offset<< ", Length = " << token->length << std::endl;

            token->cv.notify_all(); // Notify threads waiting on this token
        }
        else
        {
            std::cerr << "[ERROR] Token not found: File = " << filename << ", Token ID = " << token_id << std::endl;
        }
    }

    // void DeleteToken(const std::string &filename, const std::string &token_id)
    // {
    //     std::lock_guard<std::mutex> lock(waiting_map_mutex);

    //     // Check if the filename exists
    //     auto file_it = waiting_map.find(filename);
    //     if (file_it != waiting_map.end())
    //     {
    //         auto &token_map = file_it->second; // Access the inner map

    //         // Check if the token ID exists
    //         auto token_it = token_map.find(token_id);
    //         if (token_it != token_map.end())
    //         {
    //             // Erase the token (automatically deletes the unique_ptr)
    //             token_map.erase(token_it);
    //             // // std::cout << "[DEBUG] Token deleted: File = " << filename << ", Token ID = " << token_id << std::endl;
    //         }
    //         else
    //         {
    //             std::cerr << "[ERROR] Token ID not found: " << token_id << " in file: " << filename << std::endl;
    //         }

    //         // If the inner map is empty after deletion, remove the filename entry
    //         if (token_map.empty())
    //         {
    //             waiting_map.erase(file_it);
    //             // // std::cout << "[DEBUG] All tokens for file deleted: " << filename << std::endl;
    //         }
    //     }
    //     else
    //     {
    //         std::cerr << "[ERROR] Filename not found: " << filename << std::endl;
    //     }
    // }

    // Update RequestToken to handle token revocation and notifications
    Status RequestToken(ServerContext *context, const TokenRequest *request, TokenResponse *response) override
    {
        // std::lock_guard<std::mutex> lkkkk(total_mutex);
        int fd = request->fd();
        bool is_write = request->is_write();

        {
            std::lock_guard<std::mutex> lock(fd_map_mutex);
            auto it = fd_map.find(fd);
            if (it == fd_map.end())
            {
                response->set_granted(false);
                response->set_message("Invalid file descriptor.");
                return Status::OK;
            }
        }

        uint64_t start_offset = request->start_offset();
        uint64_t length = request->length();
        int client_id = request->client_id();

        std::string filename;
        int mode;
        {
            std::lock_guard<std::mutex> lock(fd_map_mutex);
            filename = fd_map[fd].filename;
            mode = fd_map[fd].mode;
        }

    {
        std::lock_guard<std::mutex> lock(metadata_db_mutex);
        if (metadata_db.find(filename) == metadata_db.end())
        {
            response->set_granted(false);
            response->set_message("File not found.");
            return Status::OK;
        }
    }

        // Validate mode against the requested operation
        if (mode == 1 && is_write) // Read-only mode but write operation requested
        {
            response->set_granted(false);
            response->set_message("Mode mismatch: File opened in read-only mode. Write operation is not allowed.");
            return Status::OK;
        }
        {
            std::lock_guard<std::mutex> lock(metadata_db_mutex);

            FileData &file_data = metadata_db[filename];
            uint64_t file_size = file_data.size;

            // Validate the request
            if (start_offset > file_size)
            {
                response->set_granted(false);
                response->set_message("Offset exceeds file size.");
                return Status::OK;
            }

            // If it's a write request, update the file size if needed
            if (is_write)
            {
                uint64_t new_file_size = std::max(file_size, start_offset + length);
                if (new_file_size != file_size)
                {
                    file_data.size = new_file_size;
                    // std::cout << "File size updated to " << new_file_size << " for file " << filename << std::endl;
                }
            }
            else
            {
                // For read requests, adjust length to fit within the file size
                length = std::min(length, file_size - start_offset);
            }
        }
        if (length == 0) {
            uint64_t file_size;
            {
                std::lock_guard<std::mutex> lock(metadata_db_mutex);
                file_size = metadata_db[filename].size;
            }
            // Handle zero-length token gracefully
            response->set_granted(true);
            response->set_message("No data requested.");
            response->set_start_offset(start_offset);
            response->set_length(0);
            response->set_filesize(file_size);
            return Status::OK;
        }
        // Collect clients that need to be notified for revocation
        std::vector<int32_t> clients_to_notify;
        // Step 1: Initialize the PendingAck for this request
        std::srand(std::time(nullptr)); // Seed the random number generator
        int random_number = std::rand(); // Generate a random number

        std::string request_id = filename + ":" + std::to_string(start_offset) + ":" +
                                std::to_string(length) + ":" + std::to_string(client_id) +
                                ":" + std::to_string(random_number);
        WaitForOverlappingTokens(filename, start_offset, length);

        {
            std::lock_guard<std::mutex> lock(waiting_map_mutex);

            // Create a new TokenCv and add it to the waiting_map
            waiting_map[filename].emplace(request_id, std::make_unique<TokenCv>(is_write, start_offset, length, client_id, TokenStatus::RUNNING));

            std::cout << "[DEBUG] Token added: File = " << filename << ", Token ID = " << request_id << ", Start Offset = " << start_offset << ", Length = " << length << ", Status = RUNNING" << std::endl;
        }

        {
            std::unique_lock<std::mutex> lock(pending_ack_map_mutex);
            PendingAck pending_ack;
            pending_ack.count = 0;
            pending_ack.ack_cv = std::make_shared<std::condition_variable>();
            pending_ack_map[request_id] = pending_ack;
        }
        // auto &pending_ack = pending_ack_map[request_id];

        // Check for conflicts within existing tokens for the requested filename
        std::unordered_set<int> invalid_blocks;
        // auto &tokens = token_db[filename];
        // for (auto it = tokens.begin(); it != tokens.end();)
        // {
        //     auto &existing_token = *it;

        //     // Debug: Print details of the existing token and requested token
        //     // std::cout << " Token: "
        //               << "Start Offset: " << existing_token.start_offset
        //               << ", Length: " << existing_token.length
        //               << ", Client ID: " << existing_token.client_id
        //               << ", Is Write: " << existing_token.is_write << "\n";
        //     it++;
        // }
        // {
        //     std::unique_lock<std::mutex> lock(pending_ack_map_mutex);
        {
        std::lock_guard<std::mutex> lock(token_map_mutex);
        for (int it = 0; it < token_db[filename].size(); it++)
        {
            auto &existing_token = token_db[filename][it];

            // Debug: Print details of the existing token and requested token
            // std::cout << "Existing Token: "
                    //   << "Start Offset: " << existing_token.start_offset
                    //   << ", Length: " << existing_token.length
                    //   << ", Client ID: " << existing_token.client_id
                    //   << ", Is Write: " << existing_token.is_write << "\n";

            // std::cout << "Requested Token: "
                    //   << "Start Offset: " << start_offset
                    //   << ", Length: " << length
                    //   << ", Client ID: " << client_id
                    //   << ", Is Write: " << is_write << "\n";

            // Skip tokens from the same client
            // if (existing_token.client_id == client_id)
            // {
            //     continue;
            // }

            // Check for overlap/conflict
            bool overlap = !(start_offset + length <= existing_token.start_offset ||
                             existing_token.start_offset + existing_token.length <= start_offset);

            if (overlap && (is_write || existing_token.is_write))
            {
                // Debug: Print overlapping range
                uint64_t overlap_start = std::max(start_offset, existing_token.start_offset);
                uint64_t overlap_end = std::min(start_offset + length, existing_token.start_offset + existing_token.length);
                // std::cout << "Overlapping Range: "
                        //   << "Start: " << overlap_start
                        //   << ", End: " << overlap_end << "\n";

                clients_to_notify.push_back(existing_token.client_id);

                // Send revocation notification for the overlapping range
                Notification notification;
                notification.set_action("revoke");
                notification.set_filename(filename);
                notification.set_start_offset(overlap_start);
                notification.set_length(overlap_end - overlap_start);
                notification.set_message("Token revoked due to conflicting request.");
                notification.set_requested_by(request_id);
                // Track acknowledgment
                {
                    
                    std::unique_lock<std::mutex> lock(pending_ack_map_mutex);
                    auto &pending_ack = pending_ack_map[request_id];
                    pending_ack.ack_received[existing_token.client_id] = false;
                    ++pending_ack.count;
                }
                // Send notification
                SendNotification(existing_token.client_id, notification);
                // std::cout << "Revocation notification sent to client ID: " << existing_token.client_id << std::endl;
            }
            // }
        }
        }
        // Step 3: Wait for all acknowledgments
        {
            std::unique_lock<std::mutex> ack_lock(pending_ack_map_mutex);
            auto &pending_ack = pending_ack_map[request_id];
            // std::cout << "waiting for acks" << pending_ack.count << std::endl;
            pending_ack.ack_cv->wait(ack_lock, [&]
                                     { return pending_ack.count == 0; });
        }
        // std::cout << "Completed revoking, Now invalidation starts" << std::endl;
        //  {
        //     std::unique_lock<std::mutex> lock(pending_ack_map_mutex);
        // std::cout << "After lock and entering invalidation" << std::endl;
        std::vector<BlockInfo> blocks = SplitIntoBlocks(filename, start_offset, length);

        std::unordered_map<int, Block> prev_blocks_map;
        {
            std::lock_guard<std::mutex> lock(blocks_map_mutex);
            prev_blocks_map = deepCopyBlocksForFilename(blocksMap, filename);
        }        
        // std::cout << "adjusting tokens now" << std::endl;
        {
            std::lock_guard<std::mutex> lock(token_map_mutex);
            auto &tokens = token_db[filename];
            for (auto it = 0; it < tokens.size();)
            {
                Token &existing_token = tokens[it];

                // std::cout << "Processing Existing Token: "
                        //   << "Start Offset: " << existing_token.start_offset
                        //   << ", Length: " << existing_token.length << "\n";

                // Check for overlap/conflict
                bool overlap = !(start_offset + length <= existing_token.start_offset ||
                                 existing_token.start_offset + existing_token.length <= start_offset);
                if (!(overlap && (is_write || existing_token.is_write)))
                {
                    ++it; // Move to the next token if there's no overlap
                    continue;
                }

                std::vector<Token> result = remove_overlap(existing_token, {is_write, start_offset, length, client_id});

                token_db[filename].erase(token_db[filename].begin() + it);

                for (auto &t : result)
                {
                    token_db[filename].push_back(t);
                }
            }
            RemoveOverlappingTokens(token_db[filename], {is_write, start_offset, length, client_id});
        }

        {
            std::lock_guard<std::mutex> lock(token_map_mutex);
            auto &tokens = token_db[filename];
            for (auto it = tokens.begin(); it != tokens.end();)
        {
            auto &existing_token = *it;

            // Debug: Print details of the existing token and requested token
            // std::cout << " AFTER ADJUSTING TOKENS :::::  Token: "
                    //   << "Start Offset: " << existing_token.start_offset
                    //   << ", Length: " << existing_token.length
                    //   << ", Client ID: " << existing_token.client_id
                    //   << ", Is Write: " << existing_token.is_write << "\n";
            it++;
        }
        }
        
        UpdateBlocksMapFromTokens(filename);
        // Process each block
        std::vector<int> cachable;
        {
        std::lock_guard<std::mutex> lock(blocks_map_mutex);
        for (const auto &block : blocks)
        {
            

            // // std::cout << "[DEBUG] Processing Block: " << "Block Number = " << block.block_number  << ", Start Offset = " << block.start_offset << ", Length = " << block.length<< ", Server Index = " << block.server_index << std::endl;

            Block &block_info = blocksMap[filename][block.block_number];

            // Debug: Print information about the current block in blocksMap
            // std::cout << is_write << "[DEBUG] Block Info from blocksMap: "
                    //   << "Read Tokens = " << block_info.read_tokens
                    //   << ", Write Tokens = " << block_info.write_tokens
                    //   << ", Cached Clients = [";

            for (const auto &cached_client : block_info.inCache)
            {
                // std::cout << cached_client << " ";
            }
            // std::cout << "]" << std::endl;
            // Case: Current request is Write
            if (is_write)
            {
                // Send invalidation to all clients caching the block
                for (int cached_client : block_info.inCache)
                {
                    if (cached_client != client_id)
                    {
                        Notification notification;
                        notification.set_action("invalidate");
                        notification.set_filename(filename);
                        notification.set_start_offset(block.start_offset);
                        notification.set_length(block.length);
                        notification.set_message("Write request invalidating cache");
                        notification.set_requested_by(request_id);
                        notification.set_block_number(block.block_number);
                        {
                            
                            std::unique_lock<std::mutex> lock(pending_ack_map_mutex);
                            auto &pending_ack = pending_ack_map[request_id];
                            pending_ack.ack_received[cached_client] = false;
                            ++pending_ack_map[request_id].count;
                        }
                        SendNotification(cached_client, notification);
                    }
                }
                cachable.push_back(block.block_number);
                block_info.inCache.clear();
                block_info.inCache.insert(client_id);
                // // std::cout << "[DEBUG] Block Info from blocksMap after invalidation notification sent: " << "Read Tokens = " << block_info.read_tokens << ", Write Tokens = " << block_info.write_tokens<< ", Cached Clients = [";

                for (const auto &cached_client : block_info.inCache)
                {
                    // std::cout << cached_client << " ";
                }
                // std::cout << "]" << std::endl;
            }
            else
            {
                for (int cached_client : block_info.inCache)
                {
                    // std::cout << "Block " << block.block_number << " has write tokens, skipping caching." << std::endl;
                    if (cached_client != client_id)
                    {
                        bool has_write_tokens = block_info.write_tokens > 0;
                        bool prev_write_tokens = prev_blocks_map[block.block_number].write_tokens > 0;
                        // // std::cout << "[DEBUG] Checking block " << block.block_number << " for client " << cached_client << ". Has write tokens: " << (has_write_tokens ? "Yes" : "No") << std::endl;
                        if (prev_write_tokens && has_write_tokens == 0)
                        {
                            Notification notification;
                            notification.set_action("invalidate");
                            notification.set_filename(filename);
                            notification.set_start_offset(block.start_offset);
                            notification.set_length(block.length);
                            notification.set_message("Write request invalidating cache");
                            notification.set_requested_by(request_id);
                            notification.set_block_number(block.block_number);
                            {
                                
                                std::unique_lock<std::mutex> lock(pending_ack_map_mutex);
                                auto &pending_ack = pending_ack_map[request_id];
                                pending_ack.ack_received[cached_client] = false;
                                ++pending_ack_map[request_id].count;
                            }
                            SendNotification(cached_client, notification);
                            continue;
                        }
                        if (has_write_tokens)
                        {
                            // // std::cout << "[DEBUG] Block " << block.block_number<< " has write tokens. Requesting write-back from client "<< cached_client << std::endl;

                            // TODO : WriteBack the block but it can cache it
                            Notification notification;
                            notification.set_action("writeback");
                            notification.set_filename(filename);
                            notification.set_start_offset(block.start_offset);
                            notification.set_length(block.length);
                            notification.set_message("Write back the block but you can cache it");
                            notification.set_requested_by(request_id);
                            notification.set_block_number(block.block_number);

                            {
                                std::unique_lock<std::mutex> lock(pending_ack_map_mutex);
                                auto &pending_ack = pending_ack_map[request_id];

                                pending_ack.ack_received[cached_client] = false;
                                ++pending_ack_map[request_id].count;
                            }

                            SendNotification(cached_client, notification);

                            // // std::cout << "[DEBUG] Write-back notification sent to client "<< cached_client << " for block " << block.block_number << std::endl;

                            continue;
                        }
                    }
                }
                if (block_info.write_tokens == 0)
                {
                    // // std::cout << "[DEBUG] Adding block " << block.block_number<< " to cachable list for client " << client_id << std::endl;

                    cachable.push_back(block.block_number);
                    block_info.inCache.insert(client_id);

                    // // std::cout << "[DEBUG] Updated inCache list for block " << block.block_number<< ". Clients caching this block: ";
                    for (int client : block_info.inCache)
                    {
                        // std::cout << client << " ";
                    }
                    // std::cout << std::endl;
                }
            }
        }
        }
        // Wait for all invalidation acknowledgments
        {
            
            std::unique_lock<std::mutex> lock(pending_ack_map_mutex);
            auto &pending_ack = pending_ack_map[request_id];
            // std::cout << "waiting for acks" << pending_ack.count << std::endl;

            pending_ack_map[request_id].ack_cv->wait(lock, [&]()
                                                     { return pending_ack_map[request_id].count == 0; });
        }

        // std::cout << "All invalidations complete for request: " << request_id << std::endl;
        // Grant the new token and add it to the token list for the filename
        // TODO: add lock here as  it is shared
        // Token new_token = {is_write, start_offset, length, client_id};
        // tokens.push_back(new_token);
        // Step 4: Grant the token
        Token new_token = {is_write, start_offset, length, client_id};

        // std::cout << "Granted New Token: "<< "Start Offset: " << new_token.start_offset << ", Length: " << new_token.length<< ", Client ID: " << new_token.client_id<< ", Is Write: " << new_token.is_write << "\n";
        {
            std::lock_guard<std::mutex> lock(token_map_mutex);
            token_db[filename].push_back(new_token);
        }
        // std::cout << "DONEEEEEEEEEEEEEEEEEEEE" << std::endl;
        MarkTokenCompleted(filename, request_id);
        ManageTokenRange(filename, start_offset, length, is_write, client_id);
        // Debug: Print granted token details

        response->set_granted(true);
        response->set_start_offset(start_offset);
        response->set_length(length);
        response->set_message("Token granted successfully.");
        uint64_t file_size;
    {
        std::lock_guard<std::mutex> lock(metadata_db_mutex);
        file_size = metadata_db[filename].size;
    }
        response->set_filesize(file_size);
        for (auto &block : cachable)
        {
            response->add_cacheable_blocks(block);
        }
        {
        std::lock_guard<std::mutex> lock(token_map_mutex);
        auto &tokens = token_db[filename];
        for (auto it = tokens.begin(); it != tokens.end(); it++)
        {
            auto &existing_token = *it;
            // std::cout << " AFTER GRANTING TOKENS :::::  Token: "<< "Start Offset: " << existing_token.start_offset<< ", Length: " << existing_token.length<< ", Client ID: " << existing_token.client_id<< ", Is Write: " << existing_token.is_write << "\n";
        }
    }
        return Status::OK;
    }
    Status InvalidateBlock(ServerContext *context, const InvalidateBlockRequest *request, InvalidateBlockResponse *response) override
    {
        // std::cout << "INVALIDATE BLOCK REQUEST" << std::endl;
 
        std::string filename = request->filename();
        uint64_t block_number = request->block_number();
        int requesting_client_id = request->client_id();

        std::lock_guard<std::mutex> lock(blocks_map_mutex);

        // Check if the file exists in the blocksMap
        if (blocksMap.find(filename) == blocksMap.end())
        {
            response->set_success(false);
            response->set_message("File not found in blocksMap.");
            return Status::OK;
        }

        // Check if the block exists for the file
        auto &file_blocks = blocksMap[filename];
        if (file_blocks.find(block_number) == file_blocks.end())
        {
            response->set_success(false);
            response->set_message("Block not found for the specified file.");
            return Status::OK;
        }

        // Access the block's metadata
        Block &block_info = file_blocks[block_number];

        // Collect all clients that have cached this block (except the requesting client)
        std::vector<int> clients_to_notify;
        for (int cached_client_id : block_info.inCache)
        {
            if (cached_client_id != requesting_client_id)
            {
                clients_to_notify.push_back(cached_client_id);
            }
        }

        // If no clients have this block cached, return success immediately
        if (clients_to_notify.empty())
        {
            response->set_success(true);
            response->set_can_cache(true);
            block_info.inCache.insert(requesting_client_id);
            // is_cache = true;
            response->set_message("Block invalidated successfully. No clients to notify.");
            return Status::OK;
        }

        // Set up acknowledgment tracking
        std::random_device rd; // Seed for random number engine
        std::mt19937 gen(rd()); // Mersenne Twister random number engine
        std::uniform_int_distribution<int> dist(1, 1000000); // Generate a random number between 1 and 1,000,000

        int random_number = dist(gen); // Generate the random number

        std::string request_id = filename + ":" + std::to_string(block_number) + ":" + 
                                std::to_string(requesting_client_id) + ":" + 
                                std::to_string(random_number);
        // std::cout << request_id << std::endl;

        {
            std::unique_lock<std::mutex> lock(pending_ack_map_mutex);
            PendingAck pending_ack;
            pending_ack.count = 0;
            pending_ack.ack_cv = std::make_shared<std::condition_variable>();

            pending_ack_map[request_id] = pending_ack;
        }
        auto &pending_ack = pending_ack_map[request_id];
        // Send invalidation notifications to all cached clients
        std::vector<int> cached;
        for (int client_id : clients_to_notify)
        {
            if (request->is_write())
            {
                Notification notification;
                notification.set_action("invalidate");
                notification.set_filename(filename);
                notification.set_block_number(block_number);
                notification.set_message("Invalidate cache for block due to conflict.");
                notification.set_requested_by(request_id);
                {
                    std::unique_lock<std::mutex> lock(pending_ack_map_mutex);

                    pending_ack.ack_received[client_id] = false;
                    ++pending_ack_map[request_id].count;
                }
                SendNotification(client_id, notification);
                // std::cout << "Sent invalidation notification for block " << block_number << " to client " << client_id << "." << std::endl;
            }
            else
            {
                bool has_write_tokens = block_info.write_tokens > 0;
                if (has_write_tokens)
                {
                    // std::cout << "Read Invalidate request Block " << block_number << " has write tokens, skipping caching." << std::endl;
                    // TODO : WriteBack the block but it can cache it
                    Notification notification;
                    notification.set_action("writeback");
                    notification.set_filename(filename);
                    // notification.set_start_offset(block_info.start_offset);
                    // notification.set_length(block_info.length);
                    notification.set_message("Write back the block but you can cache it");
                    notification.set_requested_by(request_id);
                    notification.set_block_number(block_number);
                    {
                        std::unique_lock<std::mutex> lock(pending_ack_map_mutex);

                        pending_ack.ack_received[client_id] = false;
                        ++pending_ack_map[request_id].count;
                    }
                    SendNotification(client_id, notification);
                    cached.push_back(client_id);
                }
            }
        }

        // Wait for all invalidation acknowledgments
        {
            std::unique_lock<std::mutex> lock(pending_ack_map_mutex);
            pending_ack_map[request_id].ack_cv->wait(lock, [&]()
                                                     { return pending_ack_map[request_id].count == 0; });
        }
        for (int client_id : clients_to_notify)
        {
            // // std::cout << "[DEBUG] Notifying client ID: " << client_id << " to remove block from cache." << std::endl;
            // Remove the client ID from the block's inCache set
            auto it = block_info.inCache.find(client_id);
            if (it != block_info.inCache.end())
            {
                block_info.inCache.erase(it);
                // // std::cout << "[DEBUG] Successfully removed client ID: " << client_id << " from inCache." << std::endl;
            }
            else
            {
                // // std::cout << "[DEBUG] Client ID: " << client_id << " not found in inCache. No action taken." << std::endl;
            }
        }

        // Clear the block's inCache list for all notified clients
        bool is_cache = false;

        if (request->is_write())
        {
            // // std::cout << "[DEBUG] Write request from client ID: " << requesting_client_id << std::endl;

            block_info.inCache.insert(requesting_client_id);
            is_cache = true;

            // // std::cout << "[DEBUG] Added requesting client ID: " << requesting_client_id << " to inCache for write operation." << std::endl;
        }
        else
        {
            // // std::cout << "[DEBUG] Read request from client ID: " << requesting_client_id << std::endl;

            if (block_info.write_tokens > 0)
            {
                // // std::cout << "[DEBUG] Write tokens are present. Adding cached clients back to inCache." << std::endl;

                for (auto &c_id : cached)
                {
                    block_info.inCache.insert(c_id);
                    // // std::cout << "[DEBUG] Added cached client ID: " << c_id << " back to inCache." << std::endl;
                }
            }
            else
            {
                block_info.inCache.insert(requesting_client_id);
                is_cache = true;

                // // std::cout << "[DEBUG] Added requesting client ID: " << requesting_client_id << " to inCache for read operation." << std::endl;
            }
        }

        // Final state of inCache for debugging
        // // std::cout << "[DEBUG] Final inCache state for block: ";
        for (const auto &client_id : block_info.inCache)
        {
            // std::cout << client_id << " ";
        }
        // std::cout << std::endl;

        // Clean up the PendingAck entry
        {
            std::unique_lock<std::mutex> lock(pending_ack_map_mutex);
            pending_ack_map.erase(request_id);
        }

        // Return success
        response->set_success(true);
        response->set_message("Block invalidated successfully.");
        response->set_can_cache(is_cache);
        return Status::OK;
    }

    Status Alive(ServerContext *context, const google::protobuf::Empty *request, MetaStatus *response) override
    {
        response->set_success(true);
        response->set_message("Meta server is alive and responding.");
        // std::cout << "META SERVER ALIVe" << std::endl;
        return Status::OK;
    }
    // InitializeClient assigns a unique client ID
    Status InitializeClient(ServerContext *context, const ClientInitRequest *request, ClientInitResponse *response) override
    {
        std::lock_guard<std::mutex> lock(client_id_mutex); // Lock mutex for thread safety
        static int client_id_counter = 1;
        response->set_client_id(client_id_counter++);
        response->set_success(true);
        response->set_message("Client initialized successfully.");
        return Status::OK;
    }

    // CreateFile adds a new file to metadata_db if it doesn’t already exist
    Status CreateFile(ServerContext *context, const CreateFileRequest *request, MetaStatus *response) override
{
    std::string filename = request->filename();
    int stripe_width = request->stripe_width();

    // Check if the file already exists
    {
        std::lock_guard<std::mutex> lock(metadata_db_mutex);
        if (metadata_db.find(filename) != metadata_db.end())
        {
            response->set_success(false);
            response->set_message("File already exists.");
            return Status::OK;
        }
    }

    // Add file metadata
    FileData file_data;
    file_data.filename = filename;
    file_data.size = 0;
    file_data.recipe.stripe_width = stripe_width;
    file_data.creation_time = time(nullptr);

    // Insert metadata into metadata_db
    {
        std::lock_guard<std::mutex> lock(metadata_db_mutex);
        metadata_db[filename] = file_data;
    }

    response->set_success(true);
    response->set_message("File created successfully.");
    return Status::OK;
}

    Status OpenFile(ServerContext *context, const OpenFileRequest *request, OpenFileResponse *response) override
{
    std::string filename = request->filename();
    int requested_mode = request->mode(); // 1 = read, 2 = read/write

    // Check if the file exists in metadata_db
    {
        std::lock_guard<std::mutex> lock(metadata_db_mutex); // Lock to ensure safe access to metadata_db
        auto it = metadata_db.find(filename);
        if (it == metadata_db.end())
        {
            response->set_success(false);
            response->set_message("File does not exist.");
            return Status::OK;
        }
    }

    // Locking around next_fd increment to ensure thread safety
    int32_t fd;
    {
        std::lock_guard<std::mutex> lock(fd_mutex); // Lock for thread-safe increment
        fd = next_fd++;
    }

    // Store filename and mode in FdInfo and add to fd_map
    {
        std::lock_guard<std::mutex> lock(fd_map_mutex);
        fd_map[fd] = {filename, requested_mode};
        opened_fd[filename].insert(fd);
    }

    // Populate response with file metadata and FD
    response->set_fd(fd);
    response->set_success(true);
    response->set_message("File opened successfully.");

    return Status::OK;
}


    Status CloseFile(ServerContext *context, const CloseFileRequest *request, MetaStatus *response) override
{
    int32_t fd = request->fd();
    int32_t client_id = request->client_id();

    // Check if the file descriptor is valid
    std::string filename;
    {
        std::lock_guard<std::mutex> lock(fd_map_mutex);
        auto it = fd_map.find(fd);
        if (it == fd_map.end())
        {
            response->set_success(false);
            response->set_message("Invalid file descriptor.");
            return Status::OK;
        }

        // Get the filename and mode from fd_map
         filename = it->second.filename;
        int mode = it->second.mode; // Retrieve mode if needed for any cleanup

        fd_map.erase(it); // Remove FD from fd_map
    }

    // Remove tokens associated with this client ID
    {
        std::lock_guard<std::mutex> lock(token_map_mutex);

        auto token_it = token_db.find(filename);
        if (token_it != token_db.end())
        {
            auto &tokens = token_it->second;
            tokens.erase(
                std::remove_if(tokens.begin(), tokens.end(),
                               [&](const Token &t)
                               { return t.client_id == client_id; }),
                tokens.end());

            if (tokens.empty())
            {
                token_db.erase(token_it);
            }

            // // std::cout << "[DEBUG] Removed tokens for client: " << client_id << " Filename: " << filename << std::endl;
        }
    }

    // Update blocksMap from tokens and handle inCache
    {
        std::lock_guard<std::mutex> lock(blocks_map_mutex);

        auto block_it = blocksMap.find(filename);
        if (block_it != blocksMap.end())
        {
            auto &blocks = block_it->second;

            for (auto &[block_num, block] : blocks)
            {
                // Remove client ID from inCache
                auto &cache = block.inCache;
                size_t erased_count = cache.erase(client_id);

                if (erased_count > 0)
                {
                    // // std::cout << "[DEBUG] Removed client ID " << client_id << " from inCache for block " << block_num << std::endl;
                }
                else
                {
                    // // std::cout << "[DEBUG] Client ID " << client_id << " not found in inCache for block " << block_num << std::endl;
                }
            }

            // // std::cout << "[DEBUG] Updated blocksMap for filename: " << filename << std::endl;
        }
    
    }

    // Update blocksMap based on remaining tokens
    UpdateBlocksMapFromTokens(filename);

    // Update the last close time and handle opened_fd
    {
        std::lock_guard<std::mutex> lock(fd_map_mutex);

        if (opened_fd.find(filename) != opened_fd.end())
        {
            opened_fd[filename].erase(fd);
            if (opened_fd[filename].empty())
            {
                opened_fd.erase(filename);
            }
        }
    }
     {
        std::lock_guard<std::mutex> lock(metadata_db_mutex);
        metadata_db[filename].last_close_time = time(nullptr);
     }
    response->set_success(true);
    response->set_message("File closed successfully.");
    return Status::OK;
}


    // DeleteFile removes a file from metadata_db if it exists
    Status DeleteFile(ServerContext *context, const FileIdentifier *request, MetaStatus *response) override
{
    std::string filename = request->filename();
    {
        std::lock_guard<std::mutex> lock(fd_map_mutex);

        auto it = opened_fd.find(filename);
        if (it != opened_fd.end() && !it->second.empty())
        {
            response->set_success(false);
            response->set_message("File is currently opened by a client. Please close it before deleting.");
            return Status::OK;
        }
    }
    // Lock to safely remove file metadata
    {
        std::lock_guard<std::mutex> lock(metadata_db_mutex);

        if (metadata_db.erase(filename) == 0)
        {
            response->set_success(false);
            response->set_message("File not found.");
            return Status::OK;
        }
    }
    // TODO: BlocksMap delete

    // Lock to check and handle opened file descriptors
    

    // File deleted successfully
    response->set_success(true);
    response->set_message("File deleted successfully.");
    return Status::OK;
}


    // GetFileMetadata retrieves metadata for a given file
    Status GetFileMetadata(ServerContext *context, const FileIdentifier *request, FileMetadata *response) override
    {
        std::string filename = request->filename();

        // Lock to safely access metadata_db
        {
            std::lock_guard<std::mutex> lock(metadata_db_mutex);

            // Check if the file exists in metadata_db
            if (metadata_db.find(filename) == metadata_db.end())
            {
                return Status(grpc::NOT_FOUND, "File not found");
            }

            // Retrieve the file metadata
            const FileData &file_data = metadata_db[filename];
            response->set_filename(file_data.filename);
            response->set_size(file_data.size);
            response->set_creation_time(file_data.creation_time);
            response->set_last_close_time(file_data.last_close_time);

            auto *recipe = response->mutable_recipe();
            recipe->set_block_size(file_data.recipe.block_size);
            recipe->set_stripe_width(file_data.recipe.stripe_width);
            recipe->set_stripe_blocks(file_data.recipe.stripe_blocks);
        }

        return Status::OK;
    }

    // RequestToken grants a read or write token if available

    // ReleaseToken releases the token held by a client
    Status ReleaseToken(ServerContext *context, const TokenRequest *request, MetaStatus *response) override
    {
        int fd = request->fd();
        // std::string filename(fd_map[fd].filename);
        // int client_id = request->client_id();

        // if (token_db.find(filename) != token_db.end() && token_db[filename].client_id == client_id) {
        //     token_db.erase(filename);
        //     response->set_success(true);
        //     response->set_message("Token released successfully.");
        // } else {
        //     response->set_success(false);
        //     response->set_message("No matching token found or unauthorized release.");
        // }
        return Status::OK;
    }
};

// Run the Metadata Server
void RunMetadataServer(const std::string &server_address)
{
    MetadataServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    // std::cout << "Metadata server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char *argv[])
{
    printf("%s:%s: PFS meta server start! Hostname: %s, IP: %s\n", __FILE__,
           __func__, getMyHostname().c_str(), getMyIP().c_str());

    // Parse pfs_list.txt
    std::ifstream pfs_list("../pfs_list.txt");
    if (!pfs_list.is_open())
    {
        fprintf(stderr, "%s: can't open pfs_list.txt file.\n", __func__);
        exit(EXIT_FAILURE);
    }

    std::string line;
    std::getline(pfs_list, line);
    if (line.substr(0, line.find(':')) != getMyHostname())
    {
        fprintf(stderr, "%s: hostname not on the first line of pfs_list.txt.\n",
                __func__);
        exit(EXIT_FAILURE);
    }
    pfs_list.close();
    std::string listen_port = line.substr(line.find(':') + 1);
    std::string server_address = "0.0.0.0:" + listen_port;

    // Run the PFS metadata server and listen to requests
    printf("%s: Launching PFS metadata server on %s, with listen port %s...\n",
           __func__, getMyHostname().c_str(), listen_port.c_str());

    RunMetadataServer(server_address);

    printf("%s:%s: PFS meta server done!\n", __FILE__, __func__);
    return 0;
}
