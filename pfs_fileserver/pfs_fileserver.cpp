#include <iostream>
#include <fstream>
#include <string>
#include <grpcpp/grpcpp.h>
#include "pfs_proto/pfs_fileserver.grpc.pb.h"
#include "pfs_fileserver.hpp"
#include "google/protobuf/empty.pb.h" // Include for google.protobuf.Empty

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using pfsfile::DeleteBlockRequest;
using pfsfile::FileServerService;
using pfsfile::FileServerStatus;
using pfsfile::ReadBlockRequest;
using pfsfile::ReadBlockResponse;
using pfsfile::WriteBlockRequest;
namespace fs = std::filesystem;

const std::string PFS_STORAGE_PATH = "";
// Implementation of FileServerService
class FileServerServiceImpl final : public FileServerService::Service
{
public:
    // Alive implementation for health check
    Status Alive(ServerContext *context, const google::protobuf::Empty *request, FileServerStatus *response) override
    {
        response->set_success(true);
        response->set_message("File server is alive and responding.");
        // std::cout << "File SERVER ALIVe" << std::endl;

        return Status::OK;
    }

    Status ReadBlock(ServerContext *context, const pfsfile::ReadBlockRequest *request, pfsfile::ReadBlockResponse *response) override
    {
        std::string filename = request->block().filename();
        uint64_t block_number = request->block().block_number();
        uint64_t offset = request->offset();
        uint64_t num_bytes = request->num_bytes();

        // Construct the file path for the block
        std::string file_dir = PFS_STORAGE_PATH + filename;
        std::string block_path = file_dir + "/block_" + std::to_string(block_number);

        // Check if the block file exists
        if (!fs::exists(block_path))
        {
            std::cerr << "ReadBlock: Block file " << block_path << " does not exist." << std::endl;
            response->set_success(false);
            response->set_message("Block file does not exist.");
            return Status::OK;
        }

        // Open block file for reading in binary mode
        std::ifstream block_file(block_path, std::ios::binary);
        if (!block_file)
        {
            std::cerr << "ReadBlock: Failed to open block file " << block_path << " for reading." << std::endl;
            response->set_success(false);
            response->set_message("Failed to open block file for reading.");
            return Status::OK;
        }

        // Seek to the specified offset within the block
        block_file.seekg(offset, std::ios::beg);
        if (block_file.fail())
        {
            std::cerr << "ReadBlock: Failed to seek to offset " << offset << " in block file " << block_path << std::endl;
            response->set_success(false);
            response->set_message("Failed to seek to the specified offset.");
            block_file.close();
            return Status::OK;
        }

        // Read the specified number of bytes
        std::vector<char> buffer(num_bytes);
        block_file.read(buffer.data(), num_bytes);
        uint64_t bytes_read = block_file.gcount();
        block_file.close();

        if (bytes_read == 0)
        {
            std::cerr << "ReadBlock: No data read from block file " << block_path << std::endl;
            response->set_success(false);
            response->set_message("No data read from block file.");
            return Status::OK;
        }

        // Set the response data
        response->set_success(true);
        response->set_data(buffer.data(), bytes_read);
        response->set_bytes_read(bytes_read);
        response->set_message("Block read successfully.");

        return Status::OK;
    }

    Status WriteBlock(ServerContext *context, const pfsfile::WriteBlockRequest *request, pfsfile::FileServerStatus *response) override
    {
        std::string filename = request->block().filename();
        uint64_t block_number = request->block().block_number();

        std::string data = request->data();
        uint64_t offset_in_block = request->offset_in_block();
        uint64_t length = request->length();

        // Construct file path for the block
        std::string file_dir = PFS_STORAGE_PATH + filename;
        std::string block_path = file_dir + "/block_" + std::to_string(block_number);

        // Create the file directory if it does not exist
        if (!fs::exists(file_dir))
        {
            std::error_code ec;
            // std::cout << fs::exists(file_dir, ec) << "\n";
            // // std::cout << "[DEBUG] Directory does not exist: " << file_dir << std::endl;

            // Check if the path is valid
            if (!file_dir.empty())
            {
                // // std::cout << "[DEBUG] Path is valid and non-empty: " << file_dir << std::endl;
            }
            else
            {
                std::cerr << "[ERROR] Path is empty!" << std::endl;
            }

            // Log the permissions of the parent directory if possible
            auto parent_path = fs::path(file_dir).parent_path();
            if (fs::exists(parent_path))
            {
                auto perms = fs::status(parent_path).permissions();
                // // std::cout << "[DEBUG] Parent directory exists: " << parent_path << std::endl;
                // // std::cout << "[DEBUG] Parent directory permissions: "<< ((perms & fs::perms::owner_read) != fs::perms::none ? "r" : "-") << ((perms & fs::perms::owner_write) != fs::perms::none ? "w" : "-")<< ((perms & fs::perms::owner_exec) != fs::perms::none ? "x" : "-") << std::endl;
            }
            else
            {
                std::cerr << "[ERROR] Parent directory does not exist: " << parent_path << std::endl;
            }

            // If it doesn't exist, attempt to create it
            try
            {
                if (!fs::create_directories(file_dir))
                {
                    std::cerr << "[ERROR] Failed to create directory: " << file_dir << std::endl;
                }
                else
                {
                    // // std::cout << "[DEBUG] Successfully created directory: " << file_dir << std::endl;
                }
            }
            
            catch (const std::exception &e)
            {
                std::cerr << "[EXCEPTION] Exception while creating directory: " << e.what() << std::endl;
            }
        }
        else
        {
            // // std::cout << "[DEBUG] Directory already exists: " << file_dir << std::endl;
        }

        // Open block file for writing in binary mode, and create it if it doesn't exist
        std::ofstream block_file(block_path, std::ios::binary | std::ios::in | std::ios::out);
        if (!block_file.is_open())
        {
            // If the file doesn't exist, open it in output mode to create it
            block_file.open(block_path, std::ios::binary | std::ios::out);
            block_file.close();
            block_file.open(block_path, std::ios::binary | std::ios::in | std::ios::out);
        }

        if (!block_file)
        {
            std::cerr << "WriteBlock: Failed to open block file " << block_path << std::endl;
            response->set_success(false);
            response->set_message("Failed to open block file.");
            return Status::OK;
        }

        // Seek to the specified offset within the block
        block_file.seekp(offset_in_block, std::ios::beg);
        if (block_file.fail())
        {
            std::cerr << "WriteBlock: Failed to seek to offset " << offset_in_block << " in block file " << block_path << std::endl;
            response->set_success(false);
            response->set_message("Failed to seek to the specified offset.");
            block_file.close();
            return Status::OK;
        }

        // Write the data to the block file
        block_file.write(data.data(), length);
        if (block_file.fail())
        {
            std::cerr << "WriteBlock: Failed to write data to block file " << block_path << std::endl;
            response->set_success(false);
            response->set_message("Failed to write data to the block file.");
            block_file.close();
            return Status::OK;
        }

        block_file.close();

        // Set response to success
        response->set_success(true);
        response->set_message("Block written successfully.");
        return Status::OK;
    }

    // DeleteBlock implementation
    // FileServer implementation

    Status DeleteFileBlocks(grpc::ServerContext *context, const pfsfile::FileBlocks *request, pfsfile::FileServerStatus *response)
    {
        // Extract the filename and list of blocks to delete from the request
        std::string filename = request->filename();
        std::vector<int> blocks_to_delete;

        // Convert RepeatedField<int> to std::vector<int>
        for (int i = 0; i < request->block_numbers_size(); ++i)
        {
            blocks_to_delete.push_back(request->block_numbers(i));
        }

        // // std::cout << "[DEBUG] DeleteFileBlocks: Received delete request for file: " << filename << std::endl;

        // Step 1: Check if the blocks exist in cache (if applicable)
        for (int block_num : blocks_to_delete)
        {
            // Check if the block exists in the server's cache or storage
            std::string block_path = getBlockPath(filename, block_num);

            if (!fs::exists(block_path))
            {
                std::cerr << "[ERROR] DeleteFileBlocks: Block " << block_num << " for file " << filename
                          << " does not exist." << std::endl;
                continue; // Skip to the next block
            }

            // Delete the block file from the storage (disk)
            try
            {
                if (fs::remove(block_path))
                {
                    // std::cout << "[INFO] DeleteFileBlocks: Successfully deleted block " << block_num << " for file " << filename << std::endl;
                }
                else
                {
                    std::cerr << "[ERROR] DeleteFileBlocks: Failed to delete block " << block_num << " for file " << filename << std::endl;
                    response->set_success(false);
                    response->set_message("Failed to delete block file.");
                    return grpc::Status::OK;
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << "[ERROR] DeleteFileBlocks: Exception occurred while deleting block " << block_num
                          << " for file " << filename << ": " << e.what() << std::endl;
                response->set_success(false);
                response->set_message("Exception occurred while deleting block.");
                return grpc::Status::OK;
            }
        }

        // Step 3: Send success response
        response->set_success(true);
        response->set_message("All blocks deleted successfully.");
        return grpc::Status::OK;
    }

    // Helper functions (example)

    std::string getBlockPath(const std::string &filename, int block_num)
    {
        // Build the full path to the block file based on filename and block number
        return PFS_STORAGE_PATH + filename + "/block_" + std::to_string(block_num);
    }
};

// Run the File Server
void RunFileServer(const std::string &server_address)
{
    FileServerServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    // std::cout << "File server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char *argv[])
{
    printf("%s:%s: PFS file server start! Hostname: %s, IP: %s\n",
           __FILE__, __func__, getMyHostname().c_str(), getMyIP().c_str());

    // Parse pfs_list.txt
    std::ifstream pfs_list("../pfs_list.txt");
    if (!pfs_list.is_open())
    {
        fprintf(stderr, "%s: can't open pfs_list.txt file.\n", __func__);
        exit(EXIT_FAILURE);
    }

    bool found = false;
    std::string line;
    std::getline(pfs_list, line); // Skip first line (metadata server)
    while (std::getline(pfs_list, line))
    {
        if (line.substr(0, line.find(':')) == getMyHostname())
        {
            found = true;
            break;
        }
    }
    if (!found)
    {
        fprintf(stderr, "%s: hostname not found in pfs_list.txt.\n", __func__);
        exit(EXIT_FAILURE);
    }
    pfs_list.close();
    std::string listen_port = line.substr(line.find(':') + 1);
    std::string server_address = "0.0.0.0:" + listen_port;

    // Run the PFS file server and listen to requests
    printf("%s: Launching PFS file server on %s, with listen port %s...\n",
           __func__, getMyHostname().c_str(), listen_port.c_str());

    RunFileServer(server_address);

    printf("%s:%s: PFS file server done!\n", __FILE__, __func__);
    return 0;
}
