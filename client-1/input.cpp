#include <iostream>
#include <fstream>
#include <thread>
#include <chrono>
#include <cstring>
#include <vector>
#include "pfs_common/pfs_common.hpp"
#include "pfs_client/pfs_api.hpp"

void process_operation(int fd, const std::string& operation, uint64_t start, uint64_t length) {
    if (operation == "read") {
        char* buf = new char[length];
        memset(buf, 0, length);

        int bytes_read = pfs_read(fd, buf, length, start);
        if (bytes_read >= 0) {
            std::cout << "[DEBUG] Read " << bytes_read << " bytes from range [" << start << ", " << start + length - 1 << "]: "
                      << std::string(buf, bytes_read) << std::endl;
        } else {
            std::cerr << "[ERROR] Failed to read from range [" << start << ", " << start + length - 1 << "]." << std::endl;
        }

        delete[] buf;
    } else if (operation == "write") {
        char* buf = new char[length];
        memset(buf, 'X', length); // Fill with dummy data

        int bytes_written = pfs_write(fd, buf, length, start);
        if (bytes_written == length) {
            std::cout << "[DEBUG] Wrote " << bytes_written << " bytes to range [" << start << ", " << start + length - 1 << "]." << std::endl;
        } else {
            std::cerr << "[ERROR] Failed to write to range [" << start << ", " << start + length - 1 << "]." << std::endl;
        }

        delete[] buf;
    } else {
        std::cerr << "[ERROR] Invalid operation: " << operation << std::endl;
    }
}

int main() {
    // Prompt user for Client ID
    std::cout << "Enter Client ID: ";
    int client_id;
    std::cin >> client_id;

    // Initialize PFS
    int c_id = pfs_initialize();
    if (c_id < 0) {
        std::cerr << "[ERROR] Failed to initialize PFS for Client " << client_id << std::endl;
        return -1;
    }

    // Get file name
    std::cout << "Enter file name: ";
    std::string filename;
    std::cin >> filename;

    // Create file if this is Client 1
    if (client_id == 1) {
        if (pfs_create(filename.c_str(), 3) != 0) {
            std::cerr << "[ERROR] Failed to create file: " << filename << std::endl;
            return -1;
        }
        std::cout << "[DEBUG] File created successfully: " << filename << std::endl;
    }

    // Open file
    int fd = pfs_open(filename.c_str(), 2);
    if (fd < 0) {
        std::cerr << "[ERROR] Failed to open the file: " << filename << std::endl;
        pfs_finish(c_id);
        return -1;
    }
    std::cout << "[DEBUG] File opened successfully with FD: " << fd << std::endl;

    // Input-based operations
    std::cout << "Input operations in the format: <operation> <start_offset> <length>\n";
    std::cout << "Example: read 100 50\n";
    std::cout << "Type 'exit' to quit.\n";

    while (true) {
        std::string operation;
        uint64_t start, length;

        std::cout << "Enter operation: ";
        std::cin >> operation;

        if (operation == "exit") {
            break;
        }

        std::cin >> start >> length;

        if (std::cin.fail()) {
            std::cerr << "[ERROR] Invalid input. Please enter in the format: <operation> <start_offset> <length>\n";
            std::cin.clear(); // Clear the error flag
            std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n'); // Discard invalid input
            continue;
        }

        process_operation(fd, operation, start, length);
    }

    // Close file and finish
    if (pfs_close(fd) != 0) {
        std::cerr << "[ERROR] Failed to close the file." << std::endl;
    } else {
        std::cout << "[DEBUG] File closed successfully." << std::endl;
    }

    if (pfs_finish(c_id) != 0) {
        std::cerr << "[ERROR] Failed to finish PFS for Client " << client_id << std::endl;
    } else {
        std::cout << "[DEBUG] PFS finalized successfully for Client " << client_id << std::endl;
    }

    return 0;
}
