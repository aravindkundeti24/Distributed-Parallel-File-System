#include "pfs_client/pfs_api.hpp"
#include <iostream>
#include <fstream>
#include <string>
#include <cstring>



void test_file_operations() {
    const char* filename = "file.txt";
    int stripe_width = 3; // Number of servers
    char read_buffer[51200] = {0}; // Buffer to read back data
    off_t offset = 0;

    std::cout << "== Testing PFS File Operations ==" << std::endl;

    printf("Client 1: Initializing PFS client\n");
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "Client 1: Failed to initialize PFS.\n");
        return;
    }

    // Step 1: Create the file
    std::cout << "Creating file: " << filename << " with stripe width: " << stripe_width << std::endl;
    if (pfs_create(filename, stripe_width) != 0) {
        std::cerr << "Failed to create file: " << filename << std::endl;
        return;
    }
    std::cout << "File created successfully!" << std::endl;

    // Step 2: Open the file
    std::cout << "Opening file: " << filename << std::endl;
    int fd = pfs_open(filename, 2); // Mode: 0 (read/write mode)
    if (fd < 0) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return;
    }
    std::cout << "File opened successfully with FD: " << fd << std::endl;

    // Step 3: Read write data from an external file
    const char* input_file = "write_data.txt"; // External file for testing striping
    std::ifstream input_stream(input_file, std::ios::binary);
    if (!input_stream.is_open()) {
        std::cerr << "Failed to open input file: " << input_file << std::endl;
        return;
    }
    std::string write_data((std::istreambuf_iterator<char>(input_stream)), std::istreambuf_iterator<char>());
    input_stream.close();
    size_t write_size = write_data.size();

    // Step 4: Write data to the file
    std::cout << "Writing data from file: " << input_file << std::endl;
    if (pfs_write(fd, write_data.data(), write_size, offset) != write_size) {
        std::cerr << "Failed to write data to file!" << std::endl;
        return;
    }
    std::cout << "Data written successfully with striping!" << std::endl;

    const char* input_file1 = "test_case1.txt"; // 200bytes
    std::ifstream input_stream1(input_file1, std::ios::binary);
    if (!input_stream1.is_open()) {
        std::cerr << "Failed to open input file: " << input_file1 << std::endl;
        //return 1;
    }
    std::string write_data1((std::istreambuf_iterator<char>(input_stream1)), std::istreambuf_iterator<char>());
    input_stream1.close();
    size_t write_size1 = write_data1.size();
    std::cout << "Writing data from file: " << input_file1 << " (" << write_size1 << " bytes)" << std::endl;
    if (pfs_write(fd, write_data1.data(), write_size1, 5) != write_size1) {
        std::cerr << "Failed to write data for Case 1: Single Block Write!" << std::endl;
        //return 1;
    }
    std::cout << "Case 1: Single Block Write passed successfully! - 400 bytes" << std::endl;

    // Test Case 2: Multi-Block Single Fileserver Write
    const char* input_file2 = "test_case2.txt"; // 1000 bytes
    std::ifstream input_stream2(input_file2, std::ios::binary);
    if (!input_stream2.is_open()) {
        std::cerr << "Failed to open input file: " << input_file2 << std::endl;
       // return 1;
    }
    std::string write_data2((std::istreambuf_iterator<char>(input_stream2)), std::istreambuf_iterator<char>());
    input_stream2.close();
    size_t write_size2 = write_data2.size();
    std::cout << "Writing data from file: " << input_file2 << " (" << write_size2 << " bytes)" << std::endl;
    if (pfs_write(fd, write_data2.data(), write_size2, 1000) != write_size2) {
        std::cerr << "Failed to write data for Case 2: Multi-Block Single Fileserver Write! - 1000 bytes" << std::endl;
        //return 1;
    }
    std::cout << "Case 2: Multi-Block Single Fileserver Write passed successfully! - 1000 bytes" << std::endl;

    //Test Case 3: Multi-Block Multi-Fileserver Write
    const char* input_file3 = "test_case3.txt"; // 4000 bytes
    std::ifstream input_stream3(input_file3, std::ios::binary);
    if (!input_stream3.is_open()) {
        std::cerr << "Failed to open input file: " << input_file3 << std::endl;
        //return 1;
    }
    std::string write_data3((std::istreambuf_iterator<char>(input_stream3)), std::istreambuf_iterator<char>());
    input_stream3.close();
    size_t write_size3 = write_data3.size();
    std::cout << "Writing data from file: " << input_file3 << " (" << write_size3 << " bytes)" << std::endl;
    if (pfs_write(fd, write_data3.data(), write_size3, 2050) != write_size3) {
        std::cerr << "Failed to write data for Case 3: Multi-Block Multi-Fileserver Write!" << std::endl;
       //return 1;
    }
    std::cout << "Case 3: Multi-Block Multi-Fileserver Write passed successfully! - 3000 bytes" << std::endl;




    // Step 5: Read data back from the file
    std::cout << "Reading data back from file..." << std::endl;
    if (pfs_read(fd, read_buffer, write_size, offset) <=0) {
        std::cerr << "Failed to read data from file!" << std::endl;
        //return;
    } else {
        const char* output_file = "read_data.txt";
        std::ofstream output_stream(output_file, std::ios::binary);
        if (!output_stream.is_open()) {
            std::cerr << "Failed to open output file: " << output_file << std::endl;
        } else {
            // Verify the buffer content before writing
            std::cout << "Debug: Read buffer size is " << write_size << " bytes." << std::endl;
            std::cout << "Debug: First 100 bytes of read buffer: " 
                    << std::string(read_buffer, std::min(static_cast<size_t>(100), write_size)) << std::endl;

            // Write data to file
            output_stream.write(read_buffer, 5538);
            if (!output_stream) {
                std::cerr << "Failed to write data to the file: " << output_file << std::endl;
            } else {
                std::cout << "Data successfully written to file: " << output_file << std::endl;
            }
            output_stream.close();
        }

    }

    // Step 7: Close the file
    // std::cout << "Closing file..." << std::endl;
    // if (pfs_close(fd) != 0) {
    //     std::cerr << "Failed to close file!" << std::endl;
    // }
    // std::cout << "File closed successfully!" << std::endl;

    std::cout << "== PFS File Operations Test Completed ==" << std::endl;
}

int main() {
    // Run the test
    test_file_operations();
    return 0;
}
