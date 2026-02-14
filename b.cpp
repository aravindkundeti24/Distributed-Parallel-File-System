#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "pfs_common/pfs_common.hpp"
#include "pfs_client/pfs_api.hpp"

// Helper function to print metadata
int pfs_print_meta(int pfs_fd, int client_id) {
    struct pfs_metadata mymeta = {0};
    int ret = pfs_fstat(pfs_fd, &mymeta);
    if (ret != -1) {
        printf("%s: PFS fd: %d, Client id: %d\n", __func__, pfs_fd, client_id);
        printf("%s: File name: %s, size: %lu\n", __func__, mymeta.filename, mymeta.file_size);
        printf("%s: Time of creation: %s", __func__, ctime(&(mymeta.ctime)));
        printf("%s: Last modification: %s", __func__, ctime(&(mymeta.mtime)));
    }
    return ret;
}

// Client 1 creates a file and writes to it
// Client 1 creates a file and writes to it
void client1_scenario() {
    printf("Client 1: Initializing PFS client\n");
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "Client 1: Failed to initialize PFS.\n");
        return;
    }

    // Create a file with 100 bytes
    if (pfs_create("pfs_file1", 1) == -1) {
        fprintf(stderr, "Client 1: Failed to create file.\n");
        return;
    }
    printf("Client 1: Created file 'pfs_file1'\n");

    // int pfs_fd = pfs_open("pfs_file1", 2);
    // if (pfs_fd == -1) {
    //     fprintf(stderr, "Client 1: Failed to open file.\n");
    //     return;
    // }
        int pfs_fd = pfs_open("pfs_file1", 2);
    char buffer[100];
    memset(buffer, 'A', 100); // Fill with dummy data
    if (pfs_write(pfs_fd, buffer, 100, 0) == -1) {
        fprintf(stderr, "Client 1: Failed to write to file.\n");
        return;
    }
    // printf("Client 1: Wrote 100 bytes to 'pfs_file1'\n");
    sleep(20);
    pfs_finish(1);

    // if (pfs_print_meta(pfs_fd, client_id) == -1) {
    //     fprintf(stderr, "Client 1: Failed to print metadata.\n");
    //     return;
    // }

    // if (pfs_close(pfs_fd) == -1) {
    //     fprintf(stderr, "Client 1: Failed to close file.\n");
    //     return;
    // }
    // if (pfs_finish(client_id) == -1) {
    //     fprintf(stderr, "Client 1: Failed to finish PFS.\n");
    //     return;
    // }
    // printf("Client 1: Finished execution.\n");
}
// Client 2 writes to a range overlapping with Client 1
void client2_scenario() {
    printf("Client 1: Initializing PFS client\n");
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "Client 1: Failed to initialize PFS.\n");
        return;
    }

    // Create a file with 100 bytes
    
    printf("Client 1: Created file 'pfs_file1'\n");

    // int pfs_fd = pfs_open("pfs_file1", 2);
    // if (pfs_fd == -1) {
    //     fprintf(stderr, "Client 1: Failed to open file.\n");
    //     return;
    // }
        int pfs_fd = pfs_open("pfs_file1", 2);

   char buffer[50];
    memset(buffer, 'B', 50);// Fill with dummy data
    if (pfs_write(pfs_fd, buffer, 50, 50) == -1) {
        fprintf(stderr, "Client 1: Failed to write to file.\n");
        return;
    }
    // printf("Client 1: Wrote 100 bytes to 'pfs_file1'\n");
    sleep(20);
    pfs_finish(1);

    // if (pfs_print_meta(pfs_fd, client_id) == -1) {
    //     fprintf(stderr, "Client 1: Failed to print metadata.\n");
    //     return;
    // }

    // if (pfs_close(pfs_fd) == -1) {
    //     fprintf(stderr, "Client 1: Failed to close file.\n");
    //     return;
    // }
    // if (pfs_finish(client_id) == -1) {
    //     fprintf(stderr, "Client 1: Failed to finish PFS.\n");
    //     return;
    // }
    // printf("Client 1: Finished execution.\n");
}

// Client 3 reads a range overlapping with Client 2's write
void client3_scenario() {
     printf("Client 1: Initializing PFS client\n");
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "Client 1: Failed to initialize PFS.\n");
        return;
    }

    // Create a file with 100 bytes
    
    printf("Client 3: Created file 'pfs_file1'\n");

    // int pfs_fd = pfs_open("pfs_file1", 2);
    // if (pfs_fd == -1) {
    //     fprintf(stderr, "Client 1: Failed to open file.\n");
    //     return;
    // }
        int pfs_fd = pfs_open("pfs_file1", 1);

    char buffer[30];
    if (pfs_read(pfs_fd, buffer, 30, 60) == -1) {
        fprintf(stderr, "Client 3: Failed to read from file.\n");
        return;
    }
    // printf("Client 1: Wrote 100 bytes to 'pfs_file1'\n");
    sleep(20);
    pfs_finish(1);

    // if (pfs_print_meta(pfs_fd, client_id) == -1) {
    //     fprintf(stderr, "Client 1: Failed to print metadata.\n");
    //     return;
    // }

    // if (pfs_close(pfs_fd) == -1) {
    //     fprintf(stderr, "Client 1: Failed to close file.\n");
    //     return;
    // }
    // if (pfs_finish(client_id) == -1) {
    //     fprintf(stderr, "Client 1: Failed to finish PFS.\n");
    //     return;
    // }
    // printf("Client 1: Finished execution.\n"); 15,16,17,18,19,21
}

// Client 4 reads a non-overlapping range
void client4_scenario() {
     printf("Client 1: Initializing PFS client\n");
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "Client 1: Failed to initialize PFS.\n");
        return;
    }

    // Create a file with 100 bytes
    
    printf("Client 1: Created file 'pfs_file1'\n");

    // int pfs_fd = pfs_open("pfs_file1", 2);
    // if (pfs_fd == -1) {
    //     fprintf(stderr, "Client 1: Failed to open file.\n");
    //     return;
    // }
        int pfs_fd = pfs_open("pfs_file1", 1);

    char buffer[20];
    if (pfs_read(pfs_fd, buffer, 20, 120) == -1) {
        fprintf(stderr, "Client 4: Failed to read from file.\n");
        return;
    }
    // printf("Client 1: Wrote 100 bytes to 'pfs_file1'\n");
    sleep(20);
    pfs_finish(1);

    // if (pfs_print_meta(pfs_fd, client_id) == -1) {
    //     fprintf(stderr, "Client 1: Failed to print metadata.\n");
    //     return;
    // }

    // if (pfs_close(pfs_fd) == -1) {
    //     fprintf(stderr, "Client 1: Failed to close file.\n");
    //     return;
    // }
    // if (pfs_finish(client_id) == -1) {
    //     fprintf(stderr, "Client 1: Failed to finish PFS.\n");
    //     return;
    // }
    // printf("Client 1: Finished execution.\n");
}

// Client 5 writes to a non-overlapping range
void client5_scenario() {
    printf("Client 1: Initializing PFS client\n");
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "Client 1: Failed to initialize PFS.\n");
        return;
    }

    // Create a file with 100 bytes
    
    printf("Client 1: Created file 'pfs_file1'\n");

    // int pfs_fd = pfs_open("pfs_file1", 2);
    // if (pfs_fd == -1) {
    //     fprintf(stderr, "Client 1: Failed to open file.\n");
    //     return;
    // }
        int pfs_fd = pfs_open("pfs_file1", 2);

    char buffer[30];
    memset(buffer, 'E', 30); // Fill with dummy data
    if (pfs_write(pfs_fd, buffer, 30, 150) == -1) {
        fprintf(stderr, "Client 5: Failed to write to file.\n");
        return;
    }
    // printf("Client 1: Wrote 100 bytes to 'pfs_file1'\n");
    sleep(20);
    pfs_finish(1);

    // if (pfs_print_meta(pfs_fd, client_id) == -1) {
    //     fprintf(stderr, "Client 1: Failed to print metadata.\n");
    //     return;
    // }

    // if (pfs_close(pfs_fd) == -1) {
    //     fprintf(stderr, "Client 1: Failed to close file.\n");
    //     return;
    // }
    // if (pfs_finish(client_id) == -1) {
    //     fprintf(stderr, "Client 1: Failed to finish PFS.\n");
    //     return;
    // }
    // printf("Client 1: Finished execution.\n");
}

// Client 6 writes to an overlapping range with multiple clients
void client6_scenario() {
     printf("Client 1: Initializing PFS client\n");
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "Client 1: Failed to initialize PFS.\n");
        return;
    }

    // Create a file with 100 bytes
    
    printf("Client 1: Created file 'pfs_file1'\n");

    // int pfs_fd = pfs_open("pfs_file1", 2);
    // if (pfs_fd == -1) {
    //     fprintf(stderr, "Client 1: Failed to open file.\n");
    //     return;
    // }
        int pfs_fd = pfs_open("pfs_file1", 2);

    char buffer[80];
    memset(buffer, 'F', 80); // Fill with dummy data
    if (pfs_write(pfs_fd, buffer, 80, 30) == -1) {
        fprintf(stderr, "Client 6: Failed to write to file.\n");
        return;
    }
    // printf("Client 1: Wrote 100 bytes to 'pfs_file1'\n");
    sleep(20);
    pfs_finish(1);

    // if (pfs_print_meta(pfs_fd, client_id) == -1) {
    //     fprintf(stderr, "Client 1: Failed to print metadata.\n");
    //     return;
    // }

    // if (pfs_close(pfs_fd) == -1) {
    //     fprintf(stderr, "Client 1: Failed to close file.\n");
    //     return;
    // }
    // if (pfs_finish(client_id) == -1) {
    //     fprintf(stderr, "Client 1: Failed to finish PFS.\n");
    //     return;
    // }
    // printf("Client 1: Finished execution.\n");
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s client1|client2|client3|client4|client5|client6\n", argv[0]);
        return -1;
    }

    if (strcmp(argv[1], "client1") == 0) {
        client1_scenario();
    } else if (strcmp(argv[1], "client2") == 0) {
        client2_scenario();
    } else if (strcmp(argv[1], "client3") == 0) {
        client3_scenario();
    } else if (strcmp(argv[1], "client4") == 0) {
        client4_scenario();
    } else if (strcmp(argv[1], "client5") == 0) {
        client5_scenario();
    } else if (strcmp(argv[1], "client6") == 0) {
        client6_scenario();
    } else {
        fprintf(stderr, "Invalid argument. Use 'client1' to 'client6'.\n");
        return -1;
    }

    sleep(200);

    return 0;
}