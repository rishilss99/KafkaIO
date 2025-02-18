#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <vector>
#include <cstdint>

template <typename T>
T swap_endian(T value)
{
    static_assert(std::is_integral_v<T>, "Only integral types are supported");

    T result = 0;
    constexpr size_t size = sizeof(T);

    for (size_t i = 0; i < size; ++i)
    {
        result |= ((value >> (8 * i)) & 0xFF) << (8 * (size - 1 - i));
    }

    return result;
}

int main(int argc, char *argv[])
{
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
    {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&server_addr), sizeof(server_addr)) != 0)
    {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0)
    {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    std::cerr << "Logs from your program will appear here!\n";

    // Uncomment this block to pass the first stage

    int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr *>(&client_addr), &client_addr_len);
    std::cout << "Client connected\n";

    // std::vector<char> request(20);
    // int read_bytes = recv(client_fd, request.data(), request.size(), 0);
    // std::cout << "Read client's request\n";

    // int corr_id = swap_endian(7);
    // std::memcpy(reinterpret_cast<void *>(response.data() + 4), reinterpret_cast<void *>(&corr_id), sizeof(corr_id));
    int32_t msg_size = htobe32(0);
    int32_t corr_id = htobe32(7);
    send(client_fd, &msg_size, sizeof(msg_size), 0);
    send(client_fd, &corr_id, sizeof(corr_id), 0);
    std::cout << "Send client response\n";

    while(true) {}

    close(client_fd);

    close(server_fd);
    return 0;
}