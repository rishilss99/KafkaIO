#include "common.h"
#include "server_setup.h"
#include "client_accept.h"

std::atomic_bool server_running = true;

void signalHandler(int)
{
    std::cout << "Server has been interrupted. Shutting down\n";
    server_running = false;
}

void setToBlockSignal()
{
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT); // Block Ctrl+C (SIGINT)

    if (pthread_sigmask(SIG_BLOCK, &mask, nullptr) != 0)
    {
        std::cerr << "Failed to block signals in thread!" << std::endl;
    }
}

void setToHandleSignal()
{
    struct sigaction sa;
    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    if (sigaction(SIGINT, &sa, nullptr) == -1)
    {
        std::perror("Error occured");
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char *argv[])
{
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    setToHandleSignal();

    int server_fd = serverSetup();
    if (server_fd == 1)
    {
        std::cerr << "Couldn't setup server socket" << std::endl;
        exit(EXIT_FAILURE);
    }

    std::cout << "Waiting for a client to connect...\n";

    struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    std::cerr << "Logs from your program will appear here!\n";

    while (true)
    {
        int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr *>(&client_addr), &client_addr_len);
        if (client_fd == -1)
        {
            if (errno == EINTR)
            {
                break;
            }
            else
            {
                std::perror("Error occured");
                exit(EXIT_FAILURE);
            }
        }
        std::thread t([client_fd]()
                      { setToBlockSignal();
                        Client client(client_fd); });
        t.detach();
        std::cout << "Client connected\n";
    }

    close(server_fd);
    exit(EXIT_SUCCESS);
}