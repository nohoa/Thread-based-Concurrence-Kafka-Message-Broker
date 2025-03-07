#include <cstddef>
#include <fstream>
#include <iostream>
#include <cstdlib>
#include <memory>
#include <ostream>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fstream>

#include<thread>

#include "Kafka.hpp"


void handle_client(int client_fd){
    while(true){
    //std :: cout << "Client is :"  << client_fd << std :: endl;
    char buf[1024] ;
    int recieved_status =recv(client_fd,buf,sizeof(buf),0);
    //std :: cout << "here" << std :: endl; 
    if(recieved_status <= 0){
        //std:: cout << "Error receiving bytes"  << std::endl;
        break;
    }
    Kafka parser{buf};
    
    Kafka_parser Kaf_par = parser.parser( buf);
   
    int value = htonl(1);
    int16_t be_error_code = htons(35); 
    uint8_t api_keys_length = 0x03; // compact array length for 2 element
    int16_t be_api_key = htons(18);
    int16_t be_min_version = htons(0); // 2
    int16_t be_max_version = htons(4); // 2
    int32_t be_throttle_time_ms = htonl(0); // 4
    uint8_t no_tags = 0x00; // no tagged fields // 1 
    uint8_t api_key_tags = 0x00; // no tags for this ApiKey entry // 1

    int16_t topic_partition_key = htons(75);

    int16_t topic_min_version = htons(0); // 2

    int16_t topic_max_version = htons(0); // 2

    uint8_t topic_key_tags = 0x00;

    if(Kaf_par.request_api_version >= 0 && Kaf_par.request_api_version <= 4) be_error_code = htons(0);

    int32_t be_correlation_id = htonl(Kaf_par.correlation_id);
    // Calculate message size : correlation id(4) + error_code(2)+array length(1) + 2*(size_of_request = 7 = 
    //api_key(2)+min_support_version(2)+max_support_version(2)+tag_buffer(1)) + throttle_time(4) + tag_buffer(1) =
    // 4 +2 +1 + 2*7+4+1 =26 
    int32_t message_size = htonl(26); // handle APIVersion Request and Describe Topic request bit 
    // Send response:
    // Note: correlation_id must be sent back in network order
    std :: cout << std::hex << message_size << std :: endl;
    send(client_fd, &message_size, sizeof(message_size), 0);
    send(client_fd, &be_correlation_id, sizeof(be_correlation_id), 0); //
    send(client_fd, &be_error_code, sizeof(be_error_code), 0);
    send(client_fd, &api_keys_length, sizeof(api_keys_length), 0);

    send(client_fd, &be_api_key, sizeof(be_api_key), 0);
    send(client_fd, &be_min_version, sizeof(be_min_version), 0);
    send(client_fd, &be_max_version, sizeof(be_max_version), 0);
    send(client_fd, &api_key_tags, sizeof(api_key_tags), 0);
   // send(client_fd, &no_tags, sizeof(no_tags), 0);

    send(client_fd, &topic_partition_key, sizeof(topic_partition_key), 0);
    send(client_fd,&topic_min_version,sizeof(topic_min_version),0);
    send(client_fd,&topic_max_version,sizeof(topic_max_version),0);
    send(client_fd,&topic_key_tags,sizeof(topic_key_tags),0);
    //send(client_fd, &no_tags, sizeof(no_tags), 0);

    send(client_fd, &be_throttle_time_ms, sizeof(be_throttle_time_ms), 0);
    send(client_fd, &no_tags, sizeof(no_tags), 0);
    }
    close(client_fd);
    return ;
}


int main(int argc, char* argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors

    // Read the input stream 
    // std :: ifstream is(argv[1]) ;
    // std :: string s;
    // while(std::getline(is,s)){
    //     std :: cout << s << std::endl;
    // }
    // std :: cout << std::endl;
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";


    // You can use print statements as follows for debugging, they'll be visible when running tests.
    std::cerr << "Logs from your program will appear here!\n";
    
    // Uncomment this block to pass the first stage

    // 

    while(true){

         struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);
 // ?
    int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_addr_len);
    std::cout << "Client connected\n";

    if (client_fd < 0) {
      std::cerr << "Accept failed" << std::endl;
      continue;
    }
    std::thread th(handle_client,client_fd);
    //handle_client(client_fd);
    th.detach();
    }
    close(server_fd);


    return 0;
}