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
#include<vector>
#include <iterator>

#include<thread>

#include "Kafka.hpp"


struct TopicMetadata {
  bool exists;
  int cnt = 0 ;
  std::vector< std::array<unsigned char, 16> >  uuid;
};
TopicMetadata get_topic_metadata(const std::string &topic_name) {
  std::cout << "Checking metadata for topic: " << topic_name << std::endl; 
  //std::vector< std::array<unsigned char, 16> >  ls_uuid ;
  TopicMetadata result = {false,0,{}}; // Initialize with exists = false
  // Read the entire metadata file
  std::ifstream file("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log", std::ios::binary);
  // std :: cout << "inside the file" << std::endl;
  //std :: cout <<  " why file ? " << file.is_open() <<std :: endl;
  if (!file) {
    //std :: cout << "failed" << std :: endl;
    std::cout << "Failed to open metadata file" << std::endl;
    return result; // Return with exists = false
  }
  //std :: cout << "inside the file" << std::endl;
  // Read the entire file into memory
  std::vector<char> metadata((std::istreambuf_iterator<char>(file)),
                             std::istreambuf_iterator<char>());

  // Look for the topic name
  for (size_t i = 0; i < metadata.size() - topic_name.length(); i++) {
    if (std::string(metadata.begin() + i,
                    metadata.begin() + i + topic_name.length()) == topic_name) {
      // Found the topic name
      result.exists = true;
      if (i + topic_name.length() + 16 < metadata.size()) {
        //std :: cout << "matched" << std::endl;
        std::array<unsigned char, 16> ansuuid ;
        std::copy_n(metadata.begin() + i + topic_name.length(), 16,
                    ansuuid.begin());
            result.cnt ++;
        result.uuid.push_back(ansuuid);
        // std:: cout << "valid" << std::endl;
        // for(int i = 0 ;i < 16 ;i ++){
        //   std ::  cout <<  (int)(metadata[i + topic_name.length()]) <<" ";
        //   } 
        //if(result.uuid.size() == 2) return result;
      }
      //return result;
    }
  }
  // If we didn't find the topic, return with exists = false
  return result;
}

void handle_client(int client_fd, char *buffer , int length ){
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
    int16_t be_error_code = htons(3); 
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

    uint8_t unknow_topic_keys_length = 0x02; 

    uint8_t topic_key_tags = 0x00;

    uint32_t topic_id = 0x00;

    uint16_t inter_topic_id = 0x00;

    uint8_t topic_id_left = 0x00;

    uint8_t is_internal = 0x00;

    std::string topic_name = Kaf_par.topic_name;


      uint8_t topic_message_size = topic_name.length();

    bool valid = parser.contains(buffer, length,topic_name);
    //std :: cout << "size is " << topic_message_size << std::endl;

    int32_t topic_authorization = 0x00;

    uint8_t partition_arr = 0x02;
    uint8_t nex_cursor = 0xff;

   //std::string tp_name = Kaf_par.topic_name;
//
//std :: cout << "topc is :" << topic_name << std::endl;
  auto meta = get_topic_metadata(topic_name);
  //std :: cout << "topic id is : " ;
  //std :: cout << meta.exists<<std::endl;
  //std :: cout <<  std::endl;
  //std :: cout << meta.uuid.size() << std::endl;
  //std :: cout << meta.exists << std::endl;
  //std :: cout << std::endl;

   // if(Kaf_par.request_api_version >= 0 && Kaf_par.request_api_version <= 4) be_error_code = htons(0);
    
    int32_t be_correlation_id = htonl(Kaf_par.correlation_id);
    // Calculate message size : correlation id(4) + error_code(2)+array length(1) + 2*(size_of_request = 7 = 
    //api_key(2)+min_support_version(2)+max_support_version(2)+tag_buffer(1)) + throttle_time(4) + tag_buffer(1) =
    // 4 +2 +1 + 2*7+4+1 =26 
    if(topic_name != ""){
      if(valid == true) {
        //std :: cout << "parition id is " << Kaf_par.partition_id << std::endl;
          //topic_name = "";
          //meta = get_topic_metadata(topic_name);
          std :: cout << "invokeddddd here ?" << std::endl;
          std :: cout << "size is " << meta.cnt << std::endl;
          partition_arr = Kaf_par.partition_id+1 ;
          be_error_code = 0x00;
          int32_t partition_id = 0x00;
          int32_t leader_id = 0x00;
          int32_t epoch = 0x00;
          int8_t len = 0x02;
          int32_t nod = 0x00;
          int8_t len_l = len-1;
          int message_size ;
          if(partition_arr == 0x1)  message_size = htonl(78); // handle APIVersion Request and Describe Topic request bit 
          else message_size = htonl(101);
    // Send response:
    // Note: correlation_id must be sent back in network order
    //std :: cout << std::hex << message_size << std :: endl;
    send(client_fd, &message_size, sizeof(message_size), 0); //4
    send(client_fd, &be_correlation_id, sizeof(be_correlation_id), 0); // 4
    //send(client_fd, &be_error_code, sizeof(be_error_code), 0);

   //send(client_fd, &api_keys_length, sizeof(api_keys_length), 0);

//     send(client_fd, &be_api_key, sizeof(be_api_key), 0);
//     send(client_fd, &be_min_version, sizeof(be_min_version), 0);
//     send(client_fd, &be_max_version, sizeof(be_max_version), 0);
//     send(client_fd, &api_key_tags, sizeof(api_key_tags), 0);
     send(client_fd, &no_tags, sizeof(no_tags), 0); // 1

//     send(client_fd, &topic_partition_key, sizeof(topic_partition_key), 0);
//     send(client_fd,&topic_min_version,sizeof(topic_min_version),0);
//     send(client_fd,&topic_max_version,sizeof(topic_max_version),0);
//     send(client_fd,&topic_key_tags,sizeof(topic_key_tags),0);
//     //send(client_fd, &no_tags, sizeof(no_tags), 0);
     send(client_fd, &be_throttle_time_ms, sizeof(be_throttle_time_ms), 0); // 4
     send(client_fd, &unknow_topic_keys_length, sizeof(unknow_topic_keys_length), 0); // 1 
     send(client_fd, &be_error_code, sizeof(be_error_code), 0); // 2
     send(client_fd,&topic_message_size,sizeof(topic_message_size),0); //1
     send(client_fd,topic_name.c_str(),topic_message_size,0); // 17

    //  send(client_fd,&topic_id,sizeof(topic_id),0); // 4
    //  send(client_fd,&topic_id,sizeof(topic_id),0); // 4
    //  send(client_fd,&topic_id,sizeof(topic_id),0); // 4
    //  send(client_fd,&topic_id,sizeof(topic_id),0); // 4
    send(client_fd,&meta.uuid[0],15,0);
     //send(client_fd,&topic_id,sizeof(topic_id),0);
     //send(client_fd,&inter_topic_id,sizeof(inter_topic_id),0);  // 2
    // send(client_fd,&topic_id_left,sizeof(topic_id_left),0); // 1
     send(client_fd,&is_internal,sizeof(is_internal),0); // 1
     send(client_fd,&partition_arr,sizeof(partition_arr),0); // 1

      send(client_fd, &be_error_code, sizeof(be_error_code), 0); // 2
      send(client_fd,&partition_id,sizeof(partition_id),0); //4
      send(client_fd,&leader_id,sizeof(leader_id),0); //4
      send(client_fd,&epoch,sizeof(epoch),0); // 4
      send(client_fd,&len,sizeof(len),0); // 1
      send(client_fd,&nod,sizeof(nod),0);//4
      send(client_fd,&len,sizeof(len),0);//1
      send(client_fd,&nod,sizeof(nod),0);//4
      send(client_fd,&len_l,sizeof(len_l),0); // 1
      send(client_fd,&len_l,sizeof(len_l),0); // 1
      send(client_fd,&len_l,sizeof(len_l),0); // 1
      send(client_fd, &no_tags, sizeof(no_tags), 0);  // 1

      //std :: cout << "par"
      if(partition_arr > 0x2){
      partition_id  = htonl(0x1);
      //std :: cout << partition_id << std :: endl;
      send(client_fd, &be_error_code, sizeof(be_error_code), 0); // 2
      send(client_fd,&partition_id,sizeof(partition_id),0); //4
      send(client_fd,&leader_id,sizeof(leader_id),0); //4
      send(client_fd,&epoch,sizeof(epoch),0); // 4
      send(client_fd,&len,sizeof(len),0); // 1
      send(client_fd,&nod,sizeof(nod),0);//4
      send(client_fd,&len,sizeof(len),0);//1
      send(client_fd,&nod,sizeof(nod),0);//4
      send(client_fd,&len_l,sizeof(len_l),0); // 1
      send(client_fd,&len_l,sizeof(len_l),0); // 1
      send(client_fd,&len_l,sizeof(len_l),0); // 1
      send(client_fd, &no_tags, sizeof(no_tags), 0);  // 1
      }

     send(client_fd,&topic_authorization,sizeof(topic_authorization),0); // 4
     send(client_fd, &no_tags, sizeof(no_tags), 0); // 1
     send(client_fd,&nex_cursor,sizeof(nex_cursor),0); // 1
     //send(client_fd,tp_name.c_str(),sizeof(tp_name.c_str()),0);
     send(client_fd, &no_tags, sizeof(no_tags), 0); 
      }
      else {
    int32_t message_size = htonl(55); // handle APIVersion Request and Describe Topic request bit 
    // Send response:
    // Note: correlation_id must be sent back in network order
    //std :: cout << std::hex << message_size << std :: endl;
    be_error_code = htons(3); // no tags for this ApiKey entry // 1
    topic_id = 0x00;
    std::string topic_name = Kaf_par.topic_name;
    uint8_t topic_message_size = topic_name.length();
    //std :: cout << "size is " << topic_message_size << std::endl;
    int32_t topic_authorization = 0x00;
    uint8_t partition_arr = 0x01;
    uint8_t nex_cursor = 0xff;

    send(client_fd, &message_size, sizeof(message_size), 0); //4
    send(client_fd, &be_correlation_id, sizeof(be_correlation_id), 0); // 4
    //send(client_fd, &be_error_code, sizeof(be_error_code), 0);

   //send(client_fd, &api_keys_length, sizeof(api_keys_length), 0);

//     send(client_fd, &be_api_key, sizeof(be_api_key), 0);
//     send(client_fd, &be_min_version, sizeof(be_min_version), 0);
//     send(client_fd, &be_max_version, sizeof(be_max_version), 0);
//     send(client_fd, &api_key_tags, sizeof(api_key_tags), 0);
     send(client_fd, &no_tags, sizeof(no_tags), 0); // 1

//     send(client_fd, &topic_partition_key, sizeof(topic_partition_key), 0);
//     send(client_fd,&topic_min_version,sizeof(topic_min_version),0);
//     send(client_fd,&topic_max_version,sizeof(topic_max_version),0);
//     send(client_fd,&topic_key_tags,sizeof(topic_key_tags),0);
//     //send(client_fd, &no_tags, sizeof(no_tags), 0);
      //std :: cout << "name is " +topic_name << std::endl;
     send(client_fd, &be_throttle_time_ms, sizeof(be_throttle_time_ms), 0); // 4
     send(client_fd, &unknow_topic_keys_length, sizeof(unknow_topic_keys_length), 0); // 1 
     send(client_fd, &be_error_code, sizeof(be_error_code), 0); // 2
     send(client_fd,&topic_message_size,sizeof(topic_message_size),0); //1
     send(client_fd,topic_name.c_str(),17,0); // 17

     send(client_fd,&topic_id,sizeof(topic_id),0); // 4
     send(client_fd,&topic_id,sizeof(topic_id),0); // 4
     send(client_fd,&topic_id,sizeof(topic_id),0); // 4
     send(client_fd,&topic_id,sizeof(topic_id),0); // 4
    //send(client_fd,&meta.uuid,sizeof(meta.uuid),0);
     //send(client_fd,&topic_id,sizeof(topic_id),0);
     //send(client_fd,&inter_topic_id,sizeof(inter_topic_id),0);  // 2
    // send(client_fd,&topic_id_left,sizeof(topic_id_left),0); // 1
     send(client_fd,&is_internal,sizeof(is_internal),0); // 1
     send(client_fd,&partition_arr,sizeof(partition_arr),0); // 1

     send(client_fd,&topic_authorization,sizeof(topic_authorization),0); // 4
     send(client_fd, &no_tags, sizeof(no_tags), 0); // 1
     send(client_fd,&nex_cursor,sizeof(nex_cursor),0); // 1
     //send(client_fd,tp_name.c_str(),sizeof(tp_name.c_str()),0);
     send(client_fd, &no_tags, sizeof(no_tags), 0); // 1
//     send(client_fd, &no_tags, sizeof(no_tags), 0);
      }
    }
    else {
       int  message_size = htonl(26); // handle APIVersion Request and Describe Topic request bit 
    // Send response:
    // Note: correlation_id must be sent back in network order
    //std :: cout << std::hex << message_size << std :: endl;
    be_error_code = htons(35);
    if(Kaf_par.request_api_version >= 0 && Kaf_par.request_api_version <= 4) be_error_code = htons(0);
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
    std::ifstream is("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");

    char * buffer = new char [1024];
    int length = 0;
    if (is.is_open()) {
    // get length of file:
    is.seekg (0, is.end);
    length = is.tellg();
    is.seekg (0, is.beg);


    std::cout << "Reading " << length << " characters... " << std::endl ;
    // read data as a block:
    //std :: cout << std::endl;
    is.read (buffer,length);
      is.close();
    }
    std :: string s ;
    std::vector<std::string > metadata ;


    
    //std :: cout << std::endl;
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
    std::thread th(handle_client,client_fd,buffer,length);
    //handle_client(client_fd);
    th.detach();
    }
    close(server_fd);


    return 0;
}