
#include "Kafka.hpp"

#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>

Kafka_parser Kafka :: parser(char *buf){
        Kafka_parser new_parser ;
        //std :: cout << "lol" << std :: endl;
        int buffer[305];
        for(int i = 0 ;i < 305 ;i ++){
                buffer[i] = (int)(buf[i]);
        }
        for(int i = 0 ;i < 4 ;i ++){
                new_parser.message_size = (new_parser.message_size << 8)|buffer[i];
        }
        for(int i = 4 ;i < 6 ;i ++){
                new_parser.request_api_key = (new_parser.request_api_key << 8)|buffer[i];
        }
        for(int i = 6 ;i < 8 ;i ++){
                new_parser.request_api_version = (new_parser.request_api_version << 8)|buffer[i];
        }
        for(int i = 8 ;i < 12 ;i ++){
                int inter_buf = 0;
                //for(int j = 0 ;j < 8 ;j ++ )
                buffer[i] = buffer[i] & 0xff;
               // std :: cout 
                new_parser.correlation_id = (new_parser.correlation_id << 8)|buffer[i];
        }
        // std :: cout << new_parser.message_size<<std :: endl;
        // std :: cout << new_parser.request_api_key<<std :: endl;
        // std :: cout << new_parser.request_api_version<<std :: endl;
        // std :: cout << new_parser.correlation_id << std :: endl;
        
        return new_parser;
}