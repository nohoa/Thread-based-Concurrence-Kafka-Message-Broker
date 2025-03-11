
#include "Kafka.hpp"

#include <string>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>

Kafka_parser Kafka :: parser(char *buf){
        Kafka_parser new_parser ;
        //std :: cout << "lol" << std :: endl;
        int buffer[1024];
        for(int i = 0 ;i < 1024 ;i ++){
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

        int value = buffer[28];
        //std :: cout << value << std:: endl;
        if(value >= 80) return new_parser;

        int curr_id = 28;
        //std :: cout << "go inside here" << std :: endl;
        while(buffer[curr_id] != 0x00){
                int value = buffer[curr_id];
                std :: string topic ;
                for(int i = 0 ;i < value ;i ++){
                         topic += (char)(buffer[curr_id+1+i]);
                }
                new_parser.topic_name.push_back(topic);
                curr_id += value;
                curr_id += 1;
        }

        int id = curr_id;
        // std :: cout << new_parser.message_size<<std :: endl;
        // std :: cout << new_parser.request_api_key<<std :: endl;
        // std :: cout << new_parser.request_api_version<<std :: endl;
        // std :: cout << new_parser.correlation_id << std :: endl;

        for(int i = 0 ;i < 4 ;i ++){
                buffer[i+id] = buffer[i+id] & 0xff;
                new_parser.partition_id = (new_parser.partition_id << 8) | buffer[i+id];
        }
        
        return new_parser;
}

bool Kafka ::contains(char *buf, int length,std::string match){
        int i = 0;
        std :: cout << "inside" << std :: endl;
        while(i < length){
                if(buf[i] == match[0]){
                        int cnt = 0 ;
                        for(int j = 0 ;j < match.length() ;j ++){
                                if(i+j < length && buf[i+j] == match[j]){
                                        cnt ++;
                                }
                                else break;
                        }
                        if(cnt == match.length()) {
                                //std :: cout << "match" << std :: endl;
                                return true;
                        }
                }
                i ++;
        }
        std :: cout << "outside ? " << std:: endl;
        return false;
}