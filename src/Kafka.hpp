#include <cstdlib>
#include<iostream>
#include<stdlib.h>

struct Kafka_parser {
    int message_size = 0 ;
    int request_api_key  = 0;
    int request_api_version = 0;
    int correlation_id = 0;
};

class Kafka {
    private :
    int* buffer;

    public :
    ~Kafka(){
        delete buffer ;
    };
    Kafka(char buf[1024]){
        buffer = (int*)malloc(sizeof(int)*1025);
        for(int i = 0 ;i < 1024 ;i ++){
            *(buffer +i) = (int)(buf[i]);
        }
    }

    virtual Kafka_parser parser(char *buf);

};