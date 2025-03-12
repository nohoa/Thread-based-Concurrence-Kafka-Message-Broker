#include <cstdlib>
#include<iostream>
#include<stdlib.h>
#include<vector>

struct Kafka_parser {
    int message_size = 0 ;
    int request_api_key  = 0;
    int request_api_version = 0;
    int correlation_id = 0;

    bool batch = false ;

    std :: vector<std :: string >  topic_name  ;
    std::array<unsigned char, 16>  fetch_uuid;
    int partition_id = 0;
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

    virtual bool contains(char*buf,int length, std:: string match);

    virtual Kafka_parser parser(char *buf);

};