#include "Kafka.hpp"
#include <arpa/inet.h>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <netdb.h>
#include <ostream>
#include <set>
#include <string>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>
namespace fs = std::filesystem;

struct TopicMetadata {
  bool exists;
  int cnt = 0;
  uint8_t par_len = 0;
  std::array<unsigned char, 16> uuid;
  std::array<unsigned char, 4> nextt;
};
bool ck_uiud(std::array<unsigned char, 16> uuid1, int len, char *buf) {
  for (int i = 0; i < len; i++) {
    if (uuid1[15] == buf[i])
      return true;
  }
  return false;
}
std ::vector<TopicMetadata>
get_topic_metadata(const std ::vector<std::string> &top) {

  std ::vector<TopicMetadata> ans;
  if (top.size() == 0)
    return ans;
  for (int i = 0; i < top.size(); i++) {
    TopicMetadata result = {false, 0, {}};
    ans.push_back(result);
  }

  std::ifstream file(
      "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log",
      std::ios::binary);

  if (!file) {

    std::cout << "Failed to open metadata file" << std::endl;
    return ans; // Return with exists = false
  }

  std::vector<char> metadata((std::istreambuf_iterator<char>(file)),
                             std::istreambuf_iterator<char>());
  int j = 0;
  size_t i = 0;

  while (j < top.size() && i < metadata.size()) {
    std ::string topic_name = top[j];
    if (std::string(metadata.begin() + i,
                    metadata.begin() + i + topic_name.length()) == topic_name) {

      ans[j].exists = true;
      if (i + topic_name.length() + 16 < metadata.size()) {

        std::array<unsigned char, 16> ansuuid;
        std::copy_n(metadata.begin() + i + topic_name.length(), 16,
                    ansuuid.begin());

        ans[j].par_len = static_cast<uint8_t>(metadata[i - 11]);
        ans[j].cnt++;
        ans[j].uuid = ansuuid;
        j++;
      }
    }
    i++;
  }

  return ans;
}
std ::vector<TopicMetadata>
get_topic_metadata1(const std ::vector<std::string> &top) {

  std ::vector<TopicMetadata> ans(top.size());
  if (top.size() == 0)
    return ans;

  std::ifstream file(
      "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log",
      std::ios::binary);

  if (!file) {

    std::cout << "Failed to open metadata file" << std::endl;
    return ans; // Return with exists = false
  }

  std::vector<char> metadata((std::istreambuf_iterator<char>(file)),
                             std::istreambuf_iterator<char>());
  int j = 0;

  size_t i = 0;
  std::set<int> st;

  while (i < metadata.size()) {
    std ::string topic_name = top[0];
    int indexing = 0;

    for (auto it : top) {

      int top_size = topic_name.length();
      if (std::string(metadata.begin() + i, metadata.begin() + i + top_size) ==
          it) {

        topic_name = it;
        break;
      }
      indexing++;
    }
    if (std::string(metadata.begin() + i,
                    metadata.begin() + i + topic_name.length()) == topic_name) {

      if (i + topic_name.length() + 16 < metadata.size()) {
        TopicMetadata result = {false, 0, {}};

        std::array<unsigned char, 16> ansuuid;

        std::copy_n(metadata.begin() + i + topic_name.length(), 16,
                    ansuuid.begin());

        result.uuid = ansuuid;

        ans[indexing] = result;
      }
    }
    i++;
  }

  return ans;
}
std ::mutex some_mutex;
void handle_client(int client_fd, char *buffer, int length, std::string takeout,
                   int batch_len, char *buffer_taken,
                   std ::map<std::array<unsigned char, 16>, std::string> mp,
                   int length2, char *buffer2,
                   std::map<int, std::pair<char *, int>> ls_buff) {
  while (true) {

    char buf[1024];
    int recieved_status = recv(client_fd, buf, sizeof(buf), 0);

    if (recieved_status <= 0) {

      break;
    }

    Kafka parser{buf};

    Kafka_parser Kaf_par = parser.parser(buf);
    std::string fname = mp[Kaf_par.fetch_uuid];
    std::ifstream is("/tmp/kraft-combined-logs/" + fname +
                     "-0/00000000000000000000.log");

    int lll = 0;

    if (is.is_open()) {

      is.seekg(0, is.end);
      lll = is.tellg();
      is.seekg(0, is.beg);
    }
    int state = 0;
    if (lll == ls_buff[0].second)
      state = 1;
    else if (lll == ls_buff[1].second)
      state = 2;
    

    int value = htonl(1);
    int16_t be_error_code = htons(3);
    uint8_t api_keys_length = 0x03; // compact array length for 2 element
    int16_t be_api_key = htons(18);
    int16_t be_min_version = htons(0);      // 2
    int16_t be_max_version = htons(4);      // 2
    int32_t be_throttle_time_ms = htonl(0); // 4
    uint8_t no_tags = 0x00;                 // no tagged fields // 1
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
    
    std ::vector<std::string> topic_name = Kaf_par.topic_name;

    uint8_t topic_message_size;
    if (topic_name.size() == 0) {
      topic_message_size = 0;
    } else
      topic_message_size = topic_name[0].length();

    bool valid = false;
    if (topic_name.size() > 0)
      valid = parser.contains(buffer, length, topic_name[0]);

    int32_t topic_authorization = 0x00;
    uint8_t partition_arr = 0x02;
    uint8_t nex_cursor = 0xff;

    auto meta = get_topic_metadata(topic_name);
    std ::vector<std ::string> topic_name1;

    int32_t be_correlation_id = htonl(Kaf_par.correlation_id);

    if (Kaf_par.request_api_version == 4) {

      int message_size =
          htonl(33); // handle APIVersion Request and Describe Topic request bit

      be_error_code = htons(35);
      api_keys_length = 0x04;
      if (Kaf_par.request_api_version >= 0 && Kaf_par.request_api_version <= 4)
        be_error_code = htons(0);
      send(client_fd, &message_size, sizeof(message_size), 0);
      send(client_fd, &be_correlation_id, sizeof(be_correlation_id), 0); //
      send(client_fd, &be_error_code, sizeof(be_error_code), 0);
      send(client_fd, &api_keys_length, sizeof(api_keys_length), 0);
      send(client_fd, &be_api_key, sizeof(be_api_key), 0);
      send(client_fd, &be_min_version, sizeof(be_min_version), 0);
      send(client_fd, &be_max_version, sizeof(be_max_version), 0);
      send(client_fd, &api_key_tags, sizeof(api_key_tags), 0);
      int16_t api_fetch_key = htons(1);
      int16_t fetch_min_version = htons(0);  // 2
      int16_t fetch_max_version = htons(16); // 2

      send(client_fd, &api_fetch_key, sizeof(api_fetch_key), 0);         // 2
      send(client_fd, &fetch_min_version, sizeof(fetch_min_version), 0); // 2
      send(client_fd, &fetch_max_version, sizeof(fetch_max_version), 0); // 2
      send(client_fd, &api_key_tags, sizeof(api_key_tags), 0);           // 1

      send(client_fd, &topic_partition_key, sizeof(topic_partition_key), 0);
      send(client_fd, &topic_min_version, sizeof(topic_min_version), 0);
      send(client_fd, &topic_max_version, sizeof(topic_max_version), 0);
      send(client_fd, &topic_key_tags, sizeof(topic_key_tags), 0);

      send(client_fd, &be_throttle_time_ms, sizeof(be_throttle_time_ms), 0);
      send(client_fd, &no_tags, sizeof(no_tags), 0);
    } else if (topic_name.size() > 0 && topic_name[0] != "") {
      if (valid == true) {

        unknow_topic_keys_length = meta.size() + 1;

        be_error_code = 0x00;
        int32_t partition_id = 0x00;
        int32_t leader_id = 0x00;
        int32_t epoch = 0x00;
        int8_t len = 0x02;
        int32_t nod = 0x00;
        int8_t len_l = len - 1;

        int message_size;

        if (partition_arr == 0x1)
          message_size = htonl(
              78); // handle APIVersion Request and Describe Topic request bit
        else
          message_size = htonl(270);

        send(client_fd, &message_size, sizeof(message_size), 0);           // 4
        send(client_fd, &be_correlation_id, sizeof(be_correlation_id), 0); // 4

        send(client_fd, &no_tags, sizeof(no_tags), 0); // 1
                                                       //
        send(client_fd, &be_throttle_time_ms, sizeof(be_throttle_time_ms),
             0); // 4
        send(client_fd, &unknow_topic_keys_length,
             sizeof(unknow_topic_keys_length), 0); // 1
        partition_arr = 0x03;
        for (int i = 0; i < topic_name.size(); i++) {
          topic_message_size = topic_name[i].size();
          // partition_arr = meta[i].par_len+2;
          send(client_fd, &be_error_code, sizeof(be_error_code), 0); // 2
          send(client_fd, &topic_message_size, sizeof(topic_message_size),
               0);                                                       // 1
          send(client_fd, topic_name[i].c_str(), topic_message_size, 0); // 17

          send(client_fd, &meta[i].uuid, 15, 0);

          send(client_fd, &is_internal, sizeof(is_internal), 0); // 1

          partition_arr = meta[i].par_len;
          send(client_fd, &partition_arr, sizeof(partition_arr), 0); // 1
          send(client_fd, &be_error_code, sizeof(be_error_code), 0); // 2
          send(client_fd, &partition_id, sizeof(partition_id), 0);   // 4
          send(client_fd, &leader_id, sizeof(leader_id), 0);         // 4
          send(client_fd, &epoch, sizeof(epoch), 0);                 // 4
          send(client_fd, &len, sizeof(len), 0);                     // 1
          send(client_fd, &nod, sizeof(nod), 0);                     // 4
          send(client_fd, &len, sizeof(len), 0);                     // 1
          send(client_fd, &nod, sizeof(nod), 0);                     // 4
          send(client_fd, &len_l, sizeof(len_l), 0);                 // 1
          send(client_fd, &len_l, sizeof(len_l), 0);                 // 1
          send(client_fd, &len_l, sizeof(len_l), 0);                 // 1
          send(client_fd, &no_tags, sizeof(no_tags), 0);             // 1

          if (partition_arr > 0x2) {
            partition_id = htonl(0x1);

            send(client_fd, &be_error_code, sizeof(be_error_code), 0); // 2
            send(client_fd, &partition_id, sizeof(partition_id), 0);   // 4
            send(client_fd, &leader_id, sizeof(leader_id), 0);         // 4
            send(client_fd, &epoch, sizeof(epoch), 0);                 // 4
            send(client_fd, &len, sizeof(len), 0);                     // 1
            send(client_fd, &nod, sizeof(nod), 0);                     // 4
            send(client_fd, &len, sizeof(len), 0);                     // 1
            send(client_fd, &nod, sizeof(nod), 0);                     // 4
            send(client_fd, &len_l, sizeof(len_l), 0);                 // 1
            send(client_fd, &len_l, sizeof(len_l), 0);                 // 1
            send(client_fd, &len_l, sizeof(len_l), 0);                 // 1
            send(client_fd, &no_tags, sizeof(no_tags), 0);             // 1
            partition_id = 0x00;
          }
          send(client_fd, &topic_authorization, sizeof(topic_authorization),
               0);                                       // 4
          send(client_fd, &no_tags, sizeof(no_tags), 0); // 1
        }
        send(client_fd, &nex_cursor, sizeof(nex_cursor), 0); // 1

        send(client_fd, &no_tags, sizeof(no_tags), 0);
      } else {
        int32_t message_size = htonl(
            55); // handle APIVersion Request and Describe Topic request bit

        be_error_code = htons(3); // no tags for this ApiKey entry // 1
        topic_id = 0x00;

        uint8_t topic_message_size = topic_name[0].length();

        int32_t topic_authorization = 0x00;
        uint8_t partition_arr = 0x01;
        uint8_t nex_cursor = 0xff;
        send(client_fd, &message_size, sizeof(message_size), 0);           // 4
        send(client_fd, &be_correlation_id, sizeof(be_correlation_id), 0); // 4

        send(client_fd, &no_tags, sizeof(no_tags), 0); // 1
        send(client_fd, &be_throttle_time_ms, sizeof(be_throttle_time_ms),
             0); // 4
        send(client_fd, &unknow_topic_keys_length,
             sizeof(unknow_topic_keys_length), 0);                 // 1
        send(client_fd, &be_error_code, sizeof(be_error_code), 0); // 2
        send(client_fd, &topic_message_size, sizeof(topic_message_size), 0); // 1
        send(client_fd, topic_name[0].c_str(), 17, 0);   // 17
        send(client_fd, &topic_id, sizeof(topic_id), 0); // 4
        send(client_fd, &topic_id, sizeof(topic_id), 0); // 4
        send(client_fd, &topic_id, sizeof(topic_id), 0); // 4
        send(client_fd, &topic_id, sizeof(topic_id), 0); // 4
        // send(client_fd,&meta.uuid,sizeof(meta.uuid),0);
        // send(client_fd,&topic_id,sizeof(topic_id),0);
        // send(client_fd,&inter_topic_id,sizeof(inter_topic_id),0);  // 2
        // send(client_fd,&topic_id_left,sizeof(topic_id_left),0); // 1
        send(client_fd, &is_internal, sizeof(is_internal), 0);     // 1
        send(client_fd, &partition_arr, sizeof(partition_arr), 0); // 1
        send(client_fd, &topic_authorization, sizeof(topic_authorization),
             0);                                             // 4
        send(client_fd, &no_tags, sizeof(no_tags), 0);       // 1
        send(client_fd, &nex_cursor, sizeof(nex_cursor), 0); // 1
        // send(client_fd,tp_name.c_str(),sizeof(tp_name.c_str()),0);
        send(client_fd, &no_tags, sizeof(no_tags), 0); // 1
        //     send(client_fd, &no_tags, sizeof(no_tags), 0);
      }
    } else if (Kaf_par.request_api_version == 16) {
      // handle APIVersion Request and Describe Topic request bit
      // Send response:
      // Note: correlation_id must be sent back in network order
      bool has_meet = false;
      if (Kaf_par.batch == true) {
        if (lll != 0) {
          std ::string clean = "";
         
          int id = 0;
          while (id < takeout.length()) {
            if ((takeout[id] >= 'A' && takeout[id] <= 'Z') ||
                (takeout[id] >= 'a' && takeout[id] <= 'z') ||
                takeout[id] == '!') {
              has_meet = true;
              clean += takeout[id];
            } else {
              if (has_meet == true) {
                clean += takeout[id];
                has_meet = false;
              }
            }
            id++;
          }

          takeout.clear();
          clean.pop_back();
          takeout = clean;
          std ::string sent2 = "";
          std ::string sent3 = "";
          int ll = 0;
          int idx = 0;
          for (int i = 0; i < ls_buff[1].second; i++) {
            sent3 += ls_buff[1].first[i];
          }
          for (int i = 0; i < ls_buff[1].second; i++) {

            if (ls_buff[1].first[i] == (char)(0x21)) {
              idx = i;
              break;
            }
          }
          idx++;
          for (int i = idx + 1; i < ls_buff[1].second; i++) {
            sent2 += ls_buff[1].first[i];
            ll++;
          }

          int message_size = htonl(72 + ls_buff[1].second);
          if (state == 1) {
            message_size = htonl(140 + clean.length());
          }
          be_error_code = htons(0);
          int16_t error_code = 0x00;

          int32_t session_id = 0x00;
          uint8_t response_body_size = 0x02;

          uint32_t fetch_id = 0x00;

          int16_t fetch_error_code = htons(0);

          if (topic_name.size() == 0) {
            fetch_error_code = htons(0);
          }
          partition_arr = 0x02;
          int32_t par_id = 0x00;
          uint32_t topic_id_fetch = 0x00;

          std ::string top_name = "f";

          topic_message_size = top_name.size();
          send(client_fd, &message_size, sizeof(message_size), 0);
          send(client_fd, &be_correlation_id, sizeof(be_correlation_id),
               0);                                       // 4
          send(client_fd, &no_tags, sizeof(no_tags), 0); // 1

          send(client_fd, &be_throttle_time_ms, sizeof(be_throttle_time_ms),
               0);                                             // 4
          send(client_fd, &error_code, sizeof(error_code), 0); // 4
          send(client_fd, &session_id, sizeof(session_id), 0); // 4
          send(client_fd, &response_body_size, sizeof(response_body_size),
               0); // 1

          send(client_fd, &Kaf_par.fetch_uuid, sizeof(Kaf_par.fetch_uuid),
               0); // 16
          if (Kaf_par.fetch_uuid[6] == 0) {
            fetch_error_code = htons(100);
          }

          send(client_fd, &partition_arr, sizeof(partition_arr), 0); // 1

          int32_t partition_id = 0x00;
          int32_t leader_id = 0x00;
          int32_t epoch = 0x00;
          int8_t len = 0x02;
          int32_t nod = 0x00;
          int8_t len_l = len - 1;
          int64_t high_watermark = 0x00;
          int64_t last_stable_offset = 0x00;
          int64_t log_start_offset = 0x00;
          uint8_t aborted_num = 0x00;
          uint8_t record_len = 0x01;

          if (ls_buff[0].second != 0)
            record_len++;
          if (ls_buff[1].second != 0)
            record_len++;

          int32_t preferred_read_replica = 0x00;
          int64_t base_offset = 0x00;
          int32_t batch_record_len = 0;

          send(client_fd, &partition_id, sizeof(partition_id), 0);         // 4
          send(client_fd, &fetch_error_code, sizeof(fetch_error_code), 0); // 2

          send(client_fd, &high_watermark, sizeof(high_watermark), 0); // 8
          send(client_fd, &last_stable_offset, sizeof(last_stable_offset),
               0);                                                         // 8
          send(client_fd, &log_start_offset, sizeof(log_start_offset), 0); // 8
          send(client_fd, &aborted_num, sizeof(aborted_num), 0);           // 1
          send(client_fd, &preferred_read_replica,
               sizeof(preferred_read_replica), 0);             // 4
          send(client_fd, &record_len, sizeof(record_len), 0); // 1

          std ::string sent = "";
          for (int i = 0; i < ls_buff[0].second; i++) {
            sent += ls_buff[0].first[i];
            
          }
          

          if (state == 2)
            send(client_fd, sent3.c_str(), ls_buff[1].second, 0);
          else
            send(client_fd, sent.c_str(), ls_buff[0].second, 0);
          send(client_fd, &no_tags, sizeof(no_tags), 0); // 1

          send(client_fd, &no_tags, sizeof(no_tags), 0); // 1

          send(client_fd, &no_tags, sizeof(no_tags), 0); // 1

          
        } else {
          int message_size = htonl(72);
          be_error_code = htons(0);
          int16_t error_code = 0x00;
          // api_keys_length = 0x04;
          int32_t session_id = 0x00;
          uint8_t response_body_size = 0x02;
          // response_body_size = 0x00;
          uint32_t fetch_id = 0x00;

          int16_t fetch_error_code = htons(0);

          if (topic_name.size() == 0) {
            fetch_error_code = htons(0);
          }
          partition_arr = 0x02;
          int32_t par_id = 0x00;
          uint32_t topic_id_fetch = 0x00;

          std ::string top_name = "foo";

          topic_message_size = top_name.size();
          send(client_fd, &message_size, sizeof(message_size), 0);
          send(client_fd, &be_correlation_id, sizeof(be_correlation_id),
               0);                                       // 4
          send(client_fd, &no_tags, sizeof(no_tags), 0); // 1

          send(client_fd, &be_throttle_time_ms, sizeof(be_throttle_time_ms),
               0);                                             // 4
          send(client_fd, &error_code, sizeof(error_code), 0); // 4
          send(client_fd, &session_id, sizeof(session_id), 0); // 4
          send(client_fd, &response_body_size, sizeof(response_body_size),
               0); // 1

          send(client_fd, &Kaf_par.fetch_uuid, sizeof(Kaf_par.fetch_uuid), 0);
          if (Kaf_par.fetch_uuid[6] == 0) {
            fetch_error_code = htons(100);
          }

          send(client_fd, &partition_arr, sizeof(partition_arr), 0); // 1

          int32_t partition_id = 0x00;
          int32_t leader_id = 0x00;
          int32_t epoch = 0x00;
          int8_t len = 0x02;
          int32_t nod = 0x00;
          int8_t len_l = len - 1;
          int64_t high_watermark = 0x00;
          int64_t last_stable_offset = 0x00;
          int64_t log_start_offset = 0x00;
          uint8_t aborted_num = 0x00;
          uint8_t record_len = 0x00;
          int32_t preferred_read_replica = 0x00;

          send(client_fd, &partition_id, sizeof(partition_id), 0);         // 4
          send(client_fd, &fetch_error_code, sizeof(fetch_error_code), 0); // 2

          send(client_fd, &high_watermark, sizeof(high_watermark), 0); // 8
          send(client_fd, &last_stable_offset, sizeof(last_stable_offset),
               0);                                                         // 8
          send(client_fd, &log_start_offset, sizeof(log_start_offset), 0); // 8
          send(client_fd, &aborted_num, sizeof(aborted_num), 0);           // 1
          send(client_fd, &preferred_read_replica,
               sizeof(preferred_read_replica), 0);             // 4
          send(client_fd, &record_len, sizeof(record_len), 0); // 1

          send(client_fd, &no_tags, sizeof(no_tags), 0); // 1

          send(client_fd, &no_tags, sizeof(no_tags), 0); // 1

          send(client_fd, &no_tags, sizeof(no_tags), 0); // 1

          
        }
      } else {
        int message_size = htonl(
            17); // handle APIVersion Request and Describe Topic request bit
                 // Send response:
                 // Note: correlation_id must be sent back in network order
                
        be_error_code = htons(0);
        int32_t error_code = 0x00;
        // api_keys_length = 0x04;
        int32_t session_id = 0x00;
        // uint8_t response_body_size = htons(1);

        send(client_fd, &message_size, sizeof(message_size), 0);
        send(client_fd, &be_correlation_id, sizeof(be_correlation_id), 0); // 4
        send(client_fd, &no_tags, sizeof(no_tags), 0);                     // 1
        // send(client_fd,&response_body_size,sizeof(response_body_size),0);// 1
        send(client_fd, &be_throttle_time_ms, sizeof(be_throttle_time_ms),
             0);                                             // 4
        send(client_fd, &error_code, sizeof(error_code), 0); // 4
        send(client_fd, &session_id, sizeof(session_id), 0); // 4
      }
    } else {
      int value1 = htonl(Kaf_par.correlation_id);
      int error_code = htons(35);
      if (Kaf_par.request_api_version >= 0 && Kaf_par.request_api_version <= 4)
        error_code = htons(0);
      write(client_fd, &value, 4);
      write(client_fd, &value1, 4);
      write(client_fd, &error_code, 2);
    }
  }
  close(client_fd);
  return;
}
int main(int argc, char *argv[]) {
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
  std ::string path = "/tmp/kraft-combined-logs/";
  std ::vector<std ::string> all_path;
  std::vector<std ::string> all_uuid;
  for (const auto &entry : fs::directory_iterator(path)) {
    std::string current = entry.path().string();
    
    if (current.compare("/tmp/kraft-combined-logs/__cluster_metadata-0") != 0 &&
        current.compare("/tmp/kraft-combined-logs/meta.properties") != 0 &&
        current.compare("/tmp/kraft-combined-logs/.kafka_cleanshutdown") != 0) {
      std::string uval = "";
      int id = current.size() - 1;
      while (id > 0 && current[id] != '/') {
        if (current[id] != '-' && current[id] != '0') {
          uval += current[id];
        }
        id--;
      }

      reverse(uval.begin(), uval.end());

      if (uval.back() != '1')
        all_uuid.push_back(uval);
     
      all_path.push_back(current);
    }
  }
  std ::map<std::array<unsigned char, 16>, std::string> mp;
  std::map<int, std::pair<char *, int>> ls_buff;
  std ::vector<TopicMetadata> tpm = get_topic_metadata1(all_uuid);
  for (int i = 0; i < tpm.size(); i++) {
    
    mp[tpm[i].uuid] = all_uuid[i];
  }
  std ::string value_take_out = "";
  char *buffer_take = new char[1024];
  char *buffer2 = new char[1024];
  int lenn;
  int lenn2;

  for (auto it : all_path) {

    std::ifstream is1(it + "/00000000000000000000.log");
    char *buffer1 = new char[1024];
    int length = 0;
    if (is1.is_open()) {
      // get length of file:
      is1.seekg(0, is1.end);
      length = is1.tellg();
      is1.seekg(0, is1.beg);
      std::cout << "Reading " << length << " characters... " << std::endl;
      is1.read(buffer1, length);
      is1.close();
    } else {
    }
    int total = 0;
    for (int i = 0; i < length - 1; i++) {

      if (buffer1[i] == '[' && buffer1[i + 1] == 'm') {

        total++;
      }
      
    }
    
    std ::string fetch_value = "";
    if (total > 3)
      total--;
    if (total == 2) {
      for (int p = 0; p < length; p++) {
        buffer_take[p] = buffer1[p];
      }
      // value_take_out = fetch_value;
      if (!ls_buff.count(0)) {
        ls_buff[0] = std::make_pair(buffer_take, length);
      } else {
        ls_buff[1] = std::make_pair(buffer_take, length);
      }
      lenn = length;
      int len_count = 0;
      for (int i = length - 1; i >= 0; i--) {
        int len = (int)(buffer1[i]);
        if (len != 0 && total * len_count == len) {
          int ct = 0;
          for (int j = i + 1; j < length; j++) {
            ct++;
            fetch_value += buffer1[j];
            if (ct == len_count)
              break;
          }
          break;
        }
        len_count++;
      }
      if (total == 2 && fetch_value != "") {
        value_take_out = fetch_value;
        // break;
      }
    } else if (total == 3) {
      for (int p = 0; p < length; p++) {
        buffer2[p] = buffer1[p];
      }
      if (!ls_buff.count(0)) {
        ls_buff[0] = std::make_pair(buffer2, length);
      } else {
        ls_buff[1] = std::make_pair(buffer2, length);
      }
      lenn2 = length;
    }
  }
  if (ls_buff.size() == 2) {
    if (ls_buff[0].second > ls_buff[1].second) {
      std::pair<char *, int> p1 = ls_buff[0];
      ls_buff[0] = ls_buff[1];
      ls_buff[1] = p1;
    }
  }
  
  //  Read the input stream
  std::ifstream is(
      "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");
  char *buffer = new char[1024];
  int length = 0;

  if (is.is_open()) {
    // get length of file:
    is.seekg(0, is.end);
    length = is.tellg();
    is.seekg(0, is.beg);
    std::cout << "Reading " << length << " characters... " << std::endl;
    // read data as a block:
    
    is.read(buffer, length);
    is.close();
  } else {
    std ::cout << "file doesn't exist" << std ::endl;
  }

  std ::string s;
  std::vector<std::string> metadata;
  
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
      0) {
    close(server_fd);
    std::cerr << "setsockopt failed: " << std::endl;
    return 1;
  }
  struct sockaddr_in server_addr {};
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(9092);
  if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&server_addr),
           sizeof(server_addr)) != 0) {
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
  // You can use print statements as follows for debugging, they'll be visible
  // when running tests.
  std::cerr << "Logs from your program will appear here!\n";

  // Uncomment this block to pass the first stage
  //
  while (true) {
    struct sockaddr_in client_addr {};
    socklen_t client_addr_len = sizeof(client_addr);
    // ?
    int client_fd =
        accept(server_fd, reinterpret_cast<struct sockaddr *>(&client_addr),
               &client_addr_len);
    std::cout << "Client connected\n";
    if (client_fd < 0) {
      std::cerr << "Accept failed" << std::endl;
      continue;
    }
    std::thread th(handle_client, client_fd, buffer, length, value_take_out,
                   lenn, buffer_take, mp, lenn2, buffer2, ls_buff);
    // handle_client(client_fd);
    th.detach();
  }
  close(server_fd);
  return 0;
}