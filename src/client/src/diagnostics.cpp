#include <dfg/client/client.hpp>
#include <fstream>
#include <iostream>
#include <iterator>

namespace dfg::client {
int run_client_test(const std::string &server_ip, int server_port) {
  std::cout << "Running system tests..." << std::endl;

  std::string test_file = "/tmp/dfg_test_file.txt";
  std::ofstream file(test_file);
  file << "This is a test file for the distributed storage system.\n";
  for (int i = 0; i < 1000; i++) {
    file << "Line " << i << ": Lorem ipsum dolor sit amet.\n";
  }
  file.close();

  std::cout << "\n=== Testing File Upload ===" << std::endl;
  if (file_upload(server_ip, server_port, test_file, "test_file.txt") != 0) {
    std::cout << "Upload test failed!" << std::endl;
    return -1;
  }

  std::cout << "\n=== Testing File Download ===" << std::endl;
  std::string download_path = "/tmp/dfg_downloaded_test.txt";
  if (file_download(server_ip, server_port, "test_file.txt", download_path) !=
      0) {
    std::cout << "Download test failed!" << std::endl;
    return -1;
  }

  std::cout << "\n=== Verifying File Integrity ===" << std::endl;
  std::ifstream original(test_file, std::ios::binary);
  std::ifstream downloaded(download_path, std::ios::binary);

  if (original && downloaded) {
    std::string orig_content((std::istreambuf_iterator<char>(original)),
                             std::istreambuf_iterator<char>());
    std::string down_content((std::istreambuf_iterator<char>(downloaded)),
                             std::istreambuf_iterator<char>());

    if (orig_content == down_content) {
      std::cout << "File integrity verified — files match!" << std::endl;
    } else {
      std::cout << "File integrity check failed — files don't match!"
                << std::endl;
      return -1;
    }
  } else {
    std::cerr << "Failed to open verification files" << std::endl;
    return -1;
  }

  std::cout << "\n=== All Tests Passed! ===" << std::endl;
  return 0;
}

} // namespace dfg::client
